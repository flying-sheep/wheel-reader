use std::{path::PathBuf, str::FromStr};

use anyhow::{Context, Result};
use async_zip::base::read::seek::ZipFileReader;
use clap::Parser;
use futures_util::{
    stream::FuturesUnordered, AsyncReadExt as _, StreamExt as _, TryStreamExt as _,
};
use lazy_static::lazy_static;
use opendal::services::{Http, Monoiofs};
use tokio_util::compat::TokioAsyncWriteCompatExt as _;
use url::Url;

#[derive(Debug, Clone, PartialEq, Eq)]
enum WheelUrlType {
    Httpx,
    File,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct WheelUrl {
    url_type: WheelUrlType,
    url: Url,
}

impl std::fmt::Display for WheelUrl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.url_type {
            WheelUrlType::Httpx => write!(f, "{}", self.url),
            WheelUrlType::File => write!(f, "{}", self.url.path()),
        }
    }
}

impl FromStr for WheelUrl {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let Ok(url) = Url::parse(s) else {
            return Ok(WheelUrl {
                url_type: WheelUrlType::File,
                url: Url::from_file_path(s).map_err(|_| anyhow::anyhow!("invalid path: {s}"))?,
            });
        };
        let url_type = match url.scheme() {
            "https" | "http" => WheelUrlType::Httpx,
            "file" => WheelUrlType::File,
            _ => anyhow::bail!("unknown scheme: {s}"),
        };
        Ok(WheelUrl { url_type, url })
    }
}

enum Service {
    Httpx(opendal::services::Http),
    File(opendal::services::Monoiofs),
}

impl Service {
    pub(crate) fn build(self) -> opendal::Result<opendal::Operator> {
        match self {
            Self::Httpx(builder) => Self::build_operator(builder),
            Self::File(builder) => Self::build_operator(builder),
        }
    }

    fn build_operator<T: opendal::Builder>(builder: T) -> opendal::Result<opendal::Operator> {
        use opendal::layers::*;

        Ok(opendal::Operator::new(builder)?
            .layer(LoggingLayer::default())
            .layer(TracingLayer)
            .finish())
    }
}

impl WheelUrl {
    fn service(&self) -> Service {
        match self.url_type {
            WheelUrlType::Httpx => {
                let endpoint = format!("{}://{}", self.url.scheme(), self.url.domain().unwrap());
                Service::Httpx(Http::default().endpoint(&endpoint))
            }
            WheelUrlType::File => Service::File(Monoiofs::default().root("/")),
        }
    }

    fn path(&self) -> &str {
        self.url.path()
    }

    fn file_name(&self) -> Result<String> {
        let path = PathBuf::from(self.url.path());
        let file_name: &str = path
            .file_name()
            .context("no file name")?
            .try_into()
            .context("invalid file name")?;
        Ok(file_name.to_owned())
    }
}

/// Simple program to greet a person
#[derive(Parser, Debug)]
struct Args {
    /// URLs or local file paths to open
    urls: Vec<WheelUrl>,
}

lazy_static! {
    static ref RE_METADATA: regex::Regex = regex::Regex::new(r".*/METADATA$").unwrap();
}

async fn run(url: WheelUrl) -> Result<(WheelUrl, String)> {
    let op = url.service().build()?;
    let reader = op.reader_with(url.path()).await?;
    let zip_file_reader = reader.into_futures_async_read(..).await?;
    let mut zip_file = ZipFileReader::new(zip_file_reader)
        .await
        .context("failed to open zip file")?;
    let entry = zip_file
        .file()
        .entries()
        .into_iter()
        .enumerate()
        .flat_map(|(i, e)| e.filename().as_str().ok().and_then(|s| Some((i, s))))
        .find_map(|(i, path)| RE_METADATA.is_match(path).then_some(i))
        .context("no METADATA file")?;
    let mut reader = zip_file.reader_with_entry(entry).await?;
    let mut buf = Vec::new();
    reader.read_to_end(&mut buf).await?;
    Ok((url, String::from_utf8(buf)?))
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::filter::EnvFilter::from_default_env())
        .init();

    let as_finished: FuturesUnordered<_> = args.urls.into_iter().map(run).collect();
    let items = as_finished.map(|r| {
        let (url, json) = r.expect("TODO: handle error");
        (url.file_name().expect("no file name"), json)
    });
    futures_util::io::copy(
        &mut destream_json::encode_map(items)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
            .into_async_read(),
        &mut tokio::io::stdout().compat_write(),
    )
    .await?;
    Ok(())
}
