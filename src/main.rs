use std::str::FromStr;
use std::{path::PathBuf, pin::pin};

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

#[derive(Debug, Clone)]
enum WheelUrl {
    Httpx(Url),
    File(PathBuf),
}

impl std::fmt::Display for WheelUrl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WheelUrl::Httpx(url) => write!(f, "{url}"),
            WheelUrl::File(path) => write!(f, "{}", path.display()),
        }
    }
}

impl FromStr for WheelUrl {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let Ok(url) = Url::parse(s) else {
            return Ok(WheelUrl::File(PathBuf::from(s)));
        };
        match url.scheme() {
            "https" | "http" => Ok(WheelUrl::Httpx(url)),
            "file" => Ok(WheelUrl::File(PathBuf::from(url.path()))),
            _ => Err(anyhow::anyhow!("unknown scheme: {s}")),
        }
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

trait UrlReadable {
    fn service(&self) -> Service;
    fn path(&self) -> Result<&str>;
}

impl UrlReadable for WheelUrl {
    fn service(&self) -> Service {
        match self {
            WheelUrl::Httpx(url) => {
                let endpoint = format!("{}://{}", url.scheme(), url.domain().unwrap());
                Service::Httpx(Http::default().endpoint(&endpoint))
            }
            WheelUrl::File(_) => Service::File(Monoiofs::default().root("/")),
        }
    }

    fn path(&self) -> Result<&str> {
        match self {
            WheelUrl::Httpx(url) => Ok(url.path()),
            WheelUrl::File(path) => path.to_str().context("path is not valid utf-8"),
        }
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
    let reader = op.reader_with(url.path()?).await?;
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

    let s = as_finished.map(|r| r.expect("TODO: handle error"));
    let mut stdout = pin!(tokio::io::stdout().compat_write());
    let mut s = destream_json::encode_map(s.map(|(a, b)| (a.to_string(), b)))
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
        .into_async_read();
    futures_util::io::copy(&mut s, &mut stdout).await?;
    Ok(())
}
