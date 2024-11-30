use std::{collections::HashMap, path::PathBuf, str::FromStr};

use anyhow::{Context, Result};
use async_zip::base::read::seek::ZipFileReader;
use futures_util::AsyncReadExt as _;
use lazy_static::lazy_static;
use opendal::services::{Http, Monoiofs};
use url::Url;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum WheelUrlType {
    Httpx,
    File,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WheelUrl {
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

impl TryFrom<Url> for WheelUrl {
    type Error = anyhow::Error;

    fn try_from(url: Url) -> Result<Self, Self::Error> {
        let url_type = match url.scheme() {
            "https" | "http" => WheelUrlType::Httpx,
            "file" => WheelUrlType::File,
            _ => anyhow::bail!("unknown scheme: {url}"),
        };
        Ok(WheelUrl { url_type, url })
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
        WheelUrl::try_from(url)
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
    fn endpoint(&self) -> String {
        match self.url_type {
            WheelUrlType::Httpx => {
                format!("{}://{}", self.url.scheme(), self.url.domain().unwrap())
            }
            WheelUrlType::File => "/".to_string(),
        }
    }

    fn service(&self) -> Service {
        match self.url_type {
            WheelUrlType::Httpx => Service::Httpx(Http::default().endpoint(&self.endpoint())),
            WheelUrlType::File => Service::File(Monoiofs::default().root(&self.endpoint())),
        }
    }

    pub fn path(&self) -> &str {
        self.url.path()
    }

    pub fn file_name(&self) -> Option<String> {
        let path = PathBuf::from(self.url.path());
        let file_name: &str = path.file_name()?.try_into().unwrap(); // we just made it from a &str
        Some(file_name.to_owned())
    }
}

lazy_static! {
    static ref RE_METADATA: regex::Regex = regex::Regex::new(r".*/METADATA$").unwrap();
}

#[derive(Default, Debug)]
pub struct Services {
    services: HashMap<(WheelUrlType, String), opendal::Operator>,
}

impl Services {
    pub fn operator(&mut self, url: &WheelUrl) -> &opendal::Operator {
        self.services
            .entry((url.url_type, url.endpoint().to_string()))
            .or_insert_with(|| url.service().build().expect("TODO: handle error"))
    }

    pub async fn run(op: opendal::Operator, url: WheelUrl) -> Result<(WheelUrl, String)> {
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
}
