use anyhow::Result;
use clap::Parser;
use futures_util::{stream::FuturesUnordered, StreamExt as _, TryStreamExt as _};

#[cfg(feature = "monoio")]
mod http_client;
mod reader;
mod rt;

use reader::{Services, WheelUrl};

#[derive(Parser, Debug)]
struct Args {
    /// URLs or local file paths to open
    urls: Vec<WheelUrl>,
}

#[crate::rt::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(tracing_subscriber::filter::EnvFilter::from_default_env())
        .init();

    let mut services = Services::default();
    let as_finished: FuturesUnordered<_> = args
        .urls
        .into_iter()
        .map(|url| (services.operator(&url).clone(), url))
        .map(|(op, url)| Services::run(op, url))
        .collect();

    let items = as_finished.map(|r| {
        let (url, json) = r.expect("TODO: handle error");
        (url.file_name().unwrap_or_else(|| url.to_string()), json)
    });
    futures_util::io::copy(
        &mut destream_json::encode_map(items)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
            .into_async_read(),
        &mut crate::rt::io::stdout(),
    )
    .await?;
    Ok(())
}
