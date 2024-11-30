#[cfg(feature = "tokio")]
pub use tokio::main;

pub mod io {
    #[cfg(feature = "tokio")]
    pub fn stdout() -> tokio_util::compat::Compat<tokio::io::Stdout> {
        use tokio_util::compat::TokioAsyncWriteCompatExt as _;

        tokio::io::stdout().compat_write()
    }
}
