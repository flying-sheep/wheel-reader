#[cfg(feature = "tokio")]
pub use tokio::{main, spawn};

#[cfg(feature = "monoio")]
pub use monoio::{main, spawn};

pub mod io {
    #[cfg(feature = "tokio")]
    pub fn stdout() -> tokio_util::compat::Compat<tokio::io::Stdout> {
        use tokio_util::compat::TokioAsyncWriteCompatExt as _;

        tokio::io::stdout().compat_write()
    }

    #[cfg(feature = "monoio")]
    pub fn stdout() -> futures::io::AllowStdIo<std::io::Stdout> {
        futures::io::AllowStdIo::new(std::io::stdout())
    }
}
