#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("turso error: {0}")]
    Turso(#[from] turso::Error),

    #[error("DataFusion error: {0}")]
    DataFusion(#[from] datafusion::error::DataFusionError),

    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    #[error("schema error: {0}")]
    Schema(String),

    #[error("write attempted through read path: {0}")]
    WriteOnReadPath(String),

    #[error("read attempted through write path: {0}")]
    ReadOnWritePath(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

pub type Result<T> = std::result::Result<T, Error>;
