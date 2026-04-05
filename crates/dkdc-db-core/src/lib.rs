mod catalog;
mod config;
mod convert;
mod db;
mod error;
mod manager;
mod plan;
mod provider;
pub mod router;
mod schema;
pub mod toml_config;
mod write;

pub use config::DbConfig;
pub use db::DkdcDb;
pub use error::{Error, Result, validate_db_name, validate_sql, validate_table_name};
pub use manager::DbManager;

pub use arrow::record_batch::RecordBatch;
pub use datafusion::dataframe::DataFrame;

/// Default host for the database server.
pub const DEFAULT_HOST: &str = "127.0.0.1";

/// Default port for the database server.
pub const DEFAULT_PORT: u16 = 4200;

/// Default server URL (`http://{DEFAULT_HOST}:{DEFAULT_PORT}`).
pub const DEFAULT_SERVER_URL: &str = "http://127.0.0.1:4200";
