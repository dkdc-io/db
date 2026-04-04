#[cfg(feature = "cli")]
pub mod repl;

use serde::{Deserialize, Serialize};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),

    #[error("server error: {0}")]
    Server(String),
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Serialize)]
struct SqlRequest {
    sql: String,
}

#[derive(Serialize)]
struct CreateDbRequest {
    name: String,
}

#[derive(Deserialize)]
struct ExecuteResponse {
    affected: u64,
}

#[derive(Deserialize, Debug, Clone)]
pub struct QueryResponse {
    pub columns: Vec<ColumnInfo>,
    pub rows: Vec<Vec<serde_json::Value>>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct ColumnInfo {
    pub name: String,
    pub r#type: String,
}

#[derive(Deserialize)]
struct ErrorResponse {
    error: String,
}

pub struct DbClient {
    base_url: String,
    client: reqwest::Client,
}

impl DbClient {
    pub fn new(base_url: &str) -> Self {
        Self {
            base_url: base_url.trim_end_matches('/').to_string(),
            client: reqwest::Client::new(),
        }
    }

    pub fn localhost(port: u16) -> Self {
        Self::new(&format!("http://127.0.0.1:{port}"))
    }

    /// Create a new database.
    pub async fn create_db(&self, name: &str) -> Result<()> {
        let resp = self
            .client
            .post(format!("{}/db", self.base_url))
            .json(&CreateDbRequest {
                name: name.to_string(),
            })
            .send()
            .await?;

        if resp.status().is_success() {
            Ok(())
        } else {
            let body: ErrorResponse = resp.json().await?;
            Err(Error::Server(body.error))
        }
    }

    /// Drop a database.
    pub async fn drop_db(&self, name: &str) -> Result<()> {
        let resp = self
            .client
            .delete(format!("{}/db/{name}", self.base_url))
            .send()
            .await?;

        if resp.status().is_success() {
            Ok(())
        } else {
            let body: ErrorResponse = resp.json().await?;
            Err(Error::Server(body.error))
        }
    }

    /// List all databases.
    pub async fn list_dbs(&self) -> Result<Vec<String>> {
        let resp = self
            .client
            .get(format!("{}/db", self.base_url))
            .send()
            .await?;

        if resp.status().is_success() {
            Ok(resp.json().await?)
        } else {
            let body: ErrorResponse = resp.json().await?;
            Err(Error::Server(body.error))
        }
    }

    /// Execute a write statement against a specific database.
    pub async fn execute(&self, db: &str, sql: &str) -> Result<u64> {
        let resp = self
            .client
            .post(format!("{}/db/{db}/execute", self.base_url))
            .json(&SqlRequest {
                sql: sql.to_string(),
            })
            .send()
            .await?;

        if resp.status().is_success() {
            let body: ExecuteResponse = resp.json().await?;
            Ok(body.affected)
        } else {
            let body: ErrorResponse = resp.json().await?;
            Err(Error::Server(body.error))
        }
    }

    /// Analytical query through DataFusion. Supports cross-db joins.
    pub async fn query(&self, sql: &str) -> Result<QueryResponse> {
        let resp = self
            .client
            .post(format!("{}/query", self.base_url))
            .json(&SqlRequest {
                sql: sql.to_string(),
            })
            .send()
            .await?;

        if resp.status().is_success() {
            Ok(resp.json().await?)
        } else {
            let body: ErrorResponse = resp.json().await?;
            Err(Error::Server(body.error))
        }
    }

    /// OLTP fast-path read against a specific database.
    pub async fn query_oltp(&self, db: &str, sql: &str) -> Result<QueryResponse> {
        let resp = self
            .client
            .post(format!("{}/db/{db}/query/oltp", self.base_url))
            .json(&SqlRequest {
                sql: sql.to_string(),
            })
            .send()
            .await?;

        if resp.status().is_success() {
            Ok(resp.json().await?)
        } else {
            let body: ErrorResponse = resp.json().await?;
            Err(Error::Server(body.error))
        }
    }

    /// List tables in a specific database.
    pub async fn list_tables(&self, db: &str) -> Result<Vec<String>> {
        let resp = self
            .client
            .get(format!("{}/db/{db}/tables", self.base_url))
            .send()
            .await?;

        if resp.status().is_success() {
            Ok(resp.json().await?)
        } else {
            let body: ErrorResponse = resp.json().await?;
            Err(Error::Server(body.error))
        }
    }

    /// Get table schema for a specific database.
    pub async fn table_schema(&self, db: &str, table: &str) -> Result<QueryResponse> {
        let resp = self
            .client
            .get(format!("{}/db/{db}/schema/{table}", self.base_url))
            .send()
            .await?;

        if resp.status().is_success() {
            Ok(resp.json().await?)
        } else {
            let body: ErrorResponse = resp.json().await?;
            Err(Error::Server(body.error))
        }
    }

    /// Health check.
    pub async fn health(&self) -> Result<bool> {
        let resp = self
            .client
            .get(format!("{}/health", self.base_url))
            .send()
            .await?;
        Ok(resp.status().is_success())
    }
}
