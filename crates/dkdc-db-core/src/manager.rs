use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use datafusion::prelude::SessionContext;
use tokio::sync::RwLock;

use datafusion::catalog::MemoryCatalogProvider;

use crate::catalog::SqliteCatalogProvider;
use crate::config::DbConfig;
use crate::db::DkdcDb;
use crate::error::{self, Error, Result};
use crate::router;
use crate::toml_config::DbTomlConfig;

const IN_MEMORY_PATH: &str = ":memory:";

struct ManagedDb {
    db: DkdcDb,
    catalog: Arc<SqliteCatalogProvider>,
}

pub struct DbManager {
    ctx: SessionContext,
    dbs: RwLock<HashMap<String, ManagedDb>>,
    /// Known database names on disk (discovered at startup, not yet opened).
    known: RwLock<Vec<String>>,
    base_path: PathBuf,
}

impl DbManager {
    /// Create a new manager. Scans base_path for existing .db files but doesn't open them.
    pub async fn new() -> Result<Self> {
        let base_path = dkdc_home::ensure("db")?;
        let ctx = SessionContext::new();

        let mut known = Vec::new();
        discover_dbs(&base_path, &base_path, &mut known);

        Ok(Self {
            ctx,
            dbs: RwLock::new(HashMap::new()),
            known: RwLock::new(known),
            base_path,
        })
    }

    /// Create a new manager for testing (in-memory, no disk scanning).
    pub async fn new_in_memory() -> Result<Self> {
        Ok(Self {
            ctx: SessionContext::new(),
            dbs: RwLock::new(HashMap::new()),
            known: RwLock::new(Vec::new()),
            base_path: PathBuf::from(IN_MEMORY_PATH),
        })
    }

    /// Create a new database. Opens it immediately and registers catalog.
    pub async fn create_db(&self, name: &str) -> Result<()> {
        error::validate_db_name(name)?;
        let mut dbs = self.dbs.write().await;
        if dbs.contains_key(name) {
            return Err(Error::Schema(format!("database '{name}' already exists")));
        }
        let managed = self.open_db(name).await?;
        let catalog_name = catalog_name(name);
        self.ctx
            .register_catalog(&catalog_name, managed.catalog.clone());
        dbs.insert(name.to_string(), managed);
        Ok(())
    }

    /// Drop a database. Deregisters catalog. Does NOT delete the file.
    pub async fn drop_db(&self, name: &str) -> Result<()> {
        let mut dbs = self.dbs.write().await;
        if dbs.remove(name).is_none() {
            return Err(Error::Schema(format!("database '{name}' not found")));
        }
        // DataFusion has no deregister_catalog — replace with an empty catalog.
        self.ctx
            .register_catalog(catalog_name(name), Arc::new(MemoryCatalogProvider::new()));
        // Remove from known list so list_dbs won't show it and ensure_db won't re-open it
        let mut known = self.known.write().await;
        known.retain(|k| k != name);
        Ok(())
    }

    /// List databases: union of loaded + known-on-disk.
    pub async fn list_dbs(&self) -> Vec<String> {
        let dbs = self.dbs.read().await;
        let known = self.known.read().await;
        let mut all: Vec<String> = dbs.keys().cloned().collect();
        for k in known.iter() {
            if !all.contains(k) {
                all.push(k.clone());
            }
        }
        all.sort();
        all
    }

    /// Ensure a database is loaded. Lazy-opens from disk if known but not loaded.
    pub async fn ensure_db(&self, name: &str) -> Result<()> {
        {
            let dbs = self.dbs.read().await;
            if dbs.contains_key(name) {
                return Ok(());
            }
        }
        // Not loaded — check if known on disk
        let is_known = self.known.read().await.contains(&name.to_string());
        if !is_known {
            return Err(Error::Schema(format!(
                "database '{name}' not found — create it with POST /db"
            )));
        }
        // Lazy open — use create_db, but tolerate "already exists" from a concurrent ensure_db
        match self.create_db(name).await {
            Ok(()) => Ok(()),
            Err(Error::Schema(msg)) if msg.contains("already exists") => Ok(()),
            Err(e) => Err(e),
        }
    }

    /// Execute a write against a specific database.
    pub async fn execute(&self, db_name: &str, sql: &str) -> Result<u64> {
        error::validate_sql(sql)?;
        self.ensure_db(db_name).await?;
        let dbs = self.dbs.read().await;
        let managed = dbs
            .get(db_name)
            .ok_or_else(|| Error::Schema(format!("database '{db_name}' not found")))?;
        let result = managed.db.execute(sql).await?;
        // Refresh catalog if DDL (selective: only the affected table)
        if router::is_ddl(sql) {
            managed
                .catalog
                .schema_provider()
                .refresh_for_ddl(sql)
                .await?;
        }
        Ok(result)
    }

    /// Analytical query through DataFusion. Supports cross-db joins.
    pub async fn query(&self, sql: &str) -> Result<Vec<RecordBatch>> {
        error::validate_sql(sql)?;
        if router::is_write(sql) {
            return Err(Error::WriteOnReadPath(sql.to_string()));
        }
        let df = self.ctx.sql(sql).await?;
        Ok(df.collect().await?)
    }

    /// OLTP fast-path read against a specific database.
    pub async fn query_oltp(&self, db_name: &str, sql: &str) -> Result<Vec<RecordBatch>> {
        error::validate_sql(sql)?;
        if router::is_write(sql) {
            return Err(Error::WriteOnReadPath(sql.to_string()));
        }
        self.ensure_db(db_name).await?;
        let dbs = self.dbs.read().await;
        dbs.get(db_name)
            .ok_or_else(|| Error::Schema(format!("database '{db_name}' not found")))?
            .db
            .query_oltp(sql)
            .await
    }

    /// List tables in a specific database.
    pub async fn list_tables(&self, db_name: &str) -> Result<Vec<String>> {
        self.ensure_db(db_name).await?;
        let dbs = self.dbs.read().await;
        dbs.get(db_name)
            .ok_or_else(|| Error::Schema(format!("database '{db_name}' not found")))?
            .db
            .list_tables()
            .await
    }

    /// Table schema for a specific database.
    pub async fn table_schema(&self, db_name: &str, table: &str) -> Result<Vec<RecordBatch>> {
        error::validate_table_name(table)?;
        self.ensure_db(db_name).await?;
        let dbs = self.dbs.read().await;
        // Table name is validated above (alphanumeric + underscores only), safe for interpolation
        let sql = format!("SELECT name, type FROM pragma_table_info('{table}')");
        dbs.get(db_name)
            .ok_or_else(|| Error::Schema(format!("database '{db_name}' not found")))?
            .db
            .query_oltp(&sql)
            .await
    }

    /// Bootstrap databases and tables from a parsed config.
    /// Creates missing databases, runs CREATE TABLE/INDEX IF NOT EXISTS.
    /// Safe to call multiple times (idempotent).
    pub async fn bootstrap(&self, config: &DbTomlConfig) -> Result<()> {
        for (db_name, db_config) in &config.databases {
            // Create database if it doesn't exist (ignore "already exists" error)
            match self.create_db(db_name).await {
                Ok(()) => tracing::info!("created database: {db_name}"),
                Err(Error::Schema(msg)) if msg.contains("already exists") => {
                    // Ensure it's loaded
                    self.ensure_db(db_name).await?;
                    tracing::debug!("database already exists: {db_name}");
                }
                Err(e) => return Err(e),
            }

            // Run table creation SQL
            for (table_name, table_config) in &db_config.tables {
                self.execute(db_name, &table_config.sql).await?;
                tracing::info!("bootstrapped table: {db_name}.{table_name}");

                // Run index creation SQL
                for (idx_name, idx_sql) in &table_config.indexes {
                    self.execute(db_name, idx_sql).await?;
                    tracing::info!("bootstrapped index: {db_name}.{table_name}.{idx_name}");
                }
            }
        }
        Ok(())
    }

    // -- internal --

    async fn open_db(&self, name: &str) -> Result<ManagedDb> {
        let is_memory = self.base_path.to_string_lossy() == IN_MEMORY_PATH;
        let db = if is_memory {
            DkdcDb::open_in_memory().await?
        } else {
            let config = DbConfig::default_for(name)?;
            DkdcDb::open_with_config(config).await?
        };
        let catalog = Arc::new(SqliteCatalogProvider::new(db.turso_db().clone()));
        catalog.schema_provider().refresh().await?;
        Ok(ManagedDb { db, catalog })
    }
}

/// Map database name to catalog name. Slashes become underscores
/// because DataFusion catalog names can't contain slashes.
fn catalog_name(db_name: &str) -> String {
    db_name.replace('/', "_")
}

/// Walk base_path, collect .db file paths as database names.
fn discover_dbs(dir: &std::path::Path, base: &std::path::Path, out: &mut Vec<String>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            discover_dbs(&path, base, out);
        } else if path.extension().is_some_and(|e| e == "db") {
            if let Ok(rel) = path.strip_prefix(base) {
                let name = rel.with_extension("").display().to_string();
                out.push(name);
            }
        }
    }
}
