use std::path::PathBuf;

use crate::error::Result;

pub struct DbConfig {
    pub path: PathBuf,
}

impl DbConfig {
    /// Resolve a database name to `~/.dkdc/db/{name}.db`.
    /// Supports nested names like `"myfolder/mydb"` → `~/.dkdc/db/myfolder/mydb.db`.
    /// Creates intermediate directories as needed.
    pub fn default_for(name: &str) -> Result<Self> {
        let db_dir = dkdc_home::ensure("db")?;
        let db_path = db_dir.join(format!("{name}.db"));

        // Ensure parent directories exist for nested names
        if let Some(parent) = db_path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        Ok(Self { path: db_path })
    }

    pub fn with_path(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }
}
