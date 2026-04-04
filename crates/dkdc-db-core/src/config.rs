use std::path::PathBuf;

use crate::error::Result;

pub struct DbConfig {
    pub path: PathBuf,
}

impl DbConfig {
    /// Resolve a database name to a path.
    ///
    /// - `"./local"` or `"../other"` → relative to CWD → `$CWD/local.db`
    /// - `"/abs/path/mydb"` → absolute → `/abs/path/mydb.db`
    /// - `"mydb"` or `"folder/mydb"` → `~/.dkdc/db/mydb.db` or `~/.dkdc/db/folder/mydb.db`
    ///
    /// Creates intermediate directories as needed. Appends `.db` if not present.
    pub fn default_for(name: &str) -> Result<Self> {
        let name = name.strip_suffix(".db").unwrap_or(name);
        let db_path = if name.starts_with('/') || name.starts_with("./") || name.starts_with("../")
        {
            // Absolute or CWD-relative path
            PathBuf::from(format!("{name}.db"))
        } else {
            // Shorthand: resolve under ~/.dkdc/db/
            let db_dir = dkdc_home::ensure("db")?;
            db_dir.join(format!("{name}.db"))
        };

        if let Some(parent) = db_path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        Ok(Self { path: db_path })
    }

    pub fn with_path(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }
}
