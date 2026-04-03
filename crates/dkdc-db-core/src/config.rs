use std::path::PathBuf;

use crate::error::Result;

pub struct DbConfig {
    pub path: PathBuf,
}

impl DbConfig {
    pub fn default_for(name: &str) -> Result<Self> {
        let db_dir = dkdc_home::ensure("db")?;
        Ok(Self {
            path: db_dir.join(format!("{name}.db")),
        })
    }

    pub fn with_path(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }
}
