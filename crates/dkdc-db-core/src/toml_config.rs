use serde::Deserialize;
use std::collections::BTreeMap;
use std::path::PathBuf;

use crate::error::{Error, Result};

#[derive(Debug, Deserialize, Default)]
pub struct DbTomlConfig {
    #[serde(default)]
    pub server: ServerConfig,
    #[serde(default)]
    pub databases: BTreeMap<String, DatabaseConfig>,
}

#[derive(Debug, Deserialize)]
pub struct ServerConfig {
    #[serde(default = "default_host")]
    pub host: String,
    #[serde(default = "default_port")]
    pub port: u16,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            host: default_host(),
            port: default_port(),
        }
    }
}

fn default_host() -> String {
    crate::DEFAULT_HOST.to_string()
}

fn default_port() -> u16 {
    crate::DEFAULT_PORT
}

#[derive(Debug, Deserialize, Default)]
pub struct DatabaseConfig {
    pub path: Option<PathBuf>,
    #[serde(default)]
    pub tables: BTreeMap<String, TableConfig>,
}

#[derive(Debug, Deserialize)]
pub struct TableConfig {
    pub sql: String,
    #[serde(default)]
    pub indexes: BTreeMap<String, String>,
}

impl DbTomlConfig {
    /// Load config from CWD `db.toml` or `~/.dkdc/db/config.toml` fallback.
    /// Returns None if neither file exists (config is optional).
    pub fn load() -> Result<Option<Self>> {
        // 1. Check CWD
        let cwd_path = PathBuf::from("db.toml");
        if cwd_path.exists() {
            let content = std::fs::read_to_string(&cwd_path)?;
            let config: DbTomlConfig =
                toml::from_str(&content).map_err(|e| Error::Config(format!("db.toml: {e}")))?;
            tracing::info!("loaded config from ./db.toml");
            return Ok(Some(config));
        }

        // 2. Check ~/.dkdc/db/config.toml
        if let Ok(db_dir) = dkdc_home::ensure("db") {
            let global_path = db_dir.join("config.toml");
            if global_path.exists() {
                let content = std::fs::read_to_string(&global_path)?;
                let config: DbTomlConfig = toml::from_str(&content)
                    .map_err(|e| Error::Config(format!("~/.dkdc/db/config.toml: {e}")))?;
                tracing::info!("loaded config from {}", global_path.display());
                return Ok(Some(config));
            }
        }

        Ok(None)
    }

    /// Load from a specific path.
    pub fn load_from(path: &std::path::Path) -> Result<Self> {
        let content = std::fs::read_to_string(path)?;
        let config: DbTomlConfig = toml::from_str(&content)
            .map_err(|e| Error::Config(format!("{}: {e}", path.display())))?;
        Ok(config)
    }
}
