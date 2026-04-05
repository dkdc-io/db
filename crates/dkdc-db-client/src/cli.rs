use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "db-client", about = "dkdc-db client")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Interactive SQL REPL
    Repl {
        /// Server URL
        #[arg(long, default_value = dkdc_db_core::DEFAULT_SERVER_URL)]
        url: String,
        /// Initial database to use
        #[arg(long)]
        db: Option<String>,
    },
    /// Execute a read query and print results
    Query {
        /// Server URL
        #[arg(long, default_value = dkdc_db_core::DEFAULT_SERVER_URL)]
        url: String,
        /// Database for OLTP query (omit for global analytical)
        #[arg(long)]
        db: Option<String>,
        /// SQL query to execute
        sql: String,
    },
    /// Execute a write statement
    Execute {
        /// Server URL
        #[arg(long, default_value = dkdc_db_core::DEFAULT_SERVER_URL)]
        url: String,
        /// Target database (required)
        #[arg(long)]
        db: String,
        /// SQL statement to execute
        sql: String,
    },
    /// List tables in a database
    Tables {
        /// Server URL
        #[arg(long, default_value = dkdc_db_core::DEFAULT_SERVER_URL)]
        url: String,
        /// Database name (required)
        #[arg(long)]
        db: String,
    },
    /// List databases
    List {
        /// Server URL
        #[arg(long, default_value = dkdc_db_core::DEFAULT_SERVER_URL)]
        url: String,
    },
}
