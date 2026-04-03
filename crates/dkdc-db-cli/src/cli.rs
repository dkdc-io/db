use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "db", about = "dkdc-db: HTAP embedded database")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Option<Commands>,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Interactive SQL REPL
    Repl {
        /// Database name (default: "default")
        #[arg(long, default_value = "default")]
        db: String,
    },
    /// Execute a read query and print results
    Query {
        /// Database name
        #[arg(long, default_value = "default")]
        db: String,
        /// SQL query to execute
        sql: String,
    },
    /// Execute a write statement
    Execute {
        /// Database name
        #[arg(long, default_value = "default")]
        db: String,
        /// SQL statement to execute
        sql: String,
    },
    /// List databases in ~/.dkdc/db/
    List,
}
