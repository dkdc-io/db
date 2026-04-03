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
        #[arg(long, default_value = "http://127.0.0.1:4200")]
        url: String,
    },
    /// Execute a read query and print results
    Query {
        /// Server URL
        #[arg(long, default_value = "http://127.0.0.1:4200")]
        url: String,
        /// SQL query to execute
        sql: String,
    },
    /// Execute a write statement
    Execute {
        /// Server URL
        #[arg(long, default_value = "http://127.0.0.1:4200")]
        url: String,
        /// SQL statement to execute
        sql: String,
    },
    /// List tables on the server
    Tables {
        /// Server URL
        #[arg(long, default_value = "http://127.0.0.1:4200")]
        url: String,
    },
    /// List databases in ~/.dkdc/db/
    List,
}
