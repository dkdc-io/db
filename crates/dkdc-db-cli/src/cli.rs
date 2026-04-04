use clap::{Parser, Subcommand};

pub const TMUX_SESSION: &str = "dkdc-db";

#[derive(Parser)]
#[command(name = "db", about = "dkdc-db: HTAP database")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Option<Commands>,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Start the database server (in tmux)
    Serve {
        /// Host to bind to
        #[arg(long, default_value = "127.0.0.1")]
        host: String,

        /// Port to bind to
        #[arg(long, default_value_t = 4200)]
        port: u16,

        /// Run in foreground (skip tmux)
        #[arg(long)]
        foreground: bool,
    },
    /// Stop the database server (kill tmux session)
    Stop,
    /// Show database server status
    Status {
        /// Port to check
        #[arg(long, default_value_t = 4200)]
        port: u16,
    },
    /// Attach to database server tmux session
    Attach,
    /// Show recent logs from tmux session
    Logs {
        /// Number of lines to show
        #[arg(short, long, default_value_t = 50)]
        lines: usize,
    },
    /// Generate a starter db.toml in the current directory
    Init,
    /// Bootstrap databases/tables from config without starting the server
    Bootstrap {
        /// Path to config file (default: auto-discover db.toml)
        #[arg(long)]
        config: Option<String>,
    },
    /// Create a new database
    Create {
        /// Database name
        #[arg()]
        name: String,
        /// Server URL
        #[arg(long, default_value = "http://127.0.0.1:4200")]
        url: String,
    },
    /// Drop a database
    Drop {
        /// Database name
        #[arg()]
        name: String,
        /// Server URL
        #[arg(long, default_value = "http://127.0.0.1:4200")]
        url: String,
    },
    /// Interactive SQL REPL
    Repl {
        /// Server URL
        #[arg(long, default_value = "http://127.0.0.1:4200")]
        url: String,
        /// Initial database to use
        #[arg(long)]
        db: Option<String>,
    },
    /// Execute a read query and print results
    Query {
        /// Server URL
        #[arg(long, default_value = "http://127.0.0.1:4200")]
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
        #[arg(long, default_value = "http://127.0.0.1:4200")]
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
        #[arg(long, default_value = "http://127.0.0.1:4200")]
        url: String,
        /// Database name (required)
        #[arg(long)]
        db: String,
    },
    /// List databases
    List {
        /// Server URL
        #[arg(long, default_value = "http://127.0.0.1:4200")]
        url: String,
    },
}
