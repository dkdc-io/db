use clap::{Parser, Subcommand};

pub const TMUX_SESSION: &str = "dkdc-db";

#[derive(Parser)]
#[command(name = "db", about = "dkdc-db: HTAP embedded database")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Option<Commands>,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Start the database server (in tmux)
    Serve {
        /// Database name (stored at ~/.dkdc/db/{name}.db)
        #[arg(long, default_value = "default")]
        db: String,

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
