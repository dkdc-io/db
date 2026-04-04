pub mod cli;

use std::sync::Arc;

use clap::Parser;
use cli::{Cli, Commands, TMUX_SESSION};
use dkdc_db_client::DbClient;

const INIT_TEMPLATE: &str = r#"# dkdc-db configuration
# https://github.com/dkdc-io/db

[server]
# host = "127.0.0.1"
# port = 4200

[databases.mydb]

[databases.mydb.tables.example]
sql = """
CREATE TABLE IF NOT EXISTS example (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
)
"""

# [databases.mydb.tables.example.indexes]
# idx_example_name = "CREATE INDEX IF NOT EXISTS idx_example_name ON example (name)"
"#;

pub fn run() {
    let rt = tokio::runtime::Runtime::new().expect("failed to create tokio runtime");
    rt.block_on(async_main()).unwrap();
}

async fn async_main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let cli = Cli::parse();

    match cli.command {
        None => {
            Cli::parse_from(["db", "--help"]);
        }
        Some(Commands::Serve {
            host,
            port,
            foreground,
        }) => {
            if foreground {
                // Load optional config
                let config = dkdc_db_core::toml_config::DbTomlConfig::load()?;

                // Apply server config (CLI flags override config file)
                let (effective_host, effective_port) = match &config {
                    Some(c) => {
                        let h = if host != "127.0.0.1" {
                            host.clone()
                        } else {
                            c.server.host.clone()
                        };
                        let p = if port != 4200 { port } else { c.server.port };
                        (h, p)
                    }
                    None => (host.clone(), port),
                };

                let manager = Arc::new(dkdc_db_core::DbManager::new().await?);

                // Bootstrap from config if present
                if let Some(ref c) = config {
                    let db_count = c.databases.len();
                    let table_count: usize = c.databases.values().map(|d| d.tables.len()).sum();
                    println!(
                        "bootstrapping from config: {db_count} database(s), {table_count} table(s)"
                    );
                    manager.bootstrap(c).await?;
                    println!("bootstrap complete");
                }

                println!("mode: multi-database (manages ~/.dkdc/db/)");
                dkdc_db_server::serve(manager, &effective_host, effective_port).await?;
            } else {
                if dkdc_sh::tmux::has_session(TMUX_SESSION) {
                    anyhow::bail!("dkdc-db already running in tmux session '{TMUX_SESSION}'");
                }
                let cmd = format!("db serve --foreground --host {host} --port {port}");
                dkdc_sh::tmux::new_session(TMUX_SESSION, &cmd)?;
                println!(
                    "dkdc-db server started (multi-database) in tmux session '{TMUX_SESSION}'"
                );
                println!();
                println!("Commands:");
                println!("  db attach    # View server output");
                println!("  db logs      # Show recent logs");
                println!("  db status    # Check server status");
                println!("  db stop      # Stop server");
            }
        }
        Some(Commands::Init) => {
            let path = std::path::Path::new("db.toml");
            if path.exists() {
                anyhow::bail!("db.toml already exists in current directory");
            }
            std::fs::write(path, INIT_TEMPLATE)?;
            println!("created db.toml");
        }
        Some(Commands::Bootstrap { config }) => {
            let cfg = match config {
                Some(path) => {
                    dkdc_db_core::toml_config::DbTomlConfig::load_from(std::path::Path::new(&path))?
                }
                None => dkdc_db_core::toml_config::DbTomlConfig::load()?.ok_or_else(|| {
                    anyhow::anyhow!("no db.toml found in CWD or ~/.dkdc/db/config.toml")
                })?,
            };
            let manager = Arc::new(dkdc_db_core::DbManager::new().await?);
            let db_count = cfg.databases.len();
            let table_count: usize = cfg.databases.values().map(|d| d.tables.len()).sum();
            println!("bootstrapping: {db_count} database(s), {table_count} table(s)");
            manager.bootstrap(&cfg).await?;
            println!("bootstrap complete");
        }
        Some(Commands::Stop) => {
            if !dkdc_sh::tmux::has_session(TMUX_SESSION) {
                println!("dkdc-db is not running");
                return Ok(());
            }
            dkdc_sh::tmux::kill_session(TMUX_SESSION)?;
            println!("dkdc-db stopped");
        }
        Some(Commands::Status { port }) => {
            let tmux_running = dkdc_sh::tmux::has_session(TMUX_SESSION);
            let url = format!("http://localhost:{port}/health");
            let http_responding = reqwest::get(&url)
                .await
                .map(|r| r.status().is_success())
                .unwrap_or(false);

            if http_responding {
                println!("dkdc-db server is running on port {port}");
                if tmux_running {
                    println!("Tmux session: {TMUX_SESSION}");
                }
            } else if tmux_running {
                println!("Tmux session exists but server may not be responding");
                println!("Use: db logs");
            } else {
                println!("dkdc-db is not running");
            }
        }
        Some(Commands::Attach) => {
            if !dkdc_sh::tmux::has_session(TMUX_SESSION) {
                println!("dkdc-db not running (no tmux session '{TMUX_SESSION}')");
                println!("Use: db serve");
                return Ok(());
            }
            dkdc_sh::tmux::attach(TMUX_SESSION)?;
        }
        Some(Commands::Logs { lines }) => {
            if !dkdc_sh::tmux::has_session(TMUX_SESSION) {
                println!("dkdc-db not running (no tmux session '{TMUX_SESSION}')");
                return Ok(());
            }
            let output = dkdc_sh::tmux::capture_pane(TMUX_SESSION, Some(lines))?;
            print!("{output}");
        }
        Some(Commands::Create { name, url }) => {
            let client = DbClient::new(&url);
            client.create_db(&name).await?;
            println!("Created database: {name}");
        }
        Some(Commands::Drop { name, url }) => {
            let client = DbClient::new(&url);
            client.drop_db(&name).await?;
            println!("Dropped database: {name}");
        }
        Some(Commands::Repl { url, db }) => {
            let client = DbClient::new(&url);
            dkdc_db_client::repl::run(&client, db.as_deref()).await?;
        }
        Some(Commands::Query { url, db, sql }) => {
            let client = DbClient::new(&url);
            let resp = match db {
                Some(db_name) => client.query_oltp(&db_name, &sql).await?,
                None => client.query(&sql).await?,
            };
            dkdc_db_client::repl::print_query_response(&resp);
        }
        Some(Commands::Execute { url, db, sql }) => {
            let client = DbClient::new(&url);
            let affected = client.execute(&db, &sql).await?;
            println!("OK ({affected} rows affected)");
        }
        Some(Commands::Tables { url, db }) => {
            let client = DbClient::new(&url);
            let tables = client.list_tables(&db).await?;
            if tables.is_empty() {
                println!("(no tables)");
            } else {
                for t in tables {
                    println!("{t}");
                }
            }
        }
        Some(Commands::List { url }) => {
            let client = DbClient::new(&url);
            let dbs = client.list_dbs().await?;
            if dbs.is_empty() {
                println!("(no databases)");
            } else {
                for db in dbs {
                    println!("{db}");
                }
            }
        }
    }

    Ok(())
}
