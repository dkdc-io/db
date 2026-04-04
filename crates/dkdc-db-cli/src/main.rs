mod cli;

use std::sync::Arc;

use clap::Parser;
use cli::{Cli, Commands, TMUX_SESSION};
use dkdc_db_client::DbClient;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
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
                let manager = Arc::new(dkdc_db_core::DbManager::new().await?);
                println!("mode: multi-database (manages ~/.dkdc/db/)");
                dkdc_db_server::serve(manager, &host, port).await?;
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
