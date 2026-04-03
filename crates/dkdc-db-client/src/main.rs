mod cli;

use clap::Parser;
use cli::{Cli, Commands};
use dkdc_db_client::DbClient;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Repl { url } => {
            let client = DbClient::new(&url);
            dkdc_db_client::repl::run(&client).await?;
        }
        Commands::Query { url, sql } => {
            let client = DbClient::new(&url);
            let resp = client.query(&sql).await?;
            dkdc_db_client::repl::print_query_response(&resp);
        }
        Commands::Execute { url, sql } => {
            let client = DbClient::new(&url);
            let affected = client.execute(&sql).await?;
            println!("OK ({affected} rows affected)");
        }
        Commands::Tables { url } => {
            let client = DbClient::new(&url);
            let tables = client.list_tables().await?;
            if tables.is_empty() {
                println!("(no tables)");
            } else {
                for t in tables {
                    println!("{t}");
                }
            }
        }
        Commands::List => {
            let db_dir = dkdc_home::ensure("db")?;
            let mut found = false;
            for entry in std::fs::read_dir(db_dir)? {
                let entry = entry?;
                let path = entry.path();
                if path.extension().is_some_and(|ext| ext == "db") {
                    if let Some(stem) = path.file_stem() {
                        println!("{}", stem.to_string_lossy());
                        found = true;
                    }
                }
            }
            if !found {
                println!("(no databases found)");
            }
        }
    }

    Ok(())
}
