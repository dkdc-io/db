mod cli;

use clap::Parser;
use cli::{Cli, Commands};
use dkdc_db_client::DbClient;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Repl { url, db } => {
            let client = DbClient::new(&url);
            dkdc_db_client::repl::run(&client, db.as_deref()).await?;
        }
        Commands::Query { url, db, sql } => {
            let client = DbClient::new(&url);
            let resp = match db {
                Some(db_name) => client.query_oltp(&db_name, &sql).await?,
                None => client.query(&sql).await?,
            };
            dkdc_db_client::repl::print_query_response(&resp);
        }
        Commands::Execute { url, db, sql } => {
            let client = DbClient::new(&url);
            let affected = client.execute(&db, &sql).await?;
            println!("OK ({affected} rows affected)");
        }
        Commands::Tables { url, db } => {
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
        Commands::List { url } => {
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
