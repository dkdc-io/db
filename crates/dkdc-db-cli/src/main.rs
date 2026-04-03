mod cli;
mod repl;

use arrow::util::pretty::pretty_format_batches;
use clap::Parser;
use cli::{Cli, Commands};
use dkdc_db_core::DkdcDb;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let cli = Cli::parse();

    match cli.command {
        Some(Commands::Repl { db: name }) => {
            let db = DkdcDb::open(&name).await?;
            repl::run(&db).await?;
        }
        Some(Commands::Query { db: name, sql }) => {
            let db = DkdcDb::open(&name).await?;
            let batches = db.query(&sql).await?;
            if batches.is_empty() {
                println!("(empty result)");
            } else {
                println!("{}", pretty_format_batches(&batches)?);
            }
        }
        Some(Commands::Execute { db: name, sql }) => {
            let db = DkdcDb::open(&name).await?;
            let affected = db.execute(&sql).await?;
            println!("OK ({affected} rows affected)");
        }
        Some(Commands::List) => {
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
        None => {
            // Default to REPL
            let db = DkdcDb::open("default").await?;
            repl::run(&db).await?;
        }
    }

    Ok(())
}
