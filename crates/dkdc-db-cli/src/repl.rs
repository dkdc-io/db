use arrow::util::pretty::pretty_format_batches;
use dkdc_db_core::DkdcDb;
use rustyline::DefaultEditor;
use rustyline::error::ReadlineError;

pub async fn run(db: &DkdcDb) -> anyhow::Result<()> {
    let mut rl = DefaultEditor::new()?;
    let history_path = dkdc_home::ensure("db")
        .ok()
        .map(|p| p.join(".repl_history"));
    if let Some(ref path) = history_path {
        let _ = rl.load_history(path);
    }

    println!("dkdc-db REPL (type .quit to exit, .tables to list tables)");

    let mut buf = String::new();
    loop {
        let prompt = if buf.is_empty() { "db> " } else { "  > " };
        match rl.readline(prompt) {
            Ok(line) => {
                let trimmed = line.trim();

                // Dot-commands
                if buf.is_empty() && trimmed.starts_with('.') {
                    rl.add_history_entry(trimmed)?;
                    match handle_dot_command(trimmed, db).await {
                        DotResult::Continue => continue,
                        DotResult::Quit => break,
                    }
                }

                buf.push_str(&line);
                buf.push(' ');

                // Execute when we see a semicolon
                if trimmed.ends_with(';') {
                    let sql = buf.trim().trim_end_matches(';').trim();
                    if !sql.is_empty() {
                        rl.add_history_entry(buf.trim())?;
                        execute_sql(sql, db).await;
                    }
                    buf.clear();
                }
            }
            Err(ReadlineError::Interrupted | ReadlineError::Eof) => break,
            Err(e) => {
                eprintln!("Error: {e}");
                break;
            }
        }
    }

    if let Some(ref path) = history_path {
        let _ = rl.save_history(path);
    }

    Ok(())
}

async fn execute_sql(sql: &str, db: &DkdcDb) {
    let upper = sql.trim_start().to_uppercase();
    let is_write = upper.starts_with("INSERT")
        || upper.starts_with("UPDATE")
        || upper.starts_with("DELETE")
        || upper.starts_with("CREATE")
        || upper.starts_with("ALTER")
        || upper.starts_with("DROP")
        || upper.starts_with("REPLACE");

    if is_write {
        match db.execute(sql).await {
            Ok(affected) => println!("OK ({affected} rows affected)"),
            Err(e) => eprintln!("Error: {e}"),
        }
    } else {
        match db.query(sql).await {
            Ok(batches) => {
                if batches.is_empty() {
                    println!("(empty result)");
                } else {
                    match pretty_format_batches(&batches) {
                        Ok(table) => println!("{table}"),
                        Err(e) => eprintln!("Error formatting results: {e}"),
                    }
                }
            }
            Err(e) => eprintln!("Error: {e}"),
        }
    }
}

enum DotResult {
    Continue,
    Quit,
}

async fn handle_dot_command(cmd: &str, db: &DkdcDb) -> DotResult {
    let parts: Vec<&str> = cmd.split_whitespace().collect();
    match parts.first().copied() {
        Some(".quit" | ".exit") => DotResult::Quit,
        Some(".tables") => {
            match db.list_tables().await {
                Ok(tables) => {
                    if tables.is_empty() {
                        println!("(no tables)");
                    } else {
                        for t in tables {
                            println!("{t}");
                        }
                    }
                }
                Err(e) => eprintln!("Error: {e}"),
            }
            DotResult::Continue
        }
        Some(".schema") => {
            if let Some(table_name) = parts.get(1) {
                let sql = format!(
                    "SELECT sql FROM sqlite_master WHERE type='table' AND name='{table_name}'"
                );
                match db.query_libsql(&sql).await {
                    Ok(batches) => {
                        if batches.is_empty() {
                            println!("Table not found: {table_name}");
                        } else {
                            match pretty_format_batches(&batches) {
                                Ok(table) => println!("{table}"),
                                Err(e) => eprintln!("Error: {e}"),
                            }
                        }
                    }
                    Err(e) => eprintln!("Error: {e}"),
                }
            } else {
                println!("Usage: .schema TABLE_NAME");
            }
            DotResult::Continue
        }
        Some(".help") => {
            println!(".tables          List tables");
            println!(".schema TABLE    Show CREATE statement for TABLE");
            println!(".quit / .exit    Exit REPL");
            println!(".help            Show this help");
            DotResult::Continue
        }
        Some(other) => {
            eprintln!("Unknown command: {other} (try .help)");
            DotResult::Continue
        }
        None => DotResult::Continue,
    }
}
