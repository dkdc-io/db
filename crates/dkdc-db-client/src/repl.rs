use crate::DbClient;
use rustyline::DefaultEditor;
use rustyline::error::ReadlineError;

pub async fn run(client: &DbClient, initial_db: Option<&str>) -> anyhow::Result<()> {
    let mut rl = DefaultEditor::new()?;
    let history_path = dkdc_home::ensure("db")
        .ok()
        .map(|p| p.join(".repl_history"));
    if let Some(ref path) = history_path {
        let _ = rl.load_history(path);
    }

    let mut current_db: Option<String> = None;

    // Auto-use initial database if provided
    if let Some(db) = initial_db {
        current_db = Some(db.to_string());
        println!("Using database: {db}");
    }

    println!("dkdc-db REPL (type .help for commands, .quit to exit)");

    let mut buf = String::new();
    loop {
        let prompt = match &current_db {
            Some(db) => format!("{db}> "),
            None => "db> ".to_string(),
        };
        let prompt = if buf.is_empty() {
            prompt
        } else {
            "  > ".to_string()
        };
        match rl.readline(&prompt) {
            Ok(line) => {
                let trimmed = line.trim();

                // Dot-commands
                if buf.is_empty() && trimmed.starts_with('.') {
                    rl.add_history_entry(trimmed)?;
                    match handle_dot_command(trimmed, client, &mut current_db).await {
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
                        execute_sql(sql, client, &current_db).await;
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

async fn execute_sql(sql: &str, client: &DbClient, current_db: &Option<String>) {
    let upper = sql.trim_start().to_uppercase();
    let is_write = upper.starts_with("INSERT")
        || upper.starts_with("UPDATE")
        || upper.starts_with("DELETE")
        || upper.starts_with("CREATE")
        || upper.starts_with("ALTER")
        || upper.starts_with("DROP")
        || upper.starts_with("REPLACE")
        || upper.starts_with("BEGIN")
        || upper.starts_with("COMMIT")
        || upper.starts_with("ROLLBACK");

    if is_write {
        let Some(db) = current_db else {
            eprintln!("Error: no database selected — use .use <db> first");
            return;
        };
        match client.execute(db, sql).await {
            Ok(affected) => println!("OK ({affected} rows affected)"),
            Err(e) => eprintln!("Error: {e}"),
        }
    } else {
        // Reads go through global analytical path (supports cross-db qualified names)
        match client.query(sql).await {
            Ok(resp) => print_query_response(&resp),
            Err(e) => eprintln!("Error: {e}"),
        }
    }
}

pub fn print_query_response(resp: &crate::QueryResponse) {
    if resp.rows.is_empty() {
        println!("(empty result)");
        return;
    }

    // Calculate column widths
    let headers: Vec<&str> = resp.columns.iter().map(|c| c.name.as_str()).collect();
    let mut widths: Vec<usize> = headers.iter().map(|h| h.len()).collect();

    let string_rows: Vec<Vec<String>> = resp
        .rows
        .iter()
        .map(|row| {
            row.iter()
                .map(|v| match v {
                    serde_json::Value::Null => "NULL".to_string(),
                    serde_json::Value::String(s) => s.clone(),
                    serde_json::Value::Number(n) => n.to_string(),
                    serde_json::Value::Bool(b) => b.to_string(),
                    other => other.to_string(),
                })
                .collect()
        })
        .collect();

    for row in &string_rows {
        for (i, val) in row.iter().enumerate() {
            if i < widths.len() {
                widths[i] = widths[i].max(val.len());
            }
        }
    }

    // Print header
    let header_line: String = headers
        .iter()
        .enumerate()
        .map(|(i, h)| format!("{:width$}", h, width = widths[i]))
        .collect::<Vec<_>>()
        .join(" | ");
    println!("{header_line}");
    let sep: String = widths
        .iter()
        .map(|w| "-".repeat(*w))
        .collect::<Vec<_>>()
        .join("-+-");
    println!("{sep}");

    // Print rows
    for row in &string_rows {
        let line: String = row
            .iter()
            .enumerate()
            .map(|(i, v)| format!("{:width$}", v, width = widths.get(i).copied().unwrap_or(0)))
            .collect::<Vec<_>>()
            .join(" | ");
        println!("{line}");
    }

    println!("({} rows)", resp.rows.len());
}

enum DotResult {
    Continue,
    Quit,
}

async fn handle_dot_command(
    cmd: &str,
    client: &DbClient,
    current_db: &mut Option<String>,
) -> DotResult {
    let parts: Vec<&str> = cmd.split_whitespace().collect();
    match parts.first().copied() {
        Some(".quit" | ".exit") => DotResult::Quit,
        Some(".use") => {
            if let Some(db_name) = parts.get(1) {
                *current_db = Some(db_name.to_string());
                println!("Using database: {db_name}");
            } else {
                match current_db {
                    Some(db) => println!("Current database: {db}"),
                    None => println!("No database selected. Usage: .use <db>"),
                }
            }
            DotResult::Continue
        }
        Some(".dbs") => {
            match client.list_dbs().await {
                Ok(dbs) => {
                    if dbs.is_empty() {
                        println!("(no databases)");
                    } else {
                        for db in dbs {
                            let marker = if current_db.as_deref() == Some(&db) {
                                " *"
                            } else {
                                ""
                            };
                            println!("{db}{marker}");
                        }
                    }
                }
                Err(e) => eprintln!("Error: {e}"),
            }
            DotResult::Continue
        }
        Some(".catalogs") => {
            match client.list_dbs().await {
                Ok(dbs) => {
                    if dbs.is_empty() {
                        println!("(no databases)");
                    } else {
                        for db in dbs {
                            let catalog = db.replace('/', "_");
                            if catalog == db {
                                println!("{db}");
                            } else {
                                println!("{db} → {catalog}");
                            }
                        }
                    }
                }
                Err(e) => eprintln!("Error: {e}"),
            }
            DotResult::Continue
        }
        Some(".create") => {
            if let Some(db_name) = parts.get(1) {
                match client.create_db(db_name).await {
                    Ok(()) => println!("Created database: {db_name}"),
                    Err(e) => eprintln!("Error: {e}"),
                }
            } else {
                println!("Usage: .create <db>");
            }
            DotResult::Continue
        }
        Some(".drop") => {
            if let Some(db_name) = parts.get(1) {
                match client.drop_db(db_name).await {
                    Ok(()) => {
                        println!("Dropped database: {db_name}");
                        if current_db.as_deref() == Some(*db_name) {
                            *current_db = None;
                        }
                    }
                    Err(e) => eprintln!("Error: {e}"),
                }
            } else {
                println!("Usage: .drop <db>");
            }
            DotResult::Continue
        }
        Some(".tables") => {
            let Some(db) = current_db.as_deref() else {
                eprintln!("No database selected — use .use <db> first");
                return DotResult::Continue;
            };
            match client.list_tables(db).await {
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
            let Some(db) = current_db.as_deref() else {
                eprintln!("No database selected — use .use <db> first");
                return DotResult::Continue;
            };
            if let Some(table_name) = parts.get(1) {
                match client.table_schema(db, table_name).await {
                    Ok(resp) => print_query_response(&resp),
                    Err(e) => eprintln!("Error: {e}"),
                }
            } else {
                println!("Usage: .schema TABLE_NAME");
            }
            DotResult::Continue
        }
        Some(".help") => {
            println!(".use <db>        Set current database");
            println!(".dbs             List all databases");
            println!(".catalogs        Show database → catalog name mapping");
            println!(".create <db>     Create a database");
            println!(".drop <db>       Drop a database");
            println!(".tables          List tables in current database");
            println!(".schema TABLE    Show schema for TABLE");
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
