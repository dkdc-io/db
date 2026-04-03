use crate::DbClient;
use rustyline::DefaultEditor;
use rustyline::error::ReadlineError;

pub async fn run(client: &DbClient) -> anyhow::Result<()> {
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
                    match handle_dot_command(trimmed, client).await {
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
                        execute_sql(sql, client).await;
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

async fn execute_sql(sql: &str, client: &DbClient) {
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
        match client.execute(sql).await {
            Ok(affected) => println!("OK ({affected} rows affected)"),
            Err(e) => eprintln!("Error: {e}"),
        }
    } else {
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

async fn handle_dot_command(cmd: &str, client: &DbClient) -> DotResult {
    let parts: Vec<&str> = cmd.split_whitespace().collect();
    match parts.first().copied() {
        Some(".quit" | ".exit") => DotResult::Quit,
        Some(".tables") => {
            match client.list_tables().await {
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
                match client.table_schema(table_name).await {
                    Ok(resp) => print_query_response(&resp),
                    Err(e) => eprintln!("Error: {e}"),
                }
            } else {
                println!("Usage: .schema TABLE_NAME");
            }
            DotResult::Continue
        }
        Some(".help") => {
            println!(".tables          List tables");
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
