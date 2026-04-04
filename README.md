# dkdc-db

[![crates.io](https://img.shields.io/crates/v/dkdc-db-cli?color=blue)](https://crates.io/crates/dkdc-db-cli)
[![CI](https://img.shields.io/github/actions/workflow/status/dkdc-io/db/ci.yml?branch=main&label=CI)](https://github.com/dkdc-io/db/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-8A2BE2.svg)](https://github.com/dkdc-io/db/blob/main/LICENSE)

HTAP database system.

## Install

```bash
cargo install dkdc-db-cli dkdc-db-server
```

Verify installation:

```bash
db --version
```

## Usage

### CLI

```bash
db serve                   # Start multi-database server in tmux
db status                  # Check server status
db logs                    # View recent logs
db attach                  # Attach to tmux session
db stop                    # Stop server

db create mydb             # Create a database
db list                    # List databases
db repl --db mydb          # Interactive SQL REPL
db execute --db mydb "SQL" # One-shot write
db query --db mydb "SQL"   # One-shot OLTP query
db query "SQL"             # Cross-db analytical query
```

### Rust

```rust
use dkdc_db_core::DbManager;

let mgr = DbManager::new().await?;
mgr.create_db("mydb").await?;
mgr.execute("mydb", "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)").await?;
mgr.execute("mydb", "INSERT INTO users VALUES (1, 'alice')").await?;
let results = mgr.query("SELECT * FROM mydb.public.users").await?;
```
