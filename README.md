# dkdc-db

[![crates.io](https://img.shields.io/crates/v/dkdc-db-cli?color=blue)](https://crates.io/crates/dkdc-db-cli)
[![CI](https://img.shields.io/github/actions/workflow/status/dkdc-io/db/ci.yml?branch=main&label=CI)](https://github.com/dkdc-io/db/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-8A2BE2.svg)](https://github.com/dkdc-io/db/blob/main/LICENSE)

HTAP embedded database: turso writes + DataFusion reads.

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
db serve                   # Start server in tmux session
db status                  # Check server status
db logs                    # View recent logs
db attach                  # Attach to tmux session
db stop                    # Stop server

db repl                    # Interactive SQL REPL
db query --db mydb "SQL"   # One-shot query
db execute --db mydb "SQL" # One-shot write
db list                    # List databases
```

### Rust

```rust
use dkdc_db_core::DkdcDb;

let db = DkdcDb::open("mydb").await?;
db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)").await?;
db.execute("INSERT INTO users VALUES (1, 'alice')").await?;
let results = db.query("SELECT * FROM users").await?;
```
