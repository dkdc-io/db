# dkdc-db

[![GitHub Release](https://img.shields.io/github/v/release/dkdc-io/db?color=blue)](https://github.com/dkdc-io/db/releases)
[![PyPI](https://img.shields.io/pypi/v/dkdc-db?color=blue)](https://pypi.org/project/dkdc-db/)
[![crates.io](https://img.shields.io/crates/v/dkdc-db-cli?color=blue)](https://crates.io/crates/dkdc-db-cli)
[![CI](https://img.shields.io/github/actions/workflow/status/dkdc-io/db/ci.yml?branch=main&label=CI)](https://github.com/dkdc-io/db/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-8A2BE2.svg)](https://github.com/dkdc-io/db/blob/main/LICENSE)

HTAP database system. Built-in input validation, SQL safety checks (rejects stacked queries, `ATTACH DATABASE`, `LOAD_EXTENSION`), request size limits, structured tracing, and graceful shutdown.

## Install

Recommended:

```bash
curl -LsSf https://dkdc.sh/db/install.sh | sh
```

Pre-built binaries are available for Linux and macOS via Python (`uv`). Windows users should install via `cargo` or use macOS/Linux.

uv:

```bash
uv tool install dkdc-db
```

cargo:

```bash
cargo install dkdc-db-cli dkdc-db-server
```

Verify installation:

```bash
db --version
```

You can use `uvx` to run it without installing:

```bash
uvx --from dkdc-db db
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

### Python

```python
from dkdc_db import Db

db = Db()  # connects to http://127.0.0.1:4200

# database management
db.create_db("mydb")
print(db.list_dbs())

# write
db.execute("mydb", "CREATE TABLE t (id INT, name TEXT)")
db.execute("mydb", "INSERT INTO t VALUES (1, 'hello')")

# read (analytical, cross-db)
result = db.query("SELECT * FROM mydb.t")

# read (fast path, single db)
result = db.query_oltp("mydb", "SELECT * FROM t")
```
