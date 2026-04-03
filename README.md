# dkdc-db

HTAP embedded database: libSQL writes + DataFusion reads.

## usage

```rust
use dkdc_db_core::DkdcDb;

let db = DkdcDb::open("mydb").await?;
db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)").await?;
db.execute("INSERT INTO users VALUES (1, 'alice')").await?;
let results = db.query("SELECT * FROM users").await?;
```

## CLI

```bash
db repl                    # interactive SQL REPL
db query --db mydb "SQL"   # one-shot query
db execute --db mydb "SQL" # one-shot write
db list                    # list databases
```
