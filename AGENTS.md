# dkdc-db

HTAP embedded database: writes enforced via libSQL, reads via DataFusion on a WAL-mode replica connection. Client/server architecture — all access goes through the REST API.

## architecture

```
crates/
  dkdc-db-core/       # library: HTAP engine (DkdcDb, TableProvider, schema introspection)
  dkdc-db-server/     # binary "db-server": axum REST API wrapping core
  dkdc-db-client/     # library: HTTP client (reqwest) with same API shape
  dkdc-db-cli/        # binary "db": REPL + commands, uses client
```

- Server owns the database file, wraps dkdc-db-core
- Client talks to server over REST (JSON), never touches SQLite directly
- Writes go through libSQL (single read-write connection)
- Reads go through DataFusion (SessionContext + SqliteTableProvider)
- Same database file, WAL mode enables concurrent reader + writer
- Schema auto-refreshes after DDL statements
- Default database location: `~/.dkdc/db/*.db` (via `dkdc-home`)

### REST API

```
POST /execute        { "sql": "..." }  → { "affected": N }
POST /query          { "sql": "..." }  → { "columns": [...], "rows": [...] }
POST /query/libsql   { "sql": "..." }  → { "columns": [...], "rows": [...] }
GET  /tables                           → ["table1", "table2"]
GET  /schema/:table                    → { "columns": [...], "rows": [...] }
GET  /health                           → { "status": "ok" }
```

Crates.io: `dkdc-db-core`, `dkdc-db-server`, `dkdc-db-client`, `dkdc-db-cli`.
Installed binaries: `db-server`, `db`.

## development

```bash
bin/setup       # install rustup if needed
bin/build       # build Rust (bin/build-rs)
bin/check       # lint + test (bin/check-rs)
bin/format      # auto-format (bin/format-rs)
bin/test        # run tests (bin/test-rs)
bin/install     # install CLI + server locally
```

Rust checks: `cargo fmt --check`, `cargo clippy -- -D warnings`, `cargo test`

## conventions

- Rust stable toolchain (edition 2024)
- No `unwrap()` in library code, all errors via `Result`
- `execute()` for writes only, `query()` for reads only (enforced)
- `query()` always routes through DataFusion
- `query_libsql()` for explicit libSQL fast path
- Client requires server running; all access through HTTP API
