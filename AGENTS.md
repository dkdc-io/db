# dkdc-db

HTAP embedded database: writes enforced via turso, reads via DataFusion on a WAL-mode replica connection. Client/server architecture — all access goes through the REST API.

## architecture

```
crates/
  dkdc-db-core/       # library: HTAP engine (DkdcDb, TableProvider, schema introspection)
  dkdc-db-server/     # binary "db-server": axum REST API wrapping core
  dkdc-db-client/     # library: HTTP client (reqwest) with same API shape
  dkdc-db-cli/        # binary "db": REPL + commands, uses client + dkdc-sh for tmux
  dkdc-db-bench/      # binary: network load testing (not published)
```

- Server owns the database, wraps dkdc-db-core
- Client talks to server over REST (JSON), never touches SQLite directly
- Tmux pattern (via `dkdc-sh`): `db serve` launches the server in a tmux session (`dkdc-db`), `db stop/attach/logs/status` manage it. `db serve --foreground` skips tmux (used by tmux itself).
- Writes go through turso (single read-write connection)
- Reads go through DataFusion (SessionContext + SqliteTableProvider) or turso fast path
- WAL mode enables concurrent reader + writer
- Schema auto-refreshes after DDL statements
- Per-request connections from `turso::Database` — supports concurrent reads

### storage

- `db serve` — in-memory (default, no persistence)
- `db serve --db mydb` — file-backed at `~/.dkdc/db/mydb.db`
- `db serve --db project/mydb` — nested path at `~/.dkdc/db/project/mydb.db` (dirs created automatically)
- `db list` — lists all databases under `~/.dkdc/db/`, including nested

### query paths

Two read paths — use the right one:

- **`query()` / `POST /query`** — routes through DataFusion. Best for joins, aggregations, window functions, analytical queries. Higher latency (~7-10ms) due to query planning.
- **`query_turso()` / `POST /query/turso`** — direct turso execution. Best for point lookups, simple SELECTs. ~15-50x faster than DataFusion path for simple queries (~0.4ms).

### REST API

```
POST /execute        { "sql": "..." }  → { "affected": N }
POST /query          { "sql": "..." }  → { "columns": [...], "rows": [...] }  (DataFusion)
POST /query/turso    { "sql": "..." }  → { "columns": [...], "rows": [...] }  (fast path)
GET  /tables                           → ["table1", "table2"]
GET  /schema/:table                    → { "columns": [...], "rows": [...] }
GET  /health                           → { "status": "ok" }
```

Crates.io: `dkdc-db-core`, `dkdc-db-server`, `dkdc-db-client`, `dkdc-db-cli`.
Installed binaries: `db-server`, `db`.

## development

```bash
bin/setup              # install rustup if needed
bin/build              # build Rust (bin/build-rs)
bin/check              # lint + test (bin/check-rs)
bin/format             # auto-format (bin/format-rs)
bin/test               # run tests (bin/test-rs)
bin/install            # install CLI + server locally
bin/bump-version       # bump version across all crates (--major/--minor/--patch)
bin/release-crates-io  # publish all crates to crates.io
```

Rust checks: `cargo fmt --check`, `cargo clippy -- -D warnings`, `cargo test`

## conventions

- Rust stable toolchain (edition 2024)
- No `unwrap()` in library code, all errors via `Result`
- `execute()` for writes only, `query()` for reads only (enforced)
- `query()` always routes through DataFusion (analytical)
- `query_turso()` for low-latency simple reads (fast path)
- Default is in-memory; use `--db name` for persistence
- Client requires server running; all access through HTTP API
