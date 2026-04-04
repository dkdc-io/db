# dkdc-db

HTAP database: writes enforced via turso, reads via DataFusion. Multi-database server — one process manages N named databases with cross-database joins via DataFusion's catalog system.

## architecture

```
crates/
  dkdc-db-core/       # library: HTAP engine (DbManager, DkdcDb, catalogs, schema introspection)
  dkdc-db-server/     # binary "db-server": axum REST API wrapping core
  dkdc-db-client/     # library: HTTP client (reqwest) with same API shape
  dkdc-db-cli/        # binary "db": REPL + commands, uses client + dkdc-sh for tmux
  dkdc-db-bench/      # binary: network load testing (not published)
```

- `DbManager` is the top-level struct: owns a shared `SessionContext` + N `DkdcDb` instances
- Each database registers as a DataFusion catalog — tables resolve as `{db}.public.{table}`
- `DkdcDb` is a thin wrapper: one `turso::Database` + one `WriteEngine`, no `SessionContext`
- Server wraps `DbManager`, all access through REST API
- Client talks to server over REST (JSON), never touches SQLite directly
- Tmux pattern (via `dkdc-sh`): `db serve` launches the server in a tmux session (`dkdc-db`), `db stop/attach/logs/status` manage it. `db serve --foreground` skips tmux.
- Writes go through turso (single read-write connection per database)
- Reads go through DataFusion (shared SessionContext with per-db catalogs) or turso fast path
- WAL mode enables concurrent reader + writer
- Schema auto-refreshes after DDL statements (via catalog refresh)
- Per-request connections from `turso::Database` — supports concurrent reads
- Lazy loading: on startup, server discovers `.db` files but doesn't open them until first access

### storage

- `db serve` — multi-database server managing `~/.dkdc/db/`
- Databases are created via `POST /db` or `db create <name>` and stored at `~/.dkdc/db/{name}.db`
- Nested names like `project/mydb` → `~/.dkdc/db/project/mydb.db` (dirs created automatically)
- Lazy loading: existing `.db` files are discovered at startup but not opened until first query
- `db list` — lists all databases (loaded + known on disk)

### query paths

Two read paths — use the right one:

- **`DbManager.query()` / `POST /query`** — routes through DataFusion (shared SessionContext). Supports cross-db joins via qualified names (`db.public.table`). Best for joins, aggregations, window functions, analytical queries. Higher latency (~7-10ms) due to query planning.
- **`DbManager.query_oltp()` / `POST /db/{name}/query/oltp`** — direct turso execution scoped to one database. Best for point lookups, simple SELECTs. ~15-50x faster than DataFusion path for simple queries (~0.4ms).

### REST API

```
POST   /db                     { "name": "mydb" }  → { "name": "mydb" }     (create database)
DELETE /db/{name}                                   → { "dropped": "mydb" }  (drop database)
GET    /db                                          → ["db1", "db2"]         (list databases)

POST   /db/{name}/execute      { "sql": "..." }    → { "affected": N }
POST   /db/{name}/query/oltp   { "sql": "..." }    → { "columns": [...], "rows": [...] }
GET    /db/{name}/tables                            → ["table1", "table2"]
GET    /db/{name}/schema/{tbl}                      → { "columns": [...], "rows": [...] }

POST   /query                  { "sql": "..." }    → { "columns": [...], "rows": [...] }  (cross-db)
GET    /health                                      → { "status": "ok" }
```

### catalog naming

Database names map to DataFusion catalog names. Slashes become underscores (`project/mydb` → catalog `project_mydb`). Unqualified table names resolve against DataFusion's default catalog.

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
- `query()` always routes through DataFusion (analytical, cross-db)
- `query_oltp()` for low-latency simple reads (fast path, single-db)
- All operations are db-scoped — writes target a specific database, reads can span databases
- Cross-db join syntax: `SELECT * FROM db1.public.t1 JOIN db2.public.t2 ON ...`
- Client requires server running; all access through HTTP API
