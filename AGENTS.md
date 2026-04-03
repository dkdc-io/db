# dkdc-db

HTAP embedded database: writes enforced via libSQL, reads via DataFusion on a WAL-mode replica connection.

## architecture

```
crates/
  dkdc-db-core/       # library: HTAP engine (DkdcDb facade, TableProvider, schema introspection)
  dkdc-db-cli/        # CLI binary: REPL, query, execute, list, bench
```

- Writes go through libSQL (single read-write connection)
- Reads go through DataFusion (SessionContext + SqliteTableProvider backed by read-only libSQL connection)
- Same database file, WAL mode enables concurrent reader + writer
- Schema auto-refreshes after DDL statements
- Default database location: `~/.dkdc/db/*.db` (via `dkdc-home`)

Crates.io: `dkdc-db-core`, `dkdc-db-cli`. Installed binary: `db`.

## development

```bash
bin/setup       # install rustup if needed
bin/build       # build Rust (bin/build-rs)
bin/check       # lint + test (bin/check-rs)
bin/format      # auto-format (bin/format-rs)
bin/test        # run tests (bin/test-rs)
bin/install     # install CLI locally
```

Rust checks: `cargo fmt --check`, `cargo clippy -- -D warnings`, `cargo test`

## conventions

- Rust stable toolchain (edition 2024)
- No `unwrap()` in library code (dkdc-db-core), all errors via `Result`
- `execute()` for writes only, `query()` for reads only (enforced)
- `query()` always routes through DataFusion
- `query_libsql()` for explicit libSQL fast path
