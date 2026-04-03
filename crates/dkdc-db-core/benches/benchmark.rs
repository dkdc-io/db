use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use dkdc_db_core::DkdcDb;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use tokio::runtime::Runtime;

const SEED: u64 = 42;

const REGIONS: &[&str] = &[
    "North",
    "South",
    "East",
    "West",
    "Central",
    "Northeast",
    "Southeast",
    "Northwest",
    "Southwest",
    "Midwest",
];
const CATEGORIES: &[&str] = &[
    "Electronics",
    "Clothing",
    "Food",
    "Books",
    "Sports",
    "Home",
    "Garden",
    "Toys",
    "Auto",
    "Health",
];

/// Create the multi-table schema and populate with data.
/// Returns the db handle ready for querying.
async fn setup_multi_table(size: usize) -> DkdcDb {
    let db = DkdcDb::open_in_memory().await.unwrap();

    // Create tables
    db.execute("CREATE TABLE regions (id INTEGER PRIMARY KEY, name TEXT, country TEXT)")
        .await
        .unwrap();

    db.execute("CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, category TEXT)")
        .await
        .unwrap();

    db.execute(
        "CREATE TABLE sales (
            id INTEGER PRIMARY KEY,
            region_id INTEGER,
            product_id INTEGER,
            amount REAL,
            quantity INTEGER,
            ts INTEGER
        )",
    )
    .await
    .unwrap();

    // Insert regions
    db.execute("BEGIN").await.unwrap();
    for i in 0..10 {
        let name = REGIONS[i];
        db.execute(&format!("INSERT INTO regions VALUES ({i}, '{name}', 'US')"))
            .await
            .unwrap();
    }
    db.execute("COMMIT").await.unwrap();

    // Insert products
    db.execute("BEGIN").await.unwrap();
    for i in 0..100 {
        let category = CATEGORIES[i % CATEGORIES.len()];
        db.execute(&format!(
            "INSERT INTO products VALUES ({i}, 'product_{i}', '{category}')"
        ))
        .await
        .unwrap();
    }
    db.execute("COMMIT").await.unwrap();

    // Insert sales
    let mut rng = StdRng::seed_from_u64(SEED);
    let batch_size = 5_000;
    for batch_start in (0..size).step_by(batch_size) {
        db.execute("BEGIN").await.unwrap();
        let batch_end = (batch_start + batch_size).min(size);
        for i in batch_start..batch_end {
            let region_id = rng.random_range(0..10);
            let product_id = rng.random_range(0..100);
            let amount: f64 = rng.random_range(1.0..1000.0);
            let quantity: i64 = rng.random_range(1..100);
            let ts: i64 = rng.random_range(1704067200..1735689600);
            db.execute(&format!(
                "INSERT INTO sales VALUES ({i}, {region_id}, {product_id}, {amount}, {quantity}, {ts})"
            ))
            .await
            .unwrap();
        }
        db.execute("COMMIT").await.unwrap();
    }

    db.refresh_schema().await.unwrap();
    db
}

/// Setup a single sales table (flat, no joins needed).
async fn setup_flat(size: usize) -> DkdcDb {
    let db = DkdcDb::open_in_memory().await.unwrap();

    db.execute(
        "CREATE TABLE sales (
            id INTEGER PRIMARY KEY,
            region TEXT,
            product TEXT,
            amount REAL,
            quantity INTEGER,
            ts INTEGER
        )",
    )
    .await
    .unwrap();

    let mut rng = StdRng::seed_from_u64(SEED);
    let batch_size = 5_000;
    for batch_start in (0..size).step_by(batch_size) {
        db.execute("BEGIN").await.unwrap();
        let batch_end = (batch_start + batch_size).min(size);
        for i in batch_start..batch_end {
            let region = REGIONS[rng.random_range(0..REGIONS.len())];
            let product = format!("product_{}", rng.random_range(0..100));
            let amount: f64 = rng.random_range(1.0..1000.0);
            let quantity: i64 = rng.random_range(1..100);
            let ts: i64 = rng.random_range(1704067200..1735689600);
            db.execute(&format!(
                "INSERT INTO sales VALUES ({i}, '{region}', '{product}', {amount}, {quantity}, {ts})"
            ))
            .await
            .unwrap();
        }
        db.execute("COMMIT").await.unwrap();
    }

    db.refresh_schema().await.unwrap();
    db
}

// ---------------------------------------------------------------------------
// Existing benchmarks (kept as-is)
// ---------------------------------------------------------------------------

fn bench_write_single_row(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let db = rt.block_on(async {
        let db = DkdcDb::open_in_memory().await.unwrap();
        db.execute("CREATE TABLE bench_write (id INTEGER PRIMARY KEY, val TEXT)")
            .await
            .unwrap();
        db
    });

    let mut i = 0i64;
    c.bench_function("write_single_row", |b| {
        b.iter(|| {
            rt.block_on(async {
                db.execute(&format!(
                    "INSERT INTO bench_write VALUES ({i}, 'value_{i}')"
                ))
                .await
                .unwrap();
            });
            i += 1;
        })
    });
}

fn bench_write_batch(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("write_batch");

    for size in [100, 1_000, 10_000] {
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            b.iter(|| {
                rt.block_on(async {
                    let db = DkdcDb::open_in_memory().await.unwrap();
                    db.execute("CREATE TABLE batch (id INTEGER PRIMARY KEY, val INTEGER)")
                        .await
                        .unwrap();
                    db.execute("BEGIN").await.unwrap();
                    for i in 0..size {
                        db.execute(&format!("INSERT INTO batch VALUES ({i}, {i})"))
                            .await
                            .unwrap();
                    }
                    db.execute("COMMIT").await.unwrap();
                });
            })
        });
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// New analytical benchmarks
// ---------------------------------------------------------------------------

fn bench_scan_full(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("scan_full");
    group.sample_size(10);

    for size in [1_000, 10_000, 100_000, 500_000] {
        let db = rt.block_on(setup_flat(size));

        group.bench_with_input(BenchmarkId::new("datafusion", size), &size, |b, _| {
            b.iter(|| {
                rt.block_on(async {
                    db.query("SELECT * FROM sales").await.unwrap();
                })
            })
        });

        group.bench_with_input(BenchmarkId::new("libsql", size), &size, |b, _| {
            b.iter(|| {
                rt.block_on(async {
                    db.query_libsql("SELECT * FROM sales").await.unwrap();
                })
            })
        });
    }
    group.finish();
}

fn bench_scan_projected(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("scan_projected");
    group.sample_size(10);

    for size in [1_000, 10_000, 100_000, 500_000] {
        let db = rt.block_on(setup_flat(size));

        group.bench_with_input(BenchmarkId::new("datafusion", size), &size, |b, _| {
            b.iter(|| {
                rt.block_on(async {
                    db.query("SELECT region, amount FROM sales").await.unwrap();
                })
            })
        });

        group.bench_with_input(BenchmarkId::new("libsql", size), &size, |b, _| {
            b.iter(|| {
                rt.block_on(async {
                    db.query_libsql("SELECT region, amount FROM sales")
                        .await
                        .unwrap();
                })
            })
        });
    }
    group.finish();
}

fn bench_agg_simple(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("agg_simple");
    group.sample_size(10);

    for size in [1_000, 10_000, 100_000, 500_000] {
        let db = rt.block_on(setup_flat(size));

        let sql = "SELECT region, COUNT(*), SUM(amount), AVG(amount) FROM sales GROUP BY region";

        group.bench_with_input(BenchmarkId::new("datafusion", size), &size, |b, _| {
            b.iter(|| rt.block_on(async { db.query(sql).await.unwrap() }))
        });

        group.bench_with_input(BenchmarkId::new("libsql", size), &size, |b, _| {
            b.iter(|| rt.block_on(async { db.query_libsql(sql).await.unwrap() }))
        });
    }
    group.finish();
}

fn bench_agg_complex(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("agg_complex");
    group.sample_size(10);

    for size in [1_000, 10_000, 100_000, 500_000] {
        let db = rt.block_on(setup_flat(size));

        let sql = "SELECT region, product, COUNT(*), SUM(amount), AVG(amount), MIN(amount), MAX(amount) \
                   FROM sales GROUP BY region, product";

        group.bench_with_input(BenchmarkId::new("datafusion", size), &size, |b, _| {
            b.iter(|| rt.block_on(async { db.query(sql).await.unwrap() }))
        });

        group.bench_with_input(BenchmarkId::new("libsql", size), &size, |b, _| {
            b.iter(|| rt.block_on(async { db.query_libsql(sql).await.unwrap() }))
        });
    }
    group.finish();
}

fn bench_agg_many_groups(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("agg_many_groups");
    group.sample_size(10);

    // Use ts as the high-cardinality group column (many distinct values)
    // Actually, product has only 100 distinct. Let's use (ts / 1000) to get ~31K groups.
    for size in [1_000, 10_000, 100_000, 500_000] {
        let db = rt.block_on(setup_flat(size));

        let sql =
            "SELECT ts / 1000 as ts_bucket, COUNT(*), SUM(amount) FROM sales GROUP BY ts / 1000";

        group.bench_with_input(BenchmarkId::new("datafusion", size), &size, |b, _| {
            b.iter(|| rt.block_on(async { db.query(sql).await.unwrap() }))
        });

        group.bench_with_input(BenchmarkId::new("libsql", size), &size, |b, _| {
            b.iter(|| rt.block_on(async { db.query_libsql(sql).await.unwrap() }))
        });
    }
    group.finish();
}

fn bench_join_two_tables(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("join_two_tables");
    group.sample_size(10);

    for size in [1_000, 10_000, 100_000] {
        let db = rt.block_on(setup_multi_table(size));

        let sql = "SELECT r.name, SUM(s.amount) as revenue \
                   FROM sales s JOIN regions r ON s.region_id = r.id \
                   GROUP BY r.name ORDER BY revenue DESC";

        group.bench_with_input(BenchmarkId::new("datafusion", size), &size, |b, _| {
            b.iter(|| rt.block_on(async { db.query(sql).await.unwrap() }))
        });

        group.bench_with_input(BenchmarkId::new("libsql", size), &size, |b, _| {
            b.iter(|| rt.block_on(async { db.query_libsql(sql).await.unwrap() }))
        });
    }
    group.finish();
}

fn bench_join_three_tables(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("join_three_tables");
    group.sample_size(10);

    for size in [1_000, 10_000, 100_000] {
        let db = rt.block_on(setup_multi_table(size));

        let sql = "SELECT r.name as region, p.category, SUM(s.amount) as revenue, COUNT(*) as cnt \
                   FROM sales s \
                   JOIN regions r ON s.region_id = r.id \
                   JOIN products p ON s.product_id = p.id \
                   GROUP BY r.name, p.category \
                   ORDER BY revenue DESC";

        group.bench_with_input(BenchmarkId::new("datafusion", size), &size, |b, _| {
            b.iter(|| rt.block_on(async { db.query(sql).await.unwrap() }))
        });

        group.bench_with_input(BenchmarkId::new("libsql", size), &size, |b, _| {
            b.iter(|| rt.block_on(async { db.query_libsql(sql).await.unwrap() }))
        });
    }
    group.finish();
}

fn bench_window_function(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("window_function");
    group.sample_size(10);

    // DataFusion only — window functions
    for size in [1_000, 10_000, 100_000] {
        let db = rt.block_on(setup_flat(size));

        let sql = "SELECT region, product, amount, \
                   SUM(amount) OVER (PARTITION BY region ORDER BY ts) as running_total, \
                   RANK() OVER (PARTITION BY region ORDER BY amount DESC) as rank \
                   FROM sales";

        group.bench_with_input(BenchmarkId::new("datafusion", size), &size, |b, _| {
            b.iter(|| rt.block_on(async { db.query(sql).await.unwrap() }))
        });
    }
    group.finish();
}

fn bench_subquery(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("subquery");
    group.sample_size(10);

    for size in [1_000, 10_000, 100_000] {
        let db = rt.block_on(setup_flat(size));

        let sql = "SELECT region, SUM(amount) as total \
                   FROM sales \
                   WHERE product IN (SELECT product FROM sales GROUP BY product HAVING AVG(quantity) > 50) \
                   GROUP BY region ORDER BY total DESC";

        group.bench_with_input(BenchmarkId::new("datafusion", size), &size, |b, _| {
            b.iter(|| rt.block_on(async { db.query(sql).await.unwrap() }))
        });

        group.bench_with_input(BenchmarkId::new("libsql", size), &size, |b, _| {
            b.iter(|| rt.block_on(async { db.query_libsql(sql).await.unwrap() }))
        });
    }
    group.finish();
}

fn bench_filter_scan(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("filter_scan");
    group.sample_size(10);

    for size in [1_000, 10_000, 100_000, 500_000] {
        let db = rt.block_on(setup_flat(size));

        // Selective filter: ~10% of rows (1 of 10 regions)
        let sql = "SELECT * FROM sales WHERE region = 'North'";

        group.bench_with_input(BenchmarkId::new("datafusion", size), &size, |b, _| {
            b.iter(|| rt.block_on(async { db.query(sql).await.unwrap() }))
        });

        group.bench_with_input(BenchmarkId::new("libsql", size), &size, |b, _| {
            b.iter(|| rt.block_on(async { db.query_libsql(sql).await.unwrap() }))
        });
    }
    group.finish();
}

fn bench_orderby_limit(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("orderby_limit");
    group.sample_size(10);

    for size in [1_000, 10_000, 100_000, 500_000] {
        let db = rt.block_on(setup_flat(size));

        let sql = "SELECT * FROM sales ORDER BY amount DESC LIMIT 10";

        group.bench_with_input(BenchmarkId::new("datafusion", size), &size, |b, _| {
            b.iter(|| rt.block_on(async { db.query(sql).await.unwrap() }))
        });

        group.bench_with_input(BenchmarkId::new("libsql", size), &size, |b, _| {
            b.iter(|| rt.block_on(async { db.query_libsql(sql).await.unwrap() }))
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_write_single_row,
    bench_write_batch,
    bench_scan_full,
    bench_scan_projected,
    bench_agg_simple,
    bench_agg_complex,
    bench_agg_many_groups,
    bench_join_two_tables,
    bench_join_three_tables,
    bench_window_function,
    bench_subquery,
    bench_filter_scan,
    bench_orderby_limit,
);
criterion_main!(benches);
