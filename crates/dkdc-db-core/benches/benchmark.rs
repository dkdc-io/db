use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use dkdc_db_core::DbManager;
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
async fn setup_multi_table(size: usize) -> DbManager {
    let mgr = DbManager::new_in_memory().await.unwrap();
    mgr.create_db("bench").await.unwrap();

    mgr.execute(
        "bench",
        "CREATE TABLE regions (id INTEGER PRIMARY KEY, name TEXT, country TEXT)",
    )
    .await
    .unwrap();

    mgr.execute(
        "bench",
        "CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, category TEXT)",
    )
    .await
    .unwrap();

    mgr.execute(
        "bench",
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
    mgr.execute("bench", "BEGIN").await.unwrap();
    for (i, name) in REGIONS.iter().enumerate() {
        mgr.execute(
            "bench",
            &format!("INSERT INTO regions VALUES ({i}, '{name}', 'US')"),
        )
        .await
        .unwrap();
    }
    mgr.execute("bench", "COMMIT").await.unwrap();

    // Insert products
    mgr.execute("bench", "BEGIN").await.unwrap();
    for i in 0..100 {
        let category = CATEGORIES[i % CATEGORIES.len()];
        mgr.execute(
            "bench",
            &format!("INSERT INTO products VALUES ({i}, 'product_{i}', '{category}')"),
        )
        .await
        .unwrap();
    }
    mgr.execute("bench", "COMMIT").await.unwrap();

    // Insert sales
    let mut rng = StdRng::seed_from_u64(SEED);
    let batch_size = 5_000;
    for batch_start in (0..size).step_by(batch_size) {
        mgr.execute("bench", "BEGIN").await.unwrap();
        let batch_end = (batch_start + batch_size).min(size);
        for i in batch_start..batch_end {
            let region_id = rng.random_range(0..10);
            let product_id = rng.random_range(0..100);
            let amount: f64 = rng.random_range(1.0..1000.0);
            let quantity: i64 = rng.random_range(1..100);
            let ts: i64 = rng.random_range(1704067200..1735689600);
            mgr.execute(
                "bench",
                &format!(
                    "INSERT INTO sales VALUES ({i}, {region_id}, {product_id}, {amount}, {quantity}, {ts})"
                ),
            )
            .await
            .unwrap();
        }
        mgr.execute("bench", "COMMIT").await.unwrap();
    }

    mgr
}

/// Setup a single sales table (flat, no joins needed).
async fn setup_flat(size: usize) -> DbManager {
    let mgr = DbManager::new_in_memory().await.unwrap();
    mgr.create_db("bench").await.unwrap();

    mgr.execute(
        "bench",
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
        mgr.execute("bench", "BEGIN").await.unwrap();
        let batch_end = (batch_start + batch_size).min(size);
        for i in batch_start..batch_end {
            let region = REGIONS[rng.random_range(0..REGIONS.len())];
            let product = format!("product_{}", rng.random_range(0..100));
            let amount: f64 = rng.random_range(1.0..1000.0);
            let quantity: i64 = rng.random_range(1..100);
            let ts: i64 = rng.random_range(1704067200..1735689600);
            mgr.execute(
                "bench",
                &format!(
                    "INSERT INTO sales VALUES ({i}, '{region}', '{product}', {amount}, {quantity}, {ts})"
                ),
            )
            .await
            .unwrap();
        }
        mgr.execute("bench", "COMMIT").await.unwrap();
    }

    mgr
}

// ---------------------------------------------------------------------------
// Write benchmarks (still use DkdcDb directly for raw write perf)
// ---------------------------------------------------------------------------

fn bench_write_single_row(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mgr = rt.block_on(async {
        let mgr = DbManager::new_in_memory().await.unwrap();
        mgr.create_db("bench").await.unwrap();
        mgr.execute(
            "bench",
            "CREATE TABLE bench_write (id INTEGER PRIMARY KEY, val TEXT)",
        )
        .await
        .unwrap();
        mgr
    });

    let mut i = 0i64;
    c.bench_function("write_single_row", |b| {
        b.iter(|| {
            rt.block_on(async {
                mgr.execute(
                    "bench",
                    &format!("INSERT INTO bench_write VALUES ({i}, 'value_{i}')"),
                )
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
                    let mgr = DbManager::new_in_memory().await.unwrap();
                    mgr.create_db("bench").await.unwrap();
                    mgr.execute(
                        "bench",
                        "CREATE TABLE batch (id INTEGER PRIMARY KEY, val INTEGER)",
                    )
                    .await
                    .unwrap();
                    mgr.execute("bench", "BEGIN").await.unwrap();
                    for i in 0..size {
                        mgr.execute("bench", &format!("INSERT INTO batch VALUES ({i}, {i})"))
                            .await
                            .unwrap();
                    }
                    mgr.execute("bench", "COMMIT").await.unwrap();
                });
            })
        });
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// Analytical benchmarks
// ---------------------------------------------------------------------------

fn bench_scan_full(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("scan_full");
    group.sample_size(10);

    for size in [1_000, 10_000, 100_000, 500_000] {
        let mgr = rt.block_on(setup_flat(size));

        group.bench_with_input(BenchmarkId::new("datafusion", size), &size, |b, _| {
            b.iter(|| {
                rt.block_on(async {
                    mgr.query("SELECT * FROM bench.public.sales").await.unwrap();
                })
            })
        });

        group.bench_with_input(BenchmarkId::new("oltp", size), &size, |b, _| {
            b.iter(|| {
                rt.block_on(async {
                    mgr.query_oltp("bench", "SELECT * FROM sales")
                        .await
                        .unwrap();
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
        let mgr = rt.block_on(setup_flat(size));

        group.bench_with_input(BenchmarkId::new("datafusion", size), &size, |b, _| {
            b.iter(|| {
                rt.block_on(async {
                    mgr.query("SELECT region, amount FROM bench.public.sales")
                        .await
                        .unwrap();
                })
            })
        });

        group.bench_with_input(BenchmarkId::new("oltp", size), &size, |b, _| {
            b.iter(|| {
                rt.block_on(async {
                    mgr.query_oltp("bench", "SELECT region, amount FROM sales")
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
        let mgr = rt.block_on(setup_flat(size));

        let df_sql = "SELECT region, COUNT(*), SUM(amount), AVG(amount) FROM bench.public.sales GROUP BY region";
        let oltp_sql =
            "SELECT region, COUNT(*), SUM(amount), AVG(amount) FROM sales GROUP BY region";

        group.bench_with_input(BenchmarkId::new("datafusion", size), &size, |b, _| {
            b.iter(|| rt.block_on(async { mgr.query(df_sql).await.unwrap() }))
        });

        group.bench_with_input(BenchmarkId::new("oltp", size), &size, |b, _| {
            b.iter(|| rt.block_on(async { mgr.query_oltp("bench", oltp_sql).await.unwrap() }))
        });
    }
    group.finish();
}

fn bench_agg_complex(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("agg_complex");
    group.sample_size(10);

    for size in [1_000, 10_000, 100_000, 500_000] {
        let mgr = rt.block_on(setup_flat(size));

        let df_sql = "SELECT region, product, COUNT(*), SUM(amount), AVG(amount), MIN(amount), MAX(amount) \
                   FROM bench.public.sales GROUP BY region, product";
        let oltp_sql = "SELECT region, product, COUNT(*), SUM(amount), AVG(amount), MIN(amount), MAX(amount) \
                   FROM sales GROUP BY region, product";

        group.bench_with_input(BenchmarkId::new("datafusion", size), &size, |b, _| {
            b.iter(|| rt.block_on(async { mgr.query(df_sql).await.unwrap() }))
        });

        group.bench_with_input(BenchmarkId::new("oltp", size), &size, |b, _| {
            b.iter(|| rt.block_on(async { mgr.query_oltp("bench", oltp_sql).await.unwrap() }))
        });
    }
    group.finish();
}

fn bench_agg_many_groups(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("agg_many_groups");
    group.sample_size(10);

    for size in [1_000, 10_000, 100_000, 500_000] {
        let mgr = rt.block_on(setup_flat(size));

        let df_sql = "SELECT ts / 1000 as ts_bucket, COUNT(*), SUM(amount) FROM bench.public.sales GROUP BY ts / 1000";
        let oltp_sql =
            "SELECT ts / 1000 as ts_bucket, COUNT(*), SUM(amount) FROM sales GROUP BY ts / 1000";

        group.bench_with_input(BenchmarkId::new("datafusion", size), &size, |b, _| {
            b.iter(|| rt.block_on(async { mgr.query(df_sql).await.unwrap() }))
        });

        group.bench_with_input(BenchmarkId::new("oltp", size), &size, |b, _| {
            b.iter(|| rt.block_on(async { mgr.query_oltp("bench", oltp_sql).await.unwrap() }))
        });
    }
    group.finish();
}

fn bench_join_two_tables(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("join_two_tables");
    group.sample_size(10);

    for size in [1_000, 10_000, 100_000] {
        let mgr = rt.block_on(setup_multi_table(size));

        let df_sql = "SELECT r.name, SUM(s.amount) as revenue \
                   FROM bench.public.sales s JOIN bench.public.regions r ON s.region_id = r.id \
                   GROUP BY r.name ORDER BY revenue DESC";
        let oltp_sql = "SELECT r.name, SUM(s.amount) as revenue \
                   FROM sales s JOIN regions r ON s.region_id = r.id \
                   GROUP BY r.name ORDER BY revenue DESC";

        group.bench_with_input(BenchmarkId::new("datafusion", size), &size, |b, _| {
            b.iter(|| rt.block_on(async { mgr.query(df_sql).await.unwrap() }))
        });

        group.bench_with_input(BenchmarkId::new("oltp", size), &size, |b, _| {
            b.iter(|| rt.block_on(async { mgr.query_oltp("bench", oltp_sql).await.unwrap() }))
        });
    }
    group.finish();
}

fn bench_join_three_tables(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("join_three_tables");
    group.sample_size(10);

    for size in [1_000, 10_000, 100_000] {
        let mgr = rt.block_on(setup_multi_table(size));

        let df_sql = "SELECT r.name as region, p.category, SUM(s.amount) as revenue, COUNT(*) as cnt \
                   FROM bench.public.sales s \
                   JOIN bench.public.regions r ON s.region_id = r.id \
                   JOIN bench.public.products p ON s.product_id = p.id \
                   GROUP BY r.name, p.category \
                   ORDER BY revenue DESC";
        let oltp_sql = "SELECT r.name as region, p.category, SUM(s.amount) as revenue, COUNT(*) as cnt \
                   FROM sales s \
                   JOIN regions r ON s.region_id = r.id \
                   JOIN products p ON s.product_id = p.id \
                   GROUP BY r.name, p.category \
                   ORDER BY revenue DESC";

        group.bench_with_input(BenchmarkId::new("datafusion", size), &size, |b, _| {
            b.iter(|| rt.block_on(async { mgr.query(df_sql).await.unwrap() }))
        });

        group.bench_with_input(BenchmarkId::new("oltp", size), &size, |b, _| {
            b.iter(|| rt.block_on(async { mgr.query_oltp("bench", oltp_sql).await.unwrap() }))
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
        let mgr = rt.block_on(setup_flat(size));

        let sql = "SELECT region, product, amount, \
                   SUM(amount) OVER (PARTITION BY region ORDER BY ts) as running_total, \
                   RANK() OVER (PARTITION BY region ORDER BY amount DESC) as rank \
                   FROM bench.public.sales";

        group.bench_with_input(BenchmarkId::new("datafusion", size), &size, |b, _| {
            b.iter(|| rt.block_on(async { mgr.query(sql).await.unwrap() }))
        });
    }
    group.finish();
}

fn bench_subquery(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("subquery");
    group.sample_size(10);

    for size in [1_000, 10_000, 100_000] {
        let mgr = rt.block_on(setup_flat(size));

        let df_sql = "SELECT region, SUM(amount) as total \
                   FROM bench.public.sales \
                   WHERE product IN (SELECT product FROM bench.public.sales GROUP BY product HAVING AVG(quantity) > 50) \
                   GROUP BY region ORDER BY total DESC";
        let oltp_sql = "SELECT region, SUM(amount) as total \
                   FROM sales \
                   WHERE product IN (SELECT product FROM sales GROUP BY product HAVING AVG(quantity) > 50) \
                   GROUP BY region ORDER BY total DESC";

        group.bench_with_input(BenchmarkId::new("datafusion", size), &size, |b, _| {
            b.iter(|| rt.block_on(async { mgr.query(df_sql).await.unwrap() }))
        });

        group.bench_with_input(BenchmarkId::new("oltp", size), &size, |b, _| {
            b.iter(|| rt.block_on(async { mgr.query_oltp("bench", oltp_sql).await.unwrap() }))
        });
    }
    group.finish();
}

fn bench_filter_scan(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("filter_scan");
    group.sample_size(10);

    for size in [1_000, 10_000, 100_000, 500_000] {
        let mgr = rt.block_on(setup_flat(size));

        let df_sql = "SELECT * FROM bench.public.sales WHERE region = 'North'";
        let oltp_sql = "SELECT * FROM sales WHERE region = 'North'";

        group.bench_with_input(BenchmarkId::new("datafusion", size), &size, |b, _| {
            b.iter(|| rt.block_on(async { mgr.query(df_sql).await.unwrap() }))
        });

        group.bench_with_input(BenchmarkId::new("oltp", size), &size, |b, _| {
            b.iter(|| rt.block_on(async { mgr.query_oltp("bench", oltp_sql).await.unwrap() }))
        });
    }
    group.finish();
}

fn bench_orderby_limit(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("orderby_limit");
    group.sample_size(10);

    for size in [1_000, 10_000, 100_000, 500_000] {
        let mgr = rt.block_on(setup_flat(size));

        let df_sql = "SELECT * FROM bench.public.sales ORDER BY amount DESC LIMIT 10";
        let oltp_sql = "SELECT * FROM sales ORDER BY amount DESC LIMIT 10";

        group.bench_with_input(BenchmarkId::new("datafusion", size), &size, |b, _| {
            b.iter(|| rt.block_on(async { mgr.query(df_sql).await.unwrap() }))
        });

        group.bench_with_input(BenchmarkId::new("oltp", size), &size, |b, _| {
            b.iter(|| rt.block_on(async { mgr.query_oltp("bench", oltp_sql).await.unwrap() }))
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
