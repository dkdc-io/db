use std::sync::Arc;
use std::time::{Duration, Instant};

use dkdc_db_client::DbClient;
use dkdc_db_core::DkdcDb;
use dkdc_db_server::api;

struct BenchConfig {
    concurrency_levels: Vec<usize>,
    requests_per_client: usize,
}

impl Default for BenchConfig {
    fn default() -> Self {
        Self {
            concurrency_levels: vec![1, 2, 4, 8, 16, 32, 64],
            requests_per_client: 100,
        }
    }
}

struct BenchResult {
    concurrency: usize,
    total_requests: usize,
    duration: Duration,
    latencies: Vec<Duration>,
}

impl BenchResult {
    fn requests_per_sec(&self) -> f64 {
        self.total_requests as f64 / self.duration.as_secs_f64()
    }

    fn p50(&self) -> Duration {
        self.percentile(50)
    }

    fn p95(&self) -> Duration {
        self.percentile(95)
    }

    fn p99(&self) -> Duration {
        self.percentile(99)
    }

    fn percentile(&self, p: usize) -> Duration {
        let mut sorted = self.latencies.clone();
        sorted.sort();
        let idx = (p as f64 / 100.0 * sorted.len() as f64) as usize;
        sorted[idx.min(sorted.len() - 1)]
    }

    fn avg(&self) -> Duration {
        let total: Duration = self.latencies.iter().sum();
        total / self.latencies.len() as u32
    }
}

async fn setup_server() -> (u16, tokio::task::JoinHandle<()>) {
    let db = DkdcDb::open_in_memory().await.unwrap();
    let state = Arc::new(db);
    let app = api::router(state);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();

    let handle = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let client = DbClient::localhost(port);
    for _ in 0..50 {
        if client.health().await.is_ok() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    (port, handle)
}

async fn seed_data(port: u16, rows: usize) {
    let client = DbClient::localhost(port);
    client
        .execute("CREATE TABLE IF NOT EXISTS bench (id INTEGER PRIMARY KEY, region TEXT, amount REAL, name TEXT)")
        .await
        .unwrap();

    for i in 0..rows {
        let region = match i % 4 {
            0 => "north",
            1 => "south",
            2 => "east",
            _ => "west",
        };
        let amount = (i as f64) * 1.5;
        client
            .execute(&format!(
                "INSERT INTO bench VALUES ({i}, '{region}', {amount}, 'user_{i}')"
            ))
            .await
            .unwrap();
    }
}

async fn bench_write(port: u16, config: &BenchConfig) -> Vec<BenchResult> {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let id_counter = Arc::new(AtomicUsize::new(100_000));
    let mut results = Vec::new();

    for &concurrency in &config.concurrency_levels {
        let reqs = config.requests_per_client;
        let mut handles = Vec::new();
        let start = Instant::now();

        for _ in 0..concurrency {
            let counter = id_counter.clone();
            handles.push(tokio::spawn(async move {
                let client = DbClient::localhost(port);
                let mut latencies = Vec::with_capacity(reqs);
                for _ in 0..reqs {
                    let id = counter.fetch_add(1, Ordering::Relaxed);
                    let sql =
                        format!("INSERT INTO bench VALUES ({id}, 'bench', {id}.0, 'bench_{id}')");
                    let t = Instant::now();
                    // Writes may fail under concurrency; record latency regardless
                    let _ = client.execute(&sql).await;
                    latencies.push(t.elapsed());
                }
                latencies
            }));
        }

        let mut all_latencies = Vec::new();
        for handle in handles {
            all_latencies.extend(handle.await.unwrap());
        }

        results.push(BenchResult {
            concurrency,
            total_requests: concurrency * reqs,
            duration: start.elapsed(),
            latencies: all_latencies,
        });
    }

    results
}

enum QueryPath {
    DataFusion,
    Oltp,
}

async fn bench_read(
    port: u16,
    config: &BenchConfig,
    sql: &str,
    path: QueryPath,
) -> Vec<BenchResult> {
    let mut results = Vec::new();

    for &concurrency in &config.concurrency_levels {
        let reqs = config.requests_per_client;
        let mut handles = Vec::new();
        let start = Instant::now();
        let use_oltp = matches!(path, QueryPath::Oltp);

        for _ in 0..concurrency {
            let sql = sql.to_string();
            handles.push(tokio::spawn(async move {
                let client = DbClient::localhost(port);
                let mut latencies = Vec::with_capacity(reqs);
                for _ in 0..reqs {
                    let t = Instant::now();
                    let result = if use_oltp {
                        client.query_oltp(&sql).await
                    } else {
                        client.query(&sql).await
                    };
                    let elapsed = t.elapsed();
                    // Under high concurrency, turso may reject concurrent reads
                    // on the same connection. Retry after a brief yield.
                    if result.is_err() {
                        tokio::task::yield_now().await;
                        let t2 = Instant::now();
                        let _ = if use_oltp {
                            client.query_oltp(&sql).await
                        } else {
                            client.query(&sql).await
                        };
                        latencies.push(t2.elapsed());
                    } else {
                        latencies.push(elapsed);
                    }
                }
                latencies
            }));
        }

        let mut all_latencies = Vec::new();
        for handle in handles {
            all_latencies.extend(handle.await.unwrap());
        }

        results.push(BenchResult {
            concurrency,
            total_requests: concurrency * reqs,
            duration: start.elapsed(),
            latencies: all_latencies,
        });
    }

    results
}

fn print_results(label: &str, results: &[BenchResult]) {
    println!("\n--- {label} ---");
    println!(
        "{:>12} {:>12} {:>10} {:>10} {:>10} {:>10} {:>10}",
        "concurrency", "total_reqs", "req/s", "avg", "p50", "p95", "p99"
    );
    println!("{}", "-".repeat(86));
    for r in results {
        println!(
            "{:>12} {:>12} {:>10.0} {:>10} {:>10} {:>10} {:>10}",
            r.concurrency,
            r.total_requests,
            r.requests_per_sec(),
            format_duration(r.avg()),
            format_duration(r.p50()),
            format_duration(r.p95()),
            format_duration(r.p99()),
        );
    }
}

fn format_duration(d: Duration) -> String {
    let us = d.as_micros();
    if us < 1000 {
        format!("{us}us")
    } else if us < 1_000_000 {
        format!("{:.1}ms", us as f64 / 1000.0)
    } else {
        format!("{:.2}s", d.as_secs_f64())
    }
}

#[tokio::main]
async fn main() {
    let config = BenchConfig::default();
    let seed_rows = 1000;

    println!("=== dkdc-db Network Load Test ===");
    println!("Seed rows: {seed_rows}");
    println!("Concurrency levels: {:?}", config.concurrency_levels);
    println!("Requests per client: {}", config.requests_per_client);

    // Read server
    let (read_port, _rh) = setup_server().await;
    seed_data(read_port, seed_rows).await;
    println!("Read server ready, data seeded.");

    // Write server (separate to avoid PK conflicts)
    let (write_port, _wh) = setup_server().await;
    seed_data(write_port, seed_rows).await;
    println!("Write server ready, data seeded.");

    // Write benchmark
    let write_results = bench_write(write_port, &config).await;
    print_results("WRITE (INSERT)", &write_results);

    // Read benchmarks
    let point_results = bench_read(
        read_port,
        &config,
        "SELECT * FROM bench WHERE id = 42",
        QueryPath::DataFusion,
    )
    .await;
    print_results("READ - Point Query (WHERE id = N)", &point_results);

    let scan_results = bench_read(
        read_port,
        &config,
        "SELECT * FROM bench",
        QueryPath::DataFusion,
    )
    .await;
    print_results("READ - Full Scan (1000 rows)", &scan_results);

    let agg_results = bench_read(
        read_port, &config,
        "SELECT region, count(*) as cnt, sum(amount) as total, avg(amount) as avg_amt FROM bench GROUP BY region ORDER BY region",
        QueryPath::DataFusion,
    ).await;
    print_results("READ - Aggregation (GROUP BY)", &agg_results);

    // Turso fast path
    let turso_results = bench_read(
        read_port,
        &config,
        "SELECT * FROM bench WHERE id = 42",
        QueryPath::Oltp,
    )
    .await;
    print_results("READ - OLTP Fast Path (WHERE id = N)", &turso_results);

    println!("\n=== Load Test Complete ===");
}
