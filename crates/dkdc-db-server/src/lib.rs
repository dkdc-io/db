pub mod api;

use std::sync::Arc;

use dkdc_db_core::DkdcDb;

/// Start the server on the given address.
pub async fn serve(db: DkdcDb, host: &str, port: u16) -> std::io::Result<()> {
    let state = Arc::new(db);
    let app = api::router(state);

    let addr = format!("{host}:{port}");
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    tracing::info!("dkdc-db server listening on {addr}");
    axum::serve(listener, app).await?;

    Ok(())
}
