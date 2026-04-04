pub mod api;

use std::sync::Arc;

use dkdc_db_core::DbManager;

/// Start the server on the given address.
pub async fn serve(manager: Arc<DbManager>, host: &str, port: u16) -> std::io::Result<()> {
    let app = api::router(manager);

    let addr = format!("{host}:{port}");
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    tracing::info!("dkdc-db server listening on {addr}");
    axum::serve(listener, app).await?;

    Ok(())
}
