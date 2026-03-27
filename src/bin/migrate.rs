//! Standalone migration runner.
//!
//!   cargo run --bin migrate
//!
//! Applies all pending migrations from ./migrations/ to the database.
//! Reads DATABASE_URL from the environment (or falls back to the default).

use sqlx::postgres::PgPoolOptions;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "migrate=info".parse().unwrap()),
        )
        .init();

    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:postgres@localhost:54320/union_find".into());

    tracing::info!("connecting to database");

    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database_url)
        .await
        .unwrap_or_else(|e| {
            tracing::error!(%e, "failed to connect to database");
            std::process::exit(1);
        });

    tracing::info!("running migrations...");

    sqlx::migrate!("./migrations")
        .run(&pool)
        .await
        .unwrap_or_else(|e| {
            tracing::error!(%e, "migration failed");
            std::process::exit(1);
        });

    tracing::info!("migrations complete");
}
