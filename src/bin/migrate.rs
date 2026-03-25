//! Standalone migration runner.
//!
//!   cargo run --bin migrate
//!
//! Applies all pending migrations from ./migrations/ to the database.
//! Reads DATABASE_URL from the environment (or falls back to the default).

use sqlx::postgres::PgPoolOptions;

#[tokio::main]
async fn main() {
    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:postgres@localhost:54320/union_find".into());

    println!("connecting to {database_url}");

    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database_url)
        .await
        .expect("failed to connect to database");

    println!("running migrations...");

    sqlx::migrate!("./migrations")
        .run(&pool)
        .await
        .expect("migration failed");

    println!("migrations complete");
}
