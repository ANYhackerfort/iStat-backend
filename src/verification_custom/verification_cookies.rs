use axum::extract::State;
use axum_extra::extract::cookie::CookieJar;
use sqlx::SqlitePool;
use std::sync::Arc;

pub struct CookieAuthentication;

impl CookieAuthentication {
    async fn verify_session(jar: CookieJar, pool: Arc<SqlitePool>) -> bool {
        if let Some(cookie) = jar.get("session_id") {
            let session_id = cookie.value();
    
            // Check if session ID exists in the database
            return sqlx::query_scalar::<_, i32>(
                "SELECT user_id FROM sessions WHERE session_id = ?"
            )
            .bind(session_id)
            .fetch_optional(&*pool)
            .await
            .unwrap_or(None)
            .is_some();
        }
        false
    }
}
