pub fn user_facing(err: &anyhow::Error) -> String {
    tracing::error!("Internal error: {err:#}");
    "❌ Operation failed. Check server logs for details.".to_string()
}
