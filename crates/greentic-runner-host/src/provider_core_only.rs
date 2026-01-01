use std::env;

/// Check whether provider-core-only enforcement is enabled.
///
/// The flag defaults to enabled. It remains enabled unless the
/// environment variable `GREENTIC_PROVIDER_CORE_ONLY` is explicitly set
/// to `0`, `false`, or `no` (case-insensitive).
pub fn is_enabled() -> bool {
    !matches!(
        env::var("GREENTIC_PROVIDER_CORE_ONLY")
            .map(|v| v.to_ascii_lowercase())
            .as_deref(),
        Ok("0") | Ok("false") | Ok("no")
    )
}

/// Standard error message for blocked legacy paths.
pub fn blocked_message(domain: &str) -> String {
    format!(
        "provider-core only mode is enabled; legacy {domain} adapter is blocked. Use provider.invoke via provider-core"
    )
}
