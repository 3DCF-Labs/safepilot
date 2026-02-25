use regex::Regex;

static PATTERNS: once_cell::sync::Lazy<Vec<Regex>> = once_cell::sync::Lazy::new(|| {
    vec![
        Regex::new(r"\bsk-[A-Za-z0-9]{16,}\b").expect("regex"),
        Regex::new(r"\bsk-ant-[A-Za-z0-9_-]{20,}\b").expect("regex"),
        Regex::new(r"\bgh[pous]_[A-Za-z0-9_]{20,}\b").expect("regex"),
        Regex::new(r"\bgithub_pat_[A-Za-z0-9_]{20,}\b").expect("regex"),
        Regex::new(r"\bxox[baprs]-[A-Za-z0-9-]{10,}\b").expect("regex"),
        Regex::new(r"\bntn_[A-Za-z0-9]{20,}\b").expect("regex"),
        Regex::new(r"\blin_api_[A-Za-z0-9]{20,}\b").expect("regex"),
        Regex::new(r"(?i)\bBearer\s+[A-Za-z0-9._-]{10,}\b").expect("regex"),
        Regex::new(r"(?i)\b(x-api-key|api[_-]?key|authorization)\s*[:=]\s*[A-Za-z0-9._=-]{10,}")
            .expect("regex"),
        Regex::new(r"\b[A-Za-z0-9+/=_-]{40,}\b").expect("regex"),
    ]
});

pub fn redact_text(input: &str) -> String {
    let mut out = input.to_string();
    for re in PATTERNS.iter() {
        out = re.replace_all(&out, "[REDACTED]").to_string();
    }
    out
}
