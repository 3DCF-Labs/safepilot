pub fn truncate_str(text: &str, limit: usize) -> String {
    if text.len() <= limit {
        return text.to_string();
    }
    let mut end = limit;
    while end > 0 && !text.is_char_boundary(end) {
        end -= 1;
    }
    if end == 0 {
        String::new()
    } else {
        format!("{}…", &text[..end])
    }
}

pub fn binary_in_path(name: &str) -> bool {
    let Some(paths) = std::env::var_os("PATH") else {
        return false;
    };
    for dir in std::env::split_paths(&paths) {
        let candidate = dir.join(name);
        if candidate.is_file() {
            return true;
        }
        #[cfg(windows)]
        {
            for ext in ["exe", "cmd", "bat"] {
                let candidate = dir.join(format!("{name}.{ext}"));
                if candidate.is_file() {
                    return true;
                }
            }
        }
    }
    false
}

pub fn normalize_github_repo_reference(input: &str) -> Option<String> {
    let trimmed = input.trim().trim_matches(|c: char| {
        c.is_whitespace()
            || matches!(
                c,
                '"' | '\'' | ',' | ';' | ')' | '(' | ']' | '[' | '}' | '{' | '>' | '<' | '.'
            )
    });
    if trimmed.is_empty() {
        return None;
    }

    if !trimmed.contains("://") && !trimmed.starts_with("git@") && trimmed.matches('/').count() == 1
    {
        let mut parts = trimmed.split('/');
        let owner = parts.next().unwrap_or("").trim();
        let repo = parts.next().unwrap_or("").trim().trim_end_matches(".git");
        if !owner.is_empty() && !repo.is_empty() {
            return Some(format!("https://github.com/{owner}/{repo}"));
        }
    }

    if let Some(rest) = trimmed.strip_prefix("git@github.com:") {
        let mut parts = rest.trim_matches('/').split('/');
        let owner = parts.next().unwrap_or("").trim();
        let repo = parts.next().unwrap_or("").trim().trim_end_matches(".git");
        if !owner.is_empty() && !repo.is_empty() {
            return Some(format!("https://github.com/{owner}/{repo}"));
        }
        return None;
    }

    if let Some(rest) = trimmed.strip_prefix("ssh://git@github.com/") {
        let mut parts = rest.trim_matches('/').split('/');
        let owner = parts.next().unwrap_or("").trim();
        let repo = parts.next().unwrap_or("").trim().trim_end_matches(".git");
        if !owner.is_empty() && !repo.is_empty() {
            return Some(format!("https://github.com/{owner}/{repo}"));
        }
        return None;
    }

    let mut sanitized = trimmed.to_string();
    if let Some(head) = sanitized.split('#').next() {
        sanitized = head.to_string();
    }
    if let Some(head) = sanitized.split('?').next() {
        sanitized = head.to_string();
    }
    let url = url::Url::parse(&sanitized).ok()?;
    if !url
        .host_str()
        .is_some_and(|h| h.eq_ignore_ascii_case("github.com"))
    {
        return None;
    }

    let mut segs = url.path_segments()?;
    let owner = segs.next().unwrap_or("").trim();
    let repo = segs.next().unwrap_or("").trim().trim_end_matches(".git");
    if owner.is_empty() || repo.is_empty() {
        return None;
    }
    Some(format!("https://github.com/{owner}/{repo}"))
}

pub fn normalize_github_repo_reference_strict(input: &str, has_signal: bool) -> Option<String> {
    let trimmed = input.trim().trim_matches(|c: char| {
        c.is_whitespace()
            || matches!(
                c,
                '"' | '\'' | ',' | ';' | ')' | '(' | ']' | '[' | '}' | '{' | '>' | '<' | '.'
            )
    });
    if trimmed.is_empty() {
        return None;
    }

    if !trimmed.contains("://") && !trimmed.starts_with("git@") && trimmed.matches('/').count() == 1
    {
        if !has_signal {
            return None;
        }
        let mut parts = trimmed.split('/');
        let owner = parts.next().unwrap_or("").trim();
        let repo = parts.next().unwrap_or("").trim().trim_end_matches(".git");
        if !owner.is_empty() && !repo.is_empty() {
            return Some(format!("https://github.com/{owner}/{repo}"));
        }
        return None;
    }

    normalize_github_repo_reference(input)
}

pub fn derive_owner_repo(default_repo: Option<&str>) -> Option<String> {
    let s = default_repo.unwrap_or("").trim();
    if s.is_empty() {
        return None;
    }

    if !s.contains("://") && !s.starts_with("git@") && s.matches('/').count() == 1 {
        return Some(s.to_string());
    }

    if let Some(rest) = s.strip_prefix("git@github.com:") {
        let rest = rest.strip_suffix(".git").unwrap_or(rest);
        return Some(rest.to_string());
    }

    if let Ok(url) = url::Url::parse(s) {
        if url
            .host_str()
            .is_some_and(|h| h.eq_ignore_ascii_case("github.com"))
        {
            let path = url.path().trim_matches('/');
            let path = path.strip_suffix(".git").unwrap_or(path);
            if path.matches('/').count() >= 1 {
                let mut parts = path.split('/');
                let owner = parts.next().unwrap_or("");
                let repo = parts.next().unwrap_or("");
                if !owner.is_empty() && !repo.is_empty() {
                    return Some(format!("{}/{}", owner, repo));
                }
            }
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strict_rejects_bare_shorthand_without_signal() {
        assert_eq!(
            normalize_github_repo_reference_strict("files/data", false),
            None
        );
        assert_eq!(
            normalize_github_repo_reference_strict("src/main", false),
            None
        );
        assert_eq!(
            normalize_github_repo_reference_strict("3DCF-Labs/safepilot", false),
            None
        );
    }

    #[test]
    fn strict_accepts_shorthand_with_signal() {
        assert_eq!(
            normalize_github_repo_reference_strict("3DCF-Labs/safepilot", true),
            Some("https://github.com/3DCF-Labs/safepilot".to_string())
        );
    }

    #[test]
    fn strict_always_accepts_full_urls() {
        assert!(
            normalize_github_repo_reference_strict("https://github.com/files/data", false)
                .is_some()
        );
        assert!(
            normalize_github_repo_reference_strict("git@github.com:owner/repo.git", false)
                .is_some()
        );
        assert!(normalize_github_repo_reference_strict(
            "ssh://git@github.com/owner/repo.git",
            false
        )
        .is_some());
    }

    #[test]
    fn permissive_still_accepts_shorthand() {
        assert!(normalize_github_repo_reference("3DCF-Labs/safepilot").is_some());
        assert!(normalize_github_repo_reference("files/data").is_some());
    }

    #[test]
    fn normalize_github_repo_reference_variants() {
        assert_eq!(
            normalize_github_repo_reference("yevh/rust-security-handbook"),
            Some("https://github.com/yevh/rust-security-handbook".to_string())
        );
        assert_eq!(
            normalize_github_repo_reference("https://github.com/yevh/rust-security-handbook"),
            Some("https://github.com/yevh/rust-security-handbook".to_string())
        );
        assert_eq!(
            normalize_github_repo_reference("https://github.com/yevh/rust-security-handbook.git"),
            Some("https://github.com/yevh/rust-security-handbook".to_string())
        );
        assert_eq!(
            normalize_github_repo_reference("git@github.com:yevh/rust-security-handbook.git"),
            Some("https://github.com/yevh/rust-security-handbook".to_string())
        );
        assert_eq!(
            normalize_github_repo_reference(
                "https://github.com/yevh/rust-security-handbook?tab=readme-ov-file"
            ),
            Some("https://github.com/yevh/rust-security-handbook".to_string())
        );
        assert_eq!(
            normalize_github_repo_reference("https://github.com/yevh/rust-security-handbook),"),
            Some("https://github.com/yevh/rust-security-handbook".to_string())
        );
    }
}
