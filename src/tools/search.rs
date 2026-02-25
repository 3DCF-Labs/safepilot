use anyhow::{anyhow, Context, Result};
use futures::StreamExt;
use reqwest::header::LOCATION;
use reqwest::redirect::Policy;
use serde::{Deserialize, Serialize};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::time::Duration;

use crate::utils::truncate_str;

const WEB_FETCH_MAX_BYTES: usize = 50_000;

const BRAVE_API_URL: &str = "https://api.search.brave.com/res/v1/web/search";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchResult {
    pub title: String,
    pub url: String,
    pub description: String,
}

#[derive(Debug, Deserialize)]
struct BraveResponse {
    web: Option<BraveWebResults>,
}

#[derive(Debug, Deserialize)]
struct BraveWebResults {
    results: Vec<BraveWebResult>,
}

#[derive(Debug, Deserialize)]
struct BraveWebResult {
    title: String,
    url: String,
    description: Option<String>,
}

pub async fn web_search(query: &str, api_key: &str, count: usize) -> Result<Vec<SearchResult>> {
    if query.trim().is_empty() {
        return Err(anyhow!("Search query cannot be empty"));
    }

    let count = count.clamp(1, 20);

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(20))
        .build()
        .unwrap_or_else(|_| reqwest::Client::new());
    let response = client
        .get(BRAVE_API_URL)
        .header("X-Subscription-Token", api_key)
        .header("Accept", "application/json")
        .query(&[("q", query), ("count", &count.to_string())])
        .send()
        .await
        .context("Failed to send request to Brave Search API")?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(anyhow!("Brave Search API error: {} - {}", status, body));
    }

    let brave_response: BraveResponse = response
        .json()
        .await
        .context("Failed to parse Brave Search response")?;

    let results = brave_response
        .web
        .map(|web| {
            web.results
                .into_iter()
                .map(|r| SearchResult {
                    title: r.title,
                    url: r.url,
                    description: r.description.unwrap_or_default(),
                })
                .collect()
        })
        .unwrap_or_default();

    Ok(results)
}

pub async fn web_fetch(url: &str, allow_private: bool) -> Result<String> {
    let parsed_url = url::Url::parse(url).context("Invalid URL")?;

    if parsed_url.scheme() != "http" && parsed_url.scheme() != "https" {
        return Err(anyhow!("Only HTTP/HTTPS URLs are allowed"));
    }

    let mut builder = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .user_agent("Mozilla/5.0 (compatible; TgOrchBot/1.0)");

    if !allow_private {
        let origin_scheme = parsed_url.scheme().to_string();
        let origin_port = parsed_url.port_or_known_default().unwrap_or(80);
        let origin_host = parsed_url
            .host_str()
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase();
        builder = builder.redirect(Policy::custom(move |attempt| {
            let next = attempt.url();
            let next_host = next
                .host_str()
                .unwrap_or_default()
                .trim()
                .to_ascii_lowercase();
            let next_port = next.port_or_known_default().unwrap_or(origin_port);
            if next.scheme() == origin_scheme
                && next_host == origin_host
                && next_port == origin_port
            {
                attempt.follow()
            } else {
                attempt.stop()
            }
        }));

        let pins = ensure_public_url_pins(&parsed_url).await?;
        if let Some(host) = parsed_url.host_str() {
            let host = host.trim().to_ascii_lowercase();
            if !host.is_empty() && host.parse::<IpAddr>().is_err() {
                for addr in pins {
                    builder = builder.resolve(&host, addr);
                }
            }
        }
    }

    let client = builder.build()?;

    let response = client
        .get(url)
        .send()
        .await
        .context("Failed to fetch URL")?;

    if !allow_private && response.status().is_redirection() {
        let loc = response
            .headers()
            .get(LOCATION)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .trim();
        let loc = if loc.is_empty() {
            "<missing Location header>".to_string()
        } else {
            parsed_url
                .join(loc)
                .map(|u| u.to_string())
                .unwrap_or_else(|_| loc.to_string())
        };
        return Err(anyhow!(
            "Refusing to follow redirect while private fetch is disabled (Location: {}). Fetch the final URL directly.",
            loc
        ));
    }

    if !response.status().is_success() {
        return Err(anyhow!("HTTP error: {}", response.status()));
    }

    let content_type = response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_lowercase();

    let is_binary = content_type.starts_with("image/")
        || content_type.starts_with("audio/")
        || content_type.starts_with("video/")
        || content_type.starts_with("application/octet-stream")
        || content_type.starts_with("application/zip");

    let mut stream = response.bytes_stream();
    let mut buf: Vec<u8> = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.context("Failed to read response body chunk")?;
        if buf.len() >= WEB_FETCH_MAX_BYTES {
            break;
        }
        let remaining = WEB_FETCH_MAX_BYTES - buf.len();
        if chunk.len() <= remaining {
            buf.extend_from_slice(&chunk);
        } else {
            buf.extend_from_slice(&chunk[..remaining]);
            break;
        }
    }

    let raw = String::from_utf8_lossy(&buf).to_string();

    let is_html = content_type.contains("text/html")
        || (!is_binary && content_type.is_empty() && looks_like_html(&raw));

    if is_html && !is_binary {
        let extracted = html_to_text(&raw);
        let extracted = truncate_str(&extracted, WEB_FETCH_MAX_BYTES);
        if extracted.trim().len() < 100 && raw.len() > 1000 {
            Ok(format!(
                "[low-quality extraction — page may require browser/JS rendering]\n\n{}",
                extracted
            ))
        } else {
            Ok(extracted)
        }
    } else {
        Ok(raw)
    }
}

fn looks_like_html(s: &str) -> bool {
    let prefix = s.get(..s.len().min(500)).unwrap_or(s);
    let lower = prefix.to_ascii_lowercase();
    lower.contains("<!doctype html") || lower.contains("<html") || lower.contains("<head")
}

fn html_to_text(html: &str) -> String {
    html2text::from_read(html.as_bytes(), 120)
}

pub(crate) async fn ensure_public_url(url: &url::Url) -> Result<()> {
    ensure_public_url_pins(url).await.map(|_| ())
}

pub(crate) async fn ensure_public_url_pins(url: &url::Url) -> Result<Vec<std::net::SocketAddr>> {
    let host = url
        .host_str()
        .ok_or_else(|| anyhow!("URL missing host"))?
        .trim()
        .to_ascii_lowercase();
    if host.is_empty() {
        return Err(anyhow!("URL missing host"));
    }
    if host == "localhost" || host.ends_with(".localhost") || host.ends_with(".local") {
        return Err(anyhow!("Refusing to fetch from local hostname"));
    }
    if host == "instance-data.ec2.internal" {
        return Err(anyhow!("Refusing to fetch cloud metadata host"));
    }

    let port = url.port_or_known_default().unwrap_or(80);

    if let Ok(ip) = host.parse::<IpAddr>() {
        if is_blocked_ip(ip) {
            return Err(anyhow!("Refusing to fetch from private address {}", ip));
        }
        return Ok(vec![]);
    }

    let addrs = tokio::net::lookup_host((host.as_str(), port))
        .await
        .context("Failed to resolve host")?;
    let mut pins = Vec::new();
    for addr in addrs {
        if is_blocked_ip(addr.ip()) {
            return Err(anyhow!(
                "Refusing to fetch from hostname resolving to private address {}",
                addr.ip()
            ));
        }
        pins.push(addr);
    }

    Ok(pins)
}

fn is_blocked_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => {
            v4.is_private()
                || v4.is_loopback()
                || v4.is_link_local()
                || v4.is_multicast()
                || v4.is_unspecified()
                || v4 == Ipv4Addr::new(169, 254, 169, 254)
        }
        IpAddr::V6(v6) => {
            if let Some(v4) = v6.to_ipv4() {
                return is_blocked_ip(IpAddr::V4(v4));
            }
            v6.is_loopback()
                || v6.is_unique_local()
                || v6.is_unicast_link_local()
                || v6.is_multicast()
                || v6.is_unspecified()
                || v6 == Ipv6Addr::LOCALHOST
        }
    }
}

pub fn format_results(results: &[SearchResult]) -> String {
    if results.is_empty() {
        return "No results found.".to_string();
    }

    results
        .iter()
        .enumerate()
        .map(|(i, r)| {
            format!(
                "{}. {}\n   URL: {}\n   Snippet: {}",
                i + 1,
                r.title,
                r.url,
                truncate_desc(&r.description, 150)
            )
        })
        .collect::<Vec<_>>()
        .join("\n\n")
}

fn truncate_desc(s: &str, max: usize) -> String {
    if s.len() <= max {
        s.to_string()
    } else {
        let mut end = max;
        while !s.is_char_boundary(end) && end > 0 {
            end -= 1;
        }
        format!("{}…", &s[..end])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_results() {
        let results = vec![SearchResult {
            title: "Test Title".into(),
            url: "https://example.com".into(),
            description: "Test description".into(),
        }];
        let formatted = format_results(&results);
        assert!(formatted.contains("Test Title"));
        assert!(formatted.contains("https://example.com"));
    }

    #[test]
    fn test_truncate_desc() {
        let long = "a".repeat(200);
        let truncated = truncate_desc(&long, 150);
        assert!(truncated.len() <= 154); // 150 + "…"
    }

    #[test]
    fn ipv4_mapped_ipv6_is_blocked() {
        let loopback = "::ffff:127.0.0.1".parse::<IpAddr>().expect("ip");
        assert!(is_blocked_ip(loopback));
        let metadata = "::ffff:169.254.169.254".parse::<IpAddr>().expect("ip");
        assert!(is_blocked_ip(metadata));
    }
}
