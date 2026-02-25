use crate::tools::registry::Tool;
use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use futures::StreamExt;
use serde::Deserialize;
use serde_json::Value;
use std::path::PathBuf;
use std::time::Duration;
use tempfile::TempDir;
use tokio_util::sync::CancellationToken;

use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine;
use chromiumoxide::browser::{Browser, BrowserConfig};
use chromiumoxide::cdp::browser_protocol::browser::SetDownloadBehaviorBehavior;
use chromiumoxide::cdp::browser_protocol::browser::SetDownloadBehaviorParams;
use chromiumoxide::cdp::browser_protocol::page::{CaptureScreenshotFormat, PrintToPdfParams};
use chromiumoxide::page::ScreenshotParams;

pub struct BrowserTool {
    allow_private: bool,
    timeout: Duration,
}

#[derive(Debug, Deserialize)]
struct Args {
    url: String,
    #[serde(default)]
    steps: Vec<Step>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
enum Step {
    #[serde(rename = "wait_ms")]
    WaitMs { ms: u64 },
    #[serde(rename = "wait_for")]
    WaitFor {
        selector: String,
        #[serde(default)]
        timeout_ms: Option<u64>,
    },
    #[serde(rename = "click")]
    Click { selector: String },
    #[serde(rename = "type")]
    Type { selector: String, text: String },
    #[serde(rename = "press")]
    Press {
        #[serde(default)]
        selector: Option<String>,
        key: String,
    },
    #[serde(rename = "dump_dom")]
    DumpDom,
    #[serde(rename = "screenshot")]
    Screenshot {
        #[serde(default)]
        full_page: Option<bool>,
    },
    #[serde(rename = "pdf")]
    Pdf,
}

impl BrowserTool {
    pub fn new(allow_private: bool, timeout: Duration) -> Result<Self> {
        Ok(Self {
            allow_private,
            timeout,
        })
    }
}

#[async_trait]
impl Tool for BrowserTool {
    fn definition(&self) -> crate::llm::types::ToolDefinition {
        crate::llm::types::ToolDefinition {
            name: "browser".into(),
            description: "Headless Chromium automation (unsafe-only): navigate, click/type/press, dump DOM, screenshot, pdf."
                .into(),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {
                    "url": { "type": "string", "description": "URL to open (http/https)" },
                    "steps": {
                        "type": "array",
                        "description": "Optional automation steps. If omitted/empty, defaults to dump_dom.",
                        "items": {
                            "type": "object",
                            "properties": {
                                "type": { "type": "string", "description": "wait_ms|wait_for|click|type|press|dump_dom|screenshot|pdf" },
                                "ms": { "type": "integer" },
                                "selector": { "type": "string" },
                                "text": { "type": "string" },
                                "key": { "type": "string" },
                                "timeout_ms": { "type": "integer" },
                                "full_page": { "type": "boolean" }
                            },
                            "required": ["type"]
                        }
                    }
                },
                "required": ["url"]
            }),
        }
    }

    async fn execute(&self, arguments: &Value, cancel: &CancellationToken) -> Result<String> {
        if cancel.is_cancelled() {
            anyhow::bail!("Cancelled");
        }

        let args: Args = serde_json::from_value(arguments.clone())?;
        let parsed_url = url::Url::parse(&args.url).context("Invalid URL")?;
        if parsed_url.scheme() != "http" && parsed_url.scheme() != "https" {
            anyhow::bail!("Only HTTP/HTTPS URLs are allowed");
        }
        if !self.allow_private {
            crate::tools::search::ensure_public_url(&parsed_url).await?;
        }

        let steps = if args.steps.is_empty() {
            vec![Step::WaitMs { ms: 1500 }, Step::DumpDom]
        } else {
            args.steps
        };

        let fut = run_steps(&args.url, &steps, cancel);
        match tokio::time::timeout(self.timeout, fut).await {
            Ok(r) => r,
            Err(_) => Err(anyhow!("browser timed out after {:?}", self.timeout)),
        }
    }
}

async fn run_steps(url: &str, steps: &[Step], cancel: &CancellationToken) -> Result<String> {
    let tmp = TempDir::new().context("Failed to create temp dir")?;
    let download_dir = tmp.path().join("downloads");
    tokio::fs::create_dir_all(&download_dir).await.ok();

    let cfg = BrowserConfig::builder()
        .build()
        .map_err(|e| anyhow!("BrowserConfig: {}", e))?;
    let (mut browser, mut handler) = Browser::launch(cfg).await.context("launch browser")?;

    let handler_task = tokio::spawn(async move {
        while let Some(h) = handler.next().await {
            if h.is_err() {
                break;
            }
        }
    });

    let _ = browser
        .execute(
            SetDownloadBehaviorParams::builder()
                .behavior(SetDownloadBehaviorBehavior::Allow)
                .download_path(download_dir.to_string_lossy().to_string())
                .build()
                .map_err(|e| anyhow!("SetDownloadBehaviorParams: {}", e))?,
        )
        .await;

    let page = browser.new_page(url).await.context("new_page")?;

    let mut outputs: Vec<String> = Vec::new();

    for step in steps {
        if cancel.is_cancelled() {
            browser.close().await.ok();
            handler_task.abort();
            return Err(anyhow!("Cancelled"));
        }

        match step {
            Step::WaitMs { ms } => {
                tokio::time::sleep(Duration::from_millis(*ms)).await;
            }
            Step::WaitFor {
                selector,
                timeout_ms,
            } => {
                let deadline = tokio::time::Instant::now()
                    + Duration::from_millis(timeout_ms.unwrap_or(10_000).min(60_000));
                loop {
                    if page.find_element(selector).await.is_ok() {
                        break;
                    }
                    if tokio::time::Instant::now() >= deadline {
                        return Err(anyhow!("wait_for timed out: {}", selector));
                    }
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
            }
            Step::Click { selector } => {
                page.find_element(selector)
                    .await
                    .with_context(|| format!("click: selector not found: {selector}"))?
                    .click()
                    .await?;
            }
            Step::Type { selector, text } => {
                page.find_element(selector)
                    .await
                    .with_context(|| format!("type: selector not found: {selector}"))?
                    .click()
                    .await?
                    .type_str(text)
                    .await?;
            }
            Step::Press { selector, key } => {
                if let Some(sel) = selector {
                    page.find_element(sel)
                        .await
                        .with_context(|| format!("press: selector not found: {sel}"))?
                        .click()
                        .await?
                        .press_key(key)
                        .await?;
                } else {
                    return Err(anyhow!("press step requires a selector"));
                }
            }
            Step::DumpDom => {
                let html = page.content().await?;
                outputs.push(format!(
                    "[dump_dom]\n{}",
                    crate::utils::truncate_str(&html, 45_000)
                ));
            }
            Step::Screenshot { full_page } => {
                let img = page
                    .screenshot(
                        ScreenshotParams::builder()
                            .format(CaptureScreenshotFormat::Png)
                            .full_page(full_page.unwrap_or(false))
                            .omit_background(true)
                            .build(),
                    )
                    .await?;
                let b64 = B64.encode(img);
                outputs.push(format!(
                    "[screenshot_png_base64]\n{}",
                    crate::utils::truncate_str(&b64, 45_000)
                ));
            }
            Step::Pdf => {
                let pdf = page.pdf(PrintToPdfParams::builder().build()).await?;
                let b64 = B64.encode(pdf);
                outputs.push(format!(
                    "[pdf_base64]\n{}",
                    crate::utils::truncate_str(&b64, 45_000)
                ));
            }
        }
    }

    let downloads = list_downloads(&download_dir).await;
    if let Ok(list) = downloads {
        if !list.is_empty() {
            outputs.push(format!(
                "[downloads]\n{}",
                crate::utils::truncate_str(&list, 2000)
            ));
        }
    }

    browser.close().await.ok();
    handler_task.abort();

    Ok(outputs.join("\n\n"))
}

async fn list_downloads(dir: &PathBuf) -> Result<String> {
    let mut out = Vec::new();
    let mut rd = tokio::fs::read_dir(dir).await?;
    while let Some(ent) = rd.next_entry().await? {
        let meta = ent.metadata().await?;
        if meta.is_file() {
            out.push(format!(
                "- {} ({} bytes)",
                ent.file_name().to_string_lossy(),
                meta.len()
            ));
        }
    }
    Ok(out.join("\n"))
}
