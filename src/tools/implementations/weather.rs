use crate::secrets::SecretSpec;
use crate::tools::registry::Tool;
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use serde::Deserialize;
use serde_json::Value;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

pub struct WeatherTool {
    api_key: SecretSpec,
    crypto: Option<Arc<crate::crypto::Crypto>>,
}

#[derive(Debug, Deserialize)]
struct Args {
    #[serde(default)]
    action: Option<String>,
    city: String,
    #[serde(default)]
    units: Option<String>,
}

impl WeatherTool {
    pub fn new(api_key: SecretSpec, crypto: Option<Arc<crate::crypto::Crypto>>) -> Self {
        Self { api_key, crypto }
    }
}

#[async_trait]
impl Tool for WeatherTool {
    fn definition(&self) -> crate::llm::types::ToolDefinition {
        crate::llm::types::ToolDefinition {
            name: "weather".into(),
            description: "Get current weather or forecast via OpenWeather.".into(),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {
                    "action": { "type": "string", "enum": ["current","forecast"], "description": "current or forecast (default current)" },
                    "city": { "type": "string", "description": "City name (e.g. 'San Francisco')" },
                    "units": { "type": "string", "enum": ["metric","imperial"], "description": "Units (default metric)" }
                },
                "required": ["city"]
            }),
        }
    }

    async fn execute(&self, arguments: &Value, cancel: &CancellationToken) -> Result<String> {
        if cancel.is_cancelled() {
            anyhow::bail!("Cancelled");
        }

        let args: Args = serde_json::from_value(arguments.clone())?;
        let units = args.units.as_deref().unwrap_or("metric");
        let action = args.action.as_deref().unwrap_or("current");

        let key = self.api_key.load_with_crypto(self.crypto.as_deref())?;
        let client = crate::tools::weather::WeatherClient::new(&key);
        match action {
            "current" => {
                let w = client.current(&args.city, units).await?;
                Ok(crate::tools::weather::format_current(&w, units))
            }
            "forecast" => {
                let f = client.forecast(&args.city, units).await?;
                Ok(crate::tools::weather::format_forecast(&f, units))
            }
            other => Err(anyhow!("Unknown weather.action: {}", other)),
        }
    }
}
