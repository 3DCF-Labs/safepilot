use anyhow::{anyhow, Result};
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct CodexAction {
    #[serde(rename = "type", alias = "action_type")]
    pub action_type: String,
    #[serde(alias = "command")]
    pub goal: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PlannedTask {
    #[serde(default)]
    pub id: Option<String>,
    #[serde(rename = "type", alias = "action_type")]
    pub action_type: String,
    #[serde(default)]
    pub agent: Option<String>,
    #[serde(alias = "command")]
    pub goal: String,
    #[serde(default)]
    pub deps: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PlanResponse {
    pub reply: String,
    #[serde(default)]
    pub actions: Vec<CodexAction>,
    #[serde(default)]
    pub tasks: Vec<PlannedTask>,
    #[allow(dead_code)]
    #[serde(default)]
    pub status: Option<String>,
    #[allow(dead_code)]
    #[serde(default)]
    pub context_remaining: Option<i64>,
}

pub fn parse_response(raw: &str) -> Result<PlanResponse> {
    serde_json::from_str(raw).or_else(|_| {
        let start = raw.find('{').ok_or_else(|| anyhow!("no json"))?;
        let end = raw.rfind('}').ok_or_else(|| anyhow!("no json end"))?;
        let slice = &raw[start..=end];
        serde_json::from_str(slice).map_err(|e| anyhow!("invalid json: {e}"))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_tasks_dag() {
        let raw = r#"
        {
          "reply": "ok",
          "tasks": [
            {"id":"t1","type":"search","goal":"q","deps":[]},
            {"id":"t2","type":"fetch","goal":"https://example.com","deps":["t1"]}
          ]
        }
        "#;
        let resp = parse_response(raw).unwrap();
        assert_eq!(resp.reply, "ok");
        assert_eq!(resp.tasks.len(), 2);
        assert_eq!(resp.tasks[0].id.as_deref(), Some("t1"));
        assert_eq!(resp.tasks[1].deps, vec!["t1"]);
    }
}
