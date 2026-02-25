use crate::llm::types::Message;

#[derive(Debug, Clone)]
pub struct ToolCallRecord {
    pub call_id: String,
    pub name: String,
    pub arguments: serde_json::Value,
    pub result: Result<String, String>,
    pub duration_ms: u64,
}

#[derive(Debug, Clone)]
pub struct AgentContext {
    pub messages: Vec<Message>,
    pub tool_calls: Vec<ToolCallRecord>,
    pub iteration: usize,
    pub max_iterations: usize,
    pub tool_call_history: Vec<(String, serde_json::Value)>,
}

impl AgentContext {
    pub fn new(initial_messages: Vec<Message>, max_iterations: usize) -> Self {
        Self {
            messages: initial_messages,
            tool_calls: Vec::new(),
            iteration: 0,
            max_iterations,
            tool_call_history: Vec::new(),
        }
    }

    pub fn can_continue(&self) -> bool {
        self.iteration < self.max_iterations
    }

    pub fn increment_iteration(&mut self) {
        self.iteration += 1;
    }

    pub fn record_tool_call(&mut self, record: ToolCallRecord) {
        self.tool_calls.push(record);
    }

    pub fn track_call_for_loop_detection(&mut self, tool_name: &str, args: &serde_json::Value) {
        self.tool_call_history
            .push((tool_name.to_string(), args.clone()));
    }

    pub fn repeated_call_count(&self, tool_name: &str, args: &serde_json::Value) -> usize {
        self.tool_call_history
            .iter()
            .filter(|(n, a)| n == tool_name && a == args)
            .count()
    }
}
