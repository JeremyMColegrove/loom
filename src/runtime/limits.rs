use crate::runtime::Runtime;
use crate::runtime::env::Value;
use crate::runtime::error::{RuntimeError, RuntimeResult};
use std::time::Instant;

impl Runtime {
    #[allow(dead_code)]
    pub(crate) fn ensure_file_size_within_limit(
        &self,
        path: &str,
        size_bytes: u64,
    ) -> RuntimeResult<()> {
        if size_bytes as usize > self.limits.max_file_size_bytes {
            return Err(RuntimeError::message(format!(
                "File '{}' is {} bytes, above configured max_file_size_bytes ({})",
                path, size_bytes, self.limits.max_file_size_bytes
            )));
        }
        Ok(())
    }

    #[allow(dead_code)]
    pub(crate) fn enforce_row_limit(&self, rows: usize, source: &str) -> RuntimeResult<()> {
        if rows > self.limits.max_rows {
            return Err(RuntimeError::message(format!(
                "CSV row limit exceeded for '{}': {} > {}",
                source, rows, self.limits.max_rows
            )));
        }
        Ok(())
    }

    pub(crate) fn enforce_timeout_budget(
        &self,
        started_at: Instant,
        stage: &str,
    ) -> RuntimeResult<()> {
        let elapsed = started_at.elapsed();
        if elapsed > self.limits.timeout_budget {
            return Err(RuntimeError::message(format!(
                "Timeout budget exceeded at {}: elapsed={}ms budget={}ms",
                stage,
                elapsed.as_millis(),
                self.limits.timeout_budget.as_millis()
            )));
        }
        Ok(())
    }

    pub(crate) fn enforce_memory_limit(&self, value: &Value, stage: &str) -> RuntimeResult<()> {
        let estimate = estimate_value_size_bytes(value);
        if estimate > self.limits.max_pipeline_memory_bytes {
            return Err(RuntimeError::message(format!(
                "Pipeline memory estimate exceeded at {}: {} > {} bytes",
                stage, estimate, self.limits.max_pipeline_memory_bytes
            )));
        }
        Ok(())
    }
}

fn estimate_value_size_bytes(value: &Value) -> usize {
    match value {
        Value::Null => 0,
        Value::Boolean(_) => 1,
        Value::Number(_) => std::mem::size_of::<f64>(),
        Value::Path(s) | Value::String(s) => s.len(),
        Value::List(items) => items.iter().map(estimate_value_size_bytes).sum(),
        Value::Record(map) => map
            .iter()
            .map(|(k, v)| k.len() + estimate_value_size_bytes(v))
            .sum(),
        Value::Lambda(_) => 64,
        Value::Function(_) => 64,
    }
}
