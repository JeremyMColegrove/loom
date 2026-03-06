use crate::runtime::Runtime;
use crate::runtime::env::Value;
use crate::runtime::error::{RuntimeError, RuntimeResult};
use std::collections::HashMap;

impl Runtime {
    pub(crate) fn http_post<'a>(
        &'a mut self,
        args: Vec<Value>,
        named_args: HashMap<String, Value>,
        pipe_val: Value,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = RuntimeResult<Value>> + 'a>> {
        Box::pin(async move {
            if !self.std_modules.contains("http") {
                return Err(RuntimeError::message(
                    "Directive @http.post requires @import \"std.http\" as http",
                ));
            }

            let url = named_args
                .get("url")
                .cloned()
                .or_else(|| args.first().cloned())
                .ok_or_else(|| RuntimeError::message("@http.post requires a url argument"))?
                .as_string();

            let headers_value = named_args
                .get("headers")
                .cloned()
                .or_else(|| args.get(1).cloned());
            let body_value = named_args
                .get("data")
                .cloned()
                .or_else(|| args.get(2).cloned())
                .unwrap_or(pipe_val);
            let body = if matches!(body_value, Value::Null) {
                String::new()
            } else {
                body_value.as_string()
            };

            let mut headers: Vec<(String, String)> = Vec::new();
            if let Some(value) = headers_value {
                match value {
                    Value::Record(map) => {
                        for (k, v) in map {
                            headers.push((k, v.as_string()));
                        }
                    }
                    _ => {
                        return Err(RuntimeError::message(
                            "@http.post headers must be an object/record",
                        ));
                    }
                }
            }
            if !headers
                .iter()
                .any(|(k, _)| k.eq_ignore_ascii_case("content-type"))
            {
                headers.push((
                    "Content-Type".to_string(),
                    "text/plain; charset=utf-8".to_string(),
                ));
            }

            self.authorize_network_url(&url)?;

            let request = HttpRequest { url, headers, body };
            let response = tokio::task::spawn_blocking(move || send_post(request))
                .await
                .map_err(|e| RuntimeError::message(format!("HTTP request task failed: {}", e)))??;

            if !(200..300).contains(&response.status) {
                return Err(RuntimeError::message(format!(
                    "HTTP POST failed: status={} url={} body_preview={}",
                    response.status,
                    response.url,
                    truncate_preview(&response.body, 200)
                )));
            }
            Ok(Value::String(response.body))
        })
    }
}

struct HttpRequest {
    url: String,
    headers: Vec<(String, String)>,
    body: String,
}

struct HttpResponse {
    status: u16,
    url: String,
    body: String,
}

fn send_post(request: HttpRequest) -> RuntimeResult<HttpResponse> {
    if let Ok(parsed) = reqwest::Url::parse(&request.url)
        && parsed.scheme() == "mock"
    {
        let mut status: u16 = 200;
        let mut body = String::new();
        let mut echo_body = false;
        let mut echo_header: Option<String> = None;
        for (k, v) in parsed.query_pairs() {
            match k.as_ref() {
                "status" => {
                    if let Ok(parsed_status) = v.parse::<u16>() {
                        status = parsed_status;
                    }
                }
                "body" => body = v.to_string(),
                "echo_body" => {
                    echo_body = v.eq_ignore_ascii_case("1") || v.eq_ignore_ascii_case("true");
                }
                "echo_header" => {
                    echo_header = Some(v.to_ascii_lowercase());
                }
                _ => {}
            }
        }
        if echo_body {
            body = request.body.clone();
        }
        if let Some(name) = echo_header {
            body = request
                .headers
                .iter()
                .find(|(k, _)| k.eq_ignore_ascii_case(&name))
                .map(|(_, v)| v.clone())
                .unwrap_or_default();
        }
        return Ok(HttpResponse {
            status,
            url: request.url,
            body,
        });
    }

    let client = reqwest::blocking::Client::builder()
        .build()
        .map_err(|e| RuntimeError::message(format!("Failed to initialize HTTP client: {}", e)))?;
    let mut req = client.post(&request.url).body(request.body);
    for (k, v) in request.headers {
        req = req.header(k, v);
    }
    let resp = req
        .send()
        .map_err(|e| RuntimeError::message(format!("HTTP request failed: {}", e)))?;
    let status = resp.status().as_u16();
    let url = resp.url().to_string();
    let body = resp
        .text()
        .map_err(|e| RuntimeError::message(format!("Failed reading HTTP response body: {}", e)))?;
    Ok(HttpResponse { status, url, body })
}

fn truncate_preview(text: &str, max_chars: usize) -> String {
    let mut out = String::new();
    for (idx, ch) in text.chars().enumerate() {
        if idx >= max_chars {
            out.push_str("...");
            break;
        }
        out.push(ch);
    }
    out.replace('\n', "\\n")
}
