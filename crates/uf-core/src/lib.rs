use std::collections::BTreeMap;

pub mod idl;

pub type Kv = BTreeMap<String, String>;

pub fn encode(kv: &Kv) -> String {
    let mut out = String::new();
    for (idx, (k, v)) in kv.iter().enumerate() {
        if idx != 0 {
            out.push(';');
        }
        out.push_str(k);
        out.push('=');
        out.push_str(v);
    }
    out.push('\n');
    out
}

pub fn decode(line: &str) -> Kv {
    let mut out = Kv::new();
    for part in line.trim_end().split(';') {
        if let Some((k, v)) = part.split_once('=') {
            out.insert(k.to_string(), v.to_string());
        }
    }
    out
}

pub fn kv(items: &[(&str, String)]) -> Kv {
    let mut out = Kv::new();
    for (k, v) in items {
        out.insert((*k).to_string(), v.clone());
    }
    out
}

pub fn get<'a>(kv: &'a Kv, key: &str) -> &'a str {
    kv.get(key).map(|s| s.as_str()).unwrap_or("")
}

pub fn get_u64(kv: &Kv, key: &str) -> u64 {
    get(kv, key).parse::<u64>().unwrap_or(0)
}

pub fn get_i64(kv: &Kv, key: &str) -> i64 {
    get(kv, key).parse::<i64>().unwrap_or(0)
}

pub fn ok(items: &[(&str, String)]) -> Kv {
    let mut out = kv(items);
    out.insert("status".to_string(), "ok".to_string());
    out
}

pub fn err(detail: impl Into<String>) -> Kv {
    kv(&[("status", "error".to_string()), ("detail", detail.into())])
}
