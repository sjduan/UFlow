//! Strict, bounded parser for the integer-only T1 snapshot projection.
//! This is deliberately not a general JSON API. Unexpected types, duplicate
//! keys, escapes, floating point numbers and trailing input fail closed.
use std::collections::BTreeMap;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum Value {
    Object(BTreeMap<String, Value>),
    Array(Vec<Value>),
    String(String),
    Number(u128),
    Bool(bool),
    Null,
}

impl Value {
    pub(super) fn field(&self, name: &str) -> Result<&Value, String> {
        match self {
            Self::Object(fields) => fields.get(name).ok_or_else(|| format!("missing {name}")),
            _ => Err("expected object".into()),
        }
    }
    pub(super) fn string(&self) -> Result<&str, String> {
        match self {
            Self::String(value) => Ok(value),
            _ => Err("expected string".into()),
        }
    }
    pub(super) fn number(&self) -> Result<u128, String> {
        match self {
            Self::Number(value) => Ok(*value),
            _ => Err("expected unsigned integer".into()),
        }
    }
    pub(super) fn boolean(&self) -> Result<bool, String> {
        match self {
            Self::Bool(value) => Ok(*value),
            _ => Err("expected boolean".into()),
        }
    }
    pub(super) fn array(&self) -> Result<&[Value], String> {
        match self {
            Self::Array(value) => Ok(value),
            _ => Err("expected array".into()),
        }
    }
}

pub(super) fn parse(input: &str) -> Result<Value, String> {
    if input.len() > 1_048_576 {
        return Err("projection too large".into());
    }
    let mut parser = Parser {
        bytes: input.as_bytes(),
        pos: 0,
    };
    let value = parser.value(0)?;
    parser.space();
    if parser.pos != parser.bytes.len() {
        return Err("trailing JSON data".into());
    }
    Ok(value)
}

struct Parser<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl Parser<'_> {
    fn space(&mut self) {
        while self
            .bytes
            .get(self.pos)
            .is_some_and(|b| b.is_ascii_whitespace())
        {
            self.pos += 1;
        }
    }
    fn take(&mut self, byte: u8) -> Result<(), String> {
        self.space();
        if self.bytes.get(self.pos) != Some(&byte) {
            return Err(format!("unexpected JSON token at {}", self.pos));
        }
        self.pos += 1;
        Ok(())
    }
    fn string(&mut self) -> Result<String, String> {
        self.take(b'"')?;
        let start = self.pos;
        while let Some(&byte) = self.bytes.get(self.pos) {
            if byte == b'"' {
                let result = std::str::from_utf8(&self.bytes[start..self.pos])
                    .map_err(|_| "invalid JSON UTF-8")?
                    .to_string();
                self.pos += 1;
                return Ok(result);
            }
            // T1 validates every emitted identity/name as a restricted ASCII
            // token, and therefore never emits escapes in this projection.
            if byte < 0x20 || byte == b'\\' {
                return Err("unsupported JSON string escape or control byte".into());
            }
            self.pos += 1;
        }
        Err("unterminated JSON string".into())
    }
    fn literal(&mut self, token: &[u8], result: Value) -> Result<Value, String> {
        if self.bytes.get(self.pos..self.pos + token.len()) != Some(token) {
            return Err("invalid JSON literal".into());
        }
        self.pos += token.len();
        Ok(result)
    }
    fn value(&mut self, depth: usize) -> Result<Value, String> {
        if depth > 8 {
            return Err("JSON nesting too deep".into());
        }
        self.space();
        match self.bytes.get(self.pos).copied() {
            Some(b'"') => Ok(Value::String(self.string()?)),
            Some(b't') => self.literal(b"true", Value::Bool(true)),
            Some(b'f') => self.literal(b"false", Value::Bool(false)),
            Some(b'n') => self.literal(b"null", Value::Null),
            Some(b'0'..=b'9') => {
                let start = self.pos;
                while self.bytes.get(self.pos).is_some_and(u8::is_ascii_digit) {
                    self.pos += 1;
                }
                let text = std::str::from_utf8(&self.bytes[start..self.pos])
                    .map_err(|_| "invalid number")?;
                if text.len() > 1 && text.starts_with('0') {
                    return Err("leading zero in number".into());
                }
                Ok(Value::Number(text.parse().map_err(|_| "integer overflow")?))
            }
            Some(b'[') => {
                self.take(b'[')?;
                let mut items = Vec::new();
                self.space();
                if self.bytes.get(self.pos) != Some(&b']') {
                    loop {
                        items.push(self.value(depth + 1)?);
                        self.space();
                        if self.bytes.get(self.pos) != Some(&b',') {
                            break;
                        }
                        self.pos += 1;
                    }
                }
                self.take(b']')?;
                Ok(Value::Array(items))
            }
            Some(b'{') => {
                self.take(b'{')?;
                let mut fields = BTreeMap::new();
                self.space();
                if self.bytes.get(self.pos) != Some(&b'}') {
                    loop {
                        let name = self.string()?;
                        self.take(b':')?;
                        let value = self.value(depth + 1)?;
                        if fields.insert(name, value).is_some() {
                            return Err("duplicate JSON field".into());
                        }
                        self.space();
                        if self.bytes.get(self.pos) != Some(&b',') {
                            break;
                        }
                        self.pos += 1;
                    }
                }
                self.take(b'}')?;
                Ok(Value::Object(fields))
            }
            _ => Err(format!("invalid JSON value at {}", self.pos)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn strict_and_bounded() {
        assert!(parse(r#"{"a":[1,true,null,"ok"]}"#).is_ok());
        for bad in [
            r#"{"a":1,"a":2}"#,
            r#"{"a":01}"#,
            r#"{"a":1}x"#,
            r#"{"a":"\n"}"#,
            r#"[1,]"#,
        ] {
            assert!(parse(bad).is_err(), "{bad}");
        }
    }
}
