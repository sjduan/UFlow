//! Bounded schema1 JSON and real SCM_RIGHTS; no payload or virtual address.
use serde::de::{self, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer};
use serde_json::{Map, Value};
use std::fmt;
use std::fs::File;
use std::io::{Read, Write};
use std::mem;
use std::os::fd::{AsRawFd, FromRawFd};
use std::os::unix::net::UnixStream;

pub const MAX_FRAME: usize = 65536;

struct Unique(Value);
struct UniqueVisitor;
impl<'de> Visitor<'de> for UniqueVisitor {
    type Value = Unique;
    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("strict control JSON")
    }
    fn visit_bool<E: de::Error>(self, v: bool) -> Result<Unique, E> {
        Ok(Unique(Value::Bool(v)))
    }
    fn visit_u64<E: de::Error>(self, v: u64) -> Result<Unique, E> {
        Ok(Unique(v.into()))
    }
    fn visit_i64<E: de::Error>(self, v: i64) -> Result<Unique, E> {
        if v < 0 {
            Err(E::custom("negative control integer"))
        } else {
            Ok(Unique(v.into()))
        }
    }
    fn visit_str<E: de::Error>(self, v: &str) -> Result<Unique, E> {
        Ok(Unique(v.into()))
    }
    fn visit_string<E: de::Error>(self, v: String) -> Result<Unique, E> {
        Ok(Unique(v.into()))
    }
    fn visit_unit<E: de::Error>(self) -> Result<Unique, E> {
        Ok(Unique(Value::Null))
    }
    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Unique, A::Error> {
        let mut values = Vec::new();
        while let Some(Unique(v)) = seq.next_element()? {
            if values.len() >= 1024 {
                return Err(de::Error::custom("array limit"));
            }
            values.push(v);
        }
        Ok(Unique(Value::Array(values)))
    }
    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Unique, A::Error> {
        let mut values = Map::new();
        while let Some(key) = map.next_key::<String>()? {
            if values.len() >= 128 || values.contains_key(&key) {
                return Err(de::Error::custom("duplicate key or object limit"));
            }
            let Unique(value) = map.next_value()?;
            values.insert(key, value);
        }
        Ok(Unique(Value::Object(values)))
    }
}
impl<'de> Deserialize<'de> for Unique {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_any(UniqueVisitor)
    }
}

pub fn parse(bytes: &[u8]) -> Result<Value, String> {
    if bytes.is_empty() || bytes.len() > MAX_FRAME {
        return Err("control frame limit".into());
    }
    let mut decoder = serde_json::Deserializer::from_slice(bytes);
    let Unique(value) =
        Unique::deserialize(&mut decoder).map_err(|e| format!("invalid control JSON: {e}"))?;
    decoder.end().map_err(|_| "trailing JSON".to_owned())?;
    Ok(value)
}

pub fn fields(value: &Value, required: &[&str], optional: &[&str]) -> Result<(), String> {
    let object = value.as_object().ok_or("object required")?;
    if object
        .keys()
        .any(|key| !required.contains(&key.as_str()) && !optional.contains(&key.as_str()))
        || required.iter().any(|key| !object.contains_key(*key))
    {
        return Err("unknown or missing control field".into());
    }
    Ok(())
}
pub fn number(value: &Value, key: &str) -> Result<u64, String> {
    value
        .get(key)
        .and_then(Value::as_u64)
        .ok_or_else(|| format!("nonnegative u64 required: {key}"))
}
pub fn text<'a>(value: &'a Value, key: &str) -> Result<&'a str, String> {
    let v = value
        .get(key)
        .and_then(Value::as_str)
        .ok_or_else(|| format!("string required: {key}"))?;
    if v.is_empty() || v.len() > 256 || v.chars().any(char::is_control) {
        return Err(format!("invalid string: {key}"));
    }
    Ok(v)
}

pub fn send(stream: &mut UnixStream, value: &Value, fd: Option<&File>) -> Result<(), String> {
    let mut bytes = serde_json::to_vec(value).map_err(|e| e.to_string())?;
    bytes.push(b'\n');
    if bytes.len() > MAX_FRAME {
        return Err("response frame limit".into());
    }
    if let Some(file) = fd {
        let mut iov = libc::iovec {
            iov_base: bytes.as_mut_ptr().cast(),
            iov_len: bytes.len(),
        };
        let mut control = vec![
            0usize;
            (unsafe { libc::CMSG_SPACE(mem::size_of::<i32>() as _) } as usize)
                .div_ceil(mem::size_of::<usize>())
        ];
        let mut msg: libc::msghdr = unsafe { mem::zeroed() };
        msg.msg_iov = &mut iov;
        msg.msg_iovlen = 1;
        msg.msg_control = control.as_mut_ptr().cast();
        msg.msg_controllen = control.len() * mem::size_of::<usize>();
        unsafe {
            let header = libc::CMSG_FIRSTHDR(&msg);
            (*header).cmsg_level = libc::SOL_SOCKET;
            (*header).cmsg_type = libc::SCM_RIGHTS;
            (*header).cmsg_len = libc::CMSG_LEN(mem::size_of::<i32>() as _) as usize;
            *(libc::CMSG_DATA(header).cast::<i32>()) = file.as_raw_fd();
            let n = libc::sendmsg(stream.as_raw_fd(), &msg, libc::MSG_NOSIGNAL);
            if n < 0 {
                return Err(std::io::Error::last_os_error().to_string());
            }
            stream
                .write_all(&bytes[n as usize..])
                .map_err(|e| e.to_string())?;
        }
    } else {
        stream.write_all(&bytes).map_err(|e| e.to_string())?;
    }
    Ok(())
}

pub fn receive(stream: &mut UnixStream) -> Result<(Value, Vec<File>), String> {
    let mut bytes = vec![0u8; MAX_FRAME + 1];
    let mut control = [0usize; 16];
    let mut iov = libc::iovec {
        iov_base: bytes.as_mut_ptr().cast(),
        iov_len: bytes.len(),
    };
    let mut msg: libc::msghdr = unsafe { mem::zeroed() };
    msg.msg_iov = &mut iov;
    msg.msg_iovlen = 1;
    msg.msg_control = control.as_mut_ptr().cast();
    msg.msg_controllen = mem::size_of_val(&control);
    let n = unsafe { libc::recvmsg(stream.as_raw_fd(), &mut msg, libc::MSG_CMSG_CLOEXEC) };
    if n <= 0 {
        return Err("missing control frame".into());
    }
    let mut files = Vec::new();
    unsafe {
        let mut header = libc::CMSG_FIRSTHDR(&msg);
        while !header.is_null() {
            if (*header).cmsg_level != libc::SOL_SOCKET || (*header).cmsg_type != libc::SCM_RIGHTS {
                return Err("unexpected ancillary message".into());
            }
            let len = (*header)
                .cmsg_len
                .saturating_sub(libc::CMSG_LEN(0) as usize);
            for i in 0..len / mem::size_of::<i32>() {
                files.push(File::from_raw_fd(
                    *(libc::CMSG_DATA(header).cast::<i32>().add(i)),
                ));
            }
            header = libc::CMSG_NXTHDR(&msg, header);
        }
    }
    if msg.msg_flags & (libc::MSG_CTRUNC | libc::MSG_TRUNC) != 0 || files.len() > 1 {
        return Err("truncated or extra FD".into());
    }
    bytes.truncate(n as usize);
    while !bytes.contains(&b'\n') && bytes.len() <= MAX_FRAME {
        let mut chunk = [0u8; 4096];
        let count = stream.read(&mut chunk).map_err(|e| e.to_string())?;
        if count == 0 {
            return Err("incomplete frame".into());
        }
        bytes.extend_from_slice(&chunk[..count]);
    }
    if bytes.len() > MAX_FRAME || bytes.last() != Some(&b'\n') {
        return Err("bounded single frame required".into());
    }
    Ok((parse(&bytes)?, files))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn duplicate_nested_float_bool_number_and_unknown_rejected() {
        assert!(parse(br#"{"schema":1,"schema":1}"#).is_err());
        assert!(parse(br#"{"nested":{"a":1,"a":2}}"#).is_err());
        assert!(parse(br#"{"n":1.0}"#).is_err());
        assert!(number(&parse(br#"{"n":true}"#).unwrap(), "n").is_err());
        assert!(parse(br#"{"n":-1}"#).is_err());
        assert!(parse(br#"{"n":18446744073709551616}"#).is_err());
        assert!(fields(
            &parse(br#"{"schema":1,"va":123}"#).unwrap(),
            &["schema"],
            &[]
        )
        .is_err());
    }
    #[test]
    fn real_unix_fd_handoff_is_same_physical_file() {
        let (mut a, mut b) = UnixStream::pair().unwrap();
        let file = File::open("/proc/self/stat").unwrap();
        send(&mut a, &serde_json::json!({"schema":1}), Some(&file)).unwrap();
        let (v, fds) = receive(&mut b).unwrap();
        assert_eq!(v["schema"], 1);
        assert_eq!(fds.len(), 1);
        use std::os::unix::fs::MetadataExt;
        assert_eq!(
            file.metadata().unwrap().ino(),
            fds[0].metadata().unwrap().ino()
        );
    }
}
