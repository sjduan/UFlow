//! Opt-in DDR call diagnostics, not capability evidence or a retry contract.
//! Collect on the handling thread; ONLY the outer request scope writes, after
//! all catalog/discovery guards have gone away. No logger I/O in inner spans.
use std::cell::RefCell;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

#[derive(Default)]
struct Buffer {
    rows: Vec<String>,
    omitted: u64,
    scans: u64,
    bytes: u64,
    scan_ns: u128,
    head: u64,
}
thread_local! { static BUFFER: RefCell<Option<Buffer>> = const { RefCell::new(None) }; }
static NEXT: AtomicU64 = AtomicU64::new(1);

/// Must be declared before any request lock, never inside a critical section.
/// Nested scopes do not flush the enclosing request's buffer.
pub struct Scope {
    id: u64,
    label: String,
    started: Instant,
    owner: bool,
}
impl Scope {
    pub fn new(label: String) -> Self {
        let enabled =
            std::env::var_os("UF_DDR_DIAGNOSTICS").as_deref() == Some(std::ffi::OsStr::new("1"));
        let owner = BUFFER.with(|b| {
            let mut b = b.borrow_mut();
            if enabled && b.is_none() {
                *b = Some(Buffer::default());
                true
            } else {
                false
            }
        });
        Self {
            id: NEXT.fetch_add(1, Ordering::Relaxed),
            label,
            started: Instant::now(),
            owner,
        }
    }
}
impl Drop for Scope {
    fn drop(&mut self) {
        if !self.owner {
            return;
        }
        let buffer = BUFFER.with(|b| b.borrow_mut().take());
        if let Some(b) = buffer {
            // Debug escaping prevents newlines/control characters in identities
            // from manufacturing log rows. Labels never contain bearer tokens.
            use std::io::Write;
            let _ = writeln!(std::io::stderr().lock(), "DDR_DIAG pid={} id={} request={:?} elapsed_ns={} scans={} scan_bytes={} scan_ns={} head={} omitted={} rows={:?}",
                std::process::id(), self.id, self.label, self.started.elapsed().as_nanos(),
                b.scans, b.bytes, b.scan_ns, b.head, b.omitted, b.rows);
        }
    }
}

pub fn note(value: String) {
    BUFFER.with(|b| {
        if let Some(b) = b.borrow_mut().as_mut() {
            if b.rows.len() < 256 {
                b.rows.push(value);
            } else {
                b.omitted += 1;
            }
        }
    });
}

pub struct Span {
    name: &'static str,
    started: Instant,
}
pub fn span(name: &'static str) -> Span {
    Span {
        name,
        started: Instant::now(),
    }
}
impl Drop for Span {
    fn drop(&mut self) {
        note(format!(
            "{} elapsed_ns={}",
            self.name,
            self.started.elapsed().as_nanos()
        ));
    }
}

pub fn scan(bytes: usize, head: u64, started: Instant) {
    BUFFER.with(|b| {
        if let Some(b) = b.borrow_mut().as_mut() {
            b.scans += 1;
            b.bytes += bytes as u64;
            b.scan_ns += started.elapsed().as_nanos();
            b.head = head;
        }
    });
}

/// Capture the original error at its syscall, before String erases its type.
/// This context is diagnostic ONLY; it never confers a retry permission.
pub fn io_error(stage: &'static str, error: std::io::Error) -> String {
    note(format!(
        "io stage={stage} kind={:?} raw_os_error={:?}",
        error.kind(),
        error.raw_os_error()
    ));
    format!(
        "{stage}: kind={:?} raw_os_error={:?}: {error}",
        error.kind(),
        error.raw_os_error()
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn collection_is_bounded_thread_local_and_preserves_scan_totals() {
        BUFFER.with(|b| *b.borrow_mut() = Some(Buffer::default()));
        for _ in 0..300 {
            note("stage".into());
        }
        scan(4096, 12489, Instant::now());
        std::thread::spawn(|| {
            note("other request".into());
            scan(99, 1, Instant::now());
        })
        .join()
        .unwrap();
        let b = BUFFER.with(|b| b.borrow_mut().take().unwrap());
        assert_eq!(
            (b.rows.len(), b.omitted, b.scans, b.bytes, b.head),
            (256, 44, 1, 4096, 12489)
        );
        assert!(
            io_error("probe.memfd-create", std::io::Error::from_raw_os_error(11))
                .contains("raw_os_error=Some(11)")
        );
        assert!(BUFFER.with(|b| b.borrow().is_none()));
    }
}
