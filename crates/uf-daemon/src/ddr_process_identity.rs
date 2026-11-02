//! Live process-birth identity used by the external DDR broker.
//!
//! PID/starttime/namespace checks prevent reuse of an existing live session by
//! a different process. They do not prove that an already-dead socket peer was
//! the process now occupying the same PID before pidfd_open; recovery still
//! requires a pre-bound trusted witness as described by the T3 contract.
use std::fs;
use std::io;
use std::os::unix::fs::MetadataExt;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ProcessBirth {
    pub pid: i32,
    pub uid: u32,
    pub gid: u32,
    pub pid_namespace_dev: u64,
    pub pid_namespace_ino: u64,
    pub starttime_ticks: u64,
}

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

fn parse_starttime(stat: &str, expected_pid: i32) -> io::Result<u64> {
    // comm is parenthesized but can itself contain spaces and ')' characters.
    // The last ')' is followed by the one-character task state and fields 4+.
    let open = stat
        .find('(')
        .ok_or_else(|| invalid("missing proc stat comm"))?;
    let close = stat
        .rfind(')')
        .ok_or_else(|| invalid("missing proc stat comm end"))?;
    if close <= open
        || !stat[..open]
            .trim()
            .parse::<i32>()
            .is_ok_and(|p| p == expected_pid)
    {
        return Err(invalid("proc stat PID mismatch"));
    }
    let suffix = stat
        .get(close + 1..)
        .ok_or_else(|| invalid("invalid proc stat suffix"))?;
    let fields: Vec<&str> = suffix.split_whitespace().collect();
    if fields.len() <= 19 || fields[0].len() != 1 {
        return Err(invalid("short or malformed proc stat"));
    }
    // The first suffix token is state (field 3), so starttime (field 22)
    // occurs at zero-based index 19.
    fields[19]
        .parse()
        .map_err(|_| invalid("invalid proc starttime"))
}

fn effective_id(status: &str, name: &str) -> io::Result<u32> {
    let mut matches = status.lines().filter_map(|line| line.strip_prefix(name));
    let line = matches
        .next()
        .ok_or_else(|| invalid("missing proc credentials"))?;
    if matches.next().is_some() {
        return Err(invalid("duplicate proc credentials"));
    }
    let ids: Vec<&str> = line.split_whitespace().collect();
    if ids.len() != 4 {
        return Err(invalid("invalid proc credentials"));
    }
    ids[1]
        .parse()
        .map_err(|_| invalid("invalid proc effective credential"))
}

/// Capture one observation, requiring the peer's effective UID/GID to still
/// match SO_PEERCRED. The caller must retain a pidfd and compare two samples
/// while checking it remains live; this function alone is not a death proof.
pub(crate) fn observe(pid: i32, uid: u32, gid: u32) -> io::Result<ProcessBirth> {
    if pid <= 0 {
        return Err(invalid("invalid peer PID"));
    }
    let base = format!("/proc/{pid}");
    let stat = fs::read_to_string(format!("{base}/stat"))?;
    let status = fs::read_to_string(format!("{base}/status"))?;
    let namespace = fs::metadata(format!("{base}/ns/pid"))?;
    if effective_id(&status, "Uid:")? != uid || effective_id(&status, "Gid:")? != gid {
        return Err(invalid("peer credentials changed"));
    }
    Ok(ProcessBirth {
        pid,
        uid,
        gid,
        pid_namespace_dev: namespace.dev(),
        pid_namespace_ino: namespace.ino(),
        starttime_ticks: parse_starttime(&stat, pid)?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_starttime_after_complex_parenthesized_comm() {
        let stat = "123 (hello ) tricky ) name) S 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 987654 23";
        assert_eq!(parse_starttime(stat, 123).unwrap(), 987654);
        assert!(parse_starttime(stat, 124).is_err());
        assert!(parse_starttime("123 (broken) S 1 2", 123).is_err());
        assert!(parse_starttime("123 broken S 1 2", 123).is_err());
    }

    #[test]
    fn effective_credentials_are_complete_and_unique() {
        let status = "Name:\ttest\nUid:\t1000\t1001\t1002\t1003\nGid:\t2000\t2001\t2002\t2003\n";
        assert_eq!(effective_id(status, "Uid:").unwrap(), 1001);
        assert_eq!(effective_id(status, "Gid:").unwrap(), 2001);
        assert!(effective_id("Uid: 1 2\n", "Uid:").is_err());
        assert!(effective_id("Uid: 1 2 3 4\nUid: 1 2 3 4\n", "Uid:").is_err());
    }

    #[test]
    fn actual_self_observation_is_stable_and_rejects_wrong_uid() {
        let pid = std::process::id() as i32;
        let uid = unsafe { libc::geteuid() };
        let gid = unsafe { libc::getegid() };
        let first = observe(pid, uid, gid).unwrap();
        let second = observe(pid, uid, gid).unwrap();
        assert_eq!(first, second);
        assert_ne!(first.pid_namespace_ino, 0);
        assert!(observe(pid, uid.wrapping_add(1), gid).is_err());
    }
}
