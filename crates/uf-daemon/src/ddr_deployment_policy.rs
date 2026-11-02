//! Explicit, immutable pre-trust for a new fixed-root deployment.
//!
//! The legacy witness policy is retained for diagnostic/experimental history;
//! normal v5 deployment does not need it. It does not itself authorize a
//! broker or dirty recovery, and ordinary requests cannot select a policy or
//! evidence path. Experimental live-role checks remain strict.

use crate::ddr_process_identity::{self, ProcessBirth};
use crate::ddr_recovery_coverage::{canonical, digest, ensure, fields, invalid, parse_line, s};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read};
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::path::Path;

const POLICY_SCHEMA: &str = "uflow-ddr-deployment-policy-v1";
const CHAIN_SCHEMA: &str = "uflow-ddr-trust-chain-v1";
const MAX_POLICY: u64 = 64 * 1024;
const MAX_CHAIN: usize = 128 * 1024;
const ZERO_DIGEST: &str = "0000000000000000000000000000000000000000000000000000000000000000";

#[derive(Clone, Debug)]
pub(crate) struct DeploymentPolicy {
    value: Value,
}

/// Dynamic values must come from the independently verified generation BIND;
/// caller-supplied values alone are never evidence or admission authority.
pub(crate) struct RoleContext<'a> {
    pub generation: &'a str,
    pub broker: &'a str,
    pub endpoint: &'a str,
    pub evidence_dir: &'a str,
}

/// Expected artifact and argv digests derived from the immutable policy. They
/// do not prove that a live process ran those bytes: that requires the live
/// pidfd/exec/argv checks in `verify_live_role` at the ready gate.
pub(crate) struct RoleEvidence {
    pub executable_sha256: String,
    pub entrypoint_sha256: String,
    pub argv_sha256: String,
}

fn resolve_template<'a>(value: &'a str, context: &'a RoleContext<'_>) -> io::Result<&'a str> {
    match value {
        "{uflow:generation}" => Ok(context.generation),
        "{uflow:broker}" => Ok(context.broker),
        "{uflow:endpoint}" => Ok(context.endpoint),
        "{uflow:evidence_dir}" => Ok(context.evidence_dir),
        _ if value.contains("{uflow:") => {
            Err(invalid("unknown or embedded deployment placeholder"))
        }
        _ => Ok(value),
    }
}

fn proc_bytes(pid: i32, name: &str) -> io::Result<Vec<u8>> {
    let mut file = File::open(format!("/proc/{pid}/{name}"))?;
    let mut bytes = Vec::new();
    (&mut file).take(64 * 1024 + 1).read_to_end(&mut bytes)?;
    ensure(
        bytes.len() <= 64 * 1024,
        "live process metadata exceeds limit",
    )?;
    Ok(bytes)
}

fn bounded_token(value: &str, maximum: usize) -> bool {
    !value.is_empty()
        && value.len() <= maximum
        && value.is_ascii()
        && !value.bytes().any(|b| b == 0 || b == b'\n' || b == b'\r')
}

fn valid_template(value: &str) -> bool {
    !value.contains("{uflow:")
        || matches!(
            value,
            "{uflow:generation}" | "{uflow:broker}" | "{uflow:endpoint}" | "{uflow:evidence_dir}"
        )
}

fn validate_artifact(
    value: &Value,
    checked: &mut BTreeSet<(String, String)>,
    check_live_artifacts: bool,
) -> io::Result<()> {
    fields(value, &["path", "sha256"])?;
    let path = s(value, "path")?;
    let hash = s(value, "sha256")?;
    ensure(
        hash.len() == 64
            && hash
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
        "invalid deployment artifact hash",
    )?;
    ensure(
        Path::new(path).is_absolute(),
        "deployment artifact path must be absolute",
    )?;
    if check_live_artifacts && checked.insert((path.to_owned(), hash.to_owned())) {
        crate::ddr_recovery_coverage::validate_code(value, "path", "sha256")?;
    }
    Ok(())
}

impl DeploymentPolicy {
    /// Used only by the explicit trusted deployment command, before any root
    /// is created. The policy and referenced artifacts must already exist.
    pub(crate) fn load(path: &Path) -> io::Result<Self> {
        let mut file = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(path)?;
        let before = file.metadata()?;
        ensure(
            before.is_file() && before.len() <= MAX_POLICY,
            "invalid deployment policy file",
        )?;
        let mut bytes = Vec::new();
        (&mut file).take(MAX_POLICY + 1).read_to_end(&mut bytes)?;
        let after = file.metadata()?;
        let entry = fs::symlink_metadata(path)?;
        ensure(
            bytes.len() as u64 == before.len()
                && before.len() == after.len()
                && (before.dev(), before.ino()) == (after.dev(), after.ino())
                && (entry.dev(), entry.ino()) == (after.dev(), after.ino()),
            "deployment policy changed during read",
        )?;
        Self::parse(&bytes)
    }

    fn parse(bytes: &[u8]) -> io::Result<Self> {
        ensure(
            bytes.len() <= MAX_POLICY as usize,
            "deployment policy exceeds limit",
        )?;
        let value = parse_line(bytes)?;
        Self::validate(value, true)
    }

    fn validate(value: Value, check_live_artifacts: bool) -> io::Result<Self> {
        fields(&value, &["schema", "roles"])?;
        ensure(
            s(&value, "schema")? == POLICY_SCHEMA,
            "unknown deployment policy schema",
        )?;
        let roles = value["roles"]
            .as_array()
            .ok_or_else(|| invalid("deployment roles must be an array"))?;
        ensure(
            roles.len() >= 6 && roles.len() <= 16,
            "invalid deployment role count",
        )?;
        let mut names = BTreeSet::new();
        let mut checked = BTreeSet::new();
        for role in roles {
            fields(
                role,
                &["role", "executable", "entrypoint", "argv", "environment"],
            )?;
            let name = s(role, "role")?;
            ensure(
                bounded_token(name, 64)
                    && name
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b"._-".contains(&b))
                    && names.insert(name),
                "invalid or duplicate deployment role",
            )?;
            validate_artifact(&role["executable"], &mut checked, check_live_artifacts)?;
            // Interpreter roles must pin the actual script/controlled entry,
            // not just /proc/PID/exe or the shared Python interpreter.
            validate_artifact(&role["entrypoint"], &mut checked, check_live_artifacts)?;
            let args = role["argv"]
                .as_array()
                .ok_or_else(|| invalid("deployment argv must be an array"))?;
            ensure(
                !args.is_empty() && args.len() <= 32,
                "invalid deployment argv",
            )?;
            for arg in args {
                ensure(
                    arg.as_str()
                        .is_some_and(|v| bounded_token(v, 1024) && valid_template(v)),
                    "invalid deployment argument",
                )?;
            }
            let exe = s(&role["executable"], "path")?;
            let entry = s(&role["entrypoint"], "path")?;
            ensure(
                args[0].as_str() == Some(exe)
                    && (entry == exe || args.get(1).and_then(Value::as_str) == Some(entry)),
                "deployment argv must select the pinned executable and entrypoint",
            )?;
            let env = role["environment"]
                .as_object()
                .ok_or_else(|| invalid("deployment environment must be an object"))?;
            ensure(env.len() <= 32, "deployment environment exceeds limit")?;
            for (key, val) in env {
                ensure(
                    !key.is_empty()
                        && key.len() <= 96
                        && key
                            .bytes()
                            .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'_')
                        && val.as_str().is_some_and(|v| {
                            v.len() <= 1024
                                && v.is_ascii()
                                && valid_template(v)
                                && !v.bytes().any(|b| b == 0 || b == b'\n' || b == b'\r')
                        }),
                    "invalid deployment environment",
                )?;
            }
        }
        for required in [
            "supervisor",
            "launcher",
            "interpreter",
            "broker",
            "owner",
            "accessor",
        ] {
            ensure(names.contains(required), "missing required deployment role")?;
        }
        Ok(Self { value })
    }

    pub(crate) fn initial_chain(&self) -> io::Result<Vec<u8>> {
        let mut frame = json!({"schema": CHAIN_SCHEMA, "sequence": 1,
            "previous": ZERO_DIGEST, "type": "DEPLOY", "policy": self.value});
        let checksum = digest(&canonical(&frame)?);
        frame
            .as_object_mut()
            .unwrap()
            .insert("checksum".into(), Value::String(checksum));
        let mut bytes = canonical(&frame)?;
        bytes.push(b'\n');
        ensure(
            bytes.len() <= MAX_CHAIN,
            "initial trust frame exceeds limit",
        )?;
        Ok(bytes)
    }

    /// Only the first immutable DEPLOY record is supported. An empty legacy
    /// slot, extra frame, short write or unknown version is never accepted as
    /// an initial policy, even when the underlying trust inode is unchanged.
    pub(crate) fn from_initial_chain(bytes: &[u8]) -> io::Result<Self> {
        Self::decode_initial_chain(bytes, true)
    }

    /// Normal work checks the immutable policy bytes and their anchor, but
    /// does not require historic witness executables to exist or be live.
    /// Experimental witness paths continue to call `from_initial_chain`.
    pub(crate) fn from_initial_chain_structural(bytes: &[u8]) -> io::Result<Self> {
        Self::decode_initial_chain(bytes, false)
    }

    fn decode_initial_chain(bytes: &[u8], check_live_artifacts: bool) -> io::Result<Self> {
        ensure(bytes.len() <= MAX_CHAIN, "trust chain exceeds limit")?;
        let mut frame = parse_line(bytes)?;
        fields(
            &frame,
            &[
                "schema", "sequence", "previous", "type", "policy", "checksum",
            ],
        )?;
        ensure(
            s(&frame, "schema")? == CHAIN_SCHEMA
                && frame["sequence"].as_u64() == Some(1)
                && s(&frame, "previous")? == ZERO_DIGEST
                && s(&frame, "type")? == "DEPLOY",
            "unsupported deployment trust history",
        )?;
        let checksum = s(&frame, "checksum")?.to_owned();
        frame.as_object_mut().unwrap().remove("checksum");
        ensure(
            checksum == digest(&canonical(&frame)?),
            "deployment trust checksum mismatch",
        )?;
        let value = frame
            .get("policy")
            .ok_or_else(|| invalid("missing deployment policy"))?
            .clone();
        Self::validate(value, check_live_artifacts)
    }

    pub(crate) fn policy_digest(&self) -> io::Result<String> {
        Ok(digest(&canonical(&self.value)?))
    }

    pub(crate) fn role_evidence(
        &self,
        role_name: &str,
        context: &RoleContext<'_>,
    ) -> io::Result<RoleEvidence> {
        let role = self.value["roles"]
            .as_array()
            .unwrap()
            .iter()
            .find(|role| role.get("role").and_then(Value::as_str) == Some(role_name))
            .ok_or_else(|| invalid("role not in deployment policy"))?;
        let args: Vec<&str> = role["argv"]
            .as_array()
            .unwrap()
            .iter()
            .map(|value| resolve_template(value.as_str().unwrap(), context))
            .collect::<io::Result<_>>()?;
        Ok(RoleEvidence {
            executable_sha256: s(&role["executable"], "sha256")?.to_owned(),
            entrypoint_sha256: s(&role["entrypoint"], "sha256")?.to_owned(),
            argv_sha256: digest(&canonical(&json!(args))?),
        })
    }

    /// Check a live role against the immutable policy, retaining a pidfd for
    /// the caller. This does not prove a BIND, parentage, roster completeness,
    /// wait status or an authority lock and cannot by itself authorize work.
    /// No process arguments or environment values are included in errors.
    pub(crate) fn verify_live_role(
        &self,
        role_name: &str,
        expected_birth: &ProcessBirth,
        context: &RoleContext<'_>,
    ) -> io::Result<File> {
        let role = self.value["roles"]
            .as_array()
            .unwrap()
            .iter()
            .find(|role| role.get("role").and_then(Value::as_str) == Some(role_name))
            .ok_or_else(|| invalid("role not in deployment policy"))?;
        let exe = s(&role["executable"], "path")?;
        let entry = s(&role["entrypoint"], "path")?;
        let expected_argv: Vec<&str> = role["argv"]
            .as_array()
            .unwrap()
            .iter()
            .map(|arg| resolve_template(arg.as_str().unwrap(), context))
            .collect::<io::Result<_>>()?;
        ensure(
            expected_argv.first() == Some(&exe)
                && (entry == exe || expected_argv.get(1) == Some(&entry)),
            "policy argv does not select the pinned entrypoint",
        )?;
        let mut checked = BTreeSet::new();
        validate_artifact(&role["executable"], &mut checked, true)?;
        validate_artifact(&role["entrypoint"], &mut checked, true)?;
        let pidfd = crate::ddr_witness_active::live_pidfd(expected_birth)?;
        let pinned_exe = fs::metadata(exe)?;
        let proc_exe = fs::metadata(format!("/proc/{}/exe", expected_birth.pid))?;
        ensure(
            fs::canonicalize(format!("/proc/{}/exe", expected_birth.pid))? == Path::new(exe),
            "live role executable differs from policy",
        )?;
        ensure(
            (pinned_exe.dev(), pinned_exe.ino()) == (proc_exe.dev(), proc_exe.ino()),
            "live role executable inode differs from policy",
        )?;
        let command = proc_bytes(expected_birth.pid, "cmdline")?;
        ensure(
            command.last() == Some(&0),
            "invalid live role argument vector",
        )?;
        let arguments: Vec<&[u8]> = command[..command.len() - 1].split(|b| *b == 0).collect();
        ensure(
            arguments.len() == expected_argv.len()
                && arguments
                    .iter()
                    .zip(&expected_argv)
                    .all(|(actual, expected)| *actual == expected.as_bytes()),
            "live role argument vector differs from policy",
        )?;
        let expected_env: BTreeMap<&str, &str> = role["environment"]
            .as_object()
            .unwrap()
            .iter()
            .map(|(key, value)| {
                Ok((
                    key.as_str(),
                    resolve_template(value.as_str().unwrap(), context)?,
                ))
            })
            .collect::<io::Result<_>>()?;
        let environment = proc_bytes(expected_birth.pid, "environ")?;
        ensure(
            environment.is_empty() || environment.last() == Some(&0),
            "invalid live role environment",
        )?;
        let mut actual_env = BTreeMap::<&[u8], &[u8]>::new();
        for item in environment
            .split(|byte| *byte == 0)
            .filter(|item| !item.is_empty())
        {
            let at = item
                .iter()
                .position(|byte| *byte == b'=')
                .ok_or_else(|| invalid("invalid live role environment"))?;
            ensure(
                at > 0 && actual_env.insert(&item[..at], &item[at + 1..]).is_none(),
                "duplicate live role environment",
            )?;
        }
        ensure(
            actual_env.len() == expected_env.len()
                && expected_env
                    .iter()
                    .all(|(key, value)| actual_env.get(key.as_bytes()) == Some(&value.as_bytes())),
            "live role environment differs from policy",
        )?;
        let mut checked_again = BTreeSet::new();
        validate_artifact(&role["executable"], &mut checked_again, true)?;
        validate_artifact(&role["entrypoint"], &mut checked_again, true)?;
        crate::ddr_witness_active::ensure_live(&pidfd)?;
        // exec() preserves PID/starttime. Verify the executable and process
        // launch data again, not just birth, before returning the held pidfd.
        ensure(
            ddr_process_identity::observe(
                expected_birth.pid,
                expected_birth.uid,
                expected_birth.gid,
            )? == *expected_birth
                && fs::canonicalize(format!("/proc/{}/exe", expected_birth.pid))? == Path::new(exe)
                && {
                    let actual = fs::metadata(format!("/proc/{}/exe", expected_birth.pid))?;
                    let current = fs::metadata(exe)?;
                    (actual.dev(), actual.ino()) == (pinned_exe.dev(), pinned_exe.ino())
                        && (current.dev(), current.ino()) == (pinned_exe.dev(), pinned_exe.ino())
                }
                && proc_bytes(expected_birth.pid, "cmdline")? == command
                && proc_bytes(expected_birth.pid, "environ")? == environment,
            "live role identity or launch changed",
        )?;
        crate::ddr_witness_active::ensure_live(&pidfd)?;
        Ok(pidfd)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{BufRead, BufReader, Write};
    use std::path::PathBuf;
    use std::process::{Command, Stdio};
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn live_role_pins_actual_interpreter_script_argv_environment_and_birth() {
        let target = std::env::var_os("CARGO_TARGET_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target"));
        let base = fs::canonicalize(target).unwrap();
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = base.join(format!("live-policy-{}-{nonce}", std::process::id()));
        fs::create_dir(&dir).unwrap();
        let python = fs::canonicalize("/usr/bin/python3").unwrap();
        let script = dir.join("entry.py");
        let different_script = dir.join("different.py");
        let source = b"import os\nos.write(1, b'READY\\n')\nif os.read(0, 1) == b'e':\n    os.execl('/usr/bin/sleep', '/usr/bin/sleep', '1')\n";
        fs::write(&script, source).unwrap();
        fs::write(&different_script, source).unwrap();
        let exe = json!({"path": python.to_str().unwrap(), "sha256": digest(&fs::read(&python).unwrap())});
        let entry = json!({"path": script.to_str().unwrap(), "sha256": digest(source)});
        let roles: Vec<_> = [
            "supervisor",
            "launcher",
            "interpreter",
            "broker",
            "owner",
            "accessor",
        ]
        .iter()
        .map(|role| {
            json!({"role": role, "executable": exe, "entrypoint": entry,
                "argv": [python.to_str().unwrap(), script.to_str().unwrap(), "{uflow:generation}"],
                "environment": {"UFLOW_TEST_ROLE": "{uflow:generation}"}})
        })
        .collect();
        let document = json!({"schema": POLICY_SCHEMA, "roles": roles});
        let policy = DeploymentPolicy::validate(document.clone(), true).unwrap();
        let mut skipped_entry = document.clone();
        skipped_entry["roles"][4]["argv"]
            .as_array_mut()
            .unwrap()
            .insert(1, "-c".into());
        assert!(DeploymentPolicy::validate(skipped_entry, true).is_err());
        let mut unknown_placeholder = document.clone();
        unknown_placeholder["roles"][4]["argv"][2] = "{uflow:untrusted}".into();
        assert!(DeploymentPolicy::validate(unknown_placeholder, true).is_err());
        let context = RoleContext {
            generation: "g1",
            broker: "b1",
            endpoint: "/workspace/uflow/runs/x/a.sock",
            evidence_dir: "/workspace/uflow/runs/x/evidence",
        };
        let mut child = Command::new(&python)
            .arg(&script)
            .arg("g1")
            .env_clear()
            .env("UFLOW_TEST_ROLE", "g1")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let mut output = BufReader::new(child.stdout.take().unwrap());
        let mut ready = String::new();
        output.read_line(&mut ready).unwrap();
        assert_eq!(ready, "READY\n");
        let pid = child.id() as i32;
        let actual = ddr_process_identity::observe(pid, unsafe { libc::geteuid() }, unsafe {
            libc::getegid()
        })
        .unwrap();
        let fd = policy.verify_live_role("owner", &actual, &context).unwrap();
        assert!(policy
            .verify_live_role("unknown-role", &actual, &context)
            .is_err());
        let mut wrong_birth = actual.clone();
        wrong_birth.starttime_ticks += 1;
        assert!(policy
            .verify_live_role("owner", &wrong_birth, &context)
            .is_err());
        let wrong_context = RoleContext {
            generation: "g2",
            ..context
        };
        assert!(policy
            .verify_live_role("owner", &actual, &wrong_context)
            .is_err());
        for fault in ["script", "argv", "environment", "interpreter"] {
            let mut changed = document.clone();
            let role = &mut changed["roles"][4];
            match fault {
                "script" => {
                    role["entrypoint"]["path"] = different_script.to_str().unwrap().into();
                    role["argv"][1] = different_script.to_str().unwrap().into();
                }
                "argv" => role["argv"].as_array_mut().unwrap().push("extra".into()),
                "environment" => role["environment"]["UFLOW_TEST_ROLE"] = "wrong".into(),
                "interpreter" => {
                    let other = fs::canonicalize("/usr/bin/true").unwrap();
                    role["executable"]["path"] = other.to_str().unwrap().into();
                    role["executable"]["sha256"] = digest(&fs::read(&other).unwrap()).into();
                    role["argv"][0] = other.to_str().unwrap().into();
                }
                _ => unreachable!(),
            }
            let altered = DeploymentPolicy::validate(changed, true).unwrap();
            assert!(
                altered
                    .verify_live_role("owner", &actual, &context)
                    .is_err(),
                "fault={fault}"
            );
        }
        child.stdin.take().unwrap().write_all(b"e").unwrap();
        let sleep_exe = fs::canonicalize("/usr/bin/sleep").unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(3);
        while fs::canonicalize(format!("/proc/{pid}/exe")).is_ok_and(|value| value != sleep_exe)
            && std::time::Instant::now() < deadline
        {
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        assert_eq!(
            fs::canonicalize(format!("/proc/{pid}/exe")).unwrap(),
            sleep_exe
        );
        assert_eq!(
            ddr_process_identity::observe(pid, actual.uid, actual.gid).unwrap(),
            actual
        );
        assert!(policy.verify_live_role("owner", &actual, &context).is_err());
        assert!(child.wait().unwrap().success());
        assert!(crate::ddr_witness_active::ensure_live(&fd).is_err());
        assert!(policy.verify_live_role("owner", &actual, &context).is_err());

        // Replacing a policy path with byte-identical code must not turn the
        // already-running executable's old inode into the new trusted one.
        let copy = dir.join("python-copy");
        let replacement = dir.join("python-replacement");
        fs::copy(&python, &copy).unwrap();
        let mut copied_document = document.clone();
        copied_document["roles"][4]["executable"]["path"] = copy.to_str().unwrap().into();
        copied_document["roles"][4]["argv"][0] = copy.to_str().unwrap().into();
        let copied_policy = DeploymentPolicy::validate(copied_document, true).unwrap();
        let mut copied_child = Command::new(&copy)
            .arg(&script)
            .arg("g1")
            .env_clear()
            .env("UFLOW_TEST_ROLE", "g1")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let mut output = BufReader::new(copied_child.stdout.take().unwrap());
        ready.clear();
        output.read_line(&mut ready).unwrap();
        assert_eq!(ready, "READY\n");
        let copied_birth = ddr_process_identity::observe(
            copied_child.id() as i32,
            unsafe { libc::geteuid() },
            unsafe { libc::getegid() },
        )
        .unwrap();
        assert!(copied_policy
            .verify_live_role("owner", &copied_birth, &context)
            .is_ok());
        fs::copy(&python, &replacement).unwrap();
        fs::rename(&replacement, &copy).unwrap();
        assert_eq!(
            ddr_process_identity::observe(copied_birth.pid, copied_birth.uid, copied_birth.gid)
                .unwrap(),
            copied_birth
        );
        assert!(copied_policy
            .verify_live_role("owner", &copied_birth, &context)
            .is_err());
        copied_child.stdin.take().unwrap().write_all(b"x").unwrap();
        assert!(copied_child.wait().unwrap().success());
    }
}
