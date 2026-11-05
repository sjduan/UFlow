//! Real owner executor adapter; calls occur outside the Catalog mutex.
use crate::pool_wire;
use serde_json::{json, Value};
use std::fs::{File, OpenOptions};
use std::mem;
use std::os::fd::{AsRawFd, FromRawFd};
use std::os::unix::fs::MetadataExt;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};
use uf_core::provider::{
    Addressability, Fidelity, ProviderDescriptor, ProviderIdentity, ProviderKind,
    ProviderResourceRef,
};

pub fn opaque_id() -> Result<String, String> {
    let id = std::fs::read_to_string("/proc/sys/kernel/random/uuid").map_err(|e| e.to_string())?;
    let id = id.trim();
    if id.len() != 36 || !id.bytes().all(|c| c.is_ascii_hexdigit() || c == b'-') {
        return Err("kernel identity unavailable".into());
    }
    Ok(id.to_owned())
}

pub struct Provider {
    pub child: Child,
    pub session: String,
    pub endpoint: PathBuf,
    pub identity: ProviderIdentity,
    pub loaded: Value,
}

impl Provider {
    pub fn start(root: &Path, python: &str) -> Result<Self, String> {
        let session = opaque_id()?;
        let endpoint = root.join("provider.sock");
        let log = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(root.join("provider.stderr"))
            .map_err(|e| e.to_string())?;
        let child = Command::new(python)
            .args(["-u", "-m", "uflow.pool_provider"])
            .arg(&endpoint)
            .arg(std::process::id().to_string())
            .arg(&session)
            .arg(root.join("provider-events.jsonl"))
            .env("TORCH_DEVICE_BACKEND_AUTOLOAD", "0")
            .env("PYTHONDONTWRITEBYTECODE", "1")
            .stdin(Stdio::null())
            .stdout(Stdio::from(log.try_clone().map_err(|e| e.to_string())?))
            .stderr(Stdio::from(log))
            .spawn()
            .map_err(|e| format!("start actual Simpler provider: {e}"))?;
        let identity = ProviderIdentity {
            provider_api_major: 1,
            provider_api_minor: 0,
            provider_id: "posix_shm_simpler".into(),
            provider_kind: ProviderKind::Local,
            adapter_revision: "managed-simpler508-owner-fd-v1".into(),
            external_platform_revision: "508becb93e182a3035a14e3f9226929dbe994346".into(),
            external_protocol_revision: "Simpler-Buffer-ABI-v1".into(),
            execution_realm_id: format!("physical:managed:{session}"),
            node_id: std::fs::read_to_string("/etc/hostname")
                .unwrap_or_default()
                .trim()
                .into(),
            session_id: session.clone(),
            boot_id: std::fs::read_to_string("/proc/sys/kernel/random/boot_id")
                .unwrap_or_default()
                .trim()
                .into(),
            session_generation: 1,
            fidelity: Fidelity::Physical,
            limitations: vec![
                "cooperative same-UID host mapping; no per-range OS protection".into(),
            ],
        };
        let mut provider = Self {
            child,
            session,
            endpoint,
            identity,
            loaded: Value::Null,
        };
        let deadline = Instant::now() + Duration::from_secs(10);
        while !provider.endpoint.exists() {
            if provider
                .child
                .try_wait()
                .map_err(|e| e.to_string())?
                .is_some()
                || Instant::now() >= deadline
            {
                return Err(format!(
                    "provider did not become ready; inspect {}",
                    root.display()
                ));
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        let (loaded, fds) = provider.call(json!({"op":"identify"}))?;
        if !fds.is_empty()
            || loaded["pid"] != provider.child.id()
            || loaded["provider_session"] != provider.session
        {
            return Err("provider loaded identity mismatch".into());
        }
        provider.loaded = loaded;
        Ok(provider)
    }

    pub fn call(&self, mut request: Value) -> Result<(Value, Vec<File>), String> {
        request["schema"] = json!(1);
        request["provider_session"] = json!(self.session);
        let mut stream = UnixStream::connect(&self.endpoint)
            .map_err(|e| format!("provider connection unknown: {e}"))?;
        stream
            .set_read_timeout(Some(Duration::from_secs(10)))
            .map_err(|e| e.to_string())?;
        stream
            .set_write_timeout(Some(Duration::from_secs(10)))
            .map_err(|e| e.to_string())?;
        let mut peer: libc::ucred = unsafe { mem::zeroed() };
        let mut len = mem::size_of::<libc::ucred>() as libc::socklen_t;
        if unsafe {
            libc::getsockopt(
                stream.as_raw_fd(),
                libc::SOL_SOCKET,
                libc::SO_PEERCRED,
                (&mut peer as *mut libc::ucred).cast(),
                &mut len,
            )
        } != 0
            || peer.pid != self.child.id() as i32
            || peer.uid != unsafe { libc::geteuid() }
        {
            return Err("provider peer identity changed".into());
        }
        pool_wire::send(&mut stream, &request, None)?;
        let (response, fds) = pool_wire::receive(&mut stream)?;
        pool_wire::fields(
            &response,
            &["schema", "status"],
            &["result", "detail", "error_type"],
        )?;
        if response["schema"] != 1 || response["status"] != "ok" {
            return Err(format!("provider outcome not confirmed: {response}"));
        }
        Ok((response["result"].clone(), fds))
    }

    pub fn allocate(
        &self,
        pool: &str,
        object_id: u64,
        nbytes: u64,
    ) -> Result<(ProviderResourceRef, ProviderDescriptor, Value, File), String> {
        let (canonical, mut fds) = self.call(
            json!({"op":"create","resource_id":pool,"buffer_id":object_id,"nbytes":nbytes}),
        )?;
        if fds.len() != 1 {
            return Err("provider create must return exactly one real FD".into());
        }
        let file = fds.remove(0);
        let info = file.metadata().map_err(|e| e.to_string())?;
        let flags = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_GETFL) };
        if !info.is_file()
            || info.len() != nbytes
            || canonical["nbytes"] != nbytes
            || canonical["dev"] != info.dev()
            || canonical["inode"] != info.ino()
            || canonical["kind"] != "posix_shm_simpler"
            || canonical["generation"] != 1
            || canonical["buffer_id"] != object_id
            || flags < 0
            || flags & libc::O_ACCMODE != libc::O_RDWR
        {
            return Err("actual provider FD/canonical descriptor mismatch".into());
        }
        let opaque = serde_json::to_vec(&canonical).map_err(|e| e.to_string())?;
        let resource = ProviderResourceRef {
            provider_resource_id: pool.into(),
            resource_generation: 1,
            size_bytes: nbytes,
            allowed_offset_bytes: 0,
            allowed_bytes: nbytes,
            addressability: Addressability::LocallyAddressable,
            opaque_token: opaque.clone(),
        };
        let descriptor = ProviderDescriptor {
            schema_major: 1,
            schema_minor: 0,
            provider_id: self.identity.provider_id.clone(),
            provider_session_id: self.session.clone(),
            resource_generation: 1,
            allowed_offset_bytes: 0,
            allowed_bytes: nbytes,
            access_mode: "rw".into(),
            opaque_bytes: opaque,
        };
        Ok((resource, descriptor, canonical, file))
    }

    pub fn release(&self, pool: &str, dev: u64, inode: u64) -> Result<Value, String> {
        let (result, fds) = self.call(json!({"op":"close","resource_id":pool}))?;
        if !fds.is_empty()
            || result["closed"] != true
            || result["unlinked"] != true
            || result["name_absent"] != true
            || result["dev"] != dev
            || result["inode"] != inode
        {
            return Err("provider real release not confirmed".into());
        }
        Ok(result)
    }

    pub fn shutdown(&mut self) -> Result<(), String> {
        let (result, fds) = self.call(json!({"op":"shutdown"}))?;
        if !fds.is_empty() || result["drained"] != true {
            return Err("provider shutdown not drained".into());
        }
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if let Some(status) = self.child.try_wait().map_err(|e| e.to_string())? {
                return if status.success() {
                    Ok(())
                } else {
                    Err("provider exit failure".into())
                };
            }
            if Instant::now() > deadline {
                return Err("provider exit unknown; owner process retained".into());
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    }
}

pub fn duplicate_access(file: &File, access: &str) -> Result<File, String> {
    if access == "rw" {
        file.try_clone().map_err(|e| e.to_string())
    } else {
        File::open(format!("/proc/self/fd/{}", file.as_raw_fd())).map_err(|e| e.to_string())
    }
}

pub fn pidfd(pid: i32) -> Result<File, String> {
    let fd = unsafe { libc::syscall(libc::SYS_pidfd_open, pid, 0) } as i32;
    if fd < 0 {
        Err(std::io::Error::last_os_error().to_string())
    } else {
        Ok(unsafe { File::from_raw_fd(fd) })
    }
}
pub fn exited(file: &File) -> bool {
    let mut poll = libc::pollfd {
        fd: file.as_raw_fd(),
        events: libc::POLLIN,
        revents: 0,
    };
    unsafe { libc::poll(&mut poll, 1, 0) > 0 }
}
