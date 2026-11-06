// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0

//! Host-only, non-expiring retained teardown. This is NOT a storage drain receipt.
//! No paths, cleanup exemptions or authority are taken from guest annotations.
//! A separate intent entry survives a missing/partial record. Never auto-clear it.

use serde::{Deserialize, Serialize};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

type Result<T> = std::result::Result<T, String>;
pub const ROOT: &str = "/var/lib/cube-retained-runtime";
pub const ACTION: &str = "StopRetainedRuntime";
pub const BINDING_ANNOTATION: &str = "cube.recovery.source_fence.binding";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Binding {
    pub operation_id: String,
    pub sandbox_id: String,
    pub node_id: String,
    pub incarnation: String,
    pub checkpoint_id: String,
    pub checkpoint_digest: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Process {
    pub pid: i32,
    pub start_time: String,
    pub boot_id: String,
    pub executable: String,
}

// Wire-compatible with Cubelet sourcefence.Record. Stage is diagnostic only.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Record {
    pub version: u32,
    pub binding: Binding,
    pub stage: String,
    pub processes: Vec<Process>,
}

fn component(id: &str) -> Result<()> {
    if id.is_empty() || id.len() > 64 || !id.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_') {
        return Err("invalid retained source ID".into());
    }
    Ok(())
}

impl Record {
    fn validate(&self, id: &str) -> Result<()> {
        component(id)?;
        if self.version != 1 || self.binding.sandbox_id != id
            || !matches!(self.stage.as_str(), "admitted" | "runtime-stopped-storage-unproven")
            || self.processes.is_empty() || self.processes.len() > 2 {
            return Err("retention binding/schema mismatch".into());
        }
        let b = &self.binding;
        for value in [&b.operation_id, &b.node_id, &b.incarnation, &b.checkpoint_id] {
            if value.is_empty() || value.len() > 256 || value.trim() != value
                || value.contains(['\0', '\r', '\n']) {
                return Err("invalid retention binding".into());
            }
        }
        if b.checkpoint_digest.len() != 64 || !b.checkpoint_digest.bytes().all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)) {
            return Err("invalid checkpoint digest".into());
        }
        for p in &self.processes {
            if p.pid <= 1 || p.start_time.parse::<u64>().is_err() || p.boot_id.is_empty()
                || !Path::new(&p.executable).is_absolute() {
                return Err("invalid retained process identity".into());
            }
        }
        if self.processes.len() == 2 && self.processes[0].pid == self.processes[1].pid {
            return Err("duplicate retained process".into());
        }
        Ok(())
    }
}

pub fn identify(pid: i32) -> Result<Process> {
    if pid <= 1 { return Err("unsafe pid".into()); }
    let stat = fs::read_to_string(format!("/proc/{pid}/stat")).map_err(|e| e.to_string())?;
    let end = stat.rfind(')').ok_or("invalid proc stat")?;
    let start_time = stat[end + 1..].split_whitespace().nth(19).ok_or("short proc stat")?.to_string();
    Ok(Process {
        pid, start_time,
        boot_id: fs::read_to_string("/proc/sys/kernel/random/boot_id").map_err(|e| e.to_string())?.trim().to_string(),
        executable: fs::read_link(format!("/proc/{pid}/exe")).map_err(|e| e.to_string())?.to_str().ok_or("non-UTF8 executable")?.to_string(),
    })
}

/// Pin first, then compare the full process incarnation. Never signal by PID.
struct Pinned(OwnedFd);
impl Pinned {
    fn open(p: &Process, allow_gone: bool) -> Result<Option<Self>> {
        let boot = fs::read_to_string("/proc/sys/kernel/random/boot_id").map_err(|e| e.to_string())?;
        if boot.trim() != p.boot_id { return Err("retained process boot mismatch".into()); }
        let fd = unsafe { libc::syscall(libc::SYS_pidfd_open, p.pid, 0) } as i32;
        if fd < 0 {
            let e = std::io::Error::last_os_error();
            if allow_gone && e.raw_os_error() == Some(libc::ESRCH) { return Ok(None); }
            return Err(format!("pidfd_open: {e}"));
        }
        let pin = Self(unsafe { OwnedFd::from_raw_fd(fd) });
        // A zombie no longer has an exe link; its start time must still match.
        let current = identify(p.pid);
        if current.as_ref() != Ok(p) {
            if allow_gone && pin.exited()? {
                let stat = fs::read_to_string(format!("/proc/{}/stat", p.pid)).map_err(|e| e.to_string())?;
                let end = stat.rfind(')').ok_or("invalid proc stat")?;
                if stat[end + 1..].split_whitespace().nth(19) == Some(p.start_time.as_str()) { return Ok(Some(pin)); }
            }
            return Err("retained process incarnation mismatch".into());
        }
        Ok(Some(pin))
    }
    fn exited(&self) -> Result<bool> {
        let mut p = libc::pollfd { fd: self.0.as_raw_fd(), events: libc::POLLIN, revents: 0 };
        let n = unsafe { libc::poll(&mut p, 1, 0) };
        if n < 0 { return Err(std::io::Error::last_os_error().to_string()); }
        Ok(p.revents & libc::POLLIN != 0)
    }
    fn kill(&self) -> Result<()> {
        if self.exited()? { return Ok(()); }
        let n = unsafe { libc::syscall(libc::SYS_pidfd_send_signal, self.0.as_raw_fd(), libc::SIGKILL, std::ptr::null::<libc::siginfo_t>(), 0) };
        if n < 0 && std::io::Error::last_os_error().raw_os_error() != Some(libc::ESRCH) {
            return Err(std::io::Error::last_os_error().to_string());
        }
        Ok(())
    }
}

/// Guard serializes arm, retained stop, and destructive reap across processes.
pub struct Guard {
    root: PathBuf,
    owner: u32,
    _lock: File,
}

impl Guard {
    pub fn host() -> Result<Self> {
        // Check all fixed ancestors; neither root nor lookup is configurable by RPC.
        for p in ["/", "/var", "/var/lib"] { trusted(Path::new(p), 0, true)?; }
        Self::open(Path::new(ROOT), 0)
    }
    fn open(root: &Path, owner: u32) -> Result<Self> {
        let lock_path = root.with_extension("lock");
        let lock = OpenOptions::new().read(true).write(true).create(true).mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC).open(lock_path).map_err(|e| e.to_string())?;
        let m = lock.metadata().map_err(|e| e.to_string())?;
        if !m.is_file() || m.uid() != owner || m.mode() & 0o022 != 0 { return Err("untrusted retention lock".into()); }
        if unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 { return Err(format!("retention operation busy: {}", std::io::Error::last_os_error())); }
        Ok(Self { root: root.into(), owner, _lock: lock })
    }
    fn record_path(&self, id: &str) -> PathBuf { self.root.join(format!("{id}.json")) }
    fn intent_path(&self, id: &str) -> PathBuf { self.root.with_extension("required").join(id) }
    pub fn load(&self, id: &str) -> Result<Option<Record>> {
        component(id)?;
        if self._lock.metadata().map_err(|e| e.to_string())?.len() != 0 {
            trusted(&self.root, self.owner, true)?;
            trusted(&self.root.with_extension("required"), self.owner, true)?;
        }
        let mut exists = false;
        for root in [&self.root, &self.root.with_extension("required")] {
            match fs::symlink_metadata(root) {
                Ok(_) => trusted(root, self.owner, true)?,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                Err(e) => return Err(e.to_string()),
            }
        }
        for p in [self.record_path(id), self.intent_path(id)] {
            match fs::symlink_metadata(p) {
                Ok(_) => exists = true,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {},
                Err(e) => return Err(e.to_string()),
            }
        }
        if !exists { return Ok(None); }
        trusted(&self.intent_path(id), self.owner, false)?;
        let p = self.record_path(id);
        trusted(&p, self.owner, false)?;
        let f = OpenOptions::new().read(true).custom_flags(libc::O_NOFOLLOW).open(&p).map_err(|e| e.to_string())?;
        if f.metadata().map_err(|e| e.to_string())?.len() > 65536 { return Err("retention record too large".into()); }
        let r: Record = serde_json::from_reader(f).map_err(|e| format!("invalid retention record: {e}"))?;
        r.validate(id)?;
        if fs::read(self.intent_path(id)).map_err(|e| e.to_string())? != r.binding.operation_id.as_bytes() {
            return Err("retention intent operation mismatch".into());
        }
        Ok(Some(r))
    }
    pub fn require_absent(&self, id: &str) -> Result<()> {
        if self.load(id)?.is_some() { return Err("retained source: destructive operation denied".into()); }
        Ok(())
    }
    /// Caller is a privileged node-side controller, NOT the shim Update handler.
    /// Persist intent first, record second, fsync both and parent before any stop.
    pub fn arm(&self, r: &Record) -> Result<()> {
        r.validate(&r.binding.sandbox_id)?;
        if let Some(old) = self.load(&r.binding.sandbox_id)? {
            if old != *r { return Err("immutable retention binding mismatch".into()); }
            return self.sync(r);
        }
        let mut pins = Vec::new();
        for p in &r.processes { pins.push(Pinned::open(p, false)?); }
        for root in [&self.root, &self.root.with_extension("required")] {
            fs::create_dir(root).or_else(|e| if e.kind() == std::io::ErrorKind::AlreadyExists { Ok(()) } else { Err(e) }).map_err(|e| e.to_string())?;
            trusted(root, self.owner, true)?;
            File::open(root.parent().ok_or("missing parent")?).and_then(|f| f.sync_all()).map_err(|e| e.to_string())?;
        }
        // Anchor survives loss of either/both directories. Initialization is
        // not retention authority; it only makes missing state fail closed.
        let mut lock = &self._lock;
        lock.write_all(b"retained-runtime-v1\n").and_then(|_| lock.sync_all()).map_err(|e| e.to_string())?;
        write_once(&self.intent_path(&r.binding.sandbox_id), r.binding.operation_id.as_bytes())?;
        File::open(self.root.with_extension("required")).and_then(|f| f.sync_all()).map_err(|e| e.to_string())?;
        write_once(&self.record_path(&r.binding.sandbox_id), &serde_json::to_vec(r).map_err(|e| e.to_string())?)?;
        self.sync(r)
    }
    fn sync(&self, r: &Record) -> Result<()> {
        for p in [self.intent_path(&r.binding.sandbox_id), self.record_path(&r.binding.sandbox_id), self.root.clone(), self.root.with_extension("required")] {
            File::open(p).and_then(|f| f.sync_all()).map_err(|e| e.to_string())?;
        }
        Ok(())
    }
    pub fn authorize_current(&self, id: &str, binding: &Binding) -> Result<Record> {
        let r = self.load(id)?.ok_or("missing durable retention authority")?;
        if &r.binding != binding || !r.processes.contains(&identify(std::process::id() as i32)?) {
            return Err("retained shutdown binding/process mismatch".into());
        }
        self.sync(&r)?;
        Ok(r)
    }
    /// Called by the fresh containerd delete process. Pins ALL processes before
    /// signalling any, waits observed exit, retains files even on error/lost ack.
    /// ESRCH on retry proves the previously recorded incarnation no longer runs;
    /// a reused PID or reboot is refused, never treated as permission to signal.
    pub fn reap_retained(&self, id: &str, timeout: Duration) -> Result<bool> {
        let Some(r) = self.load(id)? else { return Ok(false); };
        self.sync(&r)?;
        let mut pins = Vec::new();
        for p in &r.processes {
            if p.pid == std::process::id() as i32 { return Err("refusing to reap self".into()); }
            if let Some(pin) = Pinned::open(p, true)? { pins.push(pin); }
        }
        for pin in &pins { pin.kill()?; }
        let deadline = Instant::now() + timeout;
        for pin in &pins {
            while !pin.exited()? {
                if Instant::now() >= deadline { return Err("retained runtime exit timeout; files retained".into()); }
                std::thread::sleep(Duration::from_millis(10));
            }
        }
        Ok(true)
    }
}

fn trusted(p: &Path, owner: u32, directory: bool) -> Result<()> {
    let m = fs::symlink_metadata(p).map_err(|e| format!("retention state unavailable {}: {e}", p.display()))?;
    if m.uid() != owner || m.mode() & 0o022 != 0 || (directory && !m.is_dir()) || (!directory && (!m.is_file() || m.nlink() != 1)) {
        return Err(format!("untrusted retention state {}", p.display()));
    }
    Ok(())
}

fn write_once(path: &Path, b: &[u8]) -> Result<()> {
    let mut f = OpenOptions::new().write(true).create_new(true).mode(0o600)
        .custom_flags(libc::O_NOFOLLOW).open(path).map_err(|e| e.to_string())?;
    f.write_all(b).and_then(|_| f.sync_all()).map_err(|e| e.to_string())
}

/// Narrow administrative entry, no CLI path override. JSON on stdin only.
pub fn arm_from_stdin() -> Result<()> {
    if unsafe { libc::geteuid() } != 0 { return Err("retention arm requires node root".into()); }
    let mut b = Vec::new();
    std::io::stdin().take(65537).read_to_end(&mut b).map_err(|e| e.to_string())?;
    if b.len() > 65536 { return Err("record too large".into()); }
    let r: Record = serde_json::from_slice(&b).map_err(|e| e.to_string())?;
    Guard::host()?.arm(&r)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::{symlink, PermissionsExt};
    use std::process::{Child, Command};

    struct Fixture { dir: PathBuf, child: Child }
    impl Fixture {
        fn new() -> Self {
            let dir = std::env::temp_dir().join(format!("retained-test-{}", uuid::Uuid::new_v4()));
            fs::create_dir(&dir).unwrap();
            let child = Command::new("sleep").arg("120").spawn().unwrap();
            Self { dir, child }
        }
        fn guard(&self) -> Guard { Guard::open(&self.dir.join("records"), unsafe { libc::geteuid() }).unwrap() }
        fn record(&self) -> Record {
            Record { version: 1, stage: "admitted".into(), binding: Binding {
                operation_id: "operation-1".into(), sandbox_id: "source-1".into(), node_id: "node-1".into(),
                incarnation: "incarnation-1".into(), checkpoint_id: "checkpoint-1".into(), checkpoint_digest: "a".repeat(64),
            }, processes: vec![identify(self.child.id() as i32).unwrap()] }
        }
    }
    impl Drop for Fixture {
        fn drop(&mut self) { let _ = self.child.kill(); let _ = self.child.wait(); let _ = fs::remove_dir_all(&self.dir); }
    }

    #[test]
    fn retained_ordinary_missing_entry_allows_cleanup() {
        let f = Fixture::new(); let g = f.guard();
        assert!(g.load("source-1").unwrap().is_none());
        assert!(!g.reap_retained("source-1", Duration::from_secs(1)).unwrap());
        g.require_absent("source-1").unwrap();
    }

    #[test]
    fn retained_arm_retry_immutable_binding_and_no_signal() {
        let mut f = Fixture::new(); let g = f.guard(); let r = f.record();
        g.arm(&r).unwrap(); g.arm(&r).unwrap();
        assert!(f.child.try_wait().unwrap().is_none());
        assert!(g.require_absent("source-1").is_err());
        g.require_absent("other-source").unwrap();
        for change in 0..6 {
            let mut wrong = r.clone();
            match change {
                0 => wrong.binding.operation_id.push('x'), 1 => wrong.binding.node_id.push('x'),
                2 => wrong.binding.incarnation.push('x'), 3 => wrong.binding.checkpoint_id.push('x'),
                4 => wrong.binding.checkpoint_digest = "b".repeat(64), _ => wrong.processes[0].start_time.push('0'),
            }
            assert!(g.arm(&wrong).is_err());
        }
    }

    #[test]
    fn retained_corrupt_missing_record_and_intent_fail_closed() {
        for fault in 0..5 {
            let f = Fixture::new(); let g = f.guard(); g.arm(&f.record()).unwrap();
            match fault {
                0 => fs::write(g.record_path("source-1"), b"{").unwrap(),
                1 => fs::remove_file(g.record_path("source-1")).unwrap(),
                2 => fs::remove_file(g.intent_path("source-1")).unwrap(),
                3 => fs::write(g.intent_path("source-1"), b"wrong-token").unwrap(),
                _ => fs::set_permissions(g.record_path("source-1"), fs::Permissions::from_mode(0o666)).unwrap(),
            }
            assert!(g.load("source-1").is_err());
            assert!(g.require_absent("source-1").is_err());
            assert!(g.reap_retained("source-1", Duration::from_secs(1)).is_err());
        }
    }

    #[test]
    fn retained_wrong_source_symlink_and_schema_rejected() {
        let f = Fixture::new(); let g = f.guard(); let r = f.record(); g.arm(&r).unwrap();
        let mut bad = r.clone(); bad.binding.sandbox_id = "elsewhere".into();
        fs::write(g.record_path("source-1"), serde_json::to_vec(&bad).unwrap()).unwrap();
        assert!(g.load("source-1").is_err());
        fs::remove_file(g.record_path("source-1")).unwrap();
        symlink(g.intent_path("source-1"), g.record_path("source-1")).unwrap();
        assert!(g.load("source-1").is_err());
        assert!(g.load("../source-1").is_err());
        let mut value = serde_json::to_value(&r).unwrap(); value["verified"] = true.into();
        assert!(serde_json::from_value::<Record>(value).is_err());
    }

    #[test]
    fn retained_pid_mismatch_pins_all_before_signalling() {
        let mut f = Fixture::new(); let other = Fixture::new(); let g = f.guard();
        let mut r = f.record(); r.processes.push(other.record().processes.remove(0));
        g.arm(&r).unwrap();
        r.processes[1].start_time.push('0');
        fs::write(g.record_path("source-1"), serde_json::to_vec(&r).unwrap()).unwrap();
        assert!(g.reap_retained("source-1", Duration::from_secs(1)).is_err());
        assert!(f.child.try_wait().unwrap().is_none());
    }

    #[test]
    fn retained_wrong_boot_executable_and_authorization_rejected() {
        let f = Fixture::new(); let g = f.guard(); let r = f.record();
        for fault in 0..3 {
            let mut bad = r.clone();
            match fault { 0 => bad.processes[0].boot_id.push('x'), 1 => bad.processes[0].executable.push('x'), _ => bad.processes[0].start_time.push('0') }
            assert!(g.arm(&bad).is_err());
        }
        assert!(g.load("source-1").unwrap().is_none());
        g.arm(&r).unwrap();
        assert!(g.authorize_current("source-1", &r.binding).is_err());
        let mut b = r.binding.clone(); b.operation_id.push('x');
        assert!(g.authorize_current("source-1", &b).is_err());
    }

    #[test]
    fn retained_fresh_process_reap_and_lost_ack_retry() {
        let mut f = Fixture::new(); let g = f.guard(); g.arm(&f.record()).unwrap();
        fs::write(f.dir.join("disk"), b"selected-checkpoint-data").unwrap();
        drop(g);
        for _ in 0..2 {
            let status = Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "common::retained::tests::retained_reap_child", "--nocapture"])
                .env("RETAINED_TEST_ROOT", f.dir.join("records")).status().unwrap();
            assert!(status.success());
        }
        assert!(f.child.wait().unwrap().code().is_none());
        assert_eq!(fs::read(f.dir.join("disk")).unwrap(), b"selected-checkpoint-data");
        assert!(f.guard().require_absent("source-1").is_err());
    }

    #[test]
    fn retained_reap_child() {
        if let Ok(root) = std::env::var("RETAINED_TEST_ROOT") {
            let g = Guard::open(Path::new(&root), unsafe { libc::geteuid() }).unwrap();
            assert!(g.reap_retained("source-1", Duration::from_secs(2)).unwrap());
            println!("fresh-process retained reap observed exit; authority retained");
        }
    }

    #[test]
    fn retained_cross_process_busy_never_waits_or_deletes() {
        let f = Fixture::new(); let g = f.guard();
        assert!(Guard::open(&f.dir.join("records"), unsafe { libc::geteuid() }).is_err());
        drop(g);
        let _g = f.guard();
    }

    #[test]
    fn retained_initialized_store_loss_is_not_ordinary_cleanup() {
        let f = Fixture::new(); let g = f.guard(); g.arm(&f.record()).unwrap();
        fs::remove_dir_all(&g.root).unwrap();
        fs::remove_dir_all(g.root.with_extension("required")).unwrap();
        drop(g);
        assert!(f.guard().require_absent("source-1").is_err());
    }

    #[test]
    fn retained_cubelet_wire_fixture() {
        let r: Record = serde_json::from_str(include_str!("retained-wire.json")).unwrap();
        r.validate("source-1").unwrap();
        assert_eq!(serde_json::to_string(&r).unwrap(), include_str!("retained-wire.json").trim());
    }
}
