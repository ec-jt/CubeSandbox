// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0

//! Host-only receipt protocol. No guest paths, caller result annotations, error
//! text classification, or disk-boot authorization are part of this contract.
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::Write;
use std::os::fd::{AsRawFd, FromRawFd};
use std::os::unix::fs::MetadataExt;

pub const REQUEST_ANNOTATION: &str = "cube.shim.recovery.attempt.v1";
pub const RECEIPT_ROOT: &str = "/run/cube-recovery";

#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Stage {
    None,
    Unknown,
    MemoryReference,
    Metadata,
    Compatibility,
    Runtime,
}
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Cause {
    None,
    Unknown,
    MissingMemoryReference,
    ConfigurationMismatch,
    RestoreDisabled,
}
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Mechanism {
    MemoryRestored,
    Failed,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RestoreOutcome {
    pub version: u32,
    pub sandbox_id: String,
    pub attempt_id: String,
    pub mechanism: Mechanism,
    pub failure_stage: Stage,
    pub cause: Cause,
    pub process_state: String,
    pub disk_only_fallback: String,
}

impl RestoreOutcome {
    pub fn failed(id: &str, attempt: &str, stage: Stage, cause: Cause) -> Self {
        Self {
            version: 1,
            sandbox_id: id.into(),
            attempt_id: attempt.into(),
            mechanism: Mechanism::Failed,
            failure_stage: stage,
            cause,
            process_state: "unverified".into(),
            disk_only_fallback: "refused".into(),
        }
    }
    pub fn restored(id: &str, attempt: &str) -> Self {
        Self {
            mechanism: Mechanism::MemoryRestored,
            failure_stage: Stage::None,
            cause: Cause::None,
            disk_only_fallback: "not_used".into(),
            ..Self::failed(id, attempt, Stage::Unknown, Cause::Unknown)
        }
    }
    pub fn publish(&self) -> Result<(), String> {
        let root = open_dir(libc::AT_FDCWD, RECEIPT_ROOT, 0)?;
        self.publish_at(&root, 0)
    }
    fn publish_at(&self, root: &File, uid: u32) -> Result<(), String> {
        if !valid_attempt(&self.attempt_id) {
            return Err("invalid recovery attempt".into());
        }
        let dir = open_dir(root.as_raw_fd(), &self.attempt_id, uid)?;
        let raw = serde_json::to_vec(self).map_err(|e| e.to_string())?;
        if raw.len() > 4096 {
            return Err("recovery receipt too large".into());
        }
        let name = b"outcome.tmp\0";
        let fd = unsafe {
            libc::openat(
                dir.as_raw_fd(),
                name.as_ptr().cast(),
                libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL | libc::O_NOFOLLOW | libc::O_CLOEXEC,
                0o600,
            )
        };
        if fd < 0 {
            return Err(std::io::Error::last_os_error().to_string());
        }
        let mut file = unsafe { File::from_raw_fd(fd) };
        file.write_all(&raw)
            .and_then(|_| file.sync_all())
            .map_err(|e| e.to_string())?;
        // Single writer, private fresh attempt directory. Atomic visibility.
        let ret = unsafe {
            libc::renameat(
                dir.as_raw_fd(),
                name.as_ptr().cast(),
                dir.as_raw_fd(),
                b"outcome.json\0".as_ptr().cast(),
            )
        };
        if ret != 0 {
            return Err(std::io::Error::last_os_error().to_string());
        }
        dir.sync_all().map_err(|e| e.to_string())
    }
}

pub fn valid_attempt(s: &str) -> bool {
    s.len() == 64
        && s.bytes()
            .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
}
fn open_dir(parent: i32, name: &str, uid: u32) -> Result<File, String> {
    let name = std::ffi::CString::new(name).map_err(|e| e.to_string())?;
    let fd = unsafe {
        libc::openat(
            parent,
            name.as_ptr(),
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
        )
    };
    if fd < 0 {
        return Err(std::io::Error::last_os_error().to_string());
    }
    let file = unsafe { File::from_raw_fd(fd) };
    let m = file.metadata().map_err(|e| e.to_string())?;
    if m.uid() != uid || m.mode() & 0o777 != 0o700 {
        return Err("untrusted recovery directory".into());
    }
    Ok(file)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn publish_cross_language_receipt() {
        if let Ok(root) = std::env::var("RECOVERY_TEST_ROOT") {
            let attempt = std::env::var("RECOVERY_TEST_ATTEMPT").unwrap();
            let uid = unsafe { libc::geteuid() };
            let dir = open_dir(libc::AT_FDCWD, &root, uid).unwrap();
            RestoreOutcome::restored("synthetic-sandbox", &attempt)
                .publish_at(&dir, uid)
                .unwrap();
        }
    }
    #[test]
    fn serialize_contract_fixtures() {
        let attempt = "a".repeat(64);
        let mut fixtures = vec![RestoreOutcome::restored("synthetic-sandbox", &attempt)];
        for (stage, cause) in [
            (Stage::Unknown, Cause::Unknown),
            (Stage::MemoryReference, Cause::MissingMemoryReference),
            (Stage::Metadata, Cause::Unknown),
            (Stage::Compatibility, Cause::ConfigurationMismatch),
            (Stage::Compatibility, Cause::RestoreDisabled),
            (Stage::Runtime, Cause::Unknown),
        ] {
            fixtures.push(RestoreOutcome::failed(
                "synthetic-sandbox",
                &attempt,
                stage,
                cause,
            ));
        }
        let raw = serde_json::to_vec(&fixtures).unwrap();
        let back: Vec<RestoreOutcome> = serde_json::from_slice(&raw).unwrap();
        assert_eq!(back.len(), 7);
        if let Ok(path) = std::env::var("RECOVERY_FIXTURES") {
            std::fs::write(path, &raw).unwrap();
        }
        for o in fixtures.iter().skip(1) {
            assert_eq!(o.disk_only_fallback, "refused");
        }
    }
    #[test]
    fn host_receipt_writer_is_private_and_attempt_bound() {
        use std::os::unix::fs::PermissionsExt;
        let path = std::env::temp_dir().join(format!("cube-receipt-test-{}", std::process::id()));
        std::fs::create_dir(&path).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700)).unwrap();
        let attempt = "a".repeat(64);
        std::fs::create_dir(path.join(&attempt)).unwrap();
        std::fs::set_permissions(path.join(&attempt), std::fs::Permissions::from_mode(0o700))
            .unwrap();
        let root = File::open(&path).unwrap();
        let o = RestoreOutcome::restored("synthetic-sandbox", &attempt);
        o.publish_at(&root, unsafe { libc::geteuid() }).unwrap();
        assert!(path.join(&attempt).join("outcome.json").exists());
        assert!(!valid_attempt("../forged"));
        std::fs::set_permissions(path.join(&attempt), std::fs::Permissions::from_mode(0o777))
            .unwrap();
        assert!(o.publish_at(&root, unsafe { libc::geteuid() }).is_err());
        std::fs::remove_dir_all(path).unwrap();
    }
}
