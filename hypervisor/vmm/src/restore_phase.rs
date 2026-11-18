// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0

//! Restore sub-phases for the cross-node restore progress bar.
//!
//! A cross-node resume spends seconds to minutes inside one VmRestore call
//! (guest memory, then device state, then vCPU resume) and then waits for
//! the guest agent. Cubelet cannot see inside that call. When Cubelet wants
//! the detail it creates `PHASE_ROOT/<sandbox_id>/` before Create; the VMM
//! then records each step in `<dir>/phase` (atomic rename, one short line).
//! Without that directory nothing is written, so ordinary creates pay one
//! failed stat per step and leave nothing behind.

use std::fs;
use std::io::Write;
use std::path::PathBuf;

pub const PHASE_ROOT: &str = "/run/cube-restore-phase";

pub const VM_RESTORE_MEMORY: &str = "vm_restore_memory";
pub const VM_RESTORE_DEVICES: &str = "vm_restore_devices";
pub const VM_RESUME: &str = "vm_resume";
pub const AGENT_READY: &str = "agent_ready";

fn valid_sandbox_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 128
        && id
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || c == b'-' || c == b'_')
}

fn valid_phase(p: &str) -> bool {
    matches!(p, VM_RESTORE_MEMORY | VM_RESTORE_DEVICES | VM_RESUME | AGENT_READY)
}

/// Records `phase` for `sandbox_id` under `root` if the caller asked for it.
/// Returns true when the phase was written. Errors are swallowed: progress
/// reporting must never fail a restore.
pub fn mark_at(root: &str, sandbox_id: &str, phase: &str) -> bool {
    if !valid_sandbox_id(sandbox_id) || !valid_phase(phase) {
        return false;
    }
    let dir = PathBuf::from(root).join(sandbox_id);
    match fs::symlink_metadata(&dir) {
        Ok(m) if m.is_dir() => {}
        _ => return false,
    }
    let tmp = dir.join("phase.tmp");
    let dst = dir.join("phase");
    let res = (|| -> std::io::Result<()> {
        let mut f = fs::File::create(&tmp)?;
        f.write_all(phase.as_bytes())?;
        f.write_all(b"\n")?;
        drop(f);
        fs::rename(&tmp, &dst)
    })();
    if res.is_err() {
        let _ = fs::remove_file(&tmp);
        return false;
    }
    true
}

/// Records `phase` for `sandbox_id` under PHASE_ROOT.
pub fn mark(sandbox_id: &str, phase: &str) -> bool {
    mark_at(PHASE_ROOT, sandbox_id, phase)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp_root(tag: &str) -> PathBuf {
        let p = std::env::temp_dir().join(format!(
            "restore-phase-{}-{}",
            tag,
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&p);
        fs::create_dir_all(&p).unwrap();
        p
    }

    #[test]
    fn writes_only_when_requested() {
        let root = tmp_root("req");
        let r = root.to_str().unwrap();
        assert!(!mark_at(r, "sb-1", VM_RESTORE_MEMORY));
        assert!(!root.join("sb-1").exists());

        fs::create_dir(root.join("sb-1")).unwrap();
        assert!(mark_at(r, "sb-1", VM_RESTORE_MEMORY));
        assert!(mark_at(r, "sb-1", VM_RESUME));
        let got = fs::read_to_string(root.join("sb-1").join("phase")).unwrap();
        assert_eq!(got, "vm_resume\n");
        assert!(!root.join("sb-1").join("phase.tmp").exists());
        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn rejects_bad_ids_and_phases() {
        let root = tmp_root("bad");
        let r = root.to_str().unwrap();
        fs::create_dir(root.join("sb")).unwrap();
        assert!(!mark_at(r, "../sb", VM_RESUME));
        assert!(!mark_at(r, "", VM_RESUME));
        assert!(!mark_at(r, "sb", "done"));
        assert!(!root.join("sb").join("phase").exists());
        let _ = fs::remove_dir_all(&root);
    }
}
