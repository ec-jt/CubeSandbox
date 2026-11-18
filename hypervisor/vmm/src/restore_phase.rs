// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0

//! Restore sub-phases for the cross-node restore progress bar.
//!
//! A cross-node resume spends seconds to minutes inside one VmRestore call
//! (guest memory, then device state, then vCPU resume) and then waits for
//! the guest agent. Cubelet cannot see inside that call. When Cubelet wants
//! the detail it creates `PHASE_ROOT/<sandbox_id>/` before Create; the VMM
//! then records each step in `<dir>/phase` (one short line). Without that
//! directory the open fails with ENOENT, so ordinary creates pay one failed
//! open per step and leave nothing behind.
//!
//! Only open, write and close are used: this runs on the VMM thread, whose
//! seccomp filter traps rename and stat (SIGSYS, seen live). The write is not
//! atomic; the reader in Cubelet ignores anything that is not a whole known
//! phase name, so a torn read only delays the bar by one poll.

use std::fs;
use std::io::Write;
use std::os::unix::fs::OpenOptionsExt;
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
    let dst = PathBuf::from(root).join(sandbox_id).join("phase");
    let line = format!("{}\n", phase);
    fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(&dst)
        .and_then(|mut f| f.write_all(line.as_bytes()))
        .is_ok()
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
