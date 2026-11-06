// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0

/// A failed snapshot restore is not permission to cold boot. In particular,
/// VMM errors are currently strings and can include ownership, disk integrity
/// and transport failures. Never infer a memory-only failure from their text.
/// These stages describe where failure was observed, not its root cause.
#[derive(Clone, Copy, Debug)]
pub enum RestoreFailureStage {
    MemoryReference,
    Metadata,
    Compatibility,
    Runtime,
}

pub fn refuse_disk_only_fallback(stage: RestoreFailureStage, cause: &str) -> String {
    format!(
        "snapshot recovery failed: recovery_mode=failed process_state=unverified \
         disk_only_fallback=refused stage={stage:?}; {cause}"
    )
}

// Ordinary template-cache boot retains its existing best-effort fallback.
// Only application snapshot recovery requires fail-closed diagnostics.
pub fn restore_failure(required: bool, stage: RestoreFailureStage, cause: &str) -> String {
    if required {
        refuse_disk_only_fallback(stage, cause)
    } else {
        cause.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normal_template_cache_failure_is_not_a_recovery_refusal() {
        assert_eq!(
            restore_failure(false, RestoreFailureStage::Runtime, "original"),
            "original"
        );
        assert!(
            restore_failure(true, RestoreFailureStage::Runtime, "original")
                .contains("disk_only_fallback=refused")
        );
    }

    #[test]
    fn all_failure_stages_refuse_fallback_without_claiming_process_survival() {
        // Synthetic faults. The text is diagnostic, never a decision input.
        for stage in [
            RestoreFailureStage::MemoryReference,
            RestoreFailureStage::Metadata,
            RestoreFailureStage::Compatibility,
            RestoreFailureStage::Runtime,
        ] {
            for cause in [
                "memory missing",
                "memory corrupt",
                "incompatible snapshot",
                "ownership denied",
                "disk integrity failure",
                "metadata missing",
                "timeout after VM may have resumed",
            ] {
                let result = refuse_disk_only_fallback(stage, cause);
                assert!(result.contains("recovery_mode=failed"));
                assert!(result.contains("disk_only_fallback=refused"));
                assert!(result.contains("process_state=unverified"));
                assert!(result.ends_with(cause));
                assert!(!result.contains("memory_restored"));
                assert!(!result.contains("recovery_mode=disk_only"));
            }
        }
    }
}
