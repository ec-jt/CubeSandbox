#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Tencent. All rights reserved.
#
# cube-s3lvol-stop.sh -- ExecStop for cube-sandbox-s3lvol.service.
#
# Three distinct paths:
#
#   1. Planned stop (target still running, job is a plain stop): hand over to
#      rcow_stop.sh which does the full teardown in reverse start order --
#      nvme disconnect x32, rcow_unload_lvstore (drain to S3, close journal;
#      WAL is the safety net), SIGTERM the target. bstore.json / active
#      registry are left for the next start to attach/replay.
#
#   1b. Planned restart (`systemctl restart`, target running): the same
#      unload and SIGTERM, but the initiator stays connected
#      (rcow_stop.sh --keep-connected), so mounts on the namespaces survive.
#
#   2. Crash recovery (target already dead): NEVER disconnect the NVMf
#      initiator. The controllers are sitting in nvme_tcp reconnect, and a
#      disconnect here would break the no-I/O-interruption guarantee that
#      the crash-restart design depends on. Only clean target-side residue
#      (pidfile / RPC socket / cpu lock) so the next ExecStart can rebuild
#      the same NQN/NSID grid and the kernel reconnects on its own.
#
# TimeoutStopSec=180s on the unit covers rcow_stop.sh's RCOW_STOP_TIMEOUT
# budget; the crash path here is seconds.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=./common.sh
source "${SCRIPT_DIR}/common.sh"

require_root

S3LVOL_ROOT="${TOOLBOX_ROOT}/CubeS3lvol"
RCOW_COMMON="${S3LVOL_ROOT}/scripts/rcow_common.sh"
RCOW_STOP="${S3LVOL_ROOT}/scripts/rcow_stop.sh"

# Nothing installed (yet) -- nothing to stop.
if [[ ! -f "${RCOW_COMMON}" ]]; then
  exit 0
fi

# shellcheck source=/dev/null
source "${RCOW_COMMON}" # provides rcow_target_alive + RCOW_* path defaults

# stop_job_type prints the type of the job systemd is running on this unit
# ("restart", "stop", ...), or nothing when it cannot tell. Inside ExecStop
# the unit's own job is still running, so list-jobs names it.
S3LVOL_UNIT_NAME="${S3LVOL_UNIT_NAME:-cube-sandbox-s3lvol.service}"
stop_job_type() {
  command -v systemctl >/dev/null 2>&1 || return 0
  timeout 5 systemctl list-jobs --no-legend --plain 2>/dev/null </dev/null |
    awk -v u="${S3LVOL_UNIT_NAME}" '$2 == u { print $3; exit }'
}

if rcow_target_alive; then
  # A restart brings the target straight back, so the initiator stays
  # connected: nvme_tcp reconnects (ctrl_loss_tmo 600 s) to the same NQN/NSID
  # grid that rcow_start.sh rebuilds from the active registry, and every
  # mount Cubelet holds keeps its device. Disconnecting here instead (the
  # behaviour before 2026-11-18) removed the namespaces under Cubelet's
  # ext4 mounts: 9 mounts per node read EIO and every Create failed until
  # they were unmounted by hand. A plain stop (shutdown, disable, package
  # removal) still disconnects: nothing comes back to reconnect to.
  # S3LVOL_STOP_KEEP_CONNECTED=1 forces the keep-connected path, =0 forces
  # the full teardown.
  job="$(stop_job_type || true)"
  keep="${S3LVOL_STOP_KEEP_CONNECTED:-}"
  if [[ -z "${keep}" ]]; then
    case "${job}" in
      restart | try-restart | reload-or-restart) keep=1 ;;
      *) keep=0 ;;
    esac
  fi
  if [[ "${keep}" == "1" ]]; then
    log "CubeS3lvol: target alive, job '${job:-unknown}': unload via rcow_stop.sh --keep-connected"
    "${RCOW_STOP}" --keep-connected
  else
    log "CubeS3lvol: target alive, job '${job:-unknown}': full teardown via rcow_stop.sh"
    "${RCOW_STOP}"
  fi
else
  log "CubeS3lvol: target not running; cleaning target-side state, initiator untouched"
  rm -f \
    "${RCOW_PIDFILE}" \
    "${RCOW_RPC_SOCK}" \
    "${RCOW_RPC_SOCK}.lock" \
    /var/tmp/spdk_cpu_lock_* 2>/dev/null || true
fi
exit 0
