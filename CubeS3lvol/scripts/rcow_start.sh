#!/usr/bin/env bash
# Copyright (c) 2026 Tencent Inc.
# SPDX-License-Identifier: Apache-2.0
#
#  rcow_start.sh -- bring the rcow data plane up
#
#  Order of operations, and why it is this order:
#
#    1. target process               nothing else can be configured without it
#    2. TCP transport + subsystems   a subsystem's max_namespaces is fixed at
#                                    creation, so the grid is built up front --
#                                    but without listeners; see step 7
#    3. WAL bdev                     the lvstore attach needs it by name
#    4. S3 namespaces                the lvstore attach resolves its namespace
#                                    through them
#    5. lvstore: attach or create    see below
#    6. replay the active registry   volumes come back where they were
#    7. listeners, then nvme connect the host's first look at a subsystem finds
#                                    it already complete
#
#  Step 5 is the one with teeth. /data/cubelet/rcow/bstore.json records every
#  lvstore that
#  exists; an entry for ours means attach, no entry means create. Nothing else is
#  allowed to influence that choice -- in particular a failed attach never falls
#  back to create, because create formats the journal and the WAL, and after a
#  crash those hold the only copy of writes already acknowledged to the host. An
#  attach that fails is an incident to look at, not a condition to work around.
#
#  Step 6 is what makes a restart invisible to the layer above: the namespaces
#  come back on the same subsystem and nsid they were on, so the host's uuid ->
#  device lookup keeps working. See rcow_replay_registry() in rcow_common.sh for
#  why the registry has to be moved aside before the first activation call.
#
#  Step 7 is last on purpose. A subsystem becomes reachable the moment it has a
#  listener, and the host names its devices from what it finds at that moment --
#  the Y in /dev/nvmeXnY is allocated on discovery, not taken from the nsid. Open
#  the listeners before the replay and the host attaches to empty subsystems and
#  then picks the namespaces up an AEN at a time; open them after and its first
#  scan sees the finished layout. Doing it after is also what makes a recovery
#  follow the same sequence as a fresh start, which is what keeps a volume on the
#  device name it had. See rcow_add_listeners().
#
#  Usage:
#    rcow_start.sh [--no-create] [--no-connect] [--no-replay] [--force]
#                  [--no-auto-force] [--lvs-name NAME]
#
#      --no-create    fail instead of creating an lvstore when none is recorded.
#                     For a node that is only ever supposed to attach one.
#      --no-connect   leave the initiator side alone (target-only bring-up).
#      --no-replay    do not restore previously active volumes. The registry is
#                     left untouched, so a later rcow_recovery.sh still can.
#      --force        take the lvstore over even if S3 says somebody else holds
#                     it. Only when you know the holder is gone: if it is not,
#                     two processes end up writing the same objects.
#      --no-auto-force
#                     do not take it over even when the marker can be proved
#                     stale (this node's own dead pid). Every crash then needs an
#                     operator to pass --force.
#
#  Everything else is configured through the environment; see rcow_common.sh.

set -u

SELF_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=rcow_common.sh
. "${SELF_DIR}/rcow_common.sh"

DO_CREATE=1
DO_CONNECT=1
DO_REPLAY=1
FORCE=0
AUTO_FORCE=1

while [ "$#" -gt 0 ]; do
	case "$1" in
	--no-create)     DO_CREATE=0 ;;
	--no-connect)    DO_CONNECT=0 ;;
	--no-replay)     DO_REPLAY=0 ;;
	--force)         FORCE=1 ;;
	--no-auto-force) AUTO_FORCE=0 ;;
	--lvs-name)      shift; RCOW_LVS_NAME="${1:-}" ;;
	-h|--help)       sed -n '2,60p' "${BASH_SOURCE[0]}"; exit 0 ;;
	*)               rcow_die "unknown option: $1 (try --help)" ;;
	esac
	shift
done

if [ "${FORCE}" -eq 1 ]; then
	FORCE_JSON=true
else
	FORCE_JSON=false
fi

# Set once the lvstore is up. Before that point a failure leaves a process
# holding nothing, so it is cleaned up by a plain signal. After it, the process
# owns a replayed journal, an open WAL and the S3 owner marker, so a failed start
# is undone with an unload first -- see release_lvstore().
LVS_UP=0
TGT_PID=""

# A start that fails after the attach must not leave the target behind holding
# the owner marker. The supervisor exits on the failure, systemd then stops the
# unit, and its stop budget runs out on a target that is still attached -- the
# SIGKILL that follows leaves a strict marker that refuses every later attach
# (observed: B17, app2, 2026-11-13). rcow_unload_lvstore drains what was
# acknowledged into S3, closes the log and releases the marker to a free
# tombstone; only then is the target stopped. If the unload itself fails the
# WAL still holds every acknowledged write and the marker names a pid that is
# about to be dead, which the next start clears (rcow_owner_clear_stale).
release_lvstore()
{
	local out

	rcow_log "the lvstore was attached; unloading it so the owner marker is \
released before the target is stopped"
	if out="$(RCOW_RPC_TIMEOUT="${RCOW_FAIL_UNLOAD_TIMEOUT:-90}" \
			rcow_rpc rcow_unload_lvstore \
			"$(printf '{"lvs_name":"%s"}' "${RCOW_LVS_NAME}")" 2>&1)"; then
		rcow_log "unloaded: acknowledged writes are in S3 and the marker is free"
	else
		rcow_warn "rcow_unload_lvstore did not complete: ${out}"
		rcow_warn "the WAL keeps every acknowledged write; the next start \
clears the marker this pid leaves behind"
	fi
	rcow_kill_target "${TGT_PID}" 30
	rm -f "${RCOW_PIDFILE}" "${RCOW_RPC_SOCK}" "${RCOW_RPC_SOCK}.lock"
}

bail()
{
	rcow_err "$*"
	rcow_log_tail 30

	if [ -n "${TGT_PID}" ] && kill -0 "${TGT_PID}" 2>/dev/null; then
		if [ "${LVS_UP}" -eq 0 ]; then
			rcow_log "no lvstore was attached yet, so the target is being \
stopped again; nothing it held needs draining"
			rcow_kill_target "${TGT_PID}" 15
			rm -f "${RCOW_PIDFILE}"
		else
			release_lvstore
		fi
	fi
	exit 1
}

# retry_after_stale_owner <verb> <rpc> <params-without-force> <log_mark> <ns>
#
# After a refused attach/create: if the marker is provably this node's dead pid,
# take it over -- with force=true in legacy mode, and in strict mode (which
# rejects force by design) by clearing it to a free tombstone and retrying
# without force. Not run in a subshell, so its results reach the caller:
#   RETRY_OUT     the retried RPC's output (or why nothing was retried)
#   OWNER_REASON  the staleness verdict
# Returns 0 when the retry succeeded, 1 when the marker was not confirmed stale
# (nothing was touched), 2 when a takeover was attempted and failed.
retry_after_stale_owner()
{
	local verb="$1" rpc="$2" params="$3" mark="$4" ns="$5" out

	RETRY_OUT=""
	OWNER_REASON=""
	[ "${AUTO_FORCE}" -eq 1 ] && [ "${FORCE}" -eq 0 ] || return 1
	OWNER_REASON="$(rcow_owner_is_stale "${mark}")" || return 1

	if rcow_owner_mode_strict; then
		rcow_warn "the ${verb} was refused because a strict owner marker is \
still in S3: ${OWNER_REASON}. Strict ownership has no force; clearing the marker \
to a free tombstone (conditional on the holder named) and retrying"
		if ! out="$(rcow_owner_clear_stale "${mark}" "${RCOW_LVS_NAME}" "${ns}")"; then
			RETRY_OUT="the stale marker was not cleared: ${out}"
			return 2
		fi
		rcow_log "${out}"
		RETRY_OUT="$(rcow_rpc "${rpc}" "${params},\"force\":false}" 2>&1)" ||
			return 2
		return 0
	fi

	rcow_warn "the ${verb} was refused because an owner marker is still in S3: \
${OWNER_REASON}. Retrying with force=true"
	RETRY_OUT="$(rcow_rpc "${rpc}" "${params},\"force\":true}" 2>&1)" || return 2
	return 0
}

# ==========================================================================
rcow_step "preflight"

rcow_need_root
rcow_need_cmd python3 "the RPC clients are python"
rcow_need_cmd nvme "nvme-cli provides connect/disconnect (nvme-cli package)"
if [ "${DO_CONNECT}" -eq 1 ]; then
	rcow_need_nvme_tcp
fi
rcow_ensure_run_dir

[ -x "${RCOW_TGT_BIN}" ] || rcow_die "target binary not found: ${RCOW_TGT_BIN}"
rcow_check_tgt_deps
[ -x "${RCOW_SPDK_RPC_PY}" ] || rcow_die "SPDK rpc.py not found: ${RCOW_SPDK_RPC_PY} \
(set SPDK_ROOT)"
[ -r "${RCOW_S3_CFG}" ] || rcow_die "S3 config not readable: ${RCOW_S3_CFG}"
[ -e "${RCOW_WAL_IMG}" ] || rcow_die "WAL image not found: ${RCOW_WAL_IMG}. It \
is deliberately not created here -- its size fixes the journal and WAL layout, \
and an empty file where the old one used to be looks to the attach path like an \
lvstore with nothing to replay"

# A stale binary is worse than a missing one: it runs, and everything it reports
# is about code that is not in it.
if [ -z "${S3LVOL_SKIP_FRESH_CHECK:-}" ] &&
   [ -x "${RCOW_REPO_ROOT}/test/tools/check_binary_fresh.sh" ]; then
	"${RCOW_REPO_ROOT}/test/tools/check_binary_fresh.sh" "${RCOW_TGT_BIN}" ||
		rcow_die "refusing to start a stale binary (S3LVOL_SKIP_FRESH_CHECK=1 \
overrides)"
fi

if pid="$(rcow_target_pid)"; then
	rcow_die "already running as pid ${pid}; use rcow_stop.sh first"
fi
OTHERS="$(rcow_target_instances)"
if [ -n "${OTHERS}" ]; then
	rcow_die "an s3lvol_tgt is running that ${RCOW_PIDFILE} does not account \
for (pid $(printf '%s ' ${OTHERS})). Stop it by hand -- a second target takes \
over the RPC socket and leaves the first one alive holding the WAL image, which \
is not a state to recover from automatically"
fi

# Only now that nothing is running can these be assumed dead rather than in use.
rm -f "${RCOW_RPC_SOCK}" "${RCOW_RPC_SOCK}.lock" "/var/tmp/spdk_cpu_lock_"* \
	"${RCOW_PIDFILE}"

rcow_load_credentials ||
	rcow_die "could not read access_key_id/secret_access_key from ${RCOW_S3_CFG}"

S3_ENDPOINT="$(rcow_cfg_get endpoint)"
S3_REGION="$(rcow_cfg_get region)"
S3_BUCKETS="$(rcow_s3_buckets)"

[ -n "${S3_ENDPOINT}" ] || rcow_die "no endpoint in ${RCOW_S3_CFG}"
[ -n "${S3_BUCKETS}" ]  || rcow_die "no buckets in ${RCOW_S3_CFG}"
[ -n "${S3_REGION}" ]   || S3_REGION="us-east-1"

rcow_log "endpoint ${S3_ENDPOINT}, region ${S3_REGION}"
rcow_log "buckets: $(printf '%s ' ${S3_BUCKETS})"
rcow_log "lvstore '${RCOW_LVS_NAME}', WAL image ${RCOW_WAL_IMG}"

# ==========================================================================
rcow_step "starting the target"

if [ -n "${RCOW_TGT_ARGS:-}" ]; then
	TGT_ARGS="${RCOW_TGT_ARGS}"
elif [ "${RCOW_NO_HUGE}" -eq 1 ]; then
	# The chosen configuration, not a fallback -- see rcow_common.sh for why
	# this data path does not need hugepages. Logged rather than warned about,
	# so that a real warning below stands out.
	TGT_ARGS="--no-huge -s ${RCOW_TGT_MEM_MB}"
	rcow_log "no hugepages by design: --no-huge -s ${RCOW_TGT_MEM_MB}"
else
	TGT_ARGS=""
	HP="$(cat /proc/sys/vm/nr_hugepages 2>/dev/null || echo 0)"
	if [ "${HP}" -eq 0 ]; then
		rcow_die "RCOW_NO_HUGE=0 asks for hugepages but nr_hugepages is 0. \
Reserve them (sysctl vm.nr_hugepages=N, or SPDK's setup.sh) or leave \
RCOW_NO_HUGE at 1 -- starting anyway would fail inside DPDK with a message far \
less clear than this one"
	fi
	rcow_log "using hugepages (nr_hugepages=${HP})"
fi

mkdir -p "$(dirname "${RCOW_LOG}")" 2>/dev/null ||
	rcow_die "cannot create log directory: $(dirname "${RCOW_LOG}")"

# A new run gets a fresh log: the previous one is moved aside, named by the
# moment of the move, so s3lvol_tgt.log always holds exactly this run and the
# history is greppable by filename instead of by hunting timestamps inside one
# growing file. Timestamped rather than dated, so two runs in one day are told
# apart. Done here, after preflight, on purpose: every check above can still
# fall back to rcow_log_tail() on the old log, and once we are about to start
# the target nothing needs it any more.
if [ -s "${RCOW_LOG}" ]; then
	OLD_LOG="${RCOW_LOG}.$(date +%Y%m%d-%H%M%S)"
	while [ -e "${OLD_LOG}" ]; do
		OLD_LOG="${OLD_LOG}~"
	done
	mv "${RCOW_LOG}" "${OLD_LOG}" ||
		rcow_die "could not move the previous log ${RCOW_LOG} to ${OLD_LOG}"
	rcow_log "previous run's log moved to ${OLD_LOG}"
fi

: >>"${RCOW_LOG}" || rcow_die "cannot write ${RCOW_LOG}"
printf '\n==== %s: starting %s\n' "$(date -Is)" "${RCOW_TGT_BIN}" >>"${RCOW_LOG}"

# --wait-for-rpc is not optional here: iobuf's buffer sizes are startup-only
# options, and the pools are allocated during subsystem init. Passed separately
# from TGT_ARGS so that overriding those cannot drop it.
#
# -r must match RCOW_RPC_SOCK: s3lvol_tgt defaults to /var/run/s3lvol.sock,
# and rcow_wait_rpc below probes RCOW_RPC_SOCK -- a mismatch just times out.
#
# shellcheck disable=SC2086
TGT_PID="$(rcow_start_target_detached "${RCOW_TGT_BIN}" -m "${RCOW_TGT_CPUMASK}" \
	-r "${RCOW_RPC_SOCK}" --wait-for-rpc ${TGT_ARGS})" ||
	bail "the target did not come up far enough to record its pid"

rcow_wait_rpc 60 "${TGT_PID}" ||
	bail "the target did not start answering RPCs within 60s"
rcow_log "target up as pid ${TGT_PID}, log ${RCOW_LOG}"

rcow_apply_startup_opts || bail "the startup options could not be applied"

# ==========================================================================
rcow_step "NVMf: transport and ${RCOW_NUM_SUBSYS} subsystems"

rcow_create_subsystems || bail "could not create the subsystem grid"

HAVE_SUBSYS="$(rcow_subsys_count)"
[ "${HAVE_SUBSYS}" = "${RCOW_NUM_SUBSYS}" ] ||
	bail "only ${HAVE_SUBSYS} of ${RCOW_NUM_SUBSYS} subsystems exist"

# ==========================================================================
rcow_step "local device and S3 namespaces"

rcow_srpc bdev_aio_create "${RCOW_WAL_IMG}" "${RCOW_WAL_BDEV}" 4096 \
	>/dev/null 2>&1 ||
	bail "bdev_aio_create failed for ${RCOW_WAL_IMG}"
rcow_log "WAL bdev '${RCOW_WAL_BDEV}' on ${RCOW_WAL_IMG}"

# One namespace per bucket. Only the first can hold an lvstore today (one
# blobstore per node), but an import reads from whichever namespace the source
# lives in, so they are all registered.
#
# The S3 config may name a path-style, plain-HTTP backend (MinIO in CI): the
# two optional flags are read once and folded into every registration.
RCOW_S3_EXTRA_JSON=""
[ "$(rcow_cfg_get path_style)" = "true" ] && RCOW_S3_EXTRA_JSON+=',"path_style":true'
[ "$(rcow_cfg_get no_tls)" = "true" ] && RCOW_S3_EXTRA_JSON+=',"no_tls":true'

for bucket in ${S3_BUCKETS}; do
	rcow_rpc rcow_add_s3_config \
		"$(printf '{"namespace":"%s","endpoint":"%s","bucket":"%s","region":"%s"%s}' \
			"${bucket}" "${S3_ENDPOINT}" "${bucket}" "${S3_REGION}" \
			"${RCOW_S3_EXTRA_JSON}")" \
		>/dev/null || bail "rcow_add_s3_config failed for bucket ${bucket}"
	rcow_log "namespace '${bucket}' registered"
done

# ==========================================================================
rcow_step "lvstore '${RCOW_LVS_NAME}'"

if BSTORE_ENTRY="$(rcow_bstore_entry "${RCOW_LVS_NAME}")"; then
	LVS_NS="${BSTORE_ENTRY%% *}"
	LVS_WAL_RECORDED="${BSTORE_ENTRY##* }"

	[ -n "${LVS_NS}" ] || bail "the ${RCOW_BSTORE_FILE} entry for \
'${RCOW_LVS_NAME}' has no namespace; it cannot be attached without knowing \
which bucket it lives in"

	if ! printf '%s\n' "${S3_BUCKETS}" | grep -qxF "${LVS_NS}"; then
		bail "'${RCOW_LVS_NAME}' lives in namespace '${LVS_NS}', which is not \
among the buckets in ${RCOW_S3_CFG}. Attaching it against a different bucket \
would read someone else's metadata"
	fi

	# The recorded WAL bdev name is a handle, not an identity: what matters is
	# that the *image file* is the same one, which RCOW_WAL_IMG fixes. A
	# different name is worth saying out loud, though, because it usually means
	# the entry was written by a test rather than by this script.
	if [ -n "${LVS_WAL_RECORDED}" ] &&
	   [ "${LVS_WAL_RECORDED}" != "${RCOW_WAL_BDEV}" ]; then
		rcow_warn "the recorded WAL bdev for '${RCOW_LVS_NAME}' is \
'${LVS_WAL_RECORDED}', attaching with '${RCOW_WAL_BDEV}' instead. This is only \
safe because both names refer to ${RCOW_WAL_IMG}; if the entry came from \
somewhere else, stop and check which image it meant"
	fi

	rcow_log "found in ${RCOW_BSTORE_FILE} (namespace ${LVS_NS}): attaching, \
which replays the journal and the WAL"

	ATTACH_PARAMS="$(printf '{"lvs_name":"%s","namespace":"%s","wal_bdev":"%s","checkpoint_interval_sec":%s' \
		"${RCOW_LVS_NAME}" "${LVS_NS}" "${RCOW_WAL_BDEV}" \
		"${RCOW_CKPT_INTERVAL_SEC}")"

	LOG_MARK="$(rcow_log_size)"
	if ! ATTACH_OUT="$(rcow_rpc rcow_attach_lvstore \
			"${ATTACH_PARAMS},\"force\":${FORCE_JSON}}" 2>&1)"; then
		# -EBUSY here is almost always the owner marker left behind by a
		# process that died, which is the one case the marker cannot settle by
		# itself. See rcow_owner_is_stale() for what "confirmed" means.
		RETRY_RC=0
		retry_after_stale_owner attach rcow_attach_lvstore \
			"${ATTACH_PARAMS}" "${LOG_MARK}" "${LVS_NS}" || RETRY_RC=$?
		if [ "${RETRY_RC}" -eq 0 ]; then
			ATTACH_OUT="${RETRY_OUT}"
		elif [ "${RETRY_RC}" -eq 2 ]; then
			rcow_err "taking over the stale marker failed: ${RETRY_OUT}"
			bail "the lvstore could not be attached"
		else
			rcow_err "rcow_attach_lvstore failed: ${ATTACH_OUT}"
			[ -n "${OWNER_REASON:-}" ] &&
				rcow_err "the owner marker was not confirmed stale: \
${OWNER_REASON}"
			bail "the lvstore could not be attached. Do not reach for create: \
it formats the journal and the WAL, and after a crash they hold the only copy \
of writes the host has already been told are durable. If the marker is held by \
a process you know is gone, re-run with --force"
		fi
	fi
	LVS_UP=1

	NUM_LVOLS="$(printf '%s' "${ATTACH_OUT}" | python3 -c \
		'import json,sys; print(len(json.load(sys.stdin).get("lvols", [])))' \
		2>/dev/null || echo '?')"
	rcow_log "attached; ${NUM_LVOLS} volume(s) came back"
else
	[ "${DO_CREATE}" -eq 1 ] || bail "no ${RCOW_BSTORE_FILE} entry for \
'${RCOW_LVS_NAME}' and --no-create was given"

	LVS_NS="$(printf '%s\n' "${S3_BUCKETS}" | head -1)"

	rcow_log "no entry in ${RCOW_BSTORE_FILE}: creating a new lvstore in \
namespace ${LVS_NS}, capacity ${RCOW_CAPACITY_GB} GiB, journal \
${RCOW_JOURNAL_MB} MiB, WAL ${RCOW_WAL_MB} MiB"
	rcow_warn "this formats ${RCOW_WAL_IMG}. If this node was supposed to \
have an lvstore already, stop now and find out what happened to \
${RCOW_BSTORE_FILE}"

	# RCOW_CAPACITY_GB goes straight through: capacity_gib is in GiB, which is
	# the unit this script always had. It used to multiply out to bytes here.
	CREATE_PARAMS="$(printf '{"lvs_name":"%s","namespace":"%s","capacity_gib":%s,"wal_bdev":"%s","journal_size_mb":%s,"wal_size_mb":%s,"checkpoint_interval_sec":%s' \
		"${RCOW_LVS_NAME}" "${LVS_NS}" "${RCOW_CAPACITY_GB}" \
		"${RCOW_WAL_BDEV}" "${RCOW_JOURNAL_MB}" "${RCOW_WAL_MB}" \
		"${RCOW_CKPT_INTERVAL_SEC}")"

	# A create can hit the same marker: an earlier create that died after
	# writing it left no bstore.json entry, so this path is reached again.
	LOG_MARK="$(rcow_log_size)"
	if ! CREATE_OUT="$(rcow_rpc rcow_create_lvstore \
			"${CREATE_PARAMS},\"force\":${FORCE_JSON}}" 2>&1)"; then
		RETRY_RC=0
		retry_after_stale_owner create rcow_create_lvstore \
			"${CREATE_PARAMS}" "${LOG_MARK}" "${LVS_NS}" || RETRY_RC=$?
		if [ "${RETRY_RC}" -ne 0 ]; then
			rcow_err "rcow_create_lvstore failed: ${CREATE_OUT}"
			[ -n "${RETRY_OUT}" ] && rcow_err "${RETRY_OUT}"
			bail "the lvstore could not be created"
		fi
	fi
	LVS_UP=1
	rcow_log "created"
fi

# ==========================================================================
if [ "${DO_REPLAY}" -eq 1 ]; then
	rcow_step "restoring previously active volumes"
	REPLAY_RC=0
	rcow_replay_registry || REPLAY_RC=1
else
	rcow_step "restore: skipped (--no-replay)"
	REPLAY_RC=0
	if [ -s "${RCOW_ACTIVE_FILE}" ]; then
		rcow_warn "${RCOW_ACTIVE_FILE} lists volumes that are not attached in \
this process. Run rcow_recovery.sh, or the next activation of one of them will \
be reported as already active while no namespace exists"
	fi
fi

# ==========================================================================
# Only now, with every namespace back where it belongs, is the grid made
# reachable -- see the step 7 note in the header, and rcow_add_listeners().
rcow_step "NVMf: listeners on ${RCOW_LISTEN_ADDR}:${RCOW_LISTEN_PORT}"

rcow_add_listeners || bail "the subsystems exist but none of them is reachable"

# ==========================================================================
if [ "${DO_CONNECT}" -eq 1 ]; then
	rcow_step "initiator: connecting all ${RCOW_NUM_SUBSYS} subsystems"
	# Not fatal: a subsystem that failed to connect costs the volumes hashed
	# to it, and the ones that did connect are still worth having up. The
	# warning names which.
	rcow_connect_all ||
		rcow_warn "some subsystems did not connect; volumes that hash to \
them will activate on the target but never appear on this host"
else
	rcow_step "initiator: skipped (--no-connect)"
fi

# ==========================================================================
# Both of these need the device nodes, which exist only once the host has
# connected.
if [ "${DO_REPLAY}" -eq 1 ] && [ "${DO_CONNECT}" -eq 1 ]; then
	if [ -s "${RCOW_ACTIVE_FILE}" ]; then
		rcow_verify_active 30 || REPLAY_RC=1
	fi

	# Setting readahead means writing to each device's sysfs directory, and
	# that directory appears with the node. Best effort, never fatal.
	rcow_tune_readahead
fi

# ==========================================================================
rcow_step "up"

rcow_log "pid ${TGT_PID}   rpc ${RCOW_RPC_SOCK}   log ${RCOW_LOG}"
rcow_log "subsystems ${RCOW_NQN_PREFIX}00..$(printf '%02d' $((RCOW_NUM_SUBSYS - 1))) \
on ${RCOW_LISTEN_ADDR}:${RCOW_LISTEN_PORT}"
rcow_log "activate a volume:  rcow_rpc rcow_active_bdev '{\"device_name\":\"NAME\"}'"
rcow_log "find its device:    rcow_rpc rcow_get_bdev '{\"device_name\":\"NAME\"}'"

if [ "${REPLAY_RC}" -ne 0 ]; then
	# The lvstore is attached and serving; some previously active volumes did
	# not come back as block devices in time. Failing the start here makes the
	# supervisor exit and systemd stop the unit, so the whole node goes down
	# (and, before release_lvstore, stayed down behind a stale marker: B17)
	# over a few volumes. Those that did come back are usable and the missing
	# ones can be re-activated by their owner, so by default this is a warning.
	# RCOW_REPLAY_STRICT=1 restores the fatal behaviour, and bail then releases
	# the marker on the way out.
	if [ "${RCOW_REPLAY_STRICT:-0}" = "1" ]; then
		bail "the data plane came up but the previous layout was not fully \
restored; see the warnings above"
	fi
	rcow_warn "the data plane is up but the previous layout was not fully \
restored; see the warnings above. Continuing (RCOW_REPLAY_STRICT=1 makes this \
fatal)"
fi
exit 0
