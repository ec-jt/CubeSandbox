#!/usr/bin/env bash
# Copyright (c) 2026 Tencent Inc.
# SPDX-License-Identifier: Apache-2.0
#
#  Restart and attach with the object store unavailable.
#
#  Production (app2, keep-connected restart while the bucket took ~70 s per
#  PUT): WAL replay 25 s, then the blobstore metadata load took ~23 min, every
#  metadata page an S3 GET behind stalled uploads; guests had no disks for ~40
#  min. Causes, all addressed together:
#    - the metadata warm-up skipped every chunk that held any replayed write, so
#      the base of nearly all metadata chunks was read one 4 KiB GET at a time;
#    - the chunk cache index did not survive the restart, so even bytes already
#      on the local device were fetched again;
#    - the flusher, lease checks and superseded deletes all started during the
#      attach and competed for the same connections.
#
#  This test:
#    [1] creates an lvstore with a local cache and NVOL lvols (metadata), writes
#        data, flushes and checkpoints (so the metadata cache index is saved);
#    [2] writes more without flushing (a WAL backlog the attach must replay);
#    [3] SIGKILLs the target with the store stalled -- no clean unload, the
#        realistic crash -- then clears this node's dead owner marker the way
#        rcow_start.sh does (rcow_owner_clear_stale);
#    [4] makes the store DEGRADED (every forwarded segment delayed DELAY_MS)
#        and attaches. Pass: attach < ATTACH_MAX_S, metadata adopted from the
#        local cache, uploads held during the attach, and the unflushed data
#        reads back correctly while the store is still degraded.
#
#  Why degraded and not dead for [4]: the attach takes the strict ownership
#  marker with a conditional PUT and reads the checkpoint object. Those few
#  small requests are the fence that stops two writers; with no object store at
#  all no attach may succeed, by design. Everything else -- the blobstore
#  metadata, the replayed WAL -- is now served locally.
#    [5] lifts the stall and waits for the backlog to drain to S3.
#
#  Needs root, nvme-cli, and an S3 endpoint WITHOUT TLS at S3_HOST:S3_PORT
#  (a throwaway MinIO):
#    docker run -d --name s3lvol_test_minio -p 127.0.0.1:19000:9000 \
#      -e MINIO_ROOT_USER=k -e MINIO_ROOT_PASSWORD=... minio/minio server /data
#    AWS_ACCESS_KEY_ID=k AWS_SECRET_ACCESS_KEY=... S3_PORT=19000 \
#      test/dataplane/run_degraded_attach_test.sh

set -u

SELF_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "${SELF_DIR}/../.." && pwd)"
RPC="${ROOT}/test/tools/s3lvol_rpc.py"
SPDK_RPC="python3 ${ROOT}/scripts/spdk_rpc.py -s /var/run/s3lvol.sock -t 60"
[ -f "${ROOT}/scripts/spdk_rpc.py" ] || SPDK_RPC="python3 ${ROOT}/deps/spdk/scripts/rpc.py -s /var/run/s3lvol.sock -t 60"
TGT="${TGT:-${ROOT}/app/s3lvol_tgt/s3lvol_tgt}"

S3_HOST="${S3_HOST:-127.0.0.1}"
S3_PORT="${S3_PORT:-19000}"
PROXY_PORT="${PROXY_PORT:-19002}"
BUCKET="${BUCKET:-degtest}"
LVS="deg$(date +%s)"
WAL_FILE=/tmp/degattach_wal.img
CTL=/tmp/degattach_stall
NQN="nqn.2026-08.io.spdk:rcow-00"
TGT_LOG=/tmp/degattach_tgt.log
NVOL="${NVOL:-24}"
ATTACH_MAX_S="${ATTACH_MAX_S:-30}"
DELAY_MS="${DELAY_MS:-1500}"
export S3LVOL_MDCACHE_DIR=/tmp/degattach_md
export S3LVOL_BSTORE_FILE=/tmp/degattach_bstore.json
export S3LVOL_ACTIVE_FILE=/tmp/degattach_active.json
export S3LVOL_WAL_MAX_DIRTY_GB="${S3LVOL_WAL_MAX_DIRTY_GB:-1}"

PASS=0; FAIL=0
pass() { PASS=$((PASS+1)); echo "  [PASS] $*"; }
fail() { FAIL=$((FAIL+1)); echo "  [FAIL] $*"; }
info() { echo "  ---- $*"; }

TGT_PID=""; PROXY_PID=""
stop_tgt() {
	if [ -n "${TGT_PID}" ] && kill -0 "${TGT_PID}" 2>/dev/null; then
		kill -"${1:-TERM}" "${TGT_PID}" 2>/dev/null
		for _ in $(seq 100); do kill -0 "${TGT_PID}" 2>/dev/null || break; sleep 0.1; done
		kill -9 "${TGT_PID}" 2>/dev/null
	fi
	TGT_PID=""
	rm -f /var/tmp/spdk_cpu_lock_* /var/run/s3lvol.sock /var/run/s3lvol.sock.lock
}
cleanup() {
	rm -f "${CTL}"
	nvme disconnect -n "${NQN}" >/dev/null 2>&1
	stop_tgt TERM
	[ -n "${PROXY_PID}" ] && kill "${PROXY_PID}" 2>/dev/null
	rm -rf "${WAL_FILE}" "${S3LVOL_MDCACHE_DIR}" "${S3LVOL_BSTORE_FILE}" "${S3LVOL_ACTIVE_FILE}"
	echo ""
	echo "=== result: ${PASS} passed, ${FAIL} failed ==="
	[ "${FAIL}" -eq 0 ]
}
trap 'cleanup; exit $?' EXIT

start_tgt() {
	rm -f /var/tmp/spdk_cpu_lock_* /var/run/s3lvol.sock /var/run/s3lvol.sock.lock
	"${TGT}" -m 0x3 --no-huge -s 2048 -r /var/run/s3lvol.sock >>"${TGT_LOG}" 2>&1 &
	TGT_PID=$!
	for _ in $(seq 80); do [ -S /var/run/s3lvol.sock ] && break; sleep 0.25; done
	sleep 1
	"${RPC}" bdev_aio_create "$(printf '{"filename":"%s","name":"dgwal","block_size":4096}' "${WAL_FILE}")" >/dev/null || return 1
	"${RPC}" rcow_add_s3_config "$(printf '{"namespace":"%s","endpoint":"127.0.0.1:%s","bucket":"%s","region":"us-east-1","path_style":true,"no_tls":true}' \
		"${BUCKET}" "${PROXY_PORT}" "${BUCKET}")" >/dev/null
}

stat_of() {   # stat_of <key> -> write_path.<key>
	"${RPC}" rcow_get_lvstores 2>/dev/null | python3 -c '
import json,sys
d=json.load(sys.stdin)
d=d if isinstance(d,list) else [d]
for l in d:
    if l.get("lvs_name")==sys.argv[1]:
        print(l.get("write_path",{}).get(sys.argv[2],"")); break' "${LVS}" "$1"
}

[ "$(id -u)" = 0 ] || { echo "needs root" >&2; exit 1; }
[ -x "${TGT}" ] || { echo "no target at ${TGT}" >&2; exit 1; }
[ -n "${AWS_ACCESS_KEY_ID:-}" ] && [ -n "${AWS_SECRET_ACCESS_KEY:-}" ] || { echo "set AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY" >&2; exit 1; }
command -v nvme >/dev/null || { echo "needs nvme-cli" >&2; exit 1; }
pgrep -x s3lvol_tgt >/dev/null && { echo "another s3lvol_tgt is running" >&2; exit 1; }
modprobe nvme_tcp 2>/dev/null
mkdir -p "${S3LVOL_MDCACHE_DIR}"
: >"${TGT_LOG}"

echo "=== [1] lvstore with a cache, ${NVOL} lvols, data flushed and checkpointed"
python3 "${ROOT}/test/tools/s3_bucket.py" ensure -e "${S3_HOST}:${S3_PORT}" -b "${BUCKET}" \
	--path-style --no-tls >/dev/null || { fail "bucket ${BUCKET}"; exit 1; }
rm -f "${CTL}"
python3 "${ROOT}/test/tools/stall_proxy.py" "${PROXY_PORT}" "${S3_HOST}" "${S3_PORT}" "${CTL}" &
PROXY_PID=$!
rm -f "${WAL_FILE}"
# super + 64 MiB journal + 512 MiB WAL + 256 MiB cache
truncate -s $(( (64 + 512 + 256) * 1048576 + 8192 )) "${WAL_FILE}"
start_tgt || { fail "target start"; exit 1; }
"${RPC}" rcow_create_lvstore "$(printf '{"lvs_name":"%s","namespace":"%s","capacity_gib":64,"wal_bdev":"dgwal","journal_size_mb":64,"wal_size_mb":512}' \
	"${LVS}" "${BUCKET}")" >/dev/null || { fail "create lvstore"; exit 1; }
for i in $(seq 1 "${NVOL}"); do
	"${RPC}" rcow_create_lvol "$(printf '{"lvol_name":"v%s","size_gib":1}' "${i}")" >/dev/null || { fail "lvol v${i}"; exit 1; }
done
${SPDK_RPC} nvmf_create_transport -t TCP >/dev/null
${SPDK_RPC} nvmf_create_subsystem "${NQN}" -a -s RCOW00000000000000 -m 4 >/dev/null || exit 1
${SPDK_RPC} nvmf_subsystem_add_ns -n 1 "${NQN}" "${LVS}/v1" >/dev/null || exit 1
${SPDK_RPC} nvmf_subsystem_add_listener "${NQN}" -t tcp -a 127.0.0.1 -s 4420 >/dev/null || exit 1
nvme connect -t tcp -a 127.0.0.1 -s 4420 -n "${NQN}" >/dev/null || { fail "nvme connect"; exit 1; }
sleep 2
U="$("${RPC}" rcow_get_lvol '{"lvol_name":"v1"}' 2>/dev/null | python3 -c 'import json,sys; print(json.load(sys.stdin).get("uuid",""))')"
DEV=""
for _ in $(seq 40); do
	D="$(grep -lis "${U}" /sys/block/nvme*/uuid 2>/dev/null | grep -v 'c[0-9]*n' | head -1)"
	[ -n "${D}" ] && DEV="/dev/$(basename "$(dirname "${D}")")" && break; sleep 0.25
done
[ -n "${DEV}" ] && pass "v1 on the host as ${DEV}" || { fail "v1 not visible"; exit 1; }
dd if=/dev/urandom of=/tmp/degattach_a.bin bs=1M count=32 status=none
dd if=/tmp/degattach_a.bin of="${DEV}" bs=1M oflag=direct status=none || fail "write A"
"${RPC}" rcow_flush_lvstore "$(printf '{"lvs_name":"%s"}' "${LVS}")" >/dev/null && pass "flushed" || fail "flush"
"${RPC}" rcow_checkpoint_lvstore "$(printf '{"lvs_name":"%s"}' "${LVS}")" >/dev/null && pass "checkpointed" || fail "checkpoint"
IDX="${S3LVOL_MDCACHE_DIR}/mdcache.${LVS}.idx"
[ -s "${IDX}" ] && pass "metadata cache index saved ($(stat -c %s "${IDX}") bytes)" || fail "no metadata cache index at ${IDX}"

echo ""
echo "=== [2] unflushed backlog, then the store stalls"
touch "${CTL}"
dd if=/dev/urandom of=/tmp/degattach_b.bin bs=1M count=64 status=none
dd if=/tmp/degattach_b.bin of="${DEV}" bs=1M seek=32 oflag=direct status=none || fail "write B"
# More lvol metadata changes, so replay leaves metadata chunks partly overlaid.
for i in $(seq 1 4); do "${RPC}" rcow_create_lvol "$(printf '{"lvol_name":"w%s","size_gib":1}' "${i}")" >/dev/null; done
sync
OV="$(stat_of overlay_bytes)"
info "overlay bytes before the crash: ${OV}"
[ -n "${OV}" ] && [ "${OV}" -gt 0 ] && pass "unflushed backlog present (${OV} bytes)" || fail "no backlog (${OV})"

echo ""
echo "=== [3] SIGKILL with the store stalled"
nvme disconnect -n "${NQN}" >/dev/null 2>&1
DEAD_PID="${TGT_PID}"
stop_tgt KILL
pass "target killed"
rm -f "${CTL}"
python3 "${ROOT}/test/tools/s3_owner_clear.py" -e "${S3_HOST}:${S3_PORT}" -b "${BUCKET}" \
	--lvs "${LVS}" --expect-node "$(hostname)" --expect-pid "${DEAD_PID}" --path-style --no-tls \
	>/tmp/degattach_clear.log 2>&1 && pass "dead owner marker cleared" || { fail "owner clear: $(tail -1 /tmp/degattach_clear.log)"; exit 1; }

echo ""
echo "=== [4] restart and attach with the store degraded (${DELAY_MS} ms per segment)"
echo "${DELAY_MS}" > "${CTL}"
start_tgt || { fail "target restart"; exit 1; }
T0=$(date +%s%N)
AOUT="$(timeout 600 "${RPC}" --timeout 600 rcow_attach_lvstore "$(printf '{"lvs_name":"%s","namespace":"%s","wal_bdev":"dgwal"}' \
	"${LVS}" "${BUCKET}")" 2>&1)"; ARC=$?
AMS=$(( ($(date +%s%N) - T0) / 1000000 ))
[ "${ARC}" -eq 0 ] && pass "attach succeeded in ${AMS} ms with the store degraded" || fail "attach: ${AOUT}"
[ "${AMS}" -lt $((ATTACH_MAX_S * 1000)) ] && pass "attach under ${ATTACH_MAX_S} s" || fail "attach took ${AMS} ms"
grep -m1 "metadata cache index:" "${TGT_LOG}" | sed 's/^/  ---- /'
grep -q "adopted from the local cache" "${TGT_LOG}" && pass "metadata adopted locally" || fail "no adopt line"
grep -m1 "Flusher paused" "${TGT_LOG}" >/dev/null && pass "uploads held during the attach" || fail "flusher was not paused"
grep -m1 "Flusher resumed" "${TGT_LOG}" >/dev/null && pass "uploads resumed after the attach" || fail "flusher not resumed"

${SPDK_RPC} nvmf_create_transport -t TCP >/dev/null
${SPDK_RPC} nvmf_create_subsystem "${NQN}" -a -s RCOW00000000000000 -m 4 >/dev/null
${SPDK_RPC} nvmf_subsystem_add_ns -n 1 "${NQN}" "${LVS}/v1" >/dev/null
${SPDK_RPC} nvmf_subsystem_add_listener "${NQN}" -t tcp -a 127.0.0.1 -s 4420 >/dev/null
nvme connect -t tcp -a 127.0.0.1 -s 4420 -n "${NQN}" >/dev/null || fail "reconnect"
sleep 2
DEV=""
for _ in $(seq 40); do
	D="$(grep -lis "${U}" /sys/block/nvme*/uuid 2>/dev/null | grep -v 'c[0-9]*n' | head -1)"
	[ -n "${D}" ] && DEV="/dev/$(basename "$(dirname "${D}")")" && break; sleep 0.25
done
if [ -n "${DEV}" ]; then
	# Unflushed data (B) must read back from the WAL: nothing of B was ever
	# uploaded, and the store is still degraded.
	if timeout 60 dd if="${DEV}" of=/tmp/degattach_b.out bs=1M skip=32 count=64 iflag=direct status=none &&
	   cmp -s /tmp/degattach_b.bin /tmp/degattach_b.out; then
		pass "unflushed data reads back correctly with S3 degraded"
	else
		fail "unflushed data did not read back"
	fi
else
	fail "v1 not visible after attach"
fi

echo ""
echo "=== [5] store back: backlog drains"
rm -f "${CTL}"
T1=$(date +%s)
DRAINED=0
for _ in $(seq 120); do
	OV="$(stat_of overlay_bytes)"
	[ "${OV}" = 0 ] && DRAINED=1 && break
	sleep 1
done
[ "${DRAINED}" = 1 ] && pass "backlog drained in $(( $(date +%s) - T1 )) s" || fail "backlog not drained (${OV})"
if timeout 60 dd if="${DEV}" of=/tmp/degattach_a.out bs=1M count=32 iflag=direct status=none &&
   cmp -s /tmp/degattach_a.bin /tmp/degattach_a.out; then
	pass "flushed data intact"
else
	fail "flushed data mismatch"
fi
grep -q "Assertion" "${TGT_LOG}" && fail "assertion in target log" || pass "no assertion"
rm -f /tmp/degattach_*.bin /tmp/degattach_*.out
