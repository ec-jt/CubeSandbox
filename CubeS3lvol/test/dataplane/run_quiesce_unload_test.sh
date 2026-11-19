#!/usr/bin/env bash
# Copyright (c) 2026 Tencent Inc.
# SPDX-License-Identifier: Apache-2.0
#
#  B3: an lvstore unload while host reads are stuck inside S3 GETs.
#
#  Production abort (app2, three times on keep-connected restarts):
#    bdev_channel_destroy_resource: Assertion `TAILQ_EMPTY(&ch->io_submitted)'
#  The unload unregisters every lvol at once; nvmf hot-removes several
#  namespaces of one subsystem concurrently, and a later pause in the queue
#  completes without draining its nsid, so a namespace channel is put with a
#  read still on it.
#
#  This test builds exactly that: four lvols as four namespaces of ONE
#  subsystem, data flushed to S3 and no local cache (so every read is a GET),
#  host reads in flight, and the object store stalled (connections open, no
#  bytes) through a forwarding proxy. Then, keeping the host connected:
#
#    QUIESCE=1 (default): rcow_quiesce_nvmf, then rcow_unload_lvstore.
#              Pass: target alive, unload succeeds, quiesce < 10 s.
#    QUIESCE=0: rcow_unload_lvstore only -- the pre-fix sequence. Expected to
#              abort the target; run it against an old binary to show the test
#              reproduces the bug (TGT=/path/to/old/s3lvol_tgt QUIESCE=0).
#
#  Needs root, nvme-cli, fio, and an S3 endpoint WITHOUT TLS reachable at
#  S3_HOST:S3_PORT (a throwaway MinIO is fine):
#    docker run -d -p 127.0.0.1:19000:9000 -e MINIO_ROOT_USER=k \
#      -e MINIO_ROOT_PASSWORD=... minio/minio server /data
#    AWS_ACCESS_KEY_ID=k AWS_SECRET_ACCESS_KEY=... S3_PORT=19000 \
#      test/dataplane/run_quiesce_unload_test.sh

set -u

SELF_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "${SELF_DIR}/../.." && pwd)"
RPC="${ROOT}/test/tools/s3lvol_rpc.py"
SPDK_RPC="python3 ${ROOT}/scripts/spdk_rpc.py -s /var/run/s3lvol.sock -t 60"
[ -f "${ROOT}/scripts/spdk_rpc.py" ] || SPDK_RPC="python3 ${ROOT}/deps/spdk/scripts/rpc.py -s /var/run/s3lvol.sock -t 60"
TGT="${TGT:-${ROOT}/app/s3lvol_tgt/s3lvol_tgt}"
QUIESCE="${QUIESCE:-1}"

S3_HOST="${S3_HOST:-127.0.0.1}"
S3_PORT="${S3_PORT:-19000}"
PROXY_PORT="${PROXY_PORT:-19001}"
BUCKET="${BUCKET:-b3test}"
LVS="b3q$(date +%s)"
WAL_FILE=/tmp/b3quiesce_wal.img
CTL=/tmp/b3quiesce_stall
NQN="nqn.2026-08.io.spdk:rcow-00"
TGT_LOG=/tmp/b3quiesce_tgt.log
NVOL=4

PASS=0; FAIL=0
pass() { PASS=$((PASS+1)); echo "  [PASS] $*"; }
fail() { FAIL=$((FAIL+1)); echo "  [FAIL] $*"; }
info() { echo "  ---- $*"; }

TGT_PID=""; PROXY_PID=""; FIO_PID=""
cleanup() {
	rm -f "${CTL}"
	[ -n "${FIO_PID}" ] && kill "${FIO_PID}" 2>/dev/null
	nvme disconnect -n "${NQN}" >/dev/null 2>&1
	if [ -n "${TGT_PID}" ] && kill -0 "${TGT_PID}" 2>/dev/null; then
		kill -TERM "${TGT_PID}" 2>/dev/null
		for _ in $(seq 100); do kill -0 "${TGT_PID}" 2>/dev/null || break; sleep 0.1; done
		kill -9 "${TGT_PID}" 2>/dev/null
	fi
	[ -n "${PROXY_PID}" ] && kill "${PROXY_PID}" 2>/dev/null
	rm -f "${WAL_FILE}" /var/tmp/spdk_cpu_lock_* /var/run/s3lvol.sock.lock
	python3 - "${LVS}" <<-'PY'
	import json, sys
	p = '/data/cubelet/rcow/bstore.json'
	try:
	    d = json.load(open(p))
	except Exception:
	    raise SystemExit
	if d.pop(sys.argv[1], None) is not None:
	    json.dump(d, open(p, 'w'), indent=2)
	PY
	echo ""
	echo "=== result: ${PASS} passed, ${FAIL} failed ==="
	[ "${FAIL}" -eq 0 ]
}
trap 'cleanup; exit $?' EXIT

[ "$(id -u)" = 0 ] || { echo "needs root" >&2; exit 1; }
[ -x "${TGT}" ] || { echo "no target at ${TGT}" >&2; exit 1; }
[ -n "${AWS_ACCESS_KEY_ID:-}" ] && [ -n "${AWS_SECRET_ACCESS_KEY:-}" ] || { echo "set AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY" >&2; exit 1; }
command -v fio >/dev/null && command -v nvme >/dev/null || { echo "needs fio and nvme-cli" >&2; exit 1; }
pgrep -x s3lvol_tgt >/dev/null && { echo "another s3lvol_tgt is running" >&2; exit 1; }
modprobe nvme_tcp 2>/dev/null
mkdir -p /data/cubelet/rcow

echo "=== [1] object store, stall proxy, target"
python3 "${ROOT}/test/tools/s3_bucket.py" ensure -e "${S3_HOST}:${S3_PORT}" -b "${BUCKET}" \
	--path-style --no-tls >/dev/null || { fail "bucket ${BUCKET}"; exit 1; }
rm -f "${CTL}"
python3 "${ROOT}/test/tools/stall_proxy.py" "${PROXY_PORT}" "${S3_HOST}" "${S3_PORT}" "${CTL}" &
PROXY_PID=$!
rm -f "${WAL_FILE}" /var/tmp/spdk_cpu_lock_* /var/run/s3lvol.sock.lock
# super + 64 MiB journal + 128 MiB WAL + less than one chunk: no room for the
# read cache, so every read of flushed data is an S3 GET.
truncate -s $(( (64 + 128) * 1048576 + 8192 )) "${WAL_FILE}"
# Short S3 deadlines are NOT set: the point is that the quiesce does not wait
# for them.
"${TGT}" -m 0x3 --no-huge -s 2048 -r /var/run/s3lvol.sock >"${TGT_LOG}" 2>&1 &
TGT_PID=$!
for _ in $(seq 80); do [ -S /var/run/s3lvol.sock ] && break; sleep 0.25; done
sleep 1
"${RPC}" bdev_aio_create "$(printf '{"filename":"%s","name":"b3wal","block_size":4096}' "${WAL_FILE}")" >/dev/null || exit 1
"${RPC}" rcow_add_s3_config "$(printf '{"namespace":"%s","endpoint":"127.0.0.1:%s","bucket":"%s","region":"us-east-1","path_style":true,"no_tls":true}' \
	"${BUCKET}" "${PROXY_PORT}" "${BUCKET}")" >/dev/null || exit 1
"${RPC}" rcow_create_lvstore "$(printf '{"lvs_name":"%s","namespace":"%s","capacity_gib":8,"wal_bdev":"b3wal","journal_size_mb":64,"wal_size_mb":128}' \
	"${LVS}" "${BUCKET}")" >/dev/null || { fail "create lvstore"; exit 1; }
${SPDK_RPC} nvmf_create_transport -t TCP >/dev/null
${SPDK_RPC} nvmf_create_subsystem "${NQN}" -a -s RCOW00000000000000 -m 64 >/dev/null || exit 1
for i in $(seq 1 ${NVOL}); do
	"${RPC}" rcow_create_lvol "$(printf '{"lvol_name":"v%s","size_gib":1}' "${i}")" >/dev/null || { fail "lvol v${i}"; exit 1; }
	${SPDK_RPC} nvmf_subsystem_add_ns -n "${i}" "${NQN}" "${LVS}/v${i}" >/dev/null || { fail "add_ns v${i}"; exit 1; }
done
${SPDK_RPC} nvmf_subsystem_add_listener "${NQN}" -t tcp -a 127.0.0.1 -s 4420 >/dev/null || exit 1
nvme connect -t tcp -a 127.0.0.1 -s 4420 -n "${NQN}" --ctrl-loss-tmo=600 --reconnect-delay=1 >/dev/null || { fail "nvme connect"; exit 1; }
sleep 2
DEVS=()
for i in $(seq 1 ${NVOL}); do
	U="$("${RPC}" rcow_get_lvol "$(printf '{"lvol_name":"v%s"}' "${i}")" 2>/dev/null | \
		python3 -c 'import json,sys; d=json.load(sys.stdin); print(d.get("uuid",""))' 2>/dev/null)"
	for _ in $(seq 40); do
		D="$(grep -lis "${U}" /sys/block/nvme*/uuid 2>/dev/null | grep -v 'c[0-9]*n' | head -1)"
		[ -n "${U}" ] && [ -n "${D}" ] && break; sleep 0.25
	done
	[ -n "${D}" ] && DEVS+=("/dev/$(basename "$(dirname "${D}")")")
done
[ "${#DEVS[@]}" -eq "${NVOL}" ] && pass "${NVOL} namespaces of one subsystem on the host: ${DEVS[*]}" \
	|| { fail "expected ${NVOL} host devices, got ${#DEVS[@]} (${DEVS[*]:-})"; exit 1; }

echo ""
echo "=== [2] data into S3, then stall S3 under host reads"
for d in "${DEVS[@]}"; do
	dd if=/dev/urandom of="${d}" bs=1M count=48 oflag=direct status=none || fail "write ${d}"
done
"${RPC}" rcow_flush_lvstore "$(printf '{"lvs_name":"%s"}' "${LVS}")" >/dev/null && pass "flushed to S3" || fail "flush"
touch "${CTL}"
FIO_ARGS=()
for d in "${DEVS[@]}"; do FIO_ARGS+=(--name="$(basename "$d")" --filename="$d"); done
fio --rw=randread --bs=64k --size=48m --direct=1 --ioengine=libaio --iodepth=8 \
	--time_based --runtime=120 --continue_on_error=all "${FIO_ARGS[@]}" \
	>/tmp/b3quiesce_fio.log 2>&1 &
FIO_PID=$!
sleep 4
STUCK=0
for d in "${DEVS[@]}"; do
	read -r r w < "/sys/block/$(basename "$d")/inflight"
	STUCK=$((STUCK + r + w))
done
info "host reads in flight with S3 stalled: ${STUCK}"
[ "${STUCK}" != "?" ] && [ "${STUCK}" -gt 0 ] && pass "host reads are stuck in S3" || fail "no read is stuck (${STUCK}); test would prove nothing"

echo ""
echo "=== [3] keep-connected stop sequence (QUIESCE=${QUIESCE})"
T0=$(date +%s%N)
if [ "${QUIESCE}" = 1 ]; then
	if QOUT="$("${RPC}" rcow_quiesce_nvmf '{"timeout_ms":20000}' 2>&1)"; then
		QMS=$(( ($(date +%s%N) - T0) / 1000000 ))
		pass "rcow_quiesce_nvmf in ${QMS} ms: ${QOUT}"
		[ "${QMS}" -lt 10000 ] && pass "quiesce under 10 s" || fail "quiesce took ${QMS} ms"
	else
		fail "rcow_quiesce_nvmf: ${QOUT}"
	fi
	# Let the object store answer again so the unload's own flush can finish.
	rm -f "${CTL}"
fi
UOUT="$(timeout 90 "${RPC}" --timeout 90 rcow_unload_lvstore "$(printf '{"lvs_name":"%s"}' "${LVS}")" 2>&1)"; URC=$?
UMS=$(( ($(date +%s%N) - T0) / 1000000 ))
rm -f "${CTL}"
# The abort, when it comes, follows the unload's own deadline; give it time.
sleep 15
if kill -0 "${TGT_PID}" 2>/dev/null; then
	pass "target alive after the unload (${UMS} ms from stop start)"
else
	fail "target died during the unload: $(grep -m1 -E 'Assertion|Segmentation' "${TGT_LOG}" || echo 'no assertion text')"
	TGT_PID=""
fi
grep -q "io_submitted" "${TGT_LOG}" && fail "io_submitted assertion in target log" || pass "no io_submitted assertion"
[ "${URC}" -eq 0 ] && pass "rcow_unload_lvstore succeeded" || fail "rcow_unload_lvstore: ${UOUT}"
