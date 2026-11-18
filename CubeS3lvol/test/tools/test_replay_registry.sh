#!/usr/bin/env bash
# Copyright (c) 2026 Tencent Inc.
# SPDX-License-Identifier: Apache-2.0
#
# Offline test of rcow_replay_registry (no target, no root): a fake rpc
# script records the order of rcow_active_bdev calls.
#
#   - template volumes are activated before any sandbox disk;
#   - activations run in parallel (RCOW_REPLAY_PARALLEL), not one by one;
#   - a failed activation stays in the replay plan;
#   - the replay never runs a global `sync` (it flushed NVMe mounts on the
#     not-yet-listening target and stalled the start for 10 minutes).
set -u
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "${HERE}/../.." && pwd)"
T="$(mktemp -d)"
trap 'rm -rf "${T}"' EXIT
fail() { echo "FAIL: $*"; exit 1; }

mkdir -p "${T}/run" "${T}/bin"
cat >"${T}/rpc.py" <<'PY'
#!/usr/bin/env python3
import json, os, sys, time
args = sys.argv[1:]
m = args[args.index("--timeout") + 2]
p = args[args.index("--timeout") + 3] if len(args) > args.index("--timeout") + 3 else ""
log = os.environ["FAKE_LOG"]
if m == "rcow_get_lvstores":
    reg = json.load(open(os.environ["FAKE_REG"]))
    print(json.dumps([{"lvols": [{"name": k, "uuid": v["uuid"]} for k, v in reg.items()]}]))
    sys.exit(0)
if m == "rcow_active_bdev":
    name = json.loads(p)["device_name"]
    with open(log, "a") as f:
        f.write("start %s %.3f\n" % (name, time.time()))
    time.sleep(0.2)
    with open(log, "a") as f:
        f.write("end %s %.3f\n" % (name, time.time()))
    if name == os.environ.get("FAKE_FAIL", ""):
        print("boom", file=sys.stderr); sys.exit(1)
    sys.exit(0)
sys.exit(0)
PY
chmod +x "${T}/rpc.py"
# `sync` must not be called; a shim makes that visible.
printf '#!/bin/sh\necho called >> %s/sync.called\n' "${T}" >"${T}/bin/sync"
chmod +x "${T}/bin/sync"

python3 - "${T}/active" <<'PY'
import json, sys
reg = {}
names = ["sb-a-memory", "sb-a-rootfs-gen0", "s3-meta-a", "tpl-tpl-x-rootfs",
         "s3-meta-tpl-x", "tpl-tpl-x-b123abc-memory", "cubelet-s3-metadata-base",
         "sb-b-memory", "sb-b-rootfs-gen0", "s3-meta-b", "tpl-y-b9f8e7d6-metadata",
         "s3-meta-snap-zzz"]
for i, n in enumerate(names):
    reg[n] = {"uuid": "u-%d" % i, "subsys": i % 4, "nsid": 1 + i}
json.dump(reg, open(sys.argv[1], "w"))
PY
cp "${T}/active" "${T}/reg.json"

run() {
	PATH="${T}/bin:${PATH}" FAKE_LOG="${T}/log" FAKE_REG="${T}/reg.json" FAKE_FAIL="${1:-}" \
	RCOW_RUN_DIR="${T}/run" RCOW_ACTIVE_FILE="${T}/active" RCOW_REPLAY_FILE="${T}/active.replay" \
	RCOW_RPC_PY="${T}/rpc.py" RCOW_RPC_SOCK="${T}/sock" RCOW_REPLAY_PARALLEL=4 \
	bash -c '. "'"${ROOT}"'/scripts/rcow_common.sh" >/dev/null 2>&1
		rcow_verify_namespaces() { return 0; }
		rcow_replay_registry' >"${T}/out" 2>&1
}

t0=$(date +%s.%N)
run || true
t1=$(date +%s.%N)

[ ! -e "${T}/sync.called" ] || fail "rcow_replay_registry called a global sync"

# Order: the last template start comes before the first sandbox start.
python3 - "${T}/log" <<'PY' || fail "templates were not activated before sandbox disks"
import sys
tpl = ("tpl-tpl-", "s3-meta-tpl-", "cubelet-s3-metadata-base", "tpl-y-b9f8e7d6")
starts = [l.split() for l in open(sys.argv[1]) if l.startswith("start")]
ends = [l.split() for l in open(sys.argv[1]) if l.startswith("end")]
last_tpl_end = max(float(e[2]) for e in ends if e[1].startswith(tpl))
first_sb = min(float(s[2]) for s in starts if not s[1].startswith(tpl))
assert len(starts) == 12, len(starts)
assert last_tpl_end <= first_sb + 0.001, (last_tpl_end, first_sb)
PY

# Parallel: 12 calls of 0.2 s, 4 at a time in two waves -> well under 12 x 0.2.
python3 -c "import sys; d=float(sys.argv[2])-float(sys.argv[1]); sys.exit(0 if d < 1.9 else 1)" "$t0" "$t1" \
	|| fail "replay was not parallel ($(python3 -c "print(round($t1-$t0,2))") s for 12 x 0.2 s)"
grep -q "replay: 12 restored, 0 refused, 0 failed" "${T}/out" || { cat "${T}/out"; fail "summary"; }
[ ! -e "${T}/active.replay" ] || fail "plan left behind after a clean replay"

# A failure stays in the plan.
cp "${T}/reg.json" "${T}/active"; : >"${T}/log"
run "sb-b-memory" && fail "replay with a failed entry reported success"
python3 -c "import json,sys; d=json.load(open(sys.argv[1])); sys.exit(0 if list(d)==['sb-b-memory'] else 1)" \
	"${T}/active.replay" || fail "failed entry not kept in the plan"

# Classifier.
. "${ROOT}/scripts/rcow_common.sh" >/dev/null 2>&1 || true
for n in tpl-tpl-a-rootfs s3-meta-tpl-a cubelet-s3-metadata-base cubelet-s3-metadata-base-snap \
	tpl-abc-b12ab34cd-memory; do
	[ "$(rcow_replay_class "$n")" = 0 ] || fail "class of $n"
done
for n in sb-1-memory s3-meta-1 s3-meta-snap-1 tpl-snap-1-rootfs tpl-tpl-a-build-rootfs; do
	[ "$(rcow_replay_class "$n")" = 1 ] || fail "class of $n"
done
echo "PASS test_replay_registry"
