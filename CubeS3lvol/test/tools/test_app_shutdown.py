#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Bounded, offline target lifecycle regression. Requires Docker, not root.

Run separately at --memory 256 (expected init failure, immediate SIGTERM) and
512 (expected success, AIO create/examine/delete, SIGTERM). See test/README.md.
No device, network, hugepages, S3 credentials, or privileged container is used.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import socket
import subprocess
import time
import traceback
import uuid


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--target", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path,
                        help="new directory; existing results are never overwritten")
    parser.add_argument("--image", required=True, help="local runtime image sha256 ID")
    parser.add_argument("--memory", required=True, type=int, choices=(256, 512))
    args = parser.parse_args()
    if os.getuid() != 1000:
        parser.error("run as uid 1000, matching the isolated container")
    if not args.image.startswith("sha256:") or len(args.image) != 71:
        parser.error("--image must be an immutable local sha256 image ID")
    target = args.target.resolve(strict=True)
    out = args.output.resolve()
    out.mkdir()
    work = out / "runtime"
    work.mkdir(mode=0o700)
    name = "s3lvol-shutdown-" + uuid.uuid4().hex[:12]
    cpus = sorted(os.sched_getaffinity(0))[:4]
    report = {"target": str(target), "sha256": hashlib.sha256(target.read_bytes()).hexdigest(),
              "image": args.image, "memory_mib": args.memory, "kernel": os.uname().release,
              "name": name, "commands": [], "rpc": [], "passed": False}
    created = False
    fd = os.open(work, os.O_RDONLY | os.O_DIRECTORY)

    def command(argv, check=True):
        result = subprocess.run(argv, text=True, capture_output=True, timeout=12)
        report["commands"].append({"argv": argv, "returncode": result.returncode,
                                   "stdout": result.stdout, "stderr": result.stderr})
        if check and result.returncode:
            raise RuntimeError(result.stderr)
        return result

    def rpc(method, params=None):
        request = {"jsonrpc": "2.0", "id": len(report["rpc"]) + 1, "method": method}
        if params is not None:
            request["params"] = params
        record = {"request": request}
        report["rpc"].append(record)
        try:
            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sock:
                sock.settimeout(3)
                sock.connect("/proc/self/fd/%d/rpc.sock" % fd)
                sock.sendall((json.dumps(request) + "\n").encode())
                data = b""
                deadline = time.monotonic() + 3
                while time.monotonic() < deadline:
                    chunk = sock.recv(65536)
                    if not chunk:
                        raise ConnectionError("RPC closed")
                    data += chunk
                    if len(data) > 2097152:
                        raise RuntimeError("oversize RPC response")
                    try:
                        response = json.loads(data)
                        break
                    except json.JSONDecodeError:
                        continue
                else:
                    raise TimeoutError("RPC deadline")
            record["response"] = response
            assert response["id"] == request["id"]
            if "error" in response:
                raise RuntimeError(str(response["error"]))
            return response["result"]
        except Exception as exc:
            record["error"] = str(exc)
            raise

    try:
        daemon = ["/target", "--no-huge", "--no-pci", "--wait-for-rpc",
                  "--disable-cpumask-locks", "--num-trace-entries", "0", "-d",
                  "-s", str(args.memory), "-m", "[%d]" % cpus[0],
                  "-r", "/work/rpc.sock", "-L", "aio"]
        argv = ["docker", "create", "--name", name, "--network=none", "--user=1000:1000",
                "--cap-drop=ALL", "--security-opt=no-new-privileges", "--read-only",
                "--cpus=4", "--cpuset-cpus=" + ",".join(map(str, cpus)),
                "--memory=4g", "--memory-swap=4g", "--pids-limit=128",
                # DPDK no-huge mode backs its 256/512 MiB allocation with a
                # file. A smaller RLIMIT_FSIZE kills initialization before
                # this regression reaches shutdown. Cgroup memory stays 4 GiB.
                "--ulimit", "core=0:0",
                "--shm-size=64m", "--tmpfs", "/tmp:rw,noexec,nosuid,nodev,size=64m,uid=1000,gid=1000",
                "--tmpfs", "/var/tmp:rw,noexec,nosuid,nodev,size=64m,uid=1000,gid=1000",
                "--env", "XDG_RUNTIME_DIR=/work", "--workdir", "/work",
                "--mount", "type=bind,src=%s,dst=/target,readonly" % target,
                "--mount", "type=bind,src=%s,dst=/work" % work, args.image,
                "/usr/bin/timeout", "--signal=TERM", "--kill-after=5", "100"] + daemon
        report["container_id"] = command(argv).stdout.strip()
        created = True
        command(["docker", "start", name])
        deadline = time.monotonic() + 20
        while not (work / "rpc.sock").exists():
            state = json.loads(command(["docker", "inspect", name]).stdout)[0]["State"]
            assert state["Running"], "target exited before RPC readiness: %s" % state
            assert time.monotonic() < deadline, "RPC readiness timeout"
            time.sleep(0.1)
        (out / "running.json").write_text(command(["docker", "inspect", name]).stdout)
        assert rpc("bdev_set_options", {"bdev_io_pool_size": 1024, "bdev_io_cache_size": 32}) is True
        if args.memory == 256:
            try:
                rpc("framework_start_init")
            except (ConnectionError, OSError, RuntimeError) as exc:
                report["init_error"] = str(exc)
            else:
                raise AssertionError("expected initialization failure at 256 MiB")
            # Deliberately preserve the original triggering race: stop immediately
            # after the failed RPC, not after waiting for natural process exit.
        else:
            assert rpc("framework_start_init") is True
            methods = rpc("rpc_get_methods", {"current": True})
            report["rcow_methods"] = sorted(m for m in methods if m.startswith("rcow_"))
            assert report["rcow_methods"]
            with (work / "backing.img").open("xb") as backing:
                backing.truncate(32 * 1024 * 1024)
            assert rpc("bdev_aio_create", {"filename": "/work/backing.img", "name": "ShutdownAIO",
                                           "block_size": 4096}) == "ShutdownAIO"
            rpc("bdev_wait_for_examine")
            bdevs = rpc("bdev_get_bdevs", {"name": "ShutdownAIO"})
            assert len(bdevs) == 1 and bdevs[0]["num_blocks"] == 8192
            report["iostat"] = rpc("bdev_get_iostat", {"names": ["ShutdownAIO"]})
            assert rpc("bdev_aio_delete", {"name": "ShutdownAIO"}) is True
            assert rpc("bdev_get_bdevs") == []
        command(["docker", "stop", "--time=5", name])
        inspected = command(["docker", "inspect", name]).stdout
        (out / "exited.json").write_text(inspected)
        state = report["state"] = json.loads(inspected)[0]["State"]
        logs = command(["docker", "logs", name])
        text = logs.stdout + logs.stderr
        (out / "target.log").write_text(text)
        assert not state["Running"] and state["Pid"] == 0 and not state["OOMKilled"]
        assert state["ExitCode"] == (255 if args.memory == 256 else 0), state
        if args.memory == 256:
            assert "create bdev zero buffer failed" in text
            assert "spdk_app_start failed: -1" in text
            assert "s3lvol target ready" not in text
        else:
            assert "s3lvol target ready" in text
        report["passed"] = True
    except Exception:
        report["error"] = traceback.format_exc()
    finally:
        try:
            if created:
                command(["docker", "stop", "--time=5", name], check=False)
                logs = command(["docker", "logs", name], check=False)
                (out / "target.log").write_text(logs.stdout + logs.stderr)
                command(["docker", "rm", name])
                absent = command(["docker", "inspect", name], check=False)
                assert absent.returncode != 0
                report["container_absent"] = True
            # Remove only files in this invocation's private directory, after
            # container removal. Unexpected contents are preserved and fail QA.
            for filename in ("backing.img", "rpc.sock", "rpc.sock.lock"):
                (work / filename).unlink(missing_ok=True)
            for path in sorted(work.rglob("*"), key=lambda p: len(p.parts), reverse=True):
                if path.is_dir() and not path.is_symlink():
                    path.rmdir()
            assert not list(work.iterdir()), "unexpected runtime contents"
            work.rmdir()
            report["runtime_absent"] = True
        except Exception:
            report["cleanup_error"] = traceback.format_exc()
            report["passed"] = False
        os.close(fd)
        (out / "result.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({key: value for key, value in report.items() if key not in ("commands", "rpc")}, indent=2))
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
