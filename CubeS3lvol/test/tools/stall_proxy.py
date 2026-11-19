#!/usr/bin/env python3
# Copyright (c) 2026 Tencent Inc.
# SPDX-License-Identifier: Apache-2.0
#
#  A TCP forwarder that can be told to stop moving bytes without closing
#  anything: the shape of a degraded object store, where connections stay open
#  and requests simply never finish. Used by run_quiesce_unload_test.sh to hold
#  host reads inside S3 GETs while the target is being stopped.
#
#  Usage:
#    stall_proxy.py LISTEN_PORT TARGET_HOST TARGET_PORT CONTROL_FILE
#
#  While CONTROL_FILE exists no byte is forwarded in either direction and every
#  socket stays open. Removing the file resumes forwarding.
#
#  Degraded mode: if CONTROL_FILE holds a number N > 0, bytes do flow but every
#  forwarded segment (up to 64 KiB) is delayed by N milliseconds -- a slow
#  object store rather than a dead one (app2: ~70 s per 1 MiB PUT).

import os
import select
import socket
import sys
import threading
import time


def delay_ms(ctl):
    try:
        with open(ctl) as f:
            v = f.read().strip()
        return int(v) if v else -1
    except FileNotFoundError:
        return 0
    except ValueError:
        return -1


def wait_gate(ctl):
    """-1 (file present, no number): stall. N > 0: sleep N ms once."""
    while True:
        d = delay_ms(ctl)
        if d >= 0:
            if d > 0:
                time.sleep(d / 1000.0)
            return
        time.sleep(0.05)


def pump(a, b, ctl):
    try:
        while True:
            wait_gate(ctl)
            r, _, _ = select.select([a], [], [], 0.2)
            if not r:
                continue
            data = a.recv(65536)
            if not data:
                break
            while delay_ms(ctl) < 0:
                time.sleep(0.05)
            b.sendall(data)
    except OSError:
        pass
    finally:
        for s in (a, b):
            try:
                s.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass


def main():
    if len(sys.argv) != 5:
        sys.stderr.write("usage: stall_proxy.py LISTEN_PORT HOST PORT CONTROL_FILE\n")
        sys.exit(2)
    lport, host, port, ctl = int(sys.argv[1]), sys.argv[2], int(sys.argv[3]), sys.argv[4]
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("127.0.0.1", lport))
    srv.listen(256)
    while True:
        c, _ = srv.accept()
        try:
            u = socket.create_connection((host, port), timeout=5)
            u.settimeout(None)
        except OSError:
            c.close()
            continue
        threading.Thread(target=pump, args=(c, u, ctl), daemon=True).start()
        threading.Thread(target=pump, args=(u, c, ctl), daemon=True).start()


if __name__ == "__main__":
    main()
