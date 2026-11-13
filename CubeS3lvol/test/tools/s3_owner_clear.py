#!/usr/bin/env python3
# Copyright (c) 2026 Tencent Inc.
# SPDX-License-Identifier: Apache-2.0
#
#  Clear a strict (v2) owner marker left by a crashed holder.
#
#  Strict ownership has no force option on purpose (s3_owner.h, "Crash-marker
#  recovery"): a crashed holder leaves <lvs>/meta/owner in the v2 "held" state
#  and every later acquire is refused until the marker is cleared out of band.
#  This tool is that out-of-band step, done the conditional way:
#
#    1. GET the marker with its ETag.
#    2. Require exactly a v2 "held" document naming the node and pid the
#       caller has already proved dead (rcow_owner_is_stale in
#       rcow_common.sh). Anything else -- free, foreign, legacy, a different
#       holder -- is refused and nothing is written.
#    3. PUT a v2 "free" tombstone with a fresh nonce, If-Match the ETag read in
#       step 1. If the marker changed in between, the backend rejects the PUT
#       (412) and nothing is written.
#    4. GET it back and require the tombstone's nonce.
#
#  The tombstone uses exactly the fields owner_encode_v2() writes: an extra
#  key would make owner_classify() treat it as FOREIGN and the next acquire
#  would still refuse. A free tombstone is what a clean release leaves, so the
#  next strict acquire takes it with its normal If-Match CAS.
#
#  It does not decide whether the holder is dead. The caller must have stopped
#  or confirmed the death of the old writer first; this only refuses to touch
#  a marker that names anybody else.
#
#  === Usage ===
#
#    export AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=...
#    s3_owner_clear.py -e ENDPOINT -b BUCKET -r REGION --lvs NAME \
#                      --expect-node HOST --expect-pid PID \
#                      [--path-style] [--no-tls]
#
#  Exit 0 when the marker is now a free tombstone written by this run, 1 when
#  it was refused or failed (nothing written, or the outcome is reported), 2
#  for a usage error. Credentials from the environment only.

import argparse
import datetime
import hashlib
import hmac
import http.client
import json
import os
import socket
import sys
import time
import uuid

OWNER_KEY_FMT = "%s/meta/owner"
OWNER_V2 = 2
DOC_MAX = 4096


def _sign(key, msg):
    return hmac.new(key, msg.encode(), hashlib.sha256).digest()


def _quote_path(path):
    # SigV4 canonical URI: every byte except unreserved and '/' percent-encoded.
    out = []
    for ch in path.encode():
        c = chr(ch)
        if c.isalnum() or c in "-._~/":
            out.append(c)
        else:
            out.append("%%%02X" % ch)
    return "".join(out)


class Client:
    def __init__(self, endpoint, bucket, region, path_style, ak, sk, no_tls=False):
        self.bucket = bucket
        self.region = region
        self.path_style = path_style
        self.no_tls = no_tls
        self.ak = ak
        self.sk = sk
        self.host = endpoint if path_style else "%s.%s" % (bucket, endpoint)

    def _path(self, key):
        p = "/%s/%s" % (self.bucket, key) if self.path_style else "/%s" % key
        return _quote_path(p)

    def request(self, method, key, body=b"", extra=None):
        now = datetime.datetime.now(datetime.timezone.utc)
        amzdate = now.strftime("%Y%m%dT%H%M%SZ")
        datestamp = now.strftime("%Y%m%d")
        path = self._path(key)
        payload_sha = hashlib.sha256(body).hexdigest()
        canonical_headers = ("host:%s\nx-amz-content-sha256:%s\nx-amz-date:%s\n"
                             % (self.host, payload_sha, amzdate))
        signed_headers = "host;x-amz-content-sha256;x-amz-date"
        canonical_request = "\n".join([method, path, "", canonical_headers,
                                       signed_headers, payload_sha])
        scope = "%s/%s/s3/aws4_request" % (datestamp, self.region)
        to_sign = "\n".join(["AWS4-HMAC-SHA256", amzdate, scope,
                             hashlib.sha256(canonical_request.encode()).hexdigest()])
        k = _sign(("AWS4" + self.sk).encode(), datestamp)
        k = _sign(k, self.region)
        k = _sign(k, "s3")
        k = _sign(k, "aws4_request")
        sig = hmac.new(k, to_sign.encode(), hashlib.sha256).hexdigest()
        headers = {
            "Host": self.host,
            "x-amz-date": amzdate,
            "x-amz-content-sha256": payload_sha,
            "Authorization": ("AWS4-HMAC-SHA256 Credential=%s/%s, SignedHeaders=%s, "
                              "Signature=%s" % (self.ak, scope, signed_headers, sig)),
        }
        if extra:
            headers.update(extra)
        if method == "PUT":
            headers["Content-Length"] = str(len(body))
            headers["Content-Type"] = "application/json"
        cls = http.client.HTTPConnection if self.no_tls else http.client.HTTPSConnection
        conn = cls(self.host, timeout=30)
        try:
            conn.request(method, path, body=body if method == "PUT" else None,
                         headers=headers)
            resp = conn.getresponse()
            data = resp.read(DOC_MAX + 1)
            return resp.status, resp.getheader("ETag") or "", data
        finally:
            conn.close()


# Keys owner_decoders_v2 accepts. The decoder is not relaxed: any other key
# makes the document FOREIGN.
V2_KEYS = {"v", "state", "node_id", "pid", "attach_ts", "lvs_uuid", "nonce"}


def parse_v2(body):
    """Return the decoded v2 document, or None for anything else."""
    if not body or len(body) > DOC_MAX:
        return None
    try:
        doc = json.loads(body.decode())
    except (ValueError, UnicodeDecodeError):
        return None
    if not isinstance(doc, dict) or not set(doc).issubset(V2_KEYS):
        return None
    if doc.get("v") != OWNER_V2 or doc.get("state") not in ("held", "free"):
        return None
    try:
        if uuid.UUID(str(doc.get("nonce"))).int == 0:
            return None
    except ValueError:
        return None
    return doc


def encode_free(lvs_uuid):
    """The same layout as owner_encode_v2(): field order and set fixed."""
    nonce = str(uuid.uuid4())
    host = socket.gethostname()[:63]
    host = "".join(c if (c.isalnum() or c in "-._") else "_" for c in host) or "unknown"
    doc = ('{"v":%d,"state":"free","node_id":"%s","pid":%d,"attach_ts":%d,'
           '"lvs_uuid":"%s","nonce":"%s"}'
           % (OWNER_V2, host, os.getpid(), int(time.time()), lvs_uuid, nonce))
    return doc.encode(), nonce


def clear(client, lvs, expect_node, expect_pid):
    key = OWNER_KEY_FMT % lvs

    status, etag, body = client.request("GET", key)
    if status == 404:
        return 1, "no marker at %s; nothing to clear (the next acquire creates it)" % key
    if status != 200:
        return 1, "GET %s returned HTTP %d" % (key, status)
    if not etag:
        return 1, "GET %s returned no ETag; a conditional replace is impossible" % key

    doc = parse_v2(body)
    if doc is None:
        return 1, ("%s is not a strict v2 document (legacy, v1 or unparseable); "
                   "refusing to write over it" % key)
    if doc["state"] != "held":
        return 1, "%s is already free; nothing to clear" % key
    node = str(doc.get("node_id", ""))
    pid = doc.get("pid")
    if node != expect_node or pid != expect_pid:
        return 1, ("%s is held by node=%s pid=%s, not the expected node=%s pid=%s; "
                   "refusing" % (key, node, pid, expect_node, expect_pid))

    lvs_uuid = str(doc.get("lvs_uuid") or "00000000-0000-0000-0000-000000000000")
    new_body, nonce = encode_free(lvs_uuid)

    status, _, resp = client.request("PUT", key, body=new_body,
                                     extra={"If-Match": etag})
    if status in (409, 412):
        return 1, ("the conditional replace of %s was rejected (HTTP %d): the "
                   "marker changed since it was read; nothing was written" % (key, status))
    if status not in (200, 201, 204):
        # Inconclusive: read back and let the nonce decide.
        pass

    rs, _, rb = client.request("GET", key)
    back = parse_v2(rb) if rs == 200 else None
    if back is not None and back.get("state") == "free" and back.get("nonce") == nonce:
        return 0, ("%s replaced: held by node=%s pid=%s -> free tombstone "
                   "(nonce %s)" % (key, node, pid, nonce))
    return 1, ("%s: the replace returned HTTP %d and the read-back does not show "
               "this run's tombstone; the outcome is not proven" % (key, status))


def self_test():
    ok = True

    def check(name, cond):
        nonlocal ok
        print("  [%s] %s" % ("PASS" if cond else "FAIL", name))
        ok = ok and cond

    held = ('{"v":2,"state":"held","node_id":"App-2","pid":42,"attach_ts":1,'
            '"lvs_uuid":"044f07f3-85b6-4ff8-ad98-84fea3cc836b",'
            '"nonce":"18d3ea11-990f-4026-881f-cfb3ca0ec1e0"}').encode()
    check("held v2 parses", (parse_v2(held) or {}).get("state") == "held")
    check("legacy (no v) is foreign",
          parse_v2(b'{"node_id":"a","pid":1,"nonce":"18d3ea11-990f-4026-881f-cfb3ca0ec1e0"}') is None)
    check("unknown key is foreign",
          parse_v2(held.replace(b'"v":2', b'"v":2,"x":1')) is None)
    check("null nonce is foreign",
          parse_v2(held.replace(b"18d3ea11-990f-4026-881f-cfb3ca0ec1e0",
                                b"00000000-0000-0000-0000-000000000000")) is None)
    body, nonce = encode_free("044f07f3-85b6-4ff8-ad98-84fea3cc836b")
    d = parse_v2(body)
    check("tombstone is a v2 free document", d is not None and d["state"] == "free")
    check("tombstone has exactly the decoder keys", d is not None and set(d) == V2_KEYS)
    check("tombstone carries its nonce", d is not None and d["nonce"] == nonce)
    check("path-style path quotes the key",
          Client("h", "b", "r", True, "a", "s")._path("rcow-1/meta/owner")
          == "/b/rcow-1/meta/owner")
    print("result: %s" % ("ok" if ok else "FAILED"))
    return 0 if ok else 1


def main():
    p = argparse.ArgumentParser(description="clear a crashed holder's strict owner marker")
    p.add_argument("-e", "--endpoint")
    p.add_argument("-b", "--bucket")
    p.add_argument("-r", "--region", default="us-east-1")
    p.add_argument("--lvs")
    p.add_argument("--expect-node")
    p.add_argument("--expect-pid", type=int)
    p.add_argument("--path-style", action="store_true")
    p.add_argument("--no-tls", action="store_true")
    p.add_argument("--self-test", action="store_true")
    a = p.parse_args()

    if a.self_test:
        return self_test()
    if not (a.endpoint and a.bucket and a.lvs and a.expect_node and a.expect_pid):
        print("need -e, -b, --lvs, --expect-node and --expect-pid", file=sys.stderr)
        return 2
    if a.expect_pid <= 0:
        print("--expect-pid must be positive", file=sys.stderr)
        return 2
    try:
        ak = os.environ["AWS_ACCESS_KEY_ID"]
        sk = os.environ["AWS_SECRET_ACCESS_KEY"]
    except KeyError as e:
        print("%s is not set" % e.args[0], file=sys.stderr)
        return 2

    client = Client(a.endpoint, a.bucket, a.region, a.path_style, ak, sk, a.no_tls)
    try:
        rc, msg = clear(client, a.lvs, a.expect_node, a.expect_pid)
    except (OSError, http.client.HTTPException) as e:
        rc, msg = 1, "request failed: %s" % e
    print(msg, file=sys.stderr if rc else sys.stdout)
    return rc


if __name__ == "__main__":
    sys.exit(main())
