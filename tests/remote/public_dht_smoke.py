#!/usr/bin/env python3
"""Bounded default public-DHT discovery check against an existing SSH host.

Provisioning, public TCP/UDP port rules, and instance termination belong to the
caller. Fresh roots prevent persisted manual peers or disabled DHT preferences
from changing the check. Startup invitations never reach the report.
"""
import argparse
import base64
import json
import os
from pathlib import Path
import re
import sys
import time

from ec2_smoke import Suite, digest, redact


class PublicDHTSuite(Suite):
    def safe_status(self, remote):
        state = self.status(remote)
        return {name: state.get(name) for name in (
            "state", "peer_id", "addresses", "peers", "pending_files",
            "received_bytes", "sent_bytes", "errors", "warnings",
        )}

    def start_public(self, remote, initialize=False):
        args = ["start", "--background", "--no-mdns", "--scan-interval", "1s"]
        if remote and initialize:
            # The CLI accepts one listen flag. Prepare its private persisted
            # configuration while stopped to enable both transports below.
            args += ["--listen", f"/ip4/0.0.0.0/tcp/{self.args.port}"]
        if not remote:
            args += ["--key-file", str(self.work / "invitation")]
        # A failed launcher may already have detached a worker. Cleanup must
        # probe this root even when start raises before readiness is received.
        self.launch_confirmed[remote] = False
        self.worker_pids[remote] = None
        if remote:
            self.remote_running = True
        else:
            self.local_running = True
        output = self.cli(remote, *args)
        invitation = re.search(r"^Share key: (hsync1:[A-Za-z0-9_-]+)$", output, re.M)
        if not invitation:
            raise RuntimeError("public-DHT startup did not return an invitation")
        path = self.work / "invitation"
        if path.exists():
            if path.read_text() != invitation.group(1):
                raise RuntimeError("public-DHT invitation changed")
        else:
            fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
            with os.fdopen(fd, "w") as stream:
                stream.write(invitation.group(1))
        state = self.status(remote)
        if state.get("state") == "stopped" or not state.get("pid"):
            raise RuntimeError("public-DHT worker not ready")
        self.worker_pids[remote] = state["pid"]
        self.launch_confirmed[remote] = True
        if state["peers"]["mdns"] != "disabled" or state["peers"]["dht"] == "disabled":
            raise RuntimeError("public-DHT check has incorrect discovery preferences")
        if state["peers"]["discovery"] != "not configured" or state["peers"]["relay"] != "not configured":
            raise RuntimeError("public-DHT check has an alternative discovery path")

    def execute(self):
        self.remote = self.fs("init")
        self.start_public(True, initialize=True)
        self.stop(True)
        config = json.loads(base64.b64decode(self.fs("read", path=".HappySync/config.json")))
        if any(config.get(name) for name in ("peers", "bootstrap", "relays", "discovery_servers", "disable_dht")):
            raise RuntimeError("fresh public-DHT configuration unexpectedly contains hints")
        config["listen"] = [f"/ip4/0.0.0.0/tcp/{self.args.port}",
                            f"/ip4/0.0.0.0/udp/{self.args.port}/quic-v1"]
        self.fs("write", path=".HappySync/config.json",
                data=base64.b64encode(json.dumps(config).encode()).decode())
        self.start_public(True)
        self.start_public(False)
        expected = {"from-mac.txt": digest(b"public-DHT Mac marker\n"),
                    "from-linux.txt": digest(b"public-DHT Linux marker\n")}
        self.write(False, "from-mac.txt", b"public-DHT Mac marker\n")
        self.write(True, "from-linux.txt", b"public-DHT Linux marker\n")
        deadline = time.monotonic() + self.args.timeout
        observations = []
        while time.monotonic() < deadline:
            local, remote = self.safe_status(False), self.safe_status(True)
            observations.append({"elapsed_seconds": round(time.monotonic() - self.started, 2),
                                 "local": local, "remote": remote})
            for state in (local, remote):
                if state["state"] == "stopped":
                    raise RuntimeError("public-DHT worker stopped during discovery")
            if all(state["peers"]["connected"] > 0 and state["peers"]["authenticated"] > 0
                   for state in (local, remote)):
                if not any(state["peers"]["discovered"] > 0 for state in (local, remote)):
                    raise RuntimeError("authenticated connection lacked any DHT candidate")
                if self.tree(False) == expected and self.tree(True) == expected:
                    return {"passed": ["default public-DHT-only authenticated discovery",
                                       "bidirectional full SHA256 marker verification"],
                            "transport": "public-dht-default-bootstrap",
                            "elapsed_seconds": round(time.monotonic() - self.started, 2),
                            "file_sha256": expected, "observations": observations,
                            "local_status": local, "remote_status": remote}
            time.sleep(min(5, max(0, deadline - time.monotonic())))
        print(json.dumps({"result": "timeout", "observations": observations,
                          "local_file_sha256": self.tree(False),
                          "remote_file_sha256": self.tree(True)}, indent=2), file=sys.stderr)
        raise RuntimeError(f"default public-DHT-only discovery/content verification timed out after {self.args.timeout}s")

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", required=True)
    parser.add_argument("--ssh-user", default="ec2-user")
    parser.add_argument("--ssh-key", required=True)
    parser.add_argument("--remote-binary", required=True)
    parser.add_argument("--local-binary", required=True)
    parser.add_argument("--remote-ip")
    parser.add_argument("--port", type=int, default=4103)
    parser.add_argument("--timeout", type=int, default=240)
    args = parser.parse_args()
    args.remote_ip = args.remote_ip or args.host
    args.ssh_key = str(Path(args.ssh_key).resolve())
    args.local_binary = str(Path(args.local_binary).resolve())
    if not 1 <= args.port <= 65535 or args.timeout <= 0:
        parser.error("invalid port or timeout")
    suite = PublicDHTSuite(args)
    success = False
    try:
        report = suite.execute()
        print(json.dumps(report, indent=2, sort_keys=True), flush=True)
        success = True
    except Exception as error:
        print("FAIL " + redact(str(error)), file=sys.stderr)
        for remote in (False, True):
            if remote and not suite.remote:
                continue
            try:
                print(json.dumps({"side": "remote" if remote else "local",
                                  "status": suite.safe_status(remote)}), file=sys.stderr)
            except Exception:
                pass
    finally:
        success = suite.cleanup() and success
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
