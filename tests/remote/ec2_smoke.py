#!/usr/bin/env python3
"""Validate installed HappySync binaries across SSH and a real public socket.

Infrastructure provisioning and termination belong to the caller. This program
uses disposable private roots, never logs invitations, and stops its own workers.
"""
import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import shutil
import subprocess
import sys
import tempfile
import time


REMOTE_FS = r'''
import base64, hashlib, json, os, pathlib, re, shutil, signal, subprocess, sys, tempfile, time
q=json.load(sys.stdin)
if q['op']=='init':
 print(json.dumps(tempfile.mkdtemp(prefix='happysync-remote-',dir='/tmp')))
 sys.exit(0)
root=pathlib.Path(q['root'])
assert root.parent==pathlib.Path('/tmp') and root.name.startswith('happysync-remote-')
op=q['op']
if op=='cleanup': shutil.rmtree(root); result=True
elif op=='server_start':
 (root/'launch-attempted').write_text('started')
 state=root/'state'
 command=[q['binary'],'serve','--relay','--discovery','--listen','0.0.0.0:'+str(q['port']),
          '--advertise','/ip4/'+q['ip']+'/tcp/'+str(q['port']), '--state-dir',str(state)]
 with (root/'stdout').open('wb') as out, (root/'stderr').open('wb') as err:
  child=subprocess.Popen(command,stdin=subprocess.DEVNULL,stdout=out,stderr=err,start_new_session=True,close_fds=True)
 (root/'pid').write_text(str(child.pid)); result=child.pid
elif op=='server_status':
 output=(root/'stdout').read_text(errors='replace')
 match=re.search(r'^Peer ID: (\S+)$',output,re.M)
 metrics={}
 with (root/'stderr').open('rb') as f:
  f.seek(max(0,os.fstat(f.fileno()).st_size-65536)); lines=f.read().decode(errors='replace').splitlines()
 for line in lines:
  try:
   record=json.loads(line)
   if record.get('event')=='server_metrics': metrics=record['metrics']
  except (ValueError,TypeError): pass
 result={'peer_id':match.group(1) if match else None,'metrics':metrics,'pid':int((root/'pid').read_text())}
elif op=='server_stop':
 if not (root/'pid').exists():
  # An SSH response can disappear after spawning but before recording the PID.
  # The pre-spawn marker makes that case uncertain, never safe to erase.
  print(json.dumps(not (root/'launch-attempted').exists()))
  sys.exit(0)
 pid=int((root/'pid').read_text())
 def alive():
  try:
   command=pathlib.Path('/proc/'+str(pid)+'/cmdline').read_bytes().split(b'\0')
   if str(root/'state').encode() not in command: return False
   stat=pathlib.Path('/proc/'+str(pid)+'/stat').read_text()
   return stat.split(') ',1)[1].split()[0]!='Z'
  except FileNotFoundError: return False
 if alive(): os.kill(pid,signal.SIGTERM)
 deadline=time.monotonic()+15
 while alive() and time.monotonic()<deadline: time.sleep(.2)
 if alive(): os.kill(pid,signal.SIGKILL)
 deadline=time.monotonic()+3
 while alive() and time.monotonic()<deadline: time.sleep(.2)
 result=not alive()
elif op=='client_alive':
 pid=int(q['pid'])
 try:
  command=pathlib.Path('/proc/'+str(pid)+'/cmdline').read_bytes().split(b'\0')
  stat=pathlib.Path('/proc/'+str(pid)+'/stat').read_text()
  result=str(root).encode() in command and stat.split(') ',1)[1].split()[0]!='Z'
 except FileNotFoundError: result=False
elif op=='tree':
 result={}
 for directory, dirs, files in os.walk(root):
  dirs[:]=sorted(d for d in dirs if d not in ('.HappySync','.git','node_modules'))
  for name in sorted(files):
   p=pathlib.Path(directory)/name
   if p.is_symlink() or name.endswith('.tmp'): continue
   h=hashlib.sha256()
   with p.open('rb') as f:
    for chunk in iter(lambda:f.read(1024*1024), b''): h.update(chunk)
   result[p.relative_to(root).as_posix()]=h.hexdigest()
else:
 relative=pathlib.PurePosixPath(q['path'])
 assert not relative.is_absolute() and '..' not in relative.parts
 p=root.joinpath(*relative.parts)
 if op=='write':
  p.parent.mkdir(parents=True,exist_ok=True)
  p.write_bytes(base64.b64decode(q['data'])); result=True
 elif op=='delete': p.unlink(); result=True
 elif op=='read': result=base64.b64encode(p.read_bytes()).decode()
 else: raise ValueError('unknown operation')
print(json.dumps(result))
'''


def digest(data):
    return hashlib.sha256(data).hexdigest()


def redact(text):
    return re.sub(r"hsync[0-9]+:[A-Za-z0-9_-]+", "[redacted invitation]", text)


class Suite:
    def __init__(self, args):
        self.args = args
        self.work = Path(tempfile.mkdtemp(prefix="happysync-remote-host-"))
        self.local = self.work / "root"
        self.local.mkdir(mode=0o700)
        self.remote = None
        self.server_root = self.server_peer_id = None
        self.server_running = False
        self.local_running = self.remote_running = False
        self.launch_confirmed = {False: False, True: False}
        self.worker_pids = {False: None, True: None}
        self.peer_id = self.remote_peer_id = None
        self.results = []
        self.started = time.monotonic()
        self.ssh = ["ssh", "-i", args.ssh_key, "-o", "BatchMode=yes", "-o", "IdentitiesOnly=yes",
                    "-o", "ConnectTimeout=10", "-o", "ServerAliveInterval=10",
                    "-o", "ServerAliveCountMax=2", "-o", "StrictHostKeyChecking=accept-new",
                    "-o", "UserKnownHostsFile=" + str(self.work / "known_hosts"),
                    args.ssh_user + "@" + args.host]

    def run(self, command, data=b"", timeout=60):
        result = subprocess.run(command, input=data, stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE, timeout=timeout)
        if result.returncode:
            raise RuntimeError("command failed: " + redact(result.stderr.decode(errors="replace"))[-4000:])
        return result.stdout.decode()

    def fs(self, op, **values):
        request = dict(op=op, root=values.pop('root', self.remote), **values)
        output = self.run(self.ssh + ["python3 -c " + shlex.quote(REMOTE_FS)], json.dumps(request).encode())
        return json.loads(output)

    def cli(self, remote, *args):
        binary = self.args.remote_binary if remote else self.args.local_binary
        root = self.remote if remote else str(self.local)
        command = [binary, *args, "--folder", root]
        if remote:
            command = self.ssh + [shlex.join(command)]
        return self.run(command)

    def status(self, remote):
        result = self.cli(remote, "status", "--json")
        if "hsync" in result:
            raise RuntimeError("status leaked invitation marker")
        return json.loads(result)

    def start(self, remote):
        args = ["start", "--background", "--no-mdns", "--no-dht", "--scan-interval", "1s", "--ignore", "*.tmp"]
        if self.args.transport == "relay":
            server_ip = "127.0.0.1" if remote else self.args.remote_ip
            server = f"/ip4/{server_ip}/tcp/{self.args.port}/p2p/{self.server_peer_id}"
            client_port = self.args.port + (1 if remote else 2)
            args += ["--listen", f"/ip4/127.0.0.1/tcp/{client_port}",
                     "--relay-peer", server, "--discovery-server", server]
            if not remote:
                if not getattr(self.args, "discovery_only", False):
                    args += ["--peer", server + "/p2p-circuit/p2p/" + self.remote_peer_id]
                args += ["--key-file", str(self.work / "invitation")]
        elif remote:
            protocol = f"/ip4/0.0.0.0/tcp/{self.args.port}" if self.args.transport == "tcp" else f"/ip4/0.0.0.0/udp/{self.args.port}/quic-v1"
            args += ["--listen", protocol]
        else:
            endpoint = f"/ip4/{self.args.remote_ip}/tcp/{self.args.port}" if self.args.transport == "tcp" else f"/ip4/{self.args.remote_ip}/udp/{self.args.port}/quic-v1"
            args += ["--listen", "/ip4/0.0.0.0/tcp/0", "--peer", endpoint + "/p2p/" + self.remote_peer_id,
                     "--key-file", str(self.work / "invitation")]
        if remote:
            self.remote_running = True
        else:
            self.local_running = True
        self.launch_confirmed[remote] = False
        self.worker_pids[remote] = None
        output = self.cli(remote, *args)
        key = re.search(r"^Share key: (hsync1:[A-Za-z0-9_-]+)$", output, re.M)
        if not key:
            raise RuntimeError("background start did not return invitation")
        key_file = self.work / "invitation"
        if key_file.exists():
            if key_file.read_text() != key.group(1):
                raise RuntimeError("invitation changed after restart")
        else:
            fd = os.open(key_file, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
            with os.fdopen(fd, "w") as stream:
                stream.write(key.group(1))
        state = self.status(remote)
        attribute = "remote_peer_id" if remote else "peer_id"
        previous = getattr(self, attribute)
        if previous and previous != state["peer_id"]:
            raise RuntimeError("identity changed after restart")
        setattr(self, attribute, state["peer_id"])
        if not state.get("pid") or state["state"] == "stopped":
            raise RuntimeError("background worker not ready")
        self.worker_pids[remote] = state["pid"]
        self.launch_confirmed[remote] = True
        if self.args.transport == "relay":
            self.wait("relay reservation", lambda: self.status(remote)["peers"]["relay"] == "reservation active")
            self.wait("hosted discovery registration", lambda: self.status(remote)["peers"]["discovery"] == "registered")

    def stop(self, remote):
        state = self.status(remote)
        if state["state"] == "stopped" and not self.launch_confirmed[remote]:
            def uncertain_worker_ready():
                observed = self.status(remote)
                if observed["state"] == "stopped" or not observed.get("pid"):
                    return False
                self.worker_pids[remote] = observed["pid"]
                self.launch_confirmed[remote] = True
                return True
            self.wait("uncertain launch cleanup; retaining root if no worker can be verified", uncertain_worker_ready, 20)
            state = self.status(remote)
        if state["state"] != "stopped":
            self.worker_pids[remote] = state.get("pid") or self.worker_pids[remote]
            try:
                self.cli(remote, "stop")
            except RuntimeError:
                # A previous stop can complete between status and this request.
                if self.status(remote)["state"] != "stopped":
                    raise
        self.wait("remote stop" if remote else "local stop",
                  lambda: self.status(remote)["state"] == "stopped" and not self.worker_alive(remote), 20)
        if remote:
            self.remote_running = False
        else:
            self.local_running = False

    def worker_alive(self, remote):
        pid = self.worker_pids[remote]
        if not pid:
            return False
        if remote:
            return self.fs("client_alive", pid=pid)
        try:
            os.kill(pid, 0)
            return True
        except ProcessLookupError:
            return False

    def write(self, remote, path, data):
        if remote:
            self.fs("write", path=path, data=base64.b64encode(data).decode())
        else:
            target = self.local / path
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(data)

    def tree(self, remote):
        if remote:
            return self.fs("tree")
        result = {}
        for directory, dirs, files in os.walk(self.local):
            dirs[:] = sorted(d for d in dirs if d not in (".HappySync", ".git", "node_modules"))
            for name in files:
                path = Path(directory) / name
                if path.is_symlink() or name.endswith(".tmp"):
                    continue
                result[path.relative_to(self.local).as_posix()] = digest(path.read_bytes())
        return result

    def wait(self, label, condition, timeout=None):
        deadline = time.monotonic() + (timeout or self.args.timeout)
        last = None
        while time.monotonic() < deadline:
            try:
                if condition():
                    return
            except (RuntimeError, json.JSONDecodeError, OSError) as error:
                last = redact(str(error))
            time.sleep(1)
        raise RuntimeError(label + " timed out" + (": " + last if last else ""))

    def converge(self, label, expected):
        self.wait(label, lambda: self.tree(False) == expected and self.tree(True) == expected)
        self.results.append(label)
        print("PASS " + label, flush=True)

    def execute(self):
        if self.args.transport == "relay":
            self.server_root = self.fs("init")
            self.server_running = True
            self.fs("server_start", root=self.server_root, binary=self.args.remote_binary,
                    port=self.args.port, ip=self.args.remote_ip)
            self.wait("public relay server readiness", lambda: self.fs("server_status", root=self.server_root)["peer_id"] is not None)
            self.server_peer_id = self.fs("server_status", root=self.server_root)["peer_id"]
        self.remote = self.fs("init")
        self.start(True)
        self.start(False)
        self.wait("authenticated public connection", lambda: self.status(False)["peers"]["authenticated"] >= 1)
        if getattr(self.args, "discovery_only", False):
            self.results.append("hosted discovery-only authenticated circuit connection")
            print("PASS hosted discovery-only authenticated circuit connection", flush=True)
        if self.args.transport == "relay":
            self.wait("forced relayed connection", lambda: self.status(False)["peers"]["relayed"] > 0)
            if self.status(False)["peers"]["direct"] != 0:
                raise RuntimeError("relay fixture unexpectedly established a direct sync path")
        expected = {}
        # More than two protocol blocks exercises hashing/staging over the WAN.
        large = b"".join(hashlib.sha256(str(n).encode()).digest() for n in range(327681))
        initial = {"nested/large.bin": large, "delete-me.txt": b"delete later\n",
                   "offline.txt": b"original\n", "conflict.txt": b"base\n"}
        for path, data in initial.items():
            self.write(False, path, data)
            expected[path] = digest(data)
        self.converge("macOS to Linux multiblock creation", expected)
        self.write(True, "from-linux.txt", b"native Linux creation\n")
        expected["from-linux.txt"] = digest(b"native Linux creation\n")
        self.write(True, "nested/large.bin", b"Linux replacement\n")
        expected["nested/large.bin"] = digest(b"Linux replacement\n")
        self.converge("Linux to macOS creation and replacement", expected)
        self.fs("delete", path="from-linux.txt")
        del expected["from-linux.txt"]
        self.converge("Linux deletion", expected)
        self.stop(True)
        self.write(False, "offline.txt", b"edited while EC2 offline\n")
        expected["offline.txt"] = digest(b"edited while EC2 offline\n")
        (self.local / "delete-me.txt").unlink()
        del expected["delete-me.txt"]
        time.sleep(4)
        self.start(True)
        self.converge("offline edit/delete catchup and stable identity", expected)
        # Both clients must observe their edits independently before reconnection.
        self.stop(False)
        self.stop(True)
        self.write(False, "conflict.txt", b"offline Mac revision\n")
        self.write(True, "conflict.txt", b"offline Linux revision\n")
        self.start(True)
        time.sleep(4)
        self.start(False)
        def conflicted():
            left, right = self.tree(False), self.tree(True)
            copies = {p: h for p, h in left.items() if p == "conflict.txt" or p.startswith("conflict.txt.happysync-conflict-")}
            unchanged = {p: h for p, h in expected.items() if p != "conflict.txt"}
            return (left == right and {p: h for p, h in left.items() if p not in copies} == unchanged
                    and len(copies) == 2
                    and set(copies.values()) == {digest(b"offline Mac revision\n"), digest(b"offline Linux revision\n")})
        self.wait("concurrent offline content preservation", conflicted)
        expected = self.tree(False)
        self.results.append("concurrent offline content preservation")
        print("PASS concurrent offline content preservation", flush=True)
        for remote, name in [(False, "mac"), (True, "linux")]:
            for path in [".git/local", "node_modules/local", "scratch.tmp"]:
                self.write(remote, path, (name + " private").encode())
        time.sleep(5)
        self.converge("default and configured ignores", expected)
        for path in [".git/local", "node_modules/local", "scratch.tmp"]:
            if (self.local / path).read_bytes() != b"mac private" or base64.b64decode(self.fs("read", path=path)) != b"linux private":
                raise RuntimeError("ignored content was overwritten")
        self.stop(False)
        self.stop(True)
        self.start(True)
        self.start(False)
        self.converge("persisted conflict hashes after restart", expected)
        # Equal persisted trees can be observed before the restarted peers have
        # reconnected. New bytes from both devices prove live synchronization.
        for remote, name in [(False, "mac"), (True, "linux")]:
            path = "post-restart/" + name + ".txt"
            data = (name + " bytes created after final restart\n").encode()
            self.write(remote, path, data)
            expected[path] = digest(data)
        self.converge("bidirectional synchronization after final restart", expected)
        def final_connected():
            for side in [False, True]:
                peers = self.status(side)["peers"]
                if peers["authenticated"] < 1 or peers["connected"] < 1:
                    return False
                if self.args.transport == "relay" and (peers["relayed"] < 1 or peers["direct"] != 0 or peers["discovery"] != "registered"):
                    return False
            return True
        self.wait("authenticated connected path after final restart", final_connected)
        server_status = None
        if self.args.transport == "relay":
            def valid_metrics():
                metrics = self.fs("server_status", root=self.server_root)["metrics"]
                return metrics.get("relay_forwarded_bytes", 0) > len(large) and metrics.get("discovery_registrations", 0) >= 2
            self.wait("relay byte forwarding and hosted registrations", valid_metrics, 45)
            server_status = self.fs("server_status", root=self.server_root)
            self.results.append("forced circuit relay and hosted discovery registrations")
            print("PASS forced circuit relay and hosted discovery registrations", flush=True)
        report = {"passed": self.results, "transport": self.args.transport,
                "peer_discovery": ("hosted discovery only; no manual peer hint" if getattr(self.args, "discovery_only", False)
                                   else "manual circuit peer hint with hosted registration" if self.args.transport == "relay"
                                   else "manual direct peer hint"),
                "elapsed_seconds": round(time.monotonic() - self.started, 2),
                "file_sha256": expected, "local_status": self.status(False), "remote_status": self.status(True)}
        if server_status:
            report["server_status"] = server_status
        return report

    def cleanup(self):
        failures = []
        for remote in [False, True]:
            running = self.remote_running if remote else self.local_running
            if running:
                try:
                    self.stop(remote)
                except Exception as error:
                    failures.append(redact(str(error)))
        if self.server_running:
            try:
                if not self.fs("server_stop", root=self.server_root):
                    raise RuntimeError("relay server did not stop")
                self.server_running = False
            except Exception as error:
                failures.append(redact(str(error)))
        # Keep state when stopping failed: never erase a root used by a live worker.
        if not failures:
            if self.remote:
                try:
                    self.fs("cleanup")
                except Exception as error:
                    failures.append(redact(str(error)))
            if self.server_root:
                try:
                    self.fs("cleanup", root=self.server_root)
                except Exception as error:
                    failures.append(redact(str(error)))
            if not failures:
                try:
                    shutil.rmtree(self.work)
                except OSError as error:
                    failures.append(redact(str(error)))
        if failures:
            print("Cleanup warning: " + "; ".join(failures), file=sys.stderr)
            return False
        return True


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", required=True)
    parser.add_argument("--ssh-user", default="ubuntu")
    parser.add_argument("--ssh-key", required=True)
    parser.add_argument("--remote-binary", required=True)
    parser.add_argument("--local-binary", required=True)
    parser.add_argument("--remote-ip")
    parser.add_argument("--port", type=int, default=4001)
    parser.add_argument("--transport", choices=["tcp", "quic", "relay"], default="tcp")
    parser.add_argument("--discovery-only", action="store_true",
                        help="omit the manual circuit peer hint; requires --transport relay")
    parser.add_argument("--timeout", type=int, default=120)
    args = parser.parse_args()
    args.remote_ip = args.remote_ip or args.host
    args.ssh_key = str(Path(args.ssh_key).resolve())
    args.local_binary = str(Path(args.local_binary).resolve())
    if not 1 <= args.port <= (65533 if args.transport == "relay" else 65535) or args.timeout <= 0:
        parser.error("invalid port or timeout")
    if args.discovery_only and args.transport != "relay":
        parser.error("--discovery-only requires --transport relay")
    suite = Suite(args)
    success = False
    try:
        report = suite.execute()
        print(json.dumps(report, indent=2, sort_keys=True), flush=True)
        success = True
    except Exception as error:
        print("FAIL " + redact(str(error)), file=sys.stderr)
        for remote in [False, True]:
            try:
                print(json.dumps({"side": "remote" if remote else "local", "status": suite.status(remote)}), file=sys.stderr)
            except Exception:
                pass
    finally:
        success = suite.cleanup() and success
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
