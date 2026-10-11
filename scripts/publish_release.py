#!/usr/bin/env python3
"""Publish verified, same-run artifacts only for the validated version-tag push."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys


def publish(version, directory):
    if not re.fullmatch(r"v[0-9]+\.[0-9]+\.[0-9]+(?:-[A-Za-z0-9][A-Za-z0-9.-]*)?", version):
        raise ValueError("expected a safe vMAJOR.MINOR.PATCH[-prerelease] version")
    if (os.environ.get("GITHUB_EVENT_NAME"), os.environ.get("GITHUB_REF_TYPE"),
            os.environ.get("GITHUB_REF_NAME")) != ("push", "tag", version):
        raise ValueError("publication requires the matching version-tag push")
    commit = os.environ.get("GITHUB_SHA", "")
    repo = os.environ.get("GITHUB_REPOSITORY", "")
    if not re.fullmatch(r"[0-9a-f]{40}", commit) or not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", repo):
        raise ValueError("missing validated commit or repository context")

    directory = Path(directory)
    targets = [(system, arch) for system in ("darwin", "linux") for arch in ("amd64", "arm64")]
    binaries = {f"HappySync-{version}-{system}-{arch}": (system, arch) for system, arch in targets}
    names = set(binaries) | {"SHA256SUMS", "build.json"}
    if {p.name for p in directory.iterdir()} != names or any(not (directory / n).is_file() for n in names):
        raise ValueError("expected exactly four Linux/macOS binaries, SHA256SUMS, and build.json")
    metadata = json.loads((directory / "build.json").read_text())
    if metadata["version"] != version or metadata["cgo_enabled"] is not False:
        raise ValueError("artifact metadata does not match the release version/build")
    artifacts = metadata["artifacts"]
    if len(artifacts) != 4 or {a["file"] for a in artifacts} != set(binaries):
        raise ValueError("artifact metadata does not contain the four supported targets")
    checksums = {}
    for line in (directory / "SHA256SUMS").read_text().splitlines():
        match = re.fullmatch(r"([0-9a-f]{64})  (.+)", line)
        if not match or match[2] in checksums:
            raise ValueError("invalid or duplicate checksum entry")
        checksums[match[2]] = match[1]
    if set(checksums) != set(binaries):
        raise ValueError("checksums do not cover exactly the four binaries")
    for artifact in artifacts:
        name = artifact["file"]
        path = directory / name
        with path.open("rb") as binary:
            digest = hashlib.file_digest(binary, "sha256").hexdigest()
        if ((artifact["goos"], artifact["goarch"]) != binaries[name]
                or artifact["size"] != path.stat().st_size
                or digest != artifact["sha256"] or digest != checksums[name]):
            raise ValueError(f"artifact integrity check failed: {name}")

    remote = subprocess.check_output(
        ["gh", "api", f"repos/{repo}/commits/{version}", "--jq", ".sha"], text=True).strip()
    if remote != commit:
        raise ValueError("remote tag no longer points to the validated commit")
    prerelease = "-" in version
    assets = [str(directory / name) for name in sorted(binaries)]
    assets += [str(directory / name) for name in ("SHA256SUMS", "build.json")]
    create = ["gh", "release", "create", version, *assets, "--repo", repo,
              "--verify-tag", "--generate-notes", "--title", f"HappySync {version}",
              "--draft", "--latest=false"]
    if prerelease:
        create.append("--prerelease")
    # A failed upload stays a draft. An existing release is never overwritten.
    subprocess.run(create, check=True)
    subprocess.run(["gh", "release", "edit", version, "--repo", repo,
                    "--draft=false", f"--latest={'false' if prerelease else 'true'}"], check=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("version")
    parser.add_argument("directory")
    args = parser.parse_args()
    try:
        publish(args.version, args.directory)
    except (ValueError, KeyError, TypeError, OSError, subprocess.CalledProcessError) as error:
        print(f"Release publication failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
