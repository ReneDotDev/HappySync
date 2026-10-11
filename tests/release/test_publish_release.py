"""Exercise release publication without network access or a GitHub token."""

import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


REPOSITORY = Path(__file__).resolve().parents[2]
PUBLISHER = REPOSITORY / "scripts" / "publish_release.py"
COMMIT = "1234567890abcdef1234567890abcdef12345678"


class PublishReleaseTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="HappySync release ")
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.dist = self.root / "artifacts with spaces"
        self.dist.mkdir()
        self.calls = self.root / "calls.jsonl"
        self.bin = self.root / "bin"
        self.bin.mkdir()
        gh = self.bin / "gh"
        gh.write_text(
            "#!" + sys.executable + "\n"
            "import json, os, sys\n"
            "args = sys.argv[1:]\n"
            "with open(os.environ['FAKE_GH_CALLS'], 'a') as log:\n"
            "    log.write(json.dumps(args) + '\\n')\n"
            "if args[:1] == ['api']:\n"
            "    print(os.environ['FAKE_GH_SHA'])\n"
            "elif args[:2] == ['release', 'create']:\n"
            "    if os.environ.get('FAKE_GH_CREATE_FAIL') == '1':\n"
            "        print('simulated creation or upload failure', file=sys.stderr)\n"
            "        sys.exit(1)\n"
            "elif args[:2] != ['release', 'edit']:\n"
            "    sys.exit(2)\n",
            encoding="utf-8",
        )
        gh.chmod(0o755)
        self.environment = os.environ.copy()
        self.environment.update(
            PATH=str(self.bin) + os.pathsep + self.environment.get("PATH", ""),
            GITHUB_EVENT_NAME="push",
            GITHUB_REF_TYPE="tag",
            GITHUB_SHA=COMMIT,
            GITHUB_REPOSITORY="example/HappySync",
            FAKE_GH_CALLS=str(self.calls),
            FAKE_GH_SHA=COMMIT,
        )
        self.version = "v1.2.3"
        self.make_artifacts(self.version)

    def make_artifacts(self, version):
        self.version = version
        self.environment["GITHUB_REF_NAME"] = version
        artifacts = []
        checksums = []
        for goos in ("darwin", "linux"):
            for goarch in ("amd64", "arm64"):
                name = f"HappySync-{version}-{goos}-{goarch}"
                data = f"fixture {version} {goos} {goarch}\n".encode()
                (self.dist / name).write_bytes(data)
                digest = hashlib.sha256(data).hexdigest()
                artifacts.append(
                    dict(file=name, goos=goos, goarch=goarch, sha256=digest, size=len(data))
                )
                checksums.append(f"{digest}  {name}\n")
        metadata = dict(
            version=version,
            go_version="go1.26.0",
            cgo_enabled=False,
            build_flags=["-mod=readonly", "-trimpath", "-buildvcs=false"],
            go_mod_sha256="a" * 64,
            go_sum_sha256="b" * 64,
            artifacts=artifacts,
        )
        (self.dist / "build.json").write_text(json.dumps(metadata), encoding="utf-8")
        (self.dist / "SHA256SUMS").write_text("".join(checksums), encoding="utf-8")

    def metadata(self):
        return json.loads((self.dist / "build.json").read_text(encoding="utf-8"))

    def write_metadata(self, metadata):
        (self.dist / "build.json").write_text(json.dumps(metadata), encoding="utf-8")

    def invoke(self, version=None):
        return subprocess.run(
            [sys.executable, str(PUBLISHER), version or self.version, str(self.dist)],
            cwd=REPOSITORY,
            env=self.environment,
            capture_output=True,
            text=True,
            timeout=15,
        )

    def recorded_calls(self):
        if not self.calls.exists():
            return []
        return [json.loads(line) for line in self.calls.read_text().splitlines()]

    def assert_preflight_rejected(self, result):
        self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(self.recorded_calls(), [], "invalid inputs reached GitHub")

    def assert_published(self, prerelease):
        result = self.invoke()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        calls = self.recorded_calls()
        self.assertEqual(len(calls), 3, calls)
        self.assertEqual(
            calls[0],
            ["api", f"repos/example/HappySync/commits/{self.version}", "--jq", ".sha"],
        )
        create = calls[1]
        self.assertEqual(create[:3], ["release", "create", self.version])
        for name in sorted(path.name for path in self.dist.iterdir()):
            self.assertEqual(create.count(str(self.dist / name)), 1, create)
        self.assertIn("--verify-tag", create)
        self.assertIn("--generate-notes", create)
        self.assertIn("--draft", create)
        self.assertIn("--latest=false", create)
        self.assertEqual(create[create.index("--repo") + 1], "example/HappySync")
        self.assertEqual(create[create.index("--title") + 1], f"HappySync {self.version}")
        self.assertEqual("--prerelease" in create, prerelease)
        self.assertEqual(
            calls[2],
            [
                "release", "edit", self.version, "--repo", "example/HappySync",
                "--draft=false", "--latest=false" if prerelease else "--latest=true",
            ],
        )

    def test_stable_release_publishes_all_assets_after_draft_upload(self):
        self.assert_published(prerelease=False)

    def test_prerelease_is_not_latest(self):
        for path in self.dist.iterdir():
            path.unlink()
        self.make_artifacts("v1.2.3-alpha.1")
        self.assert_published(prerelease=True)

    def test_non_tag_context_is_rejected_before_github(self):
        self.environment["GITHUB_REF_TYPE"] = "branch"
        self.assert_preflight_rejected(self.invoke())

    def test_non_push_event_is_rejected_before_github(self):
        self.environment["GITHUB_EVENT_NAME"] = "pull_request"
        self.assert_preflight_rejected(self.invoke())

    def test_mismatched_tag_context_is_rejected_before_github(self):
        self.environment["GITHUB_REF_NAME"] = "v1.2.4"
        self.assert_preflight_rejected(self.invoke())

    def test_invalid_commit_is_rejected_before_github(self):
        self.environment["GITHUB_SHA"] = "not-a-commit"
        self.assert_preflight_rejected(self.invoke())

    def test_invalid_repository_is_rejected_before_github(self):
        self.environment["GITHUB_REPOSITORY"] = "../other/repo"
        self.assert_preflight_rejected(self.invoke())

    def test_unsafe_version_is_rejected_before_github(self):
        self.environment["GITHUB_REF_NAME"] = "v1.2.3;echo unsafe"
        self.assert_preflight_rejected(self.invoke("v1.2.3;echo unsafe"))

    def test_corrupt_binary_is_rejected_before_github(self):
        (self.dist / self.metadata()["artifacts"][0]["file"]).write_bytes(b"corrupted")
        self.assert_preflight_rejected(self.invoke())

    def test_missing_binary_is_rejected_before_github(self):
        (self.dist / self.metadata()["artifacts"][0]["file"]).unlink()
        self.assert_preflight_rejected(self.invoke())

    def test_unexpected_asset_is_rejected_before_github(self):
        (self.dist / "HappySync-windows.exe").write_bytes(b"unsupported")
        self.assert_preflight_rejected(self.invoke())

    def test_manifest_version_mismatch_is_rejected_before_github(self):
        metadata = self.metadata()
        metadata["version"] = "v1.2.4"
        self.write_metadata(metadata)
        self.assert_preflight_rejected(self.invoke())

    def test_manifest_platform_mismatch_is_rejected_before_github(self):
        metadata = self.metadata()
        metadata["artifacts"][0]["goos"] = "windows"
        self.write_metadata(metadata)
        self.assert_preflight_rejected(self.invoke())

    def test_manifest_size_mismatch_is_rejected_before_github(self):
        metadata = self.metadata()
        metadata["artifacts"][0]["size"] += 1
        self.write_metadata(metadata)
        self.assert_preflight_rejected(self.invoke())

    def test_manifest_digest_mismatch_is_rejected_before_github(self):
        metadata = self.metadata()
        metadata["artifacts"][0]["sha256"] = "0" * 64
        self.write_metadata(metadata)
        self.assert_preflight_rejected(self.invoke())

    def test_manifest_requires_cgo_disabled(self):
        metadata = self.metadata()
        metadata["cgo_enabled"] = True
        self.write_metadata(metadata)
        self.assert_preflight_rejected(self.invoke())

    def test_checksum_file_mismatch_is_rejected_before_github(self):
        checksums = self.dist / "SHA256SUMS"
        checksums.write_text("0" * 64 + checksums.read_text()[64:])
        self.assert_preflight_rejected(self.invoke())

    def test_checksum_file_requires_all_binaries(self):
        checksums = self.dist / "SHA256SUMS"
        checksums.write_text("".join(checksums.read_text().splitlines(keepends=True)[1:]))
        self.assert_preflight_rejected(self.invoke())

    def test_moved_remote_tag_cannot_create_release(self):
        self.environment["FAKE_GH_SHA"] = "f" * 40
        result = self.invoke()
        self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(len(self.recorded_calls()), 1)
        self.assertEqual(self.recorded_calls()[0][0], "api")

    def test_create_or_upload_failure_cannot_publish_draft(self):
        self.environment["FAKE_GH_CREATE_FAIL"] = "1"
        result = self.invoke()
        self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
        calls = self.recorded_calls()
        self.assertEqual(len(calls), 2, calls)
        self.assertEqual(calls[1][:2], ["release", "create"])
        self.assertTrue(all(call[:2] != ["release", "edit"] for call in calls))


if __name__ == "__main__":
    unittest.main()
