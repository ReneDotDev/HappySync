# Files, ignores, and reconciliation

HappySync records immutable operations and retains their causal history, including tombstones. Wall-clock time is a scan optimization; it does not resolve concurrent edits.

## Filesystem inventory

Version 1 synchronizes regular files and directories. Symlinks and other special files are skipped with visible warnings. `.HappySync/` is always reserved, including case aliases and nested occurrences. The specific running executable is excluded when it is inside the root; another file with the same name remains eligible.

By default `.git/` and `node_modules/` are ignored. Root and nested `.gitignore` files apply within their directory, followed by `.happysyncignore` at the same level. Configured rules apply last. Supported gitignore-style syntax includes comments, `!` negation, anchored `/` patterns, directory `/` suffixes, `*`, `?`, character classes, and `**`. An ignored directory must itself be unignored before a rule can reinclude its children, matching git behavior. Changes to ignore rules do not imply deletion of previously shared files. Ignore rules do not revoke access to bytes already shared with other members.

Portable paths are NFC Unicode, forward-slash relative paths. Each component is at most 255 UTF-8 bytes, and the entire path at most 4,096 bytes. Local decomposed Unicode spelling is normalized during inventory and resolved back to its physical spelling when accessed. Absolute paths, traversal, backslashes, control characters, Windows device names, Windows-invalid punctuation, trailing dots/spaces, and state-directory aliases are refused. Case-fold and Unicode spelling collisions stop application visibly instead of overwriting either file. This intentionally imposes portable restrictions on Linux as well as macOS and Windows.

Files use fixed 4 MiB blocks and BLAKE3 digests for each block and the complete content. Empty files have the BLAKE3 empty digest and zero blocks. Hashing retries changed files and verifies identity, size, and modification time before and after reading. Hash workers reuse one block-sized buffer. Ordinary scans may reuse manifests with unchanged size/time; comprehensive scans must recompute hashes so a timestamp-preserving edit is eventually found. A failed scan does not publish a partial snapshot that could be mistaken for deletion.

## Operation identity and causality

A record contains its path, origin identifier, persistent counter, version vector, deletion/directory/import flags, and complete content manifest. The origin vector component equals the operation counter. Operation IDs are lowercase hex BLAKE3 of the Go JSON encoding of all record fields with the `id` field set to the empty string. The encoder sorts vector map keys. This exact version-1 construction is deterministic across supported Go platforms; changing it requires a protocol version change.

A vector strictly dominates another when every component is at least as large and at least one is larger. `Resolve` uses the maximal causal frontier for each path. Identical concurrent content projects once. Raw operations remain immutable; joining all safely observed vectors allows the next local edit to account for the coalesced branches. Received metadata that has not been applied must not become the causal baseline of a local user edit.

Causal observation is settled per original path after every projected branch for that path has completed. An unrelated unavailable download does not prevent a completed path from joining its observed vectors. A path with pending preservation content remains incomplete, so later local edits cannot incorrectly claim to have observed unapplied branches. Optional record `mtime` preserves regular-file timestamps where the OS supports it; vectors alone determine causality.

Different concurrent contents all survive. A deterministic operation-ID ordering selects the original filename; other contents receive a sibling named `<original>.happysync-conflict-<full-operation-id>`, truncating the original component at a Unicode boundary when necessary. Deletion wins at the original path against concurrent edits, while the edited bytes receive conflict copies. A causally newer intentional recreation can restore the original path. Tombstones have no age-based expiry in version 1.

Initial local files are imported candidates. A candidate cannot supersede an established tombstone: its bytes survive as a conflict copy while deletion remains at the original. An empty initial snapshot generates no deletion operations.

## Conflict copies and directories

Conflict copies are ordinary editable and deletable replicated paths. After verified installation, the engine materializes a deterministic record at the projected path using the source operation's origin, counter, causal vector, manifest and directory flag, with the import flag cleared. Its ID changes because its path changes. This is a derived preservation operation with the source's causal stamp, rather than a fresh user edit or a new device counter. Every peer derives the same record. Equal-content generated and materialized records coalesce, preferring the explicit materialized path. Consequently, a later edit of the main path does not remove an already preserved conflict copy. An explicit edit or tombstone on the copy's own path observes its source operation and supersedes the generated/materialized copy. Recovery journals and durable source markers ensure materialization is recorded before a completed installation is acknowledged.

A live descendant requires directory ancestors. A concurrent file blocking descendants moves to a conflict sibling. Live descendants under a deleted directory move into a deterministic conflict subtree, while original descendant tombstones remain at their original locations. Filesystem application preserves conflict bytes before replacing originals and deletes children before cleaning up empty deleted directories. Nonempty directories are never removed recursively merely to make a projection fit.

The pure reconciliation package performs no IO. The engine separately verifies blocks and complete files, checks the destination against its persisted baseline, journals installations and deletions, and updates the durable applied baseline. Restart recovery verifies completed filesystem changes before treating them as local observations. An exclusive root lock protects the database writer; the baseline checks also guard against user changes during downloads.

The engine repeats its destination check after the journal transaction, since a user can edit while persistence waits on IO. File-to-directory replacement retains the original in a journaled private backup until the new directory and baseline are durable. Recovery restores or preserves that backup rather than discarding competing bytes. Startup reclaims regular orphan block-write temporaries and, after journal recovery, unreferenced assembly stages. Hash-named partial blocks remain resumable; private recovery backups are never swept as generic temporary files.

## Automated evidence

Unit tests cover block boundaries and empty files; ignore defaults, scoped patterns and negation; exact executable exclusion; symlink and pathname rejection; Unicode/case collisions; same-size and same-time content changes; causal edits/deletions/recreation; imported tombstoned content; repeated events; identical concurrent content; 1,000 shuffled merge orders; conflict-path edits/deletions; and file/directory collisions. Package tests also pass with Go's race detector.

The opt-in scale fixture creates 100 directories containing 1,000 files each, performs a comprehensive hash scan, and verifies every manifest and baseline digest:

```sh
HAPPYSYNC_SCALE_SCAN=1 go test ./internal/index -run TestScan100K -count=1 -v
```

On the local macOS arm64 development machine on 2026-10-11, this fixture scanned and verified 100,000 small files in 3.07 seconds (approximately 32,558 files/second); fixture creation and cleanup brought the test to 12.94 seconds. A separate command with Go's test-result cache disabled reported a 2.79-second scan and a maximum resident set size of 123,748,352 bytes (approximately 118 MiB) for the timed `go test` command. This measurement describes a warm local filesystem with small repeated contents. It does not estimate large-file transfer speed, cold-disk behavior, remote discovery, consumer NAT traversal, or a VPS deployment.
