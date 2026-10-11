package index

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/happysync/happysync/internal/model"
	"github.com/zeebo/blake3"
)

func put(t *testing.T, root, p string, content []byte) {
	t.Helper()
	full := filepath.Join(root, filepath.FromSlash(p))
	if err := os.MkdirAll(filepath.Dir(full), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(full, content, 0600); err != nil {
		t.Fatal(err)
	}
}
func TestManifestIntegrity(t *testing.T) {
	for _, size := range []int{0, 1, model.BlockSize, model.BlockSize + 25} {
		content := bytes.Repeat([]byte("x"), size)
		f := filepath.Join(t.TempDir(), "file")
		if err := os.WriteFile(f, content, 0600); err != nil {
			t.Fatal(err)
		}
		m, err := BuildManifest(f)
		if err != nil {
			t.Fatal(err)
		}
		sum := blake3.Sum256(content)
		if m.Hash != hex.EncodeToString(sum[:]) || m.Size != int64(size) {
			t.Fatalf("manifest mismatch %+v", m)
		}
		offset := 0
		for _, b := range m.Blocks {
			hash := blake3.Sum256(content[offset : offset+int(b.Size)])
			if b.Hash != hex.EncodeToString(hash[:]) {
				t.Fatal("block mismatch")
			}
			offset += int(b.Size)
		}
	}
}
func TestIgnoresNestedNegationAndWarnings(t *testing.T) {
	root := t.TempDir()
	files := map[string]string{
		".git/config": "secret", "node_modules/a.js": "ignore", ".HappySync/config": "secret",
		".gitignore": "*.log\n!keep.log\ncache/\n", ".happysyncignore": "private/**\n!private/keep.txt\n",
		"drop.log": "gone", "keep.log": "keep", "cache/data": "gone", "private/drop.txt": "gone", "private/keep.txt": "keep",
		"nested/.gitignore": "*.txt\n!keep.txt\n", "nested/drop.txt": "gone", "nested/keep.txt": "keep", "other/data.txt": "keep",
		"HappySync": "executable", "deeper/HappySync": "ordinary",
	}
	for p, v := range files {
		put(t, root, p, []byte(v))
	}
	if err := os.Symlink(filepath.Join(root, "keep.log"), filepath.Join(root, "link")); err != nil {
		t.Fatal(err)
	}
	opts := ScanOptions{Executable: filepath.Join(root, "HappySync")}
	baseline, _, warnings, err := Scan(root, opts)
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range []string{".git/config", "node_modules/a.js", ".HappySync/config", "drop.log", "cache/data", "private/drop.txt", "nested/drop.txt", "HappySync", "link"} {
		if _, ok := baseline[p]; ok {
			t.Errorf("ignored path inventoried: %s", p)
		}
	}
	for _, p := range []string{"keep.log", "private/keep.txt", "nested/keep.txt", "other/data.txt", "deeper/HappySync"} {
		if _, ok := baseline[p]; !ok {
			t.Errorf("expected path absent: %s", p)
		}
	}
	if len(warnings) != 1 {
		t.Fatalf("warnings %v", warnings)
	}
	for p, b := range baseline {
		ignored, err := Ignored(root, p, b.Directory, opts)
		if err != nil || ignored {
			t.Fatalf("Scan/Ignored differ at %s: %v %v", p, ignored, err)
		}
	}
}
func TestPortableSafety(t *testing.T) {
	root := t.TempDir()
	for _, p := range []string{"../escape", "/absolute", "a/../../b", "C:/windows", "a\\b", "a\x00b", "a\x7fb", "a//b", ".HappySync/key", "dir/.happysync/key", "NUL", "com1.txt", "trail.", "a:b", "e\u0301.txt"} {
		if _, err := SafePath(root, p); err == nil {
			t.Errorf("accepted unsafe path %q", p)
		}
	}
	outside := t.TempDir()
	if err := os.Symlink(outside, filepath.Join(root, "link")); err != nil {
		t.Fatal(err)
	}
	if _, err := SafePath(root, "link/data"); err == nil {
		t.Fatal("followed symlink")
	}
	put(t, root, "Case.txt", nil)
	if _, err := SafePath(root, "case.txt"); err == nil {
		t.Fatal("accepted case alias")
	}
	if p, err := SafePath(root, "new/sub/file"); err != nil || p != filepath.Join(root, "new/sub/file") {
		t.Fatalf("safe new path %s %v", p, err)
	}
}
func TestCaseAndUnicodeCollisionsAbortScan(t *testing.T) {
	root := t.TempDir()
	put(t, root, "Case", nil)
	put(t, root, "case", nil)
	entries, _ := os.ReadDir(root)
	if len(entries) > 1 {
		if _, _, _, err := Scan(root); err == nil {
			t.Fatal("collision must abort")
		}
	}
	root = t.TempDir()
	put(t, root, "e\u0301", []byte("safe"))
	baseline, _, _, err := Scan(root)
	if err != nil {
		t.Fatal(err)
	}
	if baseline["é"].Hash == "" {
		t.Fatal("local Unicode path not normalized")
	}
	resolved, err := SafePath(root, "é")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(resolved); err != nil {
		t.Fatal(err)
	}
}
func TestForcedScanVerifiesBytesBeyondCachedStat(t *testing.T) {
	root := t.TempDir()
	put(t, root, "file", []byte("first"))
	a, m, _, err := Scan(root)
	if err != nil {
		t.Fatal(err)
	}
	put(t, root, "file", []byte("later"))
	stamp := time.Unix(0, a["file"].Mtime)
	if err := os.Chtimes(filepath.Join(root, "file"), stamp, stamp); err != nil {
		t.Fatal(err)
	}
	b, _, _, err := Scan(root, ScanOptions{Previous: a, PreviousManifests: m, Force: true})
	if err != nil {
		t.Fatal(err)
	}
	if b["file"].Hash == a["file"].Hash {
		t.Fatal("full scan trusted mtime rather than content")
	}
}
func TestUnsupportedFileAbortsInsteadOfFalseDeletion(t *testing.T) {
	root := t.TempDir()
	put(t, root, "a-safe", []byte("preserve this baseline"))
	previous, manifests, _, err := Scan(root)
	if err != nil {
		t.Fatal(err)
	}
	// A colon creates an NTFS alternate data stream on Windows, rather than
	// an entry visible to WalkDir. DEL is a real filename on all supported
	// filesystems but is excluded from HappySync's portable path format.
	const unsupported = "bad\x7fname"
	put(t, root, unsupported, nil)
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, entry := range entries {
		if entry.Name() == unsupported {
			found = true
		}
	}
	if !found {
		t.Fatal("unsupported fixture must be visible in the directory inventory")
	}
	baseline, nextManifests, _, err := Scan(root, ScanOptions{Previous: previous, PreviousManifests: manifests})
	if err == nil {
		t.Fatal("unsafe path must abort snapshot")
	}
	if baseline != nil || nextManifests != nil {
		t.Fatal("failed inventory must not expose an incomplete authoritative snapshot")
	}
	if previous["a-safe"].Hash == "" {
		t.Fatal("failed inventory changed the previous baseline")
	}
}

// Opt-in acceptance fixture: validates every content digest, including cache-free
// comprehensive hashing. Run HAPPYSYNC_SCALE_SCAN=1 go test ./internal/index -run TestScan100K -v.
func TestScan100K(t *testing.T) {
	if os.Getenv("HAPPYSYNC_SCALE_SCAN") != "1" {
		t.Skip("set HAPPYSYNC_SCALE_SCAN=1 to run 100k-file acceptance")
	}
	root := t.TempDir()
	content := []byte("100k-file content-integrity fixture\n")
	for directory := 0; directory < 100; directory++ {
		dir := filepath.Join(root, fmt.Sprintf("directory-%03d", directory))
		if err := os.Mkdir(dir, 0700); err != nil {
			t.Fatal(err)
		}
		for file := 0; file < 1000; file++ {
			if err := os.WriteFile(filepath.Join(dir, fmt.Sprintf("file-%04d", file)), content, 0600); err != nil {
				t.Fatal(err)
			}
		}
	}
	start := time.Now()
	baselines, manifests, warnings, err := Scan(root, ScanOptions{Force: true})
	elapsed := time.Since(start)
	if err != nil {
		t.Fatal(err)
	}
	if len(baselines) != 100100 || len(manifests) != 100000 || len(warnings) != 0 {
		t.Fatalf("incomplete scan: %d entries, %d manifests, %v", len(baselines), len(manifests), warnings)
	}
	digest := blake3.Sum256(content)
	expected := hex.EncodeToString(digest[:])
	for p, m := range manifests {
		if m.Hash != expected || baselines[p].Hash != expected || m.Size != int64(len(content)) {
			t.Fatalf("content integrity failed at %s", p)
		}
	}
	t.Logf("verified 100000 files in %s (%.0f files/s)", elapsed, float64(len(manifests))/elapsed.Seconds())
}
