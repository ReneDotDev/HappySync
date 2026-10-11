// Package integration verifies the public CLI with independent operating-system
// processes and checks actual folder bytes, rather than trusting worker logs.
package integration

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/happysync/happysync/internal/daemon"
	_ "modernc.org/sqlite"
)

var binaryOnce sync.Once
var binaryPath, binaryBuildDir, buildFailure string
var invitationPattern = regexp.MustCompile(`hsync1:[A-Za-z0-9_-]+`)

func TestMain(m *testing.M) {
	code := m.Run()
	if binaryBuildDir != "" {
		_ = os.RemoveAll(binaryBuildDir)
	}
	os.Exit(code)
}

func redacted(s string) string { return invitationPattern.ReplaceAllString(s, "[redacted invitation]") }

func raceOptions(logPath string) string {
	// ThreadSanitizer separates unquoted options at colons and spaces. Quote the
	// full path so a colon or a spaced temporary root stays intact.
	return strings.TrimSpace(os.Getenv("GORACE") + ` atexit_sleep_ms=0 log_path="` + filepath.ToSlash(logPath) + `"`)
}

func executable(t *testing.T) string {
	t.Helper()
	if testing.Short() {
		t.Skip("process integration is omitted in short mode")
	}
	binaryOnce.Do(func() {
		if supplied := os.Getenv("HAPPYSYNC_TEST_BINARY"); supplied != "" {
			binaryPath = supplied
			return
		}
		dir, err := os.MkdirTemp("", "happysync-process-tests-")
		if err != nil {
			buildFailure = err.Error()
			return
		}
		binaryBuildDir = dir
		binaryPath = filepath.Join(dir, "HappySync")
		_, source, _, _ := runtime.Caller(0)
		repo := filepath.Clean(filepath.Join(filepath.Dir(source), "..", ".."))
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()
		args := []string{"build", "-o", binaryPath}
		if os.Getenv("HAPPYSYNC_INTEGRATION_RACE") == "1" {
			args = append(args, "-race")
		}
		args = append(args, "./cmd/HappySync")
		cmd := exec.CommandContext(ctx, "go", args...)
		cmd.Dir = repo
		out, err := cmd.CombinedOutput()
		if err != nil {
			buildFailure = fmt.Sprintf("%v: %s", err, redacted(string(out)))
		}
	})
	if buildFailure != "" {
		t.Fatalf("build process fixture: %s", buildFailure)
	}
	return binaryPath
}

type peerCounts struct {
	Connected     int `json:"connected"`
	Direct        int `json:"direct"`
	Authenticated int `json:"authenticated"`
}
type status struct {
	Version            int               `json:"version"`
	PID                int               `json:"pid"`
	PeerID             string            `json:"peer_id"`
	State              string            `json:"state"`
	Peers              peerCounts        `json:"peers"`
	Received           uint64            `json:"received_bytes"`
	Sources            map[string]uint64 `json:"source_bytes"`
	Errors             []string          `json:"errors"`
	Pending            int               `json:"pending_files"`
	LastReconciliation time.Time         `json:"last_reconciliation"`
}
type instance struct {
	t                                          *testing.T
	binary, root, key, peerID, address, listen string
	running                                    bool
}

func node(t *testing.T) *instance {
	t.Helper()
	n := &instance{t: t, binary: executable(t), root: t.TempDir(), listen: "/ip4/127.0.0.1/tcp/0"}
	t.Cleanup(func() {
		if n.running {
			n.stop()
		}
	})
	return n
}

func (n *instance) command(cwd, input string, args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 55*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, n.binary, args...)
	cmd.Dir = cwd
	// Persist race-detector diagnostics independently of detached stderr file
	// descriptors; stop() inspects every log before releasing the fixture.
	if os.Getenv("HAPPYSYNC_INTEGRATION_RACE") == "1" {
		cmd.Env = append(os.Environ(), "GORACE="+raceOptions(filepath.Join(n.root, ".HappySync", "logs", "race")))
	}
	cmd.Stdin = strings.NewReader(input)
	var stdout bytes.Buffer
	cmd.Stdout = &stdout
	// A detached worker must not keep a test's pipe-copy goroutine alive.
	errfile, err := os.CreateTemp("", "happysync-command-stderr-")
	if err != nil {
		return "", err
	}
	defer os.Remove(errfile.Name())
	defer errfile.Close()
	cmd.Stderr = errfile
	err = cmd.Run()
	if err != nil {
		_, _ = errfile.Seek(0, 0)
		diagnostics, _ := io.ReadAll(io.LimitReader(errfile, 65536))
		return stdout.String(), fmt.Errorf("%w: %s", err, redacted(string(diagnostics)))
	}
	return stdout.String(), nil
}

func (n *instance) start(key string, peers []string, extra ...string) {
	n.t.Helper()
	args := []string{"start", "--background", "--no-mdns", "--no-dht", "--listen", n.listen, "--scan-interval", "1s"}
	if key != "" {
		args = append(args, "--key-stdin")
	}
	for _, p := range peers {
		args = append(args, "--peer", p)
	}
	args = append(args, extra...)
	out, err := n.command(n.root, key, args...)
	if err != nil {
		n.t.Fatalf("background start: %v", err)
	}
	n.running = true
	var foundKey, foundID, foundAddress string
	for _, line := range strings.Split(out, "\n") {
		switch {
		case strings.HasPrefix(line, "Share key: "):
			foundKey = strings.TrimPrefix(line, "Share key: ")
		case strings.HasPrefix(line, "Peer ID: "):
			foundID = strings.TrimPrefix(line, "Peer ID: ")
		case strings.HasPrefix(line, "Address: /ip4/127.0.0.1/"):
			foundAddress = strings.TrimPrefix(line, "Address: ")
		}
	}
	if !invitationPattern.MatchString(foundKey) || foundID == "" || foundAddress == "" {
		n.t.Fatal("incomplete readiness output")
	}
	if n.key != "" && n.key != foundKey {
		n.t.Fatal("share key changed across restart")
	}
	if n.peerID != "" && n.peerID != foundID {
		n.t.Fatal("device identity changed across restart")
	}
	n.key, n.peerID, n.address = foundKey, foundID, foundAddress
	n.listen = strings.Split(foundAddress, "/p2p/")[0]
	s, err := n.status()
	if err != nil || s.PID == 0 || s.State == "stopped" || s.PeerID != foundID {
		n.t.Fatalf("READY without live status: %v", err)
	}
}

func (n *instance) status() (status, error) {
	out, err := n.command(n.root, "", "status", "--json")
	if err != nil {
		return status{}, err
	}
	if strings.Contains(out, "hsync1:") || (n.key != "" && strings.Contains(out, n.key)) {
		return status{}, fmt.Errorf("status leaked invitation")
	}
	var s status
	if err = json.Unmarshal([]byte(out), &s); err != nil {
		return status{}, fmt.Errorf("invalid JSON status")
	}
	if s.Version != 1 {
		return status{}, fmt.Errorf("unexpected status schema")
	}
	return s, nil
}

func (n *instance) stop() {
	n.t.Helper()
	if !n.running {
		return
	}
	_, err := n.command(n.root, "", "stop")
	if err != nil {
		n.t.Errorf("stop worker: %v", err)
		return
	}
	// The public stop command must complete cleanup before returning. A closed
	// IPC endpoint alone does not prove the database, logs and root are released.
	lock, err := daemon.Lock(n.root)
	if err != nil {
		n.t.Errorf("stop returned before releasing the worker root: %v", err)
		return
	}
	if err := lock.Close(); err != nil {
		n.t.Errorf("release stopped root check: %v", err)
	}
	if s, err := n.status(); err != nil || s.State != "stopped" {
		n.t.Errorf("stop returned without stopped status: state=%q error=%v", s.State, err)
	}
	n.running = false
	logs, err := os.ReadDir(filepath.Join(n.root, ".HappySync", "logs"))
	if err != nil {
		n.t.Errorf("read worker logs: %v", err)
	} else {
		for _, log := range logs {
			if log.IsDir() {
				continue
			}
			data, err := os.ReadFile(filepath.Join(n.root, ".HappySync", "logs", log.Name()))
			if err != nil {
				n.t.Errorf("read worker log: %v", err)
			} else if strings.Contains(string(data), "hsync1:") || (n.key != "" && bytes.Contains(data, []byte(n.key))) {
				n.t.Error("worker operational log leaked secret invitation")
			} else if bytes.Contains(data, []byte("WARNING: DATA RACE")) {
				n.t.Errorf("race-built worker reported a data race:\n%s", redacted(string(data)))
			}
		}
	}
}

func write(t *testing.T, root, path string, data []byte) {
	t.Helper()
	full := filepath.Join(root, filepath.FromSlash(path))
	if err := os.MkdirAll(filepath.Dir(full), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(full, data, 0644); err != nil {
		t.Fatal(err)
	}
}

func wait(t *testing.T, timeout time.Duration, label string, ready func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if ready() {
			return
		}
		time.Sleep(150 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", label)
}

func hasFile(root, path string, want []byte) bool {
	got, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(path)))
	return err == nil && bytes.Equal(got, want)
}
func absent(root, path string) bool {
	_, err := os.Lstat(filepath.Join(root, filepath.FromSlash(path)))
	return os.IsNotExist(err)
}

func tree(root string) (map[string]string, error) {
	files := map[string]string{}
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if d.Name() == ".HappySync" {
				return filepath.SkipDir
			}
			return nil
		}
		if !d.Type().IsRegular() {
			return fmt.Errorf("unsupported fixture file")
		}
		f, err := os.Open(p)
		if err != nil {
			return err
		}
		defer f.Close()
		h := sha256.New()
		if _, err = io.Copy(h, f); err != nil {
			return err
		}
		rel, err := filepath.Rel(root, p)
		if err != nil {
			return err
		}
		files[filepath.ToSlash(rel)] = hex.EncodeToString(h.Sum(nil))
		return nil
	})
	return files, err
}

func converge(t *testing.T, nodes []*instance, expected map[string][]byte) {
	t.Helper()
	defer func() {
		if t.Failed() {
			for i, n := range nodes {
				files, treeErr := tree(n.root)
				s, statusErr := n.status()
				t.Logf("peer %d directory hashes=%v tree_error=%v status=%+v status_error=%v", i, files, treeErr, s, statusErr)
			}
		}
	}()
	wait(t, 35*time.Second, "matching directory content hashes", func() bool {
		for _, n := range nodes {
			for p, b := range expected {
				if !hasFile(n.root, p, b) {
					return false
				}
			}
		}
		first, err := tree(nodes[0].root)
		if err != nil {
			return false
		}
		if len(first) != len(expected) {
			return false
		}
		for _, n := range nodes[1:] {
			other, err := tree(n.root)
			if err != nil || !reflect.DeepEqual(first, other) {
				return false
			}
		}
		return true
	})
}

// Read only: inspect durable state without opening the writer Store/migrating it.
func persistedDeletion(root, path string) bool {
	db, err := sql.Open("sqlite", "file:"+filepath.Join(root, ".HappySync", "state.db")+"?mode=ro")
	if err != nil {
		return false
	}
	defer db.Close()
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM records WHERE path=? AND json_extract(payload,'$.deleted')=1", path).Scan(&count)
	return err == nil && count > 0
}

func TestBackgroundLifecycleAndRootSafety(t *testing.T) {
	n := node(t)
	n.start("", nil)
	initial, _ := n.status()
	if _, err := n.command(n.root, "", "start", "--background", "--no-mdns", "--no-dht"); err == nil {
		t.Fatal("duplicate root owner accepted")
	}
	n.stop()
	n.start("", nil)
	restarted, _ := n.status()
	if initial.PID == restarted.PID {
		t.Fatal("restart reused worker PID")
	}
	n.stop()
	other := node(t)
	other.start("", nil)
	other.stop()
	before, err := os.ReadFile(filepath.Join(n.root, ".HappySync", "config.json"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = n.command(n.root, other.key, "start", "--background", "--key-stdin", "--no-mdns", "--no-dht"); err == nil {
		t.Fatal("mismatched group accepted")
	}
	after, err := os.ReadFile(filepath.Join(n.root, ".HappySync", "config.json"))
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("mismatch modified initialized state")
	}
	if s, err := n.status(); err != nil || s.State != "stopped" {
		t.Fatal("failed start left a live worker")
	}
}

func TestTwoProcessesOfflineCatchupAndEmptyJoin(t *testing.T) {
	a, b := node(t), node(t)
	write(t, a.root, "docs/file.txt", []byte("initial"))
	a.start("", nil)
	b.start(a.key, []string{a.address})
	converge(t, []*instance{a, b}, map[string][]byte{"docs/file.txt": []byte("initial")})
	write(t, b.root, "docs/file.txt", []byte("edited on second device"))
	write(t, a.root, "additional.txt", []byte("created on first device"))
	expected := map[string][]byte{"docs/file.txt": []byte("edited on second device"), "additional.txt": []byte("created on first device")}
	converge(t, []*instance{a, b}, expected)
	b.stop()
	if err := os.Remove(filepath.Join(a.root, "docs", "file.txt")); err != nil {
		t.Fatal(err)
	}
	write(t, a.root, "offline.txt", []byte("while the other device was away"))
	delete(expected, "docs/file.txt")
	expected["offline.txt"] = []byte("while the other device was away")
	wait(t, 10*time.Second, "persisted offline deletion", func() bool {
		s, err := a.status()
		return err == nil && !s.LastReconciliation.IsZero() && s.Pending == 0 && len(s.Errors) == 0 && persistedDeletion(a.root, "docs/file.txt")
	})
	b.start("", []string{a.address})
	converge(t, []*instance{a, b}, expected)
	c := node(t)
	c.start(a.key, []string{a.address, b.address})
	converge(t, []*instance{a, b, c}, expected)
	if !absent(b.root, "docs/file.txt") || !absent(c.root, "docs/file.txt") {
		t.Fatal("old content resurrected after restart/new join")
	}
}

func TestThreeProcessesPreserveConcurrentEdits(t *testing.T) {
	a, b, c := node(t), node(t), node(t)
	write(t, a.root, "shared.txt", []byte("baseline"))
	a.start("", nil)
	b.start(a.key, []string{a.address})
	c.start(a.key, []string{a.address, b.address})
	converge(t, []*instance{a, b, c}, map[string][]byte{"shared.txt": []byte("baseline")})
	a.stop()
	b.stop()
	c.stop()
	values := []string{"edit on A", "different edit on B", "independent edit on C"}
	// Deliberately impossible clock skew must not affect causal conflict safety.
	times := []time.Time{time.Date(1980, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2100, 1, 1, 0, 0, 0, 0, time.UTC), time.Now()}
	for i, n := range []*instance{a, b, c} {
		write(t, n.root, "shared.txt", []byte(values[i]))
		if err := os.Chtimes(filepath.Join(n.root, "shared.txt"), times[i], times[i]); err != nil {
			t.Fatal(err)
		}
	}
	a.start("", nil)
	b.start("", []string{a.address})
	c.start("", []string{a.address, b.address})
	wait(t, 40*time.Second, "three preserved concurrent versions", func() bool {
		first, err := tree(a.root)
		if err != nil || len(first) != 3 {
			return false
		}
		for _, n := range []*instance{b, c} {
			files, err := tree(n.root)
			if err != nil || !reflect.DeepEqual(first, files) {
				return false
			}
		}
		seen := map[string]bool{}
		for p := range first {
			data, err := os.ReadFile(filepath.Join(a.root, p))
			if err != nil {
				return false
			}
			seen[string(data)] = true
		}
		for _, value := range values {
			if !seen[value] {
				return false
			}
		}
		return true
	})
	before, _ := tree(a.root)
	c.stop()
	c.start("", []string{a.address, b.address})
	wait(t, 15*time.Second, "stable deterministic conflict paths after restart", func() bool { got, err := tree(c.root); return err == nil && reflect.DeepEqual(before, got) })
}

func TestDeleteEditAndPopulatedTombstoneJoin(t *testing.T) {
	a, b := node(t), node(t)
	write(t, a.root, "deleted.txt", []byte("baseline"))
	a.start("", nil)
	b.start(a.key, []string{a.address})
	converge(t, []*instance{a, b}, map[string][]byte{"deleted.txt": []byte("baseline")})
	a.stop()
	b.stop()
	if err := os.Remove(filepath.Join(a.root, "deleted.txt")); err != nil {
		t.Fatal(err)
	}
	write(t, b.root, "deleted.txt", []byte("concurrent offline edit"))
	a.start("", nil)
	b.start("", []string{a.address})
	wait(t, 35*time.Second, "delete preserved with edited conflict content", func() bool {
		if !absent(a.root, "deleted.txt") || !absent(b.root, "deleted.txt") {
			return false
		}
		left, err := tree(a.root)
		if err != nil || len(left) != 1 {
			return false
		}
		right, err := tree(b.root)
		if err != nil || !reflect.DeepEqual(left, right) {
			return false
		}
		for p := range left {
			if !strings.Contains(p, ".happysync-conflict-") || !hasFile(a.root, p, []byte("concurrent offline edit")) {
				return false
			}
		}
		return true
	})
	c := node(t)
	write(t, c.root, "deleted.txt", []byte("newly imported content"))
	c.start(a.key, []string{a.address, b.address})
	wait(t, 35*time.Second, "populated import honors existing tombstone", func() bool {
		left, err := tree(a.root)
		if err != nil || len(left) != 2 {
			return false
		}
		for _, n := range []*instance{a, b, c} {
			if !absent(n.root, "deleted.txt") {
				return false
			}
			files, err := tree(n.root)
			if err != nil || !reflect.DeepEqual(left, files) {
				return false
			}
		}
		seen := map[string]bool{}
		for p := range left {
			data, err := os.ReadFile(filepath.Join(a.root, p))
			if err != nil {
				return false
			}
			seen[string(data)] = true
		}
		return seen["newly imported content"] && seen["concurrent offline edit"]
	})
}

func TestIgnoredFilesRemainLocal(t *testing.T) {
	a, b := node(t), node(t)
	write(t, a.root, ".happysyncignore", []byte("*.secret\ncache/\n"))
	for _, p := range []string{".git/config", "node_modules/package/index.js", "nested/node_modules/pkg/file", "api.secret", "cache/generated"} {
		write(t, a.root, p, []byte("excluded local content"))
	}
	write(t, a.root, "visible.txt", []byte("supported content"))
	a.start("", nil)
	b.start(a.key, []string{a.address})
	wait(t, 20*time.Second, "visible content with exclusions", func() bool { return hasFile(b.root, "visible.txt", []byte("supported content")) })
	time.Sleep(2 * time.Second)
	for _, p := range []string{".git/config", "node_modules/package/index.js", "nested/node_modules/pkg/file", "api.secret", "cache/generated"} {
		if !absent(b.root, p) {
			t.Fatalf("ignored path was transferred: %s", p)
		}
		if !hasFile(a.root, p, []byte("excluded local content")) {
			t.Fatalf("ignored source content changed: %s", p)
		}
	}
}

func TestProcessTransferResumeAfterStagingQuota(t *testing.T) {
	a, b := node(t), node(t)
	const block = 4 * 1024 * 1024
	data := make([]byte, 3*block+17)
	for i := range data {
		data[i] = byte(i/block + 1)
	}
	write(t, a.root, "large.bin", data)
	a.start("", nil)
	b.start(a.key, []string{a.address}, "--workers", "1", "--staging-limit", fmt.Sprint(block))
	wait(t, 30*time.Second, "verified partial block before quota interruption", func() bool {
		entries, err := os.ReadDir(filepath.Join(b.root, ".HappySync", "tmp", "blocks"))
		if err != nil {
			return false
		}
		for _, entry := range entries {
			info, err := entry.Info()
			if err == nil && len(entry.Name()) == 64 && info.Size() == block {
				return true
			}
		}
		return false
	})
	if !absent(b.root, "large.bin") {
		t.Fatal("partial transfer became a visible final file")
	}
	b.stop()
	b.start("", []string{a.address}, "--workers", "1", "--staging-limit", fmt.Sprint(32*block))
	converge(t, []*instance{a, b}, map[string][]byte{"large.bin": data})
	s, err := b.status()
	if err != nil {
		t.Fatal(err)
	}
	if s.Received >= uint64(len(data)) {
		t.Fatal("restart discarded previously verified transfer work")
	}
}
