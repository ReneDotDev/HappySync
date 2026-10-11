package integration

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func workerResidentBytes(n *instance) int64 {
	if runtime.GOOS == "windows" {
		return 0
	}
	root, err := filepath.EvalSymlinks(n.root)
	if err != nil {
		return 0
	}
	out, err := exec.Command("ps", "-axo", "rss=,args=").Output()
	if err != nil {
		return 0
	}
	var maximum int64
	for _, line := range strings.Split(string(out), "\n") {
		if !strings.Contains(line, n.binary) || !strings.Contains(line, "--internal-worker") || !strings.Contains(line, root) {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		rss, _ := strconv.ParseInt(fields[0], 10, 64)
		if rss*1024 > maximum {
			maximum = rss * 1024
		}
	}
	return maximum
}

type persistedScaleState struct {
	Records, Baselines int
	Counter            uint64
}

func scaleState(t *testing.T, root string) persistedScaleState {
	t.Helper()
	db, err := sql.Open("sqlite", "file:"+filepath.Join(root, ".HappySync", "state.db")+"?mode=ro")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var state persistedScaleState
	if err = db.QueryRow("SELECT COUNT(*) FROM records").Scan(&state.Records); err != nil {
		t.Fatal(err)
	}
	if err = db.QueryRow("SELECT COUNT(*) FROM baselines").Scan(&state.Baselines); err != nil {
		t.Fatal(err)
	}
	if err = db.QueryRow("SELECT value FROM counters WHERE name='local'").Scan(&state.Counter); err != nil {
		t.Fatal(err)
	}
	var integrity string
	if err = db.QueryRow("PRAGMA quick_check").Scan(&integrity); err != nil || integrity != "ok" {
		t.Fatalf("persisted scale state integrity: %v, %s", err, integrity)
	}
	return state
}

func TestHundredThousandFilesEngineIndexAndRestart(t *testing.T) {
	if os.Getenv("HAPPYSYNC_SCALE_ENGINE") != "1" {
		t.Skip("set HAPPYSYNC_SCALE_ENGINE=1 for a real 100,000-file engine index/persistence/restart fixture")
	}
	n := node(t)
	expected := make(map[string]string, 100000)
	fixtureStarted := time.Now()
	for directory := 0; directory < 100; directory++ {
		dir := fmt.Sprintf("d%03d", directory)
		if err := os.Mkdir(filepath.Join(n.root, dir), 0755); err != nil {
			t.Fatal(err)
		}
		for file := 0; file < 1000; file++ {
			path := fmt.Sprintf("%s/f%03d.txt", dir, file)
			body := []byte(fmt.Sprintf("scale-content-%06d\n", directory*1000+file))
			if err := os.WriteFile(filepath.Join(n.root, filepath.FromSlash(path)), body, 0644); err != nil {
				t.Fatal(err)
			}
			hash := sha256.Sum256(body)
			expected[path] = hex.EncodeToString(hash[:])
		}
	}
	fixtureTime := time.Since(fixtureStarted)
	var peak atomic.Int64
	stopSampling, sampled := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(sampled)
		tick := time.NewTicker(250 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-stopSampling:
				return
			case <-tick.C:
				rss := workerResidentBytes(n)
				if rss > peak.Load() {
					peak.Store(rss)
				}
			}
		}
	}()
	defer func() { close(stopSampling); <-sampled }()
	started := time.Now()
	n.start("", nil, "--scan-interval", "10m")
	startupTime := time.Since(started)
	// Exercise one actual reconciliation pass, not merely scanner startup.
	wait(t, 90*time.Second, "100k persisted baseline reconciliation", func() bool {
		s, err := n.status()
		return err == nil && len(s.Errors) == 0 && !s.LastReconciliation.IsZero() && s.Pending == 0
	})
	firstPassTime := time.Since(started)
	n.stop()
	state := scaleState(t, n.root)
	if state.Records != 100100 || state.Baselines != 100100 || state.Counter != 100100 {
		t.Fatalf("100k initial persisted observations incomplete: %+v", state)
	}
	files, err := tree(n.root)
	if err != nil || !reflect.DeepEqual(files, expected) {
		t.Fatal("100k files changed during initial indexing")
	}
	started = time.Now()
	n.start("", nil, "--scan-interval", "10m")
	restartTime := time.Since(started)
	n.stop()
	if restarted := scaleState(t, n.root); restarted != state {
		t.Fatalf("100k restart fabricated causal operations or lost state: before=%+v after=%+v", state, restarted)
	}
	files, err = tree(n.root)
	if err != nil || !reflect.DeepEqual(files, expected) {
		t.Fatal("100k persisted restart changed folder content hashes")
	}
	if peak.Load() > 1024*1024*1024 {
		t.Fatalf("100k engine exceeded 1 GiB worker memory budget: %d bytes", peak.Load())
	}
	t.Logf("100,000 files +100 directories: fixture=%s initial READY=%s initial reconciliation=%s persisted restart READY=%s sampled peak worker RSS=%d bytes (0 means unavailable)", fixtureTime.Round(time.Millisecond), startupTime.Round(time.Millisecond), firstPassTime.Round(time.Millisecond), restartTime.Round(time.Millisecond), peak.Load())
}
