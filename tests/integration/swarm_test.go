package integration

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"
)

func swarmData(blocks int) []byte {
	data := make([]byte, blocks*4*1024*1024+17)
	for i := range data {
		data[i] = byte(i/(4*1024*1024) + 1)
	}
	return data
}

// Keep C connected while data is locally excluded. This removes the race
// between membership establishment and the first snapshot/download scheduling.
func deferredReceiver(t *testing.T, a, b *instance) *instance {
	t.Helper()
	c := node(t)
	write(t, c.root, ".happysyncignore", []byte("swarm.bin\n"))
	c.start(a.key, []string{a.address, b.address}, "--ignore", ".happysyncignore", "--workers", "4")
	wait(t, 20*time.Second, "two authenticated swarm sources", func() bool {
		s, err := c.status()
		return err == nil && s.Peers.Connected == 2 && s.Peers.Authenticated == 2
	})
	return c
}

func releaseReceiver(t *testing.T, c *instance) {
	t.Helper()
	if err := os.Remove(filepath.Join(c.root, ".happysyncignore")); err != nil {
		t.Fatal(err)
	}
}

func assertSources(t *testing.T, c *instance, peers ...*instance) {
	t.Helper()
	s, err := c.status()
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range peers {
		if s.Sources[p.peerID] == 0 {
			t.Fatalf("peer %s contributed no verified transfer bytes: %v", p.peerID, s.Sources)
		}
	}
}

func TestThreeProcessesSwarmUsesBothSources(t *testing.T) {
	a, b := node(t), node(t)
	data := swarmData(12)
	write(t, a.root, "swarm.bin", data)
	a.start("", nil)
	b.start(a.key, []string{a.address})
	converge(t, []*instance{a, b}, map[string][]byte{"swarm.bin": data})
	c := deferredReceiver(t, a, b)
	releaseReceiver(t, c)
	converge(t, []*instance{a, b, c}, map[string][]byte{"swarm.bin": data})
	assertSources(t, c, a, b)
}

func TestPartialProcessPeerUploadsBeforeCompletion(t *testing.T) {
	a, b := node(t), node(t)
	data := swarmData(6)
	write(t, a.root, "swarm.bin", data)
	a.start("", nil)
	b.start(a.key, []string{a.address}, "--workers", "1", "--staging-limit", fmt.Sprint(2*4*1024*1024))
	wait(t, 25*time.Second, "two verified blocks on incomplete peer", func() bool {
		entries, err := os.ReadDir(filepath.Join(b.root, ".HappySync", "tmp", "blocks"))
		if err != nil {
			return false
		}
		count := 0
		for _, entry := range entries {
			info, err := entry.Info()
			if err == nil && len(entry.Name()) == 64 && info.Size() == 4*1024*1024 {
				count++
			}
		}
		return count == 2
	})
	if !absent(b.root, "swarm.bin") {
		t.Fatal("incomplete peer exposed final file")
	}
	c := deferredReceiver(t, a, b)
	releaseReceiver(t, c)
	converge(t, []*instance{a, c}, map[string][]byte{"swarm.bin": data})
	assertSources(t, c, a, b)
	if !absent(b.root, "swarm.bin") {
		t.Fatal("partial-upload proof accidentally used completed file")
	}
	b.stop()
	b.start("", []string{a.address, c.address}, "--workers", "4", "--staging-limit", fmt.Sprint(128*1024*1024))
	converge(t, []*instance{a, b, c}, map[string][]byte{"swarm.bin": data})
}

func fileHash(root, path string) (string, error) {
	f, err := os.Open(filepath.Join(root, path))
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err = io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func residentBytes(pid int) int64 {
	if runtime.GOOS == "windows" {
		return 0 // RSS sampling via ps is available on the Unix test platforms.
	}
	out, err := exec.Command("ps", "-o", "rss=", "-p", strconv.Itoa(pid)).Output()
	if err != nil {
		return 0
	}
	kib, _ := strconv.ParseInt(strings.TrimSpace(string(out)), 10, 64)
	return kib * 1024
}

func stagedBytes(root string) int64 {
	var total int64
	_ = filepath.WalkDir(filepath.Join(root, ".HappySync", "tmp"), func(path string, d os.DirEntry, err error) error {
		if err == nil && !d.IsDir() {
			if info, err := d.Info(); err == nil {
				total += info.Size()
			}
		}
		return nil
	})
	return total
}

func TestMultiGiBProcessTransfer(t *testing.T) {
	if os.Getenv("HAPPYSYNC_LARGE_TRANSFER") != "1" {
		t.Skip("set HAPPYSYNC_LARGE_TRANSFER=1 for a 2 GiB + 17 byte real-process transfer")
	}
	a, b := node(t), node(t)
	f, err := os.Create(filepath.Join(a.root, "swarm.bin"))
	if err != nil {
		t.Fatal(err)
	}
	// Stream unique blocks so both fixture generation and verification retain
	// bounded memory; repeated zero blocks would exercise deduplication only.
	block := make([]byte, 4*1024*1024)
	for i := 0; i < 512; i++ {
		for j := range block {
			block[j] = byte(i*13 + j*17 + (j >> 16))
		}
		block[0], block[1] = byte(i), byte(i>>8)
		if _, err = f.Write(block); err != nil {
			t.Fatal(err)
		}
	}
	if _, err = f.Write(bytes.Repeat([]byte{77}, 17)); err != nil {
		t.Fatal(err)
	}
	if err = f.Close(); err != nil {
		t.Fatal(err)
	}
	want, err := fileHash(a.root, "swarm.bin")
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	a.start("", nil, "--scan-interval", "10m")
	b.start(a.key, []string{a.address}, "--scan-interval", "10m")
	ast, _ := a.status()
	bst, _ := b.status()
	var peakRSS, peakStaging int64
	wait(t, 5*time.Minute, "multi-GiB verified installation", func() bool {
		for _, pid := range []int{ast.PID, bst.PID} {
			if rss := residentBytes(pid); rss > peakRSS {
				peakRSS = rss
			}
		}
		if staged := stagedBytes(b.root); staged > peakStaging {
			peakStaging = staged
		}
		info, err := os.Stat(filepath.Join(b.root, "swarm.bin"))
		return err == nil && info.Size() == 2*1024*1024*1024+17
	})
	if peakRSS > 768*1024*1024 {
		t.Fatalf("worker RSS exceeded bounded-memory fixture budget: %d bytes", peakRSS)
	}
	if peakStaging > 2*(2*1024*1024*1024+17)+64*1024*1024 {
		t.Fatalf("staging exceeds verified blocks plus one assembly: %d bytes", peakStaging)
	}
	got, err := fileHash(b.root, "swarm.bin")
	if err != nil || got != want {
		t.Fatal("multi-GiB content hash mismatch")
	}
	b.stop()
	b.start("", []string{a.address}, "--scan-interval", "10m")
	got, err = fileHash(b.root, "swarm.bin")
	if err != nil || got != want {
		t.Fatal("multi-GiB hash changed after persisted restart")
	}
	t.Logf("transferred and verified %d bytes in %s; sampled peak worker RSS=%d bytes (0 means unavailable), peak staging=%d bytes", int64(2*1024*1024*1024+17), time.Since(started).Round(time.Millisecond), peakRSS, peakStaging)
}
