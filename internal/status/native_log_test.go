package status

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestNativeLogGuardKeepsInheritedDescriptorAndBoundsRetention(t *testing.T) {
	path := filepath.Join(t.TempDir(), "logs", "worker.log")
	writer, err := OpenNativeLog(path, 64)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	original, err := writer.Stat()
	if err != nil {
		t.Fatal(err)
	}
	stderr := os.Stderr
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	guard, err := StartNativeLogGuard(ctx, path, 64, 5*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	defer guard.Close()
	for range 3 {
		if _, err := writer.Write(bytes.Repeat([]byte("x"), 128)); err != nil {
			t.Fatal(err)
		}
		deadline := time.Now().Add(time.Second)
		for {
			info, err := os.Stat(path)
			if err != nil {
				t.Fatal(err)
			}
			if info.Size() <= 64 {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("native log retention remained over limit")
			}
			time.Sleep(time.Millisecond)
		}
	}
	if _, err := writer.Write([]byte("final native diagnostic\n")); err != nil {
		t.Fatal(err)
	}
	current, err := os.Stat(path)
	if err != nil || !os.SameFile(original, current) {
		t.Fatal("guard replaced inherited log inode")
	}
	data, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(data, []byte("final native diagnostic\n")) {
		t.Fatalf("inherited append descriptor failed after truncation: %q %v", data, err)
	}
	if os.Stderr != stderr {
		t.Fatal("native log guard changed global stderr")
	}
	cancel()
	select {
	case <-guard.done:
	case <-time.After(time.Second):
		t.Fatal("guard did not close on canceled context")
	}
	if err := guard.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestNativeLogRejectsSymlinksAndInvalidLimits(t *testing.T) {
	dir := t.TempDir()
	outside := filepath.Join(dir, "outside")
	wanted := []byte("preserve file outside diagnostics")
	if err := os.WriteFile(outside, wanted, 0600); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "worker.log")
	if err := os.Symlink(outside, path); err != nil {
		t.Skipf("symlink creation unavailable: %v", err)
	}
	if _, err := OpenNativeLog(path, 1); err == nil {
		t.Fatal("symlink log opened")
	}
	if _, err := StartNativeLogGuard(context.Background(), path, 1, time.Millisecond); err == nil {
		t.Fatal("guard followed symlink log")
	}
	data, err := os.ReadFile(outside)
	if err != nil || !bytes.Equal(data, wanted) {
		t.Fatal("outside file modified through native log symlink")
	}
	parent := filepath.Join(dir, "logs-link")
	if err := os.Symlink(dir, parent); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenNativeLog(filepath.Join(parent, "another.log"), 64); err == nil {
		t.Fatal("native log followed symlink directory")
	}
	if _, err := StartNativeLogGuard(context.Background(), filepath.Join(dir, "plain.log"), 64, 0); err == nil {
		t.Fatal("zero check interval accepted")
	}
	if _, err := OpenNativeLog(filepath.Join(dir, "plain.log"), 0); err == nil {
		t.Fatal("zero log size accepted")
	}
}

func TestNativeLogStartupTruncatesExistingInode(t *testing.T) {
	path := filepath.Join(t.TempDir(), "worker.log")
	if err := os.WriteFile(path, bytes.Repeat([]byte("old native diagnostics"), 100), 0600); err != nil {
		t.Fatal(err)
	}
	original, _ := os.Stat(path)
	inherited, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer inherited.Close()
	f, err := OpenNativeLog(path, 64)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil || info.Size() != 0 || !os.SameFile(original, info) {
		t.Fatal("startup did not truncate existing native descriptor")
	}
	wanted := []byte("diagnostic after startup truncation\n")
	if _, err := inherited.Write(wanted); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(data, wanted) {
		t.Fatalf("inherited descriptor failed after startup truncation: %q %v", data, err)
	}
}
