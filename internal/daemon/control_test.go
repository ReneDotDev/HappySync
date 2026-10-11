package daemon

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

func TestLockAndControl(t *testing.T) {
	root := t.TempDir()
	lock, e := Lock(root)
	if e != nil {
		t.Fatal(e)
	}
	defer lock.Close()
	if another, e := Lock(root); e == nil {
		another.Close()
		t.Fatal("duplicate root owner allowed")
	}
	c, e := Listen(root, func(cmd string) any { return map[string]string{"state": "idle"} })
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close()
	var out map[string]string
	if e = Request(root, "status", &out); e != nil || out["state"] != "idle" {
		t.Fatalf("control: %v %v", out, e)
	}
}

// The helper closes its control endpoint before delayed resource cleanup,
// reproducing a worker which still owns its root after status disappears.
func TestStopWorkerHelper(t *testing.T) {
	root := os.Getenv("HAPPYSYNC_DAEMON_STOP_HELPER")
	if root == "" {
		return
	}
	lock, err := Lock(root)
	if err != nil {
		os.Exit(2)
	}
	log, err := os.OpenFile(filepath.Join(root, "owned.log"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		os.Exit(3)
	}
	stop := make(chan struct{}, 1)
	control, err := Listen(root, func(command string) any {
		if command == "stop" {
			select {
			case stop <- struct{}{}:
			default:
			}
			return map[string]string{"state": "stopping"}
		}
		return map[string]int{"pid": os.Getpid()}
	})
	if err != nil {
		os.Exit(4)
	}
	select {
	case <-stop:
	case <-time.After(10 * time.Second):
		os.Exit(5)
	}
	control.Close()
	time.Sleep(200 * time.Millisecond)
	log.Close()
	lock.Close()
	os.Exit(0)
}

func stopWorker(t *testing.T) (string, *exec.Cmd) {
	t.Helper()
	root := t.TempDir()
	cmd := exec.Command(os.Args[0], "-test.run=^TestStopWorkerHelper$")
	cmd.Env = append(os.Environ(), "HAPPYSYNC_DAEMON_STOP_HELPER="+root)
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cmd.Process.Kill()
		cmd.Wait()
	})
	deadline := time.Now().Add(5 * time.Second)
	for {
		var status struct{ PID int }
		if Request(root, "status", &status) == nil && status.PID == cmd.Process.Pid {
			return root, cmd
		}
		if time.Now().After(deadline) {
			t.Fatal("shutdown fixture never became ready")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestStopWaitsForCleanupAndReleasesOwnership(t *testing.T) {
	root, _ := stopWorker(t)
	start := time.Now()
	if err := Stop(root, 3*time.Second); err != nil {
		t.Fatal(err)
	}
	if time.Since(start) < 150*time.Millisecond {
		t.Fatal("stop returned before delayed worker cleanup")
	}
	lock, err := Lock(root)
	if err != nil {
		t.Fatalf("stop returned while worker still owned the root: %v", err)
	}
	lock.Close()
	if err := os.Remove(filepath.Join(root, "owned.log")); err != nil {
		t.Fatalf("stop returned while worker still held its log: %v", err)
	}
}

func TestStopReturnsDeadlineWhileWorkerStillOwnsRoot(t *testing.T) {
	root, _ := stopWorker(t)
	if err := Stop(root, 20*time.Millisecond); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("stop did not report unfinished cleanup: %v", err)
	}
	if lock, err := Lock(root); err == nil {
		lock.Close()
		t.Fatal("fixture unexpectedly released root before shutdown deadline")
	}
}
