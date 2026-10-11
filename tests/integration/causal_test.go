package integration

import (
	"context"
	"encoding/hex"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/happysync/happysync/internal/config"
	"github.com/happysync/happysync/internal/engine"
	"github.com/happysync/happysync/internal/model"
	"github.com/happysync/happysync/internal/network"
	"github.com/happysync/happysync/internal/reconcile"
	"github.com/happysync/happysync/internal/transfer"
	"github.com/zeebo/blake3"
)

func fixtureManifest(data []byte) model.Manifest {
	h := blake3.Sum256(data)
	digest := hex.EncodeToString(h[:])
	return model.Manifest{Hash: digest, Size: int64(len(data)), Blocks: []model.Block{{Hash: digest, Size: int64(len(data))}}}
}

func TestCausalObservationProcessHelper(t *testing.T) {
	root := os.Getenv("HAPPYSYNC_CAUSAL_TEST_ROOT")
	if root == "" {
		return
	}
	cfg, err := config.Load(root)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	installed := make(chan struct{}, 1)
	e, err := engine.New(ctx, cfg, engine.Options{ScanInterval: time.Second, Network: network.Config{Listen: []string{"/ip4/127.0.0.1/tcp/0"}, DisableMDNS: true, DisableDHT: true}, Fault: func(at string) error {
		if at == "after-baseline" {
			select {
			case installed <- struct{}{}:
			default:
			}
		}
		return nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { cancel(); e.Wait(); _ = e.Close() }()
	initial, _, err := e.Snapshot(ctx, "", 128)
	if err != nil || len(initial) != 1 {
		t.Fatalf("initial snapshot: %v", err)
	}
	m := fixtureManifest(crashReplacement)
	cache, err := transfer.New(root, 1, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if err = cache.Put(m.Hash, crashReplacement); err != nil {
		t.Fatal(err)
	}
	alpha := reconcile.NewRecord("target.txt", "observed-alpha", 1, initial[0].Clock, m, false, false, false)
	beta := reconcile.NewRecord("target.txt", "observed-beta", 1, initial[0].Clock, m, false, false, false)
	// An unrelated offline revision remains pending. A completed path must still
	// remember all the causality of its coalesced equal-content versions.
	pending := reconcile.NewRecord("pending.txt", "offline-peer", 1, nil, fixtureManifest([]byte("unavailable unrelated file")), false, false, false)
	if err = e.Receive(ctx, []model.Record{alpha, beta, pending}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-installed:
	case <-ctx.Done():
		t.Fatal("coalesced file never installed")
	}
	if !hasFile(root, "target.txt", crashReplacement) {
		t.Fatal("coalesced content missing")
	}
	if err = os.Remove(filepath.Join(root, "target.txt")); err != nil {
		t.Fatal(err)
	}
	wait(t, 10*time.Second, "persisted deletion of coalesced file", func() bool { return persistedDeletion(root, "target.txt") })
	records, _, err := e.Snapshot(ctx, "", 128)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range records {
		if r.Path == "target.txt" && r.Deleted {
			if !reconcile.Dominates(r.Clock, alpha.Clock) || !reconcile.Dominates(r.Clock, beta.Clock) {
				t.Fatal("unrelated pending file caused deletion to forget an already observed equal-content version")
			}
			return
		}
	}
	t.Fatal("durable deletion was not in snapshot")
}

func TestCompletedPathRetainsCausalityWhileOtherFilePending(t *testing.T) {
	if testing.Short() {
		t.Skip("independent-process causal integration is omitted in short mode")
	}
	root := t.TempDir()
	write(t, root, "target.txt", crashOriginal)
	if _, err := config.Init(root, ""); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestCausalObservationProcessHelper$", "-test.count=1")
	cmd.Env = append(os.Environ(), "HAPPYSYNC_CAUSAL_TEST_ROOT="+root)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("causal observation process: %v: %s", err, redacted(string(out)))
	}
}
