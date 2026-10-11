package engine

import (
	"context"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/happysync/happysync/internal/config"
	"github.com/happysync/happysync/internal/model"
	"github.com/happysync/happysync/internal/network"
	"github.com/happysync/happysync/internal/reconcile"
	"github.com/happysync/happysync/internal/store"
	"github.com/happysync/happysync/internal/transfer"
	"github.com/zeebo/blake3"
)

func TestCanceledEngineDrainsWatcherUntilClose(t *testing.T) {
	cfg, err := config.Init(t.TempDir(), "")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	e, err := New(ctx, cfg, Options{Network: network.Config{Listen: []string{"/ip4/127.0.0.1/tcp/0"}, DisableMDNS: true, DisableDHT: true}})
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cancel()
		e.Wait()
		if err := e.Close(); err != nil {
			t.Errorf("close canceled engine: %v", err)
		}
	})
	if e.watcher == nil {
		t.Fatal("filesystem watcher unavailable")
	}
	cancel()
	e.Wait()
	// Require actual native notifications after the coordinator stops. A backend
	// can still emit these while a final Add or Close waits on its I/O reader.
	// Use the backend's writer rather than injecting into its owned channels.
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	for {
		select {
		case <-e.wake:
			continue
		default:
		}
		break
	}
	if err := os.WriteFile(filepath.Join(e.root, "late-notification"), []byte("late event"), 0600); err != nil {
		t.Fatal(err)
	}
	select {
	case <-e.wake:
	case <-deadline.C:
		t.Fatal("canceled engine stopped draining native watcher notifications")
	}
}

// A synchronous coordinator fixture exposes scan/apply boundaries without
// racing the production loop. Networking still uses a real secure local host.
func coordinator(t *testing.T) (*Engine, context.Context) {
	t.Helper()
	cfg, err := config.Init(t.TempDir(), "")
	if err != nil {
		t.Fatal(err)
	}
	db, err := store.Open(cfg.Root)
	if err != nil {
		t.Fatal(err)
	}
	cache, err := transfer.New(cfg.Root, 2, 32<<20)
	if err != nil {
		t.Fatal(err)
	}
	fs, err := os.OpenRoot(cfg.Root)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	node, err := network.New(ctx, network.Config{Identity: cfg.Identity, SyncID: cfg.SyncID, Secret: cfg.Secret, Listen: []string{"/ip4/127.0.0.1/tcp/0"}, DisableMDNS: true, DisableDHT: true})
	if err != nil {
		t.Fatal(err)
	}
	e := &Engine{root: cfg.Root, cfg: cfg, db: db, cache: cache, fs: fs, node: node, baselines: map[string]model.Baseline{}, manifests: map[string]model.Manifest{}, sources: map[string]map[string]blockSource{}, downloads: map[string]*download{}, wake: make(chan struct{}, 1)}
	node.SetBackend(e)
	t.Cleanup(func() { cancel(); _ = e.Close() })
	return e, ctx
}

func content(data string) model.Manifest {
	h := blake3.Sum256([]byte(data))
	hash := hex.EncodeToString(h[:])
	return model.Manifest{Hash: hash, Size: int64(len(data)), Blocks: []model.Block{{Hash: hash, Size: int64(len(data))}}}
}

func TestMetadataReceptionDoesNotObserveUnappliedRevision(t *testing.T) {
	e, ctx := coordinator(t)
	p := filepath.Join(e.root, "file")
	if err := os.WriteFile(p, []byte("initial"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := e.scan(true); err != nil {
		t.Fatal(err)
	}
	initial := e.baselines["file"].Clock
	remote := reconcile.NewRecord("file", "remote", 1, initial, content("remote missing bytes"), false, false, false)
	if err := e.Receive(ctx, []model.Record{remote}); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte("local edit"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := e.scan(true); err != nil {
		t.Fatal(err)
	}
	records, err := e.db.Records()
	if err != nil {
		t.Fatal(err)
	}
	var local model.Record
	for _, r := range records {
		if r.Manifest.Hash == content("local edit").Hash {
			local = r
		}
	}
	if local.ID == "" || local.Clock["remote"] != 0 || reconcile.Dominates(local.Clock, remote.Clock) {
		t.Fatal("unapplied remote bytes were treated as observed")
	}
	entries, err := reconcile.Resolve(records)
	if err != nil || len(entries) != 2 {
		t.Fatalf("concurrent content not preserved: %d %v", len(entries), err)
	}
}

func TestAllConflictCopiesMustBePreservedBeforeOriginalDeletion(t *testing.T) {
	e, ctx := coordinator(t)
	p := filepath.Join(e.root, "file")
	if err := os.WriteFile(p, []byte("initial"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := e.scan(true); err != nil {
		t.Fatal(err)
	}
	initial := e.baselines["file"].Clock
	if err := os.WriteFile(p, []byte("local surviving edit"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := e.scan(true); err != nil {
		t.Fatal(err)
	}
	remote := reconcile.NewRecord("file", "remote", 1, initial, content("unavailable other edit"), false, false, false)
	deleted := reconcile.NewRecord("file", "deleter", 1, initial, model.Manifest{}, true, false, false)
	if err := e.Receive(ctx, []model.Record{remote, deleted}); err != nil {
		t.Fatal(err)
	}
	if err := e.step(ctx, true); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(p)
	if err != nil || string(data) != "local surviving edit" {
		t.Fatal("original removed before every conflict copy was safe")
	}
	if e.Status().PendingFiles < 2 {
		t.Fatal("unavailable conflict did not keep deletion pending")
	}
	files, err := filepath.Glob(filepath.Join(e.root, "*.happysync-conflict-*"))
	if err != nil || len(files) != 1 {
		t.Fatalf("available conflict not materialized: %v %v", files, err)
	}
	data, err = os.ReadFile(files[0])
	if err != nil || string(data) != "local surviving edit" {
		t.Fatal("materialized conflict bytes differ")
	}
}

func TestVerifiedInstallPreservesModificationTime(t *testing.T) {
	e, ctx := coordinator(t)
	m := content("verified timestamp fixture")
	if err := e.cache.Put(m.Blocks[0].Hash, []byte("verified timestamp fixture")); err != nil {
		t.Fatal(err)
	}
	stamp := time.Date(1980, 1, 2, 3, 4, 5, 0, time.UTC)
	r := reconcile.NewRecord("file", "remote", 1, nil, m, false, false, false, stamp.UnixNano())
	if err := e.Receive(ctx, []model.Record{r}); err != nil {
		t.Fatal(err)
	}
	if err := e.apply(ctx, model.Entry{Path: "file", Record: r}); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(e.root, "file"))
	if err != nil || string(data) != "verified timestamp fixture" {
		t.Fatal("installed bytes differ")
	}
	info, err := os.Stat(filepath.Join(e.root, "file"))
	if err != nil || !info.ModTime().Equal(stamp) {
		t.Fatalf("mtime: %v %v", info, err)
	}
	if err := e.scan(true); err != nil {
		t.Fatal(err)
	}
	records, err := e.db.Records()
	if err != nil || len(records) != 1 {
		t.Fatal("best-effort mtime created a new local content revision")
	}
}

func TestOrphanAssemblyCleanupRetainsOriginalBackups(t *testing.T) {
	e, _ := coordinator(t)
	dir := filepath.Join(config.StateDir(e.root), "tmp")
	stage := filepath.Join(dir, "install-interrupted")
	backup := filepath.Join(dir, "backup-original")
	if err := os.WriteFile(stage, []byte("unpublished assembly"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(backup, []byte("original must survive"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := e.cleanupStages(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(stage); !os.IsNotExist(err) {
		t.Fatal("orphan assembly retained")
	}
	data, err := os.ReadFile(backup)
	if err != nil || string(data) != "original must survive" {
		t.Fatal("recovery backup discarded")
	}
}
