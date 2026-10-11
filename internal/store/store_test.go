package store

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"

	"github.com/happysync/happysync/internal/config"
	"github.com/happysync/happysync/internal/model"
)

func TestDigestBatchCountersAndBaselinePatchesSurviveRestart(t *testing.T) {
	root := t.TempDir()
	s, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	empty, err := s.RecordDigest(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	first, err := s.ReserveCounters(100000)
	if err != nil || first != 1 {
		t.Fatalf("reservation: %d %v", first, err)
	}
	r := model.Record{ID: "a", Path: "file", Counter: first}
	if err = s.Merge([]model.Record{r}); err != nil {
		t.Fatal(err)
	}
	digest, err := s.RecordDigest(context.Background())
	if err != nil || digest == empty {
		t.Fatal("insert did not change digest")
	}
	bad := r
	bad.Path = "changed"
	if err = s.Merge([]model.Record{{ID: "b", Path: "other"}, bad}); err == nil {
		t.Fatal("mutable ID accepted")
	}
	after, err := s.RecordDigest(context.Background())
	if err != nil || after != digest {
		t.Fatal("rolled back merge changed digest")
	}
	if err = s.PatchBaselines(map[string]model.Baseline{"keep": {Hash: "one"}, "remove": {Hash: "two"}}, nil); err != nil {
		t.Fatal(err)
	}
	if err = s.PatchBaselines(map[string]model.Baseline{"keep": {Hash: "new"}}, []string{"remove"}); err != nil {
		t.Fatal(err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(root)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	after, err = s.RecordDigest(context.Background())
	if err != nil || after != digest {
		t.Fatal("digest changed across restart")
	}
	n, err := s.NextCounter()
	if err != nil || n != 100001 {
		t.Fatalf("reserved counters reused: %d %v", n, err)
	}
	b, err := s.Baselines()
	if err != nil || len(b) != 1 || b["keep"].Hash != "new" {
		t.Fatal("baseline patch did not persist")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err = s.RecordDigest(ctx); err == nil {
		t.Fatal("cached digest ignored cancellation")
	}
}

func TestDurableStateAndRollback(t *testing.T) {
	root := t.TempDir()
	s, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	r := model.Record{ID: "a", Path: "file", Origin: "device", Counter: 1, Clock: model.Vector{"device": 1}}
	if err = s.Merge([]model.Record{r, r}); err != nil {
		t.Fatal(err)
	}
	bad := r
	bad.Path = "collision"
	extra := r
	extra.ID = "b"
	if err = s.Merge([]model.Record{extra, bad}); err == nil {
		t.Fatal("accepted mutable ID")
	}
	if n, err := s.NextCounter(); err != nil || n != 1 {
		t.Fatalf("counter %d %v", n, err)
	}
	if err = s.SetBaselines(map[string]model.Baseline{"file": {Hash: "abc", Size: 3}}); err != nil {
		t.Fatal(err)
	}
	if err = s.PutJournal(Journal{ID: "operation", Path: "file", Stage: "tmp/install", Record: r}); err != nil {
		t.Fatal(err)
	}
	if err = s.PutCheckpoint(Checkpoint{Hash: "block", Path: "tmp/block", Size: 3}); err != nil {
		t.Fatal(err)
	}
	if err = s.SetMeta("reconciled", "yes"); err != nil {
		t.Fatal(err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(root)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if v, err := s.GetMeta("reconciled"); err != nil || v != "yes" {
		t.Fatalf("persisted metadata: %q %v", v, err)
	}
	records, err := s.Records()
	if err != nil || len(records) != 1 || records[0].ID != "a" {
		t.Fatalf("records rollback: %v %v", records, err)
	}
	if n, err := s.NextCounter(); err != nil || n != 2 {
		t.Fatalf("durable counter %d %v", n, err)
	}
	b, err := s.Baselines()
	if err != nil || b["file"].Hash != "abc" {
		t.Fatalf("baseline %v %v", b, err)
	}
	j, err := s.Journals()
	if err != nil || len(j) != 1 || j[0].Record.ID != "a" {
		t.Fatalf("journal %v %v", j, err)
	}
	c, err := s.Checkpoints()
	if err != nil || len(c) != 1 || c[0].Size != 3 {
		t.Fatalf("checkpoint %v %v", c, err)
	}
	if err = s.DeleteJournal("operation"); err != nil {
		t.Fatal(err)
	}
	if err = s.DeleteCheckpoint("block"); err != nil {
		t.Fatal(err)
	}
}

func TestCounterConcurrencyAndPagination(t *testing.T) {
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	var wg sync.WaitGroup
	seen := make(chan uint64, 100)
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			n, e := s.NextCounter()
			if e != nil {
				t.Error(e)
			}
			seen <- n
		}()
	}
	wg.Wait()
	close(seen)
	set := make(map[uint64]bool)
	for n := range seen {
		if set[n] {
			t.Fatal("counter reused")
		}
		set[n] = true
	}
	records := make([]model.Record, 1000)
	for i := range records {
		records[i] = model.Record{ID: fmt.Sprintf("%04d", i), Path: fmt.Sprintf("f/%04d", i), Deleted: i%2 == 0}
	}
	if err = s.Merge(records); err != nil {
		t.Fatal(err)
	}
	after := ""
	total := 0
	for {
		page, err := s.RecordPage(after, 127)
		if err != nil {
			t.Fatal(err)
		}
		if len(page) == 0 {
			break
		}
		for _, r := range page {
			if r.ID <= after {
				t.Fatal("unordered page")
			}
			after = r.ID
			total++
		}
	}
	if total != 1000 {
		t.Fatalf("pagination lost records: %d", total)
	}
}

func TestCorruptionAndFutureSchema(t *testing.T) {
	root := t.TempDir()
	if err := config.EnsureStateDir(root); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(config.StateDir(root), "state.db"), []byte("broken database"), 0600); err != nil {
		t.Fatal(err)
	}
	if s, err := Open(root); err == nil {
		s.Close()
		t.Fatal("accepted corrupt database")
	}
	root = t.TempDir()
	s, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.db.Exec("PRAGMA user_version=999"); err != nil {
		t.Fatal(err)
	}
	s.Close()
	if s, err = Open(root); err == nil {
		s.Close()
		t.Fatal("accepted future schema")
	}
}

func TestMigrationFailureRollsBack(t *testing.T) {
	root := t.TempDir()
	if err := config.EnsureStateDir(root); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", filepath.Join(config.StateDir(root), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err = db.Exec("CREATE TABLE records (invalid TEXT)"); err != nil {
		t.Fatal(err)
	}
	if s, err := Open(root); err == nil {
		s.Close()
		t.Fatal("migration unexpectedly accepted conflicting schema")
	}
	var version, count int
	if err = db.QueryRow("PRAGMA user_version").Scan(&version); err != nil {
		t.Fatal(err)
	}
	if err = db.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE name='counters'").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if version != 0 || count != 0 {
		t.Fatal("failed migration left partial state")
	}
}

func TestRejectIncompleteClaimedSchema(t *testing.T) {
	root := t.TempDir()
	s, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.db.Exec("DROP TABLE baselines"); err != nil {
		t.Fatal(err)
	}
	s.Close()
	if s, err = Open(root); err == nil {
		s.Close()
		t.Fatal("accepted incomplete schema")
	}
}

func TestVersionOneUpgradePreservesState(t *testing.T) {
	root := t.TempDir()
	s, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Merge([]model.Record{{ID: "tombstone", Path: "gone", Deleted: true}}); err != nil {
		t.Fatal(err)
	}
	if _, err = s.db.Exec("DROP TABLE metadata"); err != nil {
		t.Fatal(err)
	}
	if _, err = s.db.Exec("PRAGMA user_version=1"); err != nil {
		t.Fatal(err)
	}
	s.Close()
	s, err = Open(root)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err = s.SetMeta("upgraded", "yes"); err != nil {
		t.Fatal(err)
	}
	records, err := s.Records()
	if err != nil || len(records) != 1 || !records[0].Deleted {
		t.Fatal("upgrade lost tombstone")
	}
}

func TestDatabaseAndWALPermissions(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows ACL protection is tested by config")
	}
	root := t.TempDir()
	s, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for _, suffix := range []string{"", "-wal", "-shm"} {
		info, err := os.Stat(filepath.Join(config.StateDir(root), "state.db"+suffix))
		if err != nil {
			t.Fatal(err)
		}
		if info.Mode().Perm()&0077 != 0 {
			t.Fatalf("database%s has unsafe permissions %o", suffix, info.Mode().Perm())
		}
	}
}

func TestIncrementalBaselineUpdates(t *testing.T) {
	root := t.TempDir()
	s, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.SetBaselines(map[string]model.Baseline{"original": {Hash: "keep"}}); err != nil {
		t.Fatal(err)
	}
	if err = s.PutBaseline("deleted", model.Baseline{Deleted: true, Clock: model.Vector{"peer": 2}, SourceID: "revision", SourcePath: "source"}); err != nil {
		t.Fatal(err)
	}
	if err = s.PutBaseline("original", model.Baseline{Hash: "updated"}); err != nil {
		t.Fatal(err)
	}
	s.Close()
	s, err = Open(root)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	baselines, err := s.Baselines()
	if err != nil || len(baselines) != 2 || baselines["original"].Hash != "updated" || !baselines["deleted"].Deleted || baselines["deleted"].Clock["peer"] != 2 || baselines["deleted"].SourceID != "revision" {
		t.Fatal("incremental baseline state not durable")
	}
	if err = s.DeleteBaseline("original"); err != nil {
		t.Fatal(err)
	}
	baselines, err = s.Baselines()
	if err != nil || len(baselines) != 1 || !baselines["deleted"].Deleted {
		t.Fatal("DeleteBaseline damaged unrelated state")
	}
}
