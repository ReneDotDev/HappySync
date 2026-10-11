package integration

import (
	"context"
	"encoding/hex"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/happysync/happysync/internal/config"
	"github.com/happysync/happysync/internal/engine"
	"github.com/happysync/happysync/internal/model"
	"github.com/happysync/happysync/internal/network"
	"github.com/happysync/happysync/internal/reconcile"
	"github.com/happysync/happysync/internal/store"
	"github.com/happysync/happysync/internal/transfer"
	"github.com/zeebo/blake3"
)

var crashOriginal = []byte("original bytes must survive until a verified atomic installation")
var crashReplacement = []byte("verified remote replacement after durable journal creation")
var postCrashEdit = []byte("independent local edit made after the process crashed")

// This helper runs only inside the test binary. Production CLI has no
// environment variable which can deliberately crash a user's sync process.
func TestCrashProcessHelper(t *testing.T) {
	root := os.Getenv("HAPPYSYNC_CRASH_TEST_ROOT")
	if root == "" {
		return
	}
	target := os.Getenv("HAPPYSYNC_CRASH_TEST_PATH")
	if target == "" {
		target = "target.txt"
	}
	cfg, err := config.Load(root)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	stage := os.Getenv("HAPPYSYNC_CRASH_TEST_STAGE")
	directoryMode := os.Getenv("HAPPYSYNC_CRASH_TEST_DIRECTORY") == "1"
	_, beforeErr := os.Lstat(filepath.Join(root, target))
	opts := engine.Options{Network: network.Config{Listen: []string{"/ip4/127.0.0.1/tcp/0"}, DisableMDNS: true, DisableDHT: true}, ScanInterval: time.Second}
	if stage != "" {
		changed := false
		opts.Fault = func(at string) error {
			if stage == "edit-after-journal" && at == "after-journal" && !changed {
				changed = true
				return os.WriteFile(filepath.Join(root, target), postCrashEdit, 0644)
			}
			if at == stage {
				os.Exit(73) // no defers, SQLite close, or graceful engine shutdown
			}
			return nil
		}
	}
	e, err := engine.New(ctx, cfg, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { cancel(); e.Wait(); _ = e.Close() }()
	if directoryMode && stage == "" && os.IsNotExist(beforeErr) && !hasFile(root, target, crashOriginal) {
		t.Fatal("type-backup recovery did not restore original before initial scan")
	}
	if stage != "" {
		records, _, err := e.Snapshot(ctx, "", 128)
		if err != nil || len(records) != 1 {
			t.Fatalf("initial snapshot: %v, count %d", err, len(records))
		}
		h := blake3.Sum256(crashReplacement)
		hash := hex.EncodeToString(h[:])
		m := model.Manifest{Hash: hash, Size: int64(len(crashReplacement)), Blocks: []model.Block{{Hash: hash, Size: int64(len(crashReplacement))}}}
		cache, err := transfer.New(root, 1, 1<<20)
		if err != nil {
			t.Fatal(err)
		}
		if err = cache.Put(hash, crashReplacement); err != nil {
			t.Fatal(err)
		}
		r := reconcile.NewRecord(target, "remote-crash-fixture", 1, records[0].Clock, m, false, false, false)
		if directoryMode {
			r = reconcile.NewRecord(target, "remote-crash-fixture", 1, records[0].Clock, model.Manifest{}, false, true, false)
		}
		if err = e.Receive(ctx, []model.Record{r}); err != nil {
			t.Fatal(err)
		}
		if stage != "edit-after-journal" {
			<-ctx.Done()
			t.Fatal("engine never reached requested crash point")
		}
	}
	if os.Getenv("HAPPYSYNC_CRASH_TEST_EDITED") == "1" || stage == "edit-after-journal" {
		wait(t, 12*time.Second, "local edit and remote version preserved after crash", func() bool {
			files, err := tree(root)
			if err != nil || len(files) != 2 {
				return false
			}
			foundRemote, foundLocal := false, false
			remoteBytes := crashReplacement
			if directoryMode {
				remoteBytes = crashOriginal // retained private backup must also survive
			}
			for path := range files {
				foundRemote = foundRemote || hasFile(root, path, remoteBytes)
				foundLocal = foundLocal || hasFile(root, path, postCrashEdit)
			}
			return foundRemote && foundLocal
		})
	} else if directoryMode {
		wait(t, 12*time.Second, "recovered file-to-directory install", func() bool {
			info, err := os.Stat(filepath.Join(root, target))
			files, treeErr := tree(root)
			return err == nil && info.IsDir() && treeErr == nil && len(files) == 0
		})
	} else {
		wait(t, 12*time.Second, "verified recovered target", func() bool { return hasFile(root, target, crashReplacement) })
	}
}

func crashChild(t *testing.T, root, stage string, edited ...bool) error {
	t.Helper()
	return crashChildWithPath(t, root, stage, "target.txt", edited...)
}

func crashChildWithPath(t *testing.T, root, stage, target string, edited ...bool) error {
	t.Helper()
	if testing.Short() {
		t.Skip("abrupt-process crash integration is omitted in short mode")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestCrashProcessHelper$", "-test.count=1")
	cmd.Env = append(os.Environ(), "HAPPYSYNC_CRASH_TEST_ROOT="+root, "HAPPYSYNC_CRASH_TEST_STAGE="+stage, "HAPPYSYNC_CRASH_TEST_PATH="+target)
	if len(edited) > 0 && edited[0] {
		cmd.Env = append(cmd.Env, "HAPPYSYNC_CRASH_TEST_EDITED=1")
	}
	if len(edited) > 1 && edited[1] {
		cmd.Env = append(cmd.Env, "HAPPYSYNC_CRASH_TEST_DIRECTORY=1")
	}
	out, err := cmd.CombinedOutput()
	if (stage == "" || stage == "edit-after-journal") && err != nil {
		t.Fatalf("restart recovery: %v: %s", err, redacted(string(out)))
	}
	return err
}

func TestFileToDirectoryCrashRecovery(t *testing.T) {
	for _, stage := range []string{"after-type-backup", "after-install", "after-baseline"} {
		t.Run(stage, func(t *testing.T) {
			root := t.TempDir()
			write(t, root, "target.txt", crashOriginal)
			if _, err := config.Init(root, ""); err != nil {
				t.Fatal(err)
			}
			err := crashChild(t, root, stage, false, true)
			if exit, ok := err.(*exec.ExitError); !ok || exit.ExitCode() != 73 {
				t.Fatalf("expected abrupt type-transition crash exit73: %v", err)
			}
			if stage == "after-type-backup" && !absent(root, "target.txt") {
				t.Fatal("type-backup crash did not occur between removal and directory creation")
			}
			entries, err := os.ReadDir(filepath.Join(root, ".HappySync", "tmp"))
			if err != nil {
				t.Fatal(err)
			}
			foundOriginal := false
			for _, entry := range entries {
				if !entry.IsDir() && hasFile(root, ".HappySync/tmp/"+entry.Name(), crashOriginal) {
					foundOriginal = true
				}
			}
			if !foundOriginal {
				t.Fatal("file-to-directory gap lost original instead of keeping durable backup")
			}
			_ = crashChild(t, root, "", false, true)
			_ = crashChild(t, root, "", false, true)
			db, err := store.Open(root)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			records, err := db.Records()
			if err != nil || len(records) != 2 {
				t.Fatalf("type recovery fabricated changes: count=%d error=%v", len(records), err)
			}
			journals, err := db.Journals()
			if err != nil || len(journals) != 0 {
				t.Fatal("type recovery left durable journal")
			}
			entries, err = os.ReadDir(filepath.Join(root, ".HappySync", "tmp"))
			if err != nil {
				t.Fatal(err)
			}
			for _, entry := range entries {
				if !entry.IsDir() {
					t.Fatalf("orphan private type backup: %s", entry.Name())
				}
			}
		})
	}
}

func TestFileToDirectoryBackupPreservesPostCrashLocalFile(t *testing.T) {
	for _, target := range []string{"target.txt", strings.Repeat("x", 190) + ".txt"} {
		t.Run(fmt.Sprintf("basename-bytes-%d", len(target)), func(t *testing.T) {
			root := t.TempDir()
			write(t, root, target, crashOriginal)
			if _, err := config.Init(root, ""); err != nil {
				t.Fatal(err)
			}
			err := crashChildWithPath(t, root, "after-type-backup", target, false, true)
			if exit, ok := err.(*exec.ExitError); !ok || exit.ExitCode() != 73 {
				t.Fatalf("expected abrupt type-backup crash exit73: %v", err)
			}
			write(t, root, target, postCrashEdit)
			_ = crashChildWithPath(t, root, "", target, true, true)
			_ = crashChildWithPath(t, root, "", target, true, true)
		})
	}
}

func TestInstallationPreservesEditAfterJournalCommit(t *testing.T) {
	root := t.TempDir()
	write(t, root, "target.txt", crashOriginal)
	if _, err := config.Init(root, ""); err != nil {
		t.Fatal(err)
	}
	_ = crashChild(t, root, "edit-after-journal")
	_ = crashChild(t, root, "", true)
}

func TestCrashRecoveryPreservesPostCrashLocalEdits(t *testing.T) {
	for _, stage := range []string{"after-journal", "after-install"} {
		t.Run(stage, func(t *testing.T) {
			root := t.TempDir()
			write(t, root, "target.txt", crashOriginal)
			if _, err := config.Init(root, ""); err != nil {
				t.Fatal(err)
			}
			err := crashChild(t, root, stage)
			if exit, ok := err.(*exec.ExitError); !ok || exit.ExitCode() != 73 {
				t.Fatalf("expected abrupt crash exit 73: %v", err)
			}
			write(t, root, "target.txt", postCrashEdit)
			_ = crashChild(t, root, "", true)
			_ = crashChild(t, root, "", true)
		})
	}
}

func TestAbruptProcessCrashRecovery(t *testing.T) {
	for _, stage := range []string{"after-journal", "after-install", "after-baseline"} {
		t.Run(stage, func(t *testing.T) {
			root := t.TempDir()
			write(t, root, "target.txt", crashOriginal)
			if _, err := config.Init(root, ""); err != nil {
				t.Fatal(err)
			}
			err := crashChild(t, root, stage)
			if exit, ok := err.(*exec.ExitError); !ok || exit.ExitCode() != 73 {
				t.Fatalf("expected abrupt crash exit 73: %v", err)
			}
			want := crashReplacement
			if stage == "after-journal" {
				want = crashOriginal
			}
			if !hasFile(root, "target.txt", want) {
				t.Fatal("crash exposed truncated content or lost original before install")
			}
			_ = crashChild(t, root, "")
			// Reopen in a separate process twice: recovery must be durable and not
			// synthesize an additional local edit from the interrupted rename.
			_ = crashChild(t, root, "")
			db, err := store.Open(root)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			records, err := db.Records()
			if err != nil || len(records) != 2 {
				t.Fatalf("crash recovery fabricated causal revisions: count %d, error %v", len(records), err)
			}
			journals, err := db.Journals()
			if err != nil || len(journals) != 0 {
				t.Fatalf("recovery left durable install journal: %v", err)
			}
			baseline, err := db.Baselines()
			if err != nil || baseline["target.txt"].Hash == "" {
				t.Fatal("verified target baseline missing")
			}
			if files, err := tree(root); err != nil || len(files) != 1 {
				t.Fatalf("unexpected recovery conflict or extra user file: %v", err)
			}
			entries, err := os.ReadDir(filepath.Join(root, ".HappySync", "tmp"))
			if err != nil {
				t.Fatal(err)
			}
			for _, entry := range entries {
				if !entry.IsDir() {
					t.Fatalf("orphan assembly file after recovery: %s", entry.Name())
				}
			}
		})
	}
}
