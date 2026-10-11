package transfer

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/happysync/happysync/internal/model"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestSwarmResumeAndCorruption(t *testing.T) {
	root := t.TempDir()
	c, e := New(root, 4, 32<<20)
	if e != nil {
		t.Fatal(e)
	}
	a := make([]byte, model.BlockSize)
	b := make([]byte, model.BlockSize)
	for i := range b {
		b[i] = 1
	}
	all := append(append([]byte{}, a...), b...)
	m := model.Manifest{Hash: digest(all), Size: int64(len(all)), Blocks: []model.Block{{Hash: digest(a), Size: int64(len(a))}, {Hash: digest(b), Size: int64(len(b))}}}
	var mu sync.Mutex
	used := map[int]bool{}
	fetch := func(ctx context.Context, source int, hash string, size int64) ([]byte, error) {
		mu.Lock()
		used[source] = true
		mu.Unlock()
		if hash == digest(a) {
			return a, nil
		}
		return b, nil
	}
	if e = c.Ensure(context.Background(), m, 2, fetch); e != nil {
		t.Fatal(e)
	}
	if len(used) != 2 {
		t.Fatalf("expected two sources, got %v", used)
	}
	c2, _ := New(root, 2, 32<<20)
	if e = c2.Ensure(context.Background(), m, 0, nil); e != nil {
		t.Fatal(e)
	}
	p, e := c2.Assemble(m)
	if e != nil {
		t.Fatal(e)
	}
	got, e := os.ReadFile(p)
	if e != nil || digest(got) != m.Hash {
		t.Fatal("assembled content mismatch")
	}
	if e = c.Put(digest(a), b); e == nil {
		t.Fatal("accepted corrupt block")
	}
}

func smallManifest(parts ...[]byte) model.Manifest {
	var all []byte
	var blocks []model.Block
	for _, part := range parts {
		all = append(all, part...)
		blocks = append(blocks, model.Block{Hash: digest(part), Size: int64(len(part))})
	}
	return model.Manifest{Hash: digest(all), Size: int64(len(all)), Blocks: blocks}
}

func TestConcurrentManifestsShareWorkerBudget(t *testing.T) {
	c, err := New(t.TempDir(), 2, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	var manifests []model.Manifest
	contents := map[string][]byte{}
	for task := 0; task < 3; task++ {
		var parts [][]byte
		for block := 0; block < 3; block++ {
			part := []byte(fmt.Sprintf("unique task %d block %d", task, block))
			parts = append(parts, part)
			contents[digest(part)] = part
		}
		manifests = append(manifests, smallManifest(parts...))
	}
	var active, maximum atomic.Int32
	started := make(chan struct{}, 16)
	release := make(chan struct{})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	fetch := func(ctx context.Context, _ int, hash string, _ int64) ([]byte, error) {
		n := active.Add(1)
		defer active.Add(-1)
		for old := maximum.Load(); n > old; old = maximum.Load() {
			if maximum.CompareAndSwap(old, n) {
				break
			}
		}
		started <- struct{}{}
		select {
		case <-release:
			return contents[hash], nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	results := make(chan error, len(manifests))
	for _, m := range manifests {
		go func(m model.Manifest) { results <- c.EnsureAvailable(ctx, m, 2, nil, fetch) }(m)
	}
	for range 2 {
		select {
		case <-started:
		case <-ctx.Done():
			t.Fatal("workers did not start")
		}
	}
	select {
	case <-started:
		t.Fatal("concurrent manifests exceeded global worker budget")
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	for range manifests {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	if maximum.Load() != 2 {
		t.Fatalf("maximum workers %d", maximum.Load())
	}
	for _, m := range manifests {
		path, err := c.Assemble(m)
		if err != nil {
			t.Fatal(err)
		}
		data, err := os.ReadFile(path)
		if err != nil || digest(data) != m.Hash {
			t.Fatal("concurrent content integrity")
		}
	}
}

func TestRetainedOverlappingManifestsSurviveEviction(t *testing.T) {
	c, err := New(t.TempDir(), 2, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	shared, one, two := []byte("shared verified block"), []byte("first file"), []byte("second file")
	a, b := smallManifest(shared, one, shared), smallManifest(shared, two)
	for _, part := range [][]byte{shared, one, two} {
		if err := c.Put(digest(part), part); err != nil {
			t.Fatal(err)
		}
	}
	c.Retain(a)
	c.Retain(b)
	c.Unretain(a)
	if err := c.Release(a); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Read(digest(shared), int64(len(shared))); err != nil {
		t.Fatal("shared block evicted despite active second manifest")
	}
	if _, err := c.Read(digest(one), int64(len(one))); !os.IsNotExist(err) {
		t.Fatal("unreferenced block was not released")
	}
	path, err := c.Assemble(b)
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(data, append(append([]byte{}, shared...), two...)) {
		t.Fatal("retained manifest assembled incorrect content")
	}
	c.Unretain(b)
	if err := c.Release(b); err != nil {
		t.Fatal(err)
	}
	if c.used != 0 || len(c.refs) != 0 {
		t.Fatalf("leases/accounting leaked used=%d refs=%v", c.used, c.refs)
	}
}

func TestEnsureRetainsBlocksAcrossConcurrentRelease(t *testing.T) {
	c, err := New(t.TempDir(), 1, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	shared, missing := []byte("needed shared block"), []byte("new block")
	m := smallManifest(shared, missing)
	if err := c.Put(digest(shared), shared); err != nil {
		t.Fatal(err)
	}
	started, release := make(chan struct{}), make(chan struct{})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	result := make(chan error, 1)
	go func() {
		result <- c.Ensure(ctx, m, 1, func(ctx context.Context, _ int, _ string, _ int64) ([]byte, error) {
			close(started)
			select {
			case <-release:
				return missing, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		})
	}()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("fetch did not start")
	}
	if err := c.Release(smallManifest(shared)); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Read(digest(shared), int64(len(shared))); err != nil {
		t.Fatal("active transfer lost verified shared block")
	}
	close(release)
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	if _, err := c.Assemble(m); err != nil {
		t.Fatal(err)
	}
}

func TestCancellationReleasesGlobalWorkersAndPreservesResume(t *testing.T) {
	root := t.TempDir()
	c, err := New(root, 1, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	verified, missing := []byte("durable verified block"), []byte("interrupt this transfer")
	m := smallManifest(verified, missing)
	if err := c.Put(digest(verified), verified); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	started := make(chan struct{})
	result := make(chan error, 1)
	go func() {
		result <- c.Ensure(ctx, m, 1, func(ctx context.Context, _ int, _ string, _ int64) ([]byte, error) {
			close(started)
			<-ctx.Done()
			return nil, ctx.Err()
		})
	}()
	<-started
	waiting, wcancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer wcancel()
	if err := c.Ensure(waiting, smallManifest([]byte("queued")), 1, func(context.Context, int, string, int64) ([]byte, error) {
		t.Error("waiting task bypassed worker budget")
		return nil, nil
	}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("waiting cancellation %v", err)
	}
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("cancellation did not release worker")
	}
	resumed, err := New(root, 1, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	var fetched atomic.Int32
	if err := resumed.Ensure(context.Background(), m, 1, func(_ context.Context, _ int, hash string, _ int64) ([]byte, error) {
		fetched.Add(1)
		if hash != digest(missing) {
			return nil, errors.New("verified work unexpectedly refetched")
		}
		return missing, nil
	}); err != nil {
		t.Fatal(err)
	}
	if fetched.Load() != 1 {
		t.Fatal("restart did not preserve verified work")
	}
	path, err := resumed.Assemble(m)
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(data, append(append([]byte{}, verified...), missing...)) {
		t.Fatal("resumed transfer content differs from verified expected bytes")
	}
	if len(c.workers) != 0 || len(c.refs) != 0 {
		t.Fatal("canceled task leaked global worker or manifest lease")
	}
}

func TestCorruptSourceRetriesAnotherSource(t *testing.T) {
	c, err := New(t.TempDir(), 2, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	data := []byte("correct verified content")
	m := smallManifest(data)
	var attempted []int
	if err := c.EnsureAvailable(context.Background(), m, 2, [][]bool{{true}, {true}}, func(_ context.Context, source int, _ string, _ int64) ([]byte, error) {
		attempted = append(attempted, source)
		if source == 0 {
			return []byte("malicious corrupt response"), nil
		}
		return data, nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(attempted) != 2 || attempted[0] != 0 || attempted[1] != 1 {
		t.Fatalf("corrupt response did not retry %v", attempted)
	}
	got, err := c.Read(m.Blocks[0].Hash, m.Blocks[0].Size)
	if err != nil || !bytes.Equal(got, data) {
		t.Fatal("retry installed unverified bytes")
	}
	if c.Received.Load() != uint64(len(data)) {
		t.Fatal("corrupt response counted as verified received content")
	}
}

func TestStartupRemovesOnlyUncommittedBlockTempsAndRecoversQuota(t *testing.T) {
	root := t.TempDir()
	c, err := New(root, 1, 64)
	if err != nil {
		t.Fatal(err)
	}
	verified := []byte("verified resume chunk")
	if err := c.Put(digest(verified), verified); err != nil {
		t.Fatal(err)
	}
	orphan := filepath.Join(c.Dir, "block-crashed-write")
	if err := os.WriteFile(orphan, bytes.Repeat([]byte("x"), 1024), 0600); err != nil {
		t.Fatal(err)
	}
	unknown := filepath.Join(c.Dir, "unknown-private-file")
	if err := os.WriteFile(unknown, []byte("keep"), 0600); err != nil {
		t.Fatal(err)
	}
	restarted, err := New(root, 1, 64)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(orphan); !os.IsNotExist(err) {
		t.Fatal("uncommitted block temp survived startup")
	}
	if data, err := os.ReadFile(unknown); err != nil || string(data) != "keep" {
		t.Fatal("startup removed unknown private file")
	}
	if got, err := restarted.Read(digest(verified), int64(len(verified))); err != nil || !bytes.Equal(got, verified) {
		t.Fatal("startup removed verified resumable chunk")
	}
	if restarted.used != int64(len(verified)+4) {
		t.Fatalf("orphan retained staging quota %d", restarted.used)
	}
	newData := []byte("new verified chunk")
	if err := restarted.Put(digest(newData), newData); err != nil {
		t.Fatalf("orphan prevented subsequent staging %v", err)
	}
}

func TestStartupRejectsNonregularCacheEntriesAndSymlinkAncestors(t *testing.T) {
	root := t.TempDir()
	c, err := New(root, 1, 1024)
	if err != nil {
		t.Fatal(err)
	}
	outside := filepath.Join(t.TempDir(), "outside")
	if err := os.WriteFile(outside, []byte("external file"), 0600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(c.Dir, "block-symlink")
	if err := os.Symlink(outside, link); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	if _, err := New(root, 1, 1024); err == nil {
		t.Fatal("nonregular cache entry accepted")
	}
	if data, err := os.ReadFile(outside); err != nil || string(data) != "external file" {
		t.Fatal("outside file modified through orphan symlink")
	}
	if _, err := os.Lstat(link); err != nil {
		t.Fatal("unsafe entry removed instead of rejecting cache")
	}
	for _, ancestor := range []string{".HappySync", filepath.Join(".HappySync", "tmp"), filepath.Join(".HappySync", "tmp", "blocks")} {
		t.Run(ancestor, func(t *testing.T) {
			root := t.TempDir()
			path := filepath.Join(root, ancestor)
			if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
				t.Fatal(err)
			}
			outside := t.TempDir()
			if err := os.Symlink(outside, path); err != nil {
				t.Fatal(err)
			}
			if _, err := New(root, 1, 1024); err == nil {
				t.Fatal("symlink staging ancestor accepted")
			}
			entries, err := os.ReadDir(outside)
			if err != nil || len(entries) != 0 {
				t.Fatal("startup created staging children outside root")
			}
		})
	}
}

func TestStartupPreservesHashNamedChunksAndVerifiesBeforeReuse(t *testing.T) {
	root := t.TempDir()
	c, err := New(root, 1, 1024)
	if err != nil {
		t.Fatal(err)
	}
	data := []byte("expected intact chunk")
	m := smallManifest(data)
	path := filepath.Join(c.Dir, digest(data))
	if err := os.WriteFile(path, bytes.Repeat([]byte("x"), len(data)), 0600); err != nil {
		t.Fatal(err)
	}
	restarted, err := New(root, 1, 1024)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatal("startup deleted hash-named partial before verification")
	}
	if _, err := restarted.Read(digest(data), int64(len(data))); err == nil {
		t.Fatal("corrupt persisted chunk reused without hash verification")
	}
	if err := restarted.Ensure(context.Background(), m, 1, func(context.Context, int, string, int64) ([]byte, error) { return data, nil }); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(got, data) {
		t.Fatal("persisted corrupt chunk did not recover exact verified bytes")
	}
}
