// Package transfer stages and verifies resumable blocks, using several peers concurrently.
package transfer

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/happysync/happysync/internal/model"
	"github.com/zeebo/blake3"
)

type Fetcher func(context.Context, int, string, int64) ([]byte, error)
type Cache struct {
	Dir      string
	Workers  int
	MaxBytes int64
	mu       sync.Mutex
	used     int64
	refs     map[string]int
	workers  chan struct{}
	Received atomic.Uint64
	Sources  sync.Map
}

func New(root string, workers int, maxBytes int64) (*Cache, error) {
	if maxBytes < 0 {
		return nil, errors.New("staging quota must be nonnegative")
	}
	if workers < 1 || workers > 64 {
		return nil, errors.New("transfer workers must be between 1 and 64")
	}
	dir := filepath.Join(root, ".HappySync", "tmp", "blocks")
	// Validate each app-owned ancestor before creating its child. MkdirAll
	// would follow a preexisting symlink in .HappySync or tmp.
	for _, path := range []string{filepath.Join(root, ".HappySync"), filepath.Dir(dir), dir} {
		info, err := os.Lstat(path)
		if os.IsNotExist(err) {
			if err := os.Mkdir(path, 0700); err != nil && !os.IsExist(err) {
				return nil, err
			}
			info, err = os.Lstat(path)
		}
		if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return nil, errors.New("unsafe block staging directory")
		}
	}
	c := &Cache{Dir: dir, Workers: workers, MaxBytes: maxBytes, refs: map[string]int{}, workers: make(chan struct{}, workers)}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	for _, entry := range entries {
		info, err := entry.Info()
		if err != nil {
			return nil, err
		}
		if !info.Mode().IsRegular() {
			return nil, errors.New("unsafe nonregular block staging entry")
		}
	}
	for _, entry := range entries {
		// Put renames verified chunks to their content hash. A remaining
		// block-* name is an interrupted private write, never resumable data.
		if strings.HasPrefix(entry.Name(), "block-") {
			if err := os.Remove(filepath.Join(dir, entry.Name())); err != nil {
				return nil, err
			}
			continue
		}
		info, err := entry.Info()
		if err != nil {
			return nil, err
		}
		c.used += info.Size()
	}
	return c, nil
}
func digest(b []byte) string  { h := blake3.Sum256(b); return hex.EncodeToString(h[:]) }
func validHash(s string) bool { b, e := hex.DecodeString(s); return e == nil && len(b) == 32 }
func (c *Cache) Read(hash string, size int64) ([]byte, error) {
	if !validHash(hash) || size < 0 || size > model.BlockSize {
		return nil, errors.New("invalid block request")
	}
	f, e := os.Open(filepath.Join(c.Dir, hash))
	if e != nil {
		return nil, e
	}
	defer f.Close()
	b, e := io.ReadAll(io.LimitReader(f, size+1))
	if e != nil {
		return nil, e
	}
	if int64(len(b)) != size || digest(b) != hash {
		return nil, errors.New("staged block failed verification")
	}
	return b, nil
}
func (c *Cache) Put(hash string, b []byte) error {
	if !validHash(hash) || len(b) > model.BlockSize || digest(b) != hash {
		return errors.New("received block failed verification")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, e := c.Read(hash, int64(len(b))); e == nil {
		return nil
	}
	oldSize := int64(0)
	if info, err := os.Stat(filepath.Join(c.Dir, hash)); err == nil {
		oldSize = info.Size()
	}
	if c.MaxBytes > 0 && c.used-oldSize+int64(len(b)) > c.MaxBytes {
		return errors.New("staging quota exceeded; increase --staging-limit")
	}
	f, e := os.CreateTemp(c.Dir, "block-")
	if e != nil {
		return e
	}
	name := f.Name()
	defer os.Remove(name)
	if e = f.Chmod(0600); e == nil {
		_, e = f.Write(b)
	}
	if e == nil {
		e = f.Sync()
	}
	closeErr := f.Close()
	if e != nil {
		return e
	}
	if closeErr != nil {
		return closeErr
	}
	if e = os.Rename(name, filepath.Join(c.Dir, hash)); e != nil {
		return e
	}
	c.used += int64(len(b)) - oldSize
	return nil
}
func (c *Cache) Ensure(ctx context.Context, m model.Manifest, sources int, fetch Fetcher) error {
	return c.EnsureAvailable(ctx, m, sources, nil, fetch)
}
func (c *Cache) EnsureAvailable(ctx context.Context, m model.Manifest, sources int, available [][]bool, fetch Fetcher) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	for _, block := range m.Blocks {
		if !validHash(block.Hash) || block.Size < 0 || block.Size > model.BlockSize {
			return errors.New("invalid block manifest")
		}
	}
	c.Retain(m)
	defer c.Unretain(m)
	if sources < 1 {
		for _, b := range m.Blocks {
			select {
			case c.workers <- struct{}{}:
			case <-ctx.Done():
				return ctx.Err()
			}
			_, e := c.Read(b.Hash, b.Size)
			<-c.workers
			if e != nil {
				return errors.New("no usable peer has the requested content")
			}
		}
		return ctx.Err()
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	jobs := make(chan int)
	errs := make(chan error, 1)
	var wg sync.WaitGroup
	workers := c.Workers
	if workers > len(m.Blocks) {
		workers = len(m.Blocks)
	}
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range jobs {
				select {
				case c.workers <- struct{}{}:
				case <-ctx.Done():
					return
				}
				err := c.ensureBlock(ctx, m, i, sources, available, fetch)
				<-c.workers
				if err != nil {
					select {
					case errs <- fmt.Errorf("block transfer failed: %w", err):
					default:
					}
					cancel()
					return
				}
			}
		}()
	}
	order := make([]int, len(m.Blocks))
	for i := range order {
		order[i] = i
	}
	rarity := func(i int) int {
		count := 0
		for _, a := range available {
			if len(a) == len(m.Blocks) && a[i] {
				count++
			}
		}
		if count == 0 {
			return sources + 1
		}
		return count
	}
	sort.SliceStable(order, func(a, b int) bool { return rarity(order[a]) < rarity(order[b]) })
send:
	for _, i := range order {
		select {
		case jobs <- i:
		case <-ctx.Done():
			break send
		}
	}
	close(jobs)
	wg.Wait()
	select {
	case e := <-errs:
		return e
	default:
		return ctx.Err()
	}
}

// ensureBlock runs under the cache-wide worker lease. Concurrent manifests share
// the same bound on network requests and block-sized buffers.
func (c *Cache) ensureBlock(ctx context.Context, m model.Manifest, i, sources int, available [][]bool, fetch Fetcher) error {
	b := m.Blocks[i]
	if _, e := c.Read(b.Hash, b.Size); e == nil {
		return nil
	}
	if fetch == nil {
		return errors.New("no block fetcher configured")
	}
	var last error
	for attempt := 0; attempt < sources; attempt++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		source := (i + attempt) % sources
		if source < len(available) && len(available[source]) == len(m.Blocks) && !available[source][i] {
			last = errors.New("block not available from advertised sources")
			continue
		}
		data, e := fetch(ctx, source, b.Hash, b.Size)
		if e == nil {
			e = c.Put(b.Hash, data)
		}
		if e == nil {
			c.Received.Add(uint64(len(data)))
			v, _ := c.Sources.LoadOrStore(source, new(atomic.Uint64))
			v.(*atomic.Uint64).Add(uint64(len(data)))
			last = nil
			break
		}
		last = e
	}
	return last
}

// Assemble returns a private file only after whole-file verification and fsync.
func (c *Cache) Assemble(m model.Manifest) (string, error) {
	if !validHash(m.Hash) || m.Size < 0 {
		return "", errors.New("invalid manifest")
	}
	c.Retain(m)
	defer c.Unretain(m)
	f, e := os.CreateTemp(filepath.Dir(c.Dir), "install-")
	if e != nil {
		return "", e
	}
	name := f.Name()
	ok := false
	defer func() {
		f.Close()
		if !ok {
			os.Remove(name)
		}
	}()
	h := blake3.New()
	var n int64
	for _, b := range m.Blocks {
		data, e := c.Read(b.Hash, b.Size)
		if e != nil {
			return "", e
		}
		written, e := io.MultiWriter(f, h).Write(data)
		if e != nil {
			return "", e
		}
		n += int64(written)
	}
	if n != m.Size || hex.EncodeToString(h.Sum(nil)) != m.Hash {
		return "", errors.New("whole-file verification failed")
	}
	if e = f.Sync(); e != nil {
		return "", e
	}
	if e = f.Close(); e != nil {
		return "", e
	}
	ok = true
	return name, nil
}

// Release removes completed staging blocks; installed files remain available through the engine's source index.
func (c *Cache) Release(m model.Manifest) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, b := range m.Blocks {
		if c.refs[b.Hash] > 0 {
			continue
		}
		path := filepath.Join(c.Dir, b.Hash)
		info, e := os.Stat(path)
		if os.IsNotExist(e) {
			continue
		}
		if e != nil {
			return e
		}
		if e = os.Remove(path); e != nil {
			return e
		}
		c.used -= info.Size()
	}
	return nil
}

// Retain prevents a completed concurrent file from evicting blocks still used
// by another active manifest. Repeated hashes within a manifest are one lease.
func (c *Cache) Retain(m model.Manifest) {
	c.mu.Lock()
	defer c.mu.Unlock()
	seen := map[string]bool{}
	for _, b := range m.Blocks {
		if !seen[b.Hash] {
			c.refs[b.Hash]++
			seen[b.Hash] = true
		}
	}
}
func (c *Cache) Unretain(m model.Manifest) {
	c.mu.Lock()
	defer c.mu.Unlock()
	seen := map[string]bool{}
	for _, b := range m.Blocks {
		if !seen[b.Hash] && c.refs[b.Hash] > 0 {
			c.refs[b.Hash]--
			if c.refs[b.Hash] == 0 {
				delete(c.refs, b.Hash)
			}
			seen[b.Hash] = true
		}
	}
}
