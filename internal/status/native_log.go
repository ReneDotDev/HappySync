package status

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// OpenNativeLog keeps an append descriptor on one regular inode. Periodic
// truncation can then bound retention without changing an inherited stderr
// descriptor or the process-global os.Stderr pointer.
func OpenNativeLog(path string, maximum int64) (*os.File, error) {
	if maximum < 1 {
		return nil, errors.New("invalid native log limit")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return nil, err
	}
	parent, err := os.Lstat(filepath.Dir(path))
	if err != nil || !parent.IsDir() || parent.Mode()&os.ModeSymlink != 0 {
		return nil, errors.New("unsafe native log directory")
	}
	info, err := os.Lstat(path)
	flags := os.O_APPEND | os.O_WRONLY
	if os.IsNotExist(err) {
		flags |= os.O_CREATE | os.O_EXCL
	} else if err != nil {
		return nil, err
	} else if !info.Mode().IsRegular() {
		return nil, errors.New("unsafe native log path")
	}
	f, err := os.OpenFile(path, flags, 0600)
	if err != nil {
		return nil, err
	}
	opened, err := f.Stat()
	current, pathErr := os.Lstat(path)
	if err != nil || pathErr != nil || !current.Mode().IsRegular() || !os.SameFile(opened, current) {
		f.Close()
		return nil, errors.New("native log path changed while opening")
	}
	if opened.Size() > maximum {
		if err := f.Truncate(0); err != nil {
			f.Close()
			return nil, err
		}
	}
	return f, nil
}

type NativeLogGuard struct {
	cancel   context.CancelFunc
	done     chan struct{}
	failures chan error
	once     sync.Once
}

// StartNativeLogGuard restores the retention limit every interval, truncating
// the same inode used by native append writers. Growth between checks is
// possible; this is a retention policy rather than a hard write quota.
func StartNativeLogGuard(ctx context.Context, path string, maximum int64, interval time.Duration) (*NativeLogGuard, error) {
	if interval <= 0 {
		return nil, errors.New("invalid native log check interval")
	}
	f, err := OpenNativeLog(path, maximum)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(ctx)
	g := &NativeLogGuard{cancel: cancel, done: make(chan struct{}), failures: make(chan error, 1)}
	go func() {
		defer close(g.done)
		defer close(g.failures)
		defer f.Close()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				info, err := f.Stat()
				if err == nil && info.Size() > maximum {
					err = f.Truncate(0)
				}
				if err != nil {
					g.failures <- err
					return
				}
			}
		}
	}()
	return g, nil
}

func (g *NativeLogGuard) Failures() <-chan error { return g.failures }
func (g *NativeLogGuard) Close() error {
	g.once.Do(g.cancel)
	<-g.done
	return nil
}
