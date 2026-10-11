//go:build !windows

package daemon

import (
	"crypto/sha256"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"time"
)

type RootLock struct{ f *os.File }

func lockFile(path string) (*RootLock, error) {
	f, e := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if e != nil {
		return nil, e
	}
	if e = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); e != nil {
		f.Close()
		return nil, fmt.Errorf("already running; use HappySync status")
	}
	return &RootLock{f}, nil
}
func (l *RootLock) Close() error { syscall.Flock(int(l.f.Fd()), syscall.LOCK_UN); return l.f.Close() }
func endpoint(root string) string {
	hash := sha256.Sum256([]byte(root))
	name := fmt.Sprintf("happysync-%d-%x.sock", os.Getuid(), hash[:16])
	p := filepath.Join(os.TempDir(), name)
	if len(p) > 100 {
		p = filepath.Join("/tmp", name)
	}
	return p
}
func listenLocal(root string) (net.Listener, string, error) {
	p := endpoint(root)
	os.Remove(p)
	l, e := net.Listen("unix", p)
	if e != nil {
		return nil, p, e
	}
	if e = os.Chmod(p, 0600); e != nil {
		l.Close()
		os.Remove(p)
		return nil, p, e
	}
	return l, p, nil
}
func dialLocal(root string) (net.Conn, error) {
	return net.DialTimeout("unix", endpoint(root), time.Second)
}
func cleanupLocal(p string) { os.Remove(p) }
func Detach(cmd *exec.Cmd)  { cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true} }
