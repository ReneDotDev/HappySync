//go:build windows

package daemon

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"github.com/Microsoft/go-winio"
	"golang.org/x/sys/windows"
	"net"
	"os"
	"os/exec"
	"syscall"
	"time"
)

type RootLock struct{ f *os.File }

func lockFile(path string) (*RootLock, error) {
	f, e := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if e != nil {
		return nil, e
	}
	var o windows.Overlapped
	if e = windows.LockFileEx(windows.Handle(f.Fd()), windows.LOCKFILE_EXCLUSIVE_LOCK|windows.LOCKFILE_FAIL_IMMEDIATELY, 0, 1, 0, &o); e != nil {
		f.Close()
		if errors.Is(e, windows.ERROR_LOCK_VIOLATION) {
			return nil, fmt.Errorf("%w; use HappySync status", ErrAlreadyRunning)
		}
		return nil, e
	}
	return &RootLock{f}, nil
}
func (l *RootLock) Close() error { return l.f.Close() }
func endpoint(root string) string {
	return fmt.Sprintf(`\\.\pipe\HappySync-%x`, sha256.Sum256([]byte(root)))
}
func listenLocal(root string) (net.Listener, string, error) {
	p := endpoint(root)
	token := windows.GetCurrentProcessToken()
	user, e := token.GetTokenUser()
	if e != nil {
		return nil, p, e
	}
	sddl := "D:P(A;;GA;;;" + user.User.Sid.String() + ")"
	l, e := winio.ListenPipe(p, &winio.PipeConfig{SecurityDescriptor: sddl})
	return l, p, e
}
func dialLocal(root string) (net.Conn, error) {
	timeout := time.Second
	return winio.DialPipe(endpoint(root), &timeout)
}
func cleanupLocal(p string) {}
func Detach(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{CreationFlags: windows.CREATE_NEW_PROCESS_GROUP | windows.DETACHED_PROCESS}
}

func watchProcess(pid int) (func(time.Duration) error, func(), error) {
	handle, err := windows.OpenProcess(windows.SYNCHRONIZE, false, uint32(pid))
	if err != nil {
		return nil, nil, fmt.Errorf("watch worker shutdown: %w", err)
	}
	wait := func(timeout time.Duration) error {
		if timeout <= 0 {
			return context.DeadlineExceeded
		}
		milliseconds := uint64(timeout / time.Millisecond)
		if timeout%time.Millisecond != 0 {
			milliseconds++
		}
		milliseconds = min(milliseconds, uint64(windows.INFINITE-1))
		result, err := windows.WaitForSingleObject(handle, uint32(milliseconds))
		if err != nil {
			return fmt.Errorf("wait for worker shutdown: %w", err)
		}
		if result == uint32(windows.WAIT_TIMEOUT) {
			return context.DeadlineExceeded
		}
		if result != windows.WAIT_OBJECT_0 {
			return fmt.Errorf("unexpected worker shutdown wait result: %d", result)
		}
		return nil
	}
	return wait, func() { windows.CloseHandle(handle) }, nil
}
