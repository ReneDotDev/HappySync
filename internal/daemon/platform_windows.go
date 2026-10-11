//go:build windows

package daemon

import (
	"crypto/sha256"
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
		return nil, fmt.Errorf("already running; use HappySync status")
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
