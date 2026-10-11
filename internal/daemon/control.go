// Package daemon manages root ownership, detached workers and local user-only control.
package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"time"
)

var ErrAlreadyRunning = errors.New("already running")

type Handler func(string) any
type Control struct {
	listener net.Listener
	path     string
}

func Listen(root string, handle Handler) (*Control, error) {
	l, p, e := listenLocal(root)
	if e != nil {
		return nil, e
	}
	c := &Control{l, p}
	go func() {
		for {
			conn, e := l.Accept()
			if e != nil {
				return
			}
			go func() {
				defer conn.Close()
				conn.SetDeadline(time.Now().Add(5 * time.Second))
				var req struct {
					Command string `json:"command"`
				}
				d := json.NewDecoder(conn)
				if d.Decode(&req) != nil {
					return
				}
				if req.Command != "status" && req.Command != "stop" {
					return
				}
				json.NewEncoder(conn).Encode(handle(req.Command))
			}()
		}
	}()
	return c, nil
}
func (c *Control) Close() error { e := c.listener.Close(); cleanupLocal(c.path); return e }
func Request(root, command string, out any) error {
	conn, e := dialLocal(root)
	if e != nil {
		return errors.New("HappySync is stopped or its control endpoint is unavailable")
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(5 * time.Second))
	if e = json.NewEncoder(conn).Encode(map[string]string{"command": command}); e != nil {
		return e
	}
	return json.NewDecoder(conn).Decode(out)
}

// Stop acknowledges shutdown only after the worker releases its resources and
// root ownership. On Windows, waiting on the captured process handle also
// ensures inherited diagnostic files are no longer held open.
func Stop(root string, timeout time.Duration) error {
	if timeout <= 0 {
		return errors.New("invalid shutdown timeout")
	}
	var current struct {
		PID int `json:"pid"`
	}
	if err := Request(root, "status", &current); err != nil {
		return err
	}
	if current.PID <= 0 {
		return errors.New("invalid worker process identity")
	}
	waitProcess, closeProcess, err := watchProcess(current.PID)
	if err != nil {
		return err
	}
	defer closeProcess()
	deadline := time.Now().Add(timeout)
	var reply struct {
		State string `json:"state"`
	}
	if err := Request(root, "stop", &reply); err != nil {
		return err
	}
	if reply.State != "stopping" {
		return errors.New("invalid shutdown acknowledgment")
	}
	if err := waitProcess(time.Until(deadline)); err != nil {
		return err
	}
	for {
		lock, err := Lock(root)
		if err == nil {
			return lock.Close()
		}
		if !errors.Is(err, ErrAlreadyRunning) {
			return err
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return context.DeadlineExceeded
		}
		time.Sleep(min(10*time.Millisecond, remaining))
	}
}
func Lock(root string) (*RootLock, error) {
	dir := filepath.Join(root, ".HappySync", "runtime")
	if e := os.MkdirAll(dir, 0700); e != nil {
		return nil, e
	}
	return lockFile(filepath.Join(dir, "instance.lock"))
}
