// Package daemon manages root ownership, detached workers and local user-only control.
package daemon

import (
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"time"
)

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
func Lock(root string) (*RootLock, error) {
	dir := filepath.Join(root, ".HappySync", "runtime")
	if e := os.MkdirAll(dir, 0700); e != nil {
		return nil, e
	}
	return lockFile(filepath.Join(dir, "instance.lock"))
}
