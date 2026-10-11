// Package status provides bounded operational logs with invitation redaction.
package status

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"sync"
)

var invitations = regexp.MustCompile(`hsync[0-9]+:[A-Za-z0-9_-]+`)

func Redact(s string) string { return invitations.ReplaceAllString(s, "[share key redacted]") }

type RedactingWriter struct{ Writer io.Writer }

func (w RedactingWriter) Write(p []byte) (int, error) {
	_, err := io.WriteString(w.Writer, Redact(string(p)))
	if err != nil {
		return 0, err
	}
	return len(p), nil
}

type RotatingWriter struct {
	mu      sync.Mutex
	path    string
	max     int64
	backups int
	file    *os.File
	size    int64
}

func NewRotatingWriter(path string, max int64, backups int) (*RotatingWriter, error) {
	if max < 1 || backups < 1 || backups > 10 {
		return nil, errors.New("invalid log limits")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return nil, err
	}
	w := &RotatingWriter{path: path, max: max, backups: backups}
	if err := w.open(); err != nil {
		return nil, err
	}
	return w, nil
}
func (w *RotatingWriter) open() error {
	if info, err := os.Lstat(w.path); err == nil && !info.Mode().IsRegular() {
		return errors.New("unsafe log path")
	}
	f, err := os.OpenFile(w.path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return err
	}
	w.file = f
	w.size = info.Size()
	return nil
}
func (w *RotatingWriter) rotate() error {
	if err := w.file.Close(); err != nil {
		return err
	}
	for i := w.backups; i > 0; i-- {
		source := w.path
		if i > 1 {
			source = w.path + suffix(i-1)
		}
		target := w.path + suffix(i)
		if i == w.backups {
			if err := os.Remove(target); err != nil && !os.IsNotExist(err) {
				return err
			}
		}
		if err := os.Rename(source, target); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return w.open()
}
func suffix(n int) string { return "." + strconv.Itoa(n) }
func (w *RotatingWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	redacted := []byte(Redact(string(p)))
	if int64(len(redacted)) > w.max {
		return 0, errors.New("operational log entry exceeds log size limit")
	}
	if w.size+int64(len(redacted)) > w.max {
		if err := w.rotate(); err != nil {
			return 0, err
		}
	}
	n, err := w.file.Write(redacted)
	w.size += int64(n)
	if err != nil {
		return 0, err
	}
	return len(p), nil
}
func (w *RotatingWriter) Close() error { w.mu.Lock(); defer w.mu.Unlock(); return w.file.Close() }
