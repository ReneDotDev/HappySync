package index

import (
	"encoding/hex"
	"errors"
	"io"
	"os"
	"sync"

	"github.com/happysync/happysync/internal/model"
	"github.com/zeebo/blake3"
)

var hashBuffers = sync.Pool{New: func() any { return make([]byte, model.BlockSize) }}

// BuildManifest retries unstable reads and bounds hashing memory to one block.
func BuildManifest(path string) (model.Manifest, error) {
	for attempt := 0; attempt < 3; attempt++ {
		m, stable, err := buildManifest(path)
		if err != nil {
			return model.Manifest{}, err
		}
		if stable {
			return m, nil
		}
	}
	return model.Manifest{}, errors.New("file changed while hashing; retry scan")
}
func buildManifest(path string) (model.Manifest, bool, error) {
	before, err := os.Lstat(path)
	if err != nil {
		return model.Manifest{}, false, err
	}
	if !before.Mode().IsRegular() {
		return model.Manifest{}, false, errors.New("manifest source is not regular")
	}
	f, err := os.Open(path)
	if err != nil {
		return model.Manifest{}, false, err
	}
	defer f.Close()
	opened, err := f.Stat()
	if err != nil {
		return model.Manifest{}, false, err
	}
	if !os.SameFile(before, opened) {
		return model.Manifest{}, false, errors.New("manifest source replaced during open")
	}
	h := blake3.New()
	m := model.Manifest{}
	buf := hashBuffers.Get().([]byte)
	defer hashBuffers.Put(buf)
	for {
		n, err := io.ReadFull(f, buf)
		if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
			return model.Manifest{}, false, err
		}
		if n > 0 {
			sum := blake3.Sum256(buf[:n])
			m.Blocks = append(m.Blocks, model.Block{Hash: hex.EncodeToString(sum[:]), Size: int64(n)})
			m.Size += int64(n)
			_, _ = h.Write(buf[:n])
		}
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		}
	}
	m.Hash = hex.EncodeToString(h.Sum(nil))
	after, err := f.Stat()
	if err != nil {
		return model.Manifest{}, false, err
	}
	named, err := os.Lstat(path)
	if err != nil {
		return model.Manifest{}, false, err
	}
	stable := os.SameFile(before, named) && named.Mode().IsRegular() && before.Size() == after.Size() && before.Size() == m.Size && before.ModTime() == after.ModTime() && before.ModTime() == named.ModTime()
	return m, stable, nil
}
