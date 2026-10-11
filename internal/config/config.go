// Package config owns versioned invitations and private per-folder identity.
package config

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/libp2p/go-libp2p/core/crypto"
)

const Version = 1
const prefix = "hsync1:"
const payloadSize = 1 + 16 + 32 + 8

type Config struct {
	Version                int            `json:"version"`
	Key                    string         `json:"key"`
	Ignore                 []string       `json:"ignore"`
	SyncID                 []byte         `json:"-"`
	Secret                 []byte         `json:"-"`
	Identity               crypto.PrivKey `json:"-"`
	Root                   string         `json:"-"`
	Listen                 []string       `json:"listen,omitempty"`
	Peers                  []string       `json:"peers,omitempty"`
	Bootstrap              []string       `json:"bootstrap,omitempty"`
	Relays                 []string       `json:"relays,omitempty"`
	DiscoveryServers       []string       `json:"discovery_servers,omitempty"`
	DisableMDNS            bool           `json:"disable_mdns,omitempty"`
	DisableDHT             bool           `json:"disable_dht,omitempty"`
	UploadBytesPerSecond   int64          `json:"upload_bytes_per_second,omitempty"`
	DownloadBytesPerSecond int64          `json:"download_bytes_per_second,omitempty"`
}

// String deliberately redacts both invitation and private identity when the
// configuration is accidentally included in a diagnostic message.
func (c Config) String() string {
	return fmt.Sprintf("HappySync config version=%d (credentials redacted)", c.Version)
}

func (c Config) GoString() string { return c.String() }

type diskConfig Config

// MarshalJSON is safe for structured diagnostics. Private persistence uses
// diskConfig explicitly, so the bearer invitation only reaches config.json.
func (c Config) MarshalJSON() ([]byte, error) {
	safe := diskConfig(c)
	safe.Key = "[redacted]"
	return json.Marshal(safe)
}

func StateDir(root string) string { return filepath.Join(root, ".HappySync") }

// GenerateKey produces the v1 fixed invitation: version, 16-byte group ID,
// 32-byte bearer secret, and the first 8 bytes of SHA-256 of those 49 bytes.
// The checksum detects transcription errors; it is not authentication.
func GenerateKey() (string, error) {
	p := make([]byte, payloadSize)
	p[0] = Version
	if _, err := rand.Read(p[1:49]); err != nil {
		return "", err
	}
	sum := sha256.Sum256(p[:49])
	copy(p[49:], sum[:8])
	return prefix + base64.RawURLEncoding.EncodeToString(p), nil
}

func DecodeKey(key string) ([]byte, []byte, error) {
	invalid := errors.New("invalid share key: expected a supported hsync1 invitation")
	if len(key) != len(prefix)+base64.RawURLEncoding.EncodedLen(payloadSize) || !strings.HasPrefix(key, prefix) {
		return nil, nil, invalid
	}
	p, err := base64.RawURLEncoding.Strict().DecodeString(key[len(prefix):])
	if err != nil || len(p) != payloadSize || p[0] != Version {
		return nil, nil, invalid
	}
	sum := sha256.Sum256(p[:49])
	if !bytes.Equal(sum[:8], p[49:]) {
		return nil, nil, invalid
	}
	return bytes.Clone(p[1:17]), bytes.Clone(p[17:49]), nil
}

// EnsureStateDir refuses a symlinked state directory and creates private
// staging/control directories. Callers must hold the root process lock before
// initializing or opening the database.
func EnsureStateDir(root string) error {
	for _, name := range []string{"", "tmp", "logs", "runtime"} {
		path := filepath.Join(StateDir(root), name)
		if info, err := os.Lstat(path); err == nil {
			if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
				return errors.New("unsafe HappySync state directory")
			}
		} else if !os.IsNotExist(err) {
			return err
		} else if err = os.Mkdir(path, 0700); err != nil && !os.IsExist(err) {
			return err
		}
		if err := ProtectPrivatePath(path, true); err != nil {
			return err
		}
	}
	return nil
}

func readPrivate(path string) ([]byte, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Size() > 64*1024 {
		return nil, errors.New("invalid private state file")
	}
	if err = ProtectPrivatePath(path, false); err != nil {
		return nil, err
	}
	return os.ReadFile(path)
}

// Load only reads initialized state and never generates a replacement identity.
func Load(root string) (*Config, error) {
	root, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	info, err := os.Lstat(StateDir(root))
	if err != nil {
		return nil, err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return nil, errors.New("unsafe HappySync state directory")
	}
	if err = ProtectPrivatePath(StateDir(root), true); err != nil {
		return nil, err
	}
	data, err := readPrivate(filepath.Join(StateDir(root), "config.json"))
	if err != nil {
		return nil, err
	}
	var c Config
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err = dec.Decode(&c); err != nil {
		return nil, errors.New("invalid folder configuration")
	}
	var extra any
	if err = dec.Decode(&extra); err != io.EOF {
		return nil, errors.New("invalid folder configuration")
	}
	if c.Version != Version {
		return nil, errors.New("unsupported folder configuration version")
	}
	if c.UploadBytesPerSecond < 0 || c.DownloadBytesPerSecond < 0 {
		return nil, errors.New("invalid persisted bandwidth limits")
	}
	c.SyncID, c.Secret, err = DecodeKey(c.Key)
	if err != nil {
		return nil, errors.New("invalid persisted share key")
	}
	raw, err := readPrivate(filepath.Join(StateDir(root), "identity.key"))
	if err != nil {
		return nil, err
	}
	c.Identity, err = crypto.UnmarshalPrivateKey(raw)
	if err != nil {
		return nil, errors.New("invalid persisted device identity")
	}
	c.Root = root
	return &c, nil
}

// Init reuses existing group state; a different invitation is always an error.
func Init(root, key string) (*Config, error) {
	root, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	if key != "" {
		if _, _, err = DecodeKey(key); err != nil {
			return nil, err
		}
	}
	path := filepath.Join(StateDir(root), "config.json")
	if _, err = os.Lstat(path); err == nil {
		c, err := Load(root)
		if err != nil {
			return nil, err
		}
		if key != "" && key != c.Key {
			return nil, errors.New("folder already belongs to a different group; use a new folder")
		}
		return c, nil
	} else if !os.IsNotExist(err) {
		return nil, err
	}
	if err = EnsureStateDir(root); err != nil {
		return nil, err
	}
	if key == "" {
		key, err = GenerateKey()
		if err != nil {
			return nil, err
		}
	}
	syncID, secret, err := DecodeKey(key)
	if err != nil {
		return nil, err
	}
	identityPath := filepath.Join(StateDir(root), "identity.key")
	var identity crypto.PrivKey
	if raw, e := readPrivate(identityPath); e == nil {
		identity, err = crypto.UnmarshalPrivateKey(raw)
		if err != nil {
			return nil, errors.New("invalid persisted device identity")
		}
	} else if !os.IsNotExist(e) {
		return nil, e
	} else {
		identity, _, err = crypto.GenerateEd25519Key(rand.Reader)
		if err != nil {
			return nil, err
		}
		raw, err = crypto.MarshalPrivateKey(identity)
		if err != nil {
			return nil, err
		}
		if err = writePrivate(identityPath, raw); err != nil {
			return nil, err
		}
	}
	c := &Config{Version: Version, Key: key, Ignore: []string{".git/", "node_modules/"}, SyncID: syncID, Secret: secret, Identity: identity, Root: root}
	data, err := json.MarshalIndent((*diskConfig)(c), "", "  ")
	if err != nil {
		return nil, err
	}
	if err = writePrivate(path, append(data, '\n')); err != nil {
		return nil, err
	}
	return c, nil
}

// Save atomically updates local preferences without allowing a caller to move
// persisted membership to another group or regenerate its device identity.
func Save(c *Config) error {
	if c == nil {
		return errors.New("missing folder configuration")
	}
	if c.UploadBytesPerSecond < 0 || c.DownloadBytesPerSecond < 0 {
		return errors.New("bandwidth limits cannot be negative")
	}
	old, err := Load(c.Root)
	if err != nil {
		return err
	}
	if old.Key != c.Key || c.Version != Version {
		return errors.New("cannot change persisted group membership")
	}
	data, err := json.MarshalIndent((*diskConfig)(c), "", "  ")
	if err != nil {
		return err
	}
	if len(data) > 64*1024 {
		return errors.New("folder configuration exceeds size limit")
	}
	return writePrivate(filepath.Join(StateDir(c.Root), "config.json"), append(data, '\n'))
}

func (c *Config) Save() error { return Save(c) }

func writePrivate(path string, data []byte) error {
	f, err := os.CreateTemp(filepath.Dir(path), ".state-*")
	if err != nil {
		return err
	}
	name := f.Name()
	defer os.Remove(name)
	if err = f.Chmod(0600); err == nil {
		_, err = f.Write(data)
	}
	if err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	if err = ProtectPrivatePath(name, false); err != nil {
		return err
	}
	if err = os.Rename(name, path); err != nil {
		return fmt.Errorf("persist private state: %w", err)
	}
	// Best effort directory flush is portable: some operating systems refuse it.
	if d, e := os.Open(filepath.Dir(path)); e == nil {
		_ = d.Sync()
		_ = d.Close()
	}
	return nil
}
