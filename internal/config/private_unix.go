//go:build !windows

package config

import "os"

// ProtectPrivatePath restricts local secrets and control state to its owner.
func ProtectPrivatePath(path string, directory bool) error {
	mode := os.FileMode(0600)
	if directory {
		mode = 0700
	}
	return os.Chmod(path, mode)
}
