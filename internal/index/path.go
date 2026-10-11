// Package index safely inventories portable regular files and directories.
package index

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"unicode/utf8"

	"golang.org/x/text/cases"
	"golang.org/x/text/unicode/norm"
)

// FoldPath is the portable collision key used even on case-sensitive hosts.
func FoldPath(p string) string { return cases.Fold().String(norm.NFC.String(p)) }

// ValidatePath accepts a bounded NFC relative path portable to supported systems.
func ValidatePath(p string) error {
	if p == "" || len(p) > 4096 || !utf8.ValidString(p) || !norm.NFC.IsNormalString(p) || strings.ContainsAny(p, "\\\x00") || strings.HasPrefix(p, "/") {
		return errors.New("invalid portable relative path")
	}
	for _, part := range strings.Split(p, "/") {
		if part == "" || part == "." || part == ".." || len(part) > 255 || strings.ContainsAny(part, ":<>\"|?*") || strings.TrimRight(part, " .") != part {
			return errors.New("invalid portable path component")
		}
		for _, r := range part {
			if r < 32 || r == 127 {
				return errors.New("control character in path")
			}
		}
		base := strings.ToUpper(strings.SplitN(part, ".", 2)[0])
		if base == "CON" || base == "PRN" || base == "AUX" || base == "NUL" || (len(base) == 4 && (strings.HasPrefix(base, "COM") || strings.HasPrefix(base, "LPT")) && base[3] >= '1' && base[3] <= '9') {
			return errors.New("reserved device path")
		}
		if FoldPath(part) == FoldPath(".HappySync") {
			return errors.New("reserved state path")
		}
	}
	return nil
}

// SafePath rejects symlinks, state paths and existing spelling collisions. It
// resolves filesystem Unicode decomposition but never case aliases. Callers
// must retain the root lock and recheck immediately before a filesystem write.
func SafePath(root, relative string) (string, error) {
	if err := ValidatePath(relative); err != nil {
		return "", err
	}
	abs, err := filepath.Abs(root)
	if err != nil {
		return "", err
	}
	info, err := os.Lstat(abs)
	if err != nil {
		return "", err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return "", errors.New("sync root must be a real directory")
	}
	current := abs
	for _, part := range strings.Split(relative, "/") {
		entries, err := os.ReadDir(current)
		if err != nil && !os.IsNotExist(err) {
			return "", err
		}
		actual := part
		matches := 0
		for _, entry := range entries {
			if FoldPath(entry.Name()) == FoldPath(part) {
				matches++
				if norm.NFC.String(entry.Name()) != part {
					return "", fmt.Errorf("portable case collision at %q", relative)
				}
				actual = entry.Name()
			}
		}
		if matches > 1 {
			return "", fmt.Errorf("Unicode collision at %q", relative)
		}
		current = filepath.Join(current, actual)
		info, err = os.Lstat(current)
		if err != nil && !os.IsNotExist(err) {
			return "", err
		}
		if err == nil && info.Mode()&os.ModeSymlink != 0 {
			return "", errors.New("symlink traversal refused")
		}
	}
	return current, nil
}
