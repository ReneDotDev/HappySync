//go:build !windows

package status

import "os"

func truncateNativeLog(f *os.File) error { return f.Truncate(0) }
