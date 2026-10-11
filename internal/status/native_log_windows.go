package status

import (
	"errors"
	"os"
)

// Go opens Windows append handles without FILE_WRITE_DATA, which is required
// for truncation. Keep the inherited append handle and truncate through a
// separate write handle only after checking that it refers to the same file.
func truncateNativeLog(appendFile *os.File) error {
	original, err := appendFile.Stat()
	if err != nil {
		return err
	}
	current, err := os.Lstat(appendFile.Name())
	if err != nil || !current.Mode().IsRegular() || !os.SameFile(original, current) {
		return errors.New("native log path changed before truncation")
	}
	writer, err := os.OpenFile(appendFile.Name(), os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer writer.Close()
	opened, err := writer.Stat()
	if err != nil || !os.SameFile(original, opened) {
		return errors.New("native log path changed while opening for truncation")
	}
	return writer.Truncate(0)
}
