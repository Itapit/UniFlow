package counter

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"senders/internal/constants"

	"golang.org/x/sys/unix"
)

const (
	CounterSizeInBytes = 8
	SeekStartOffset    = 0
)

func InitCounterFile(path string) (*os.File, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return nil, fmt.Errorf("failed to create directory for counter file: %w", err)
	}
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, constants.DefaultFileMode)
	if err == nil {
		_ = unix.Flock(int(file.Fd()), unix.LOCK_EX)
		buf := make([]byte, CounterSizeInBytes)
		binary.LittleEndian.PutUint64(buf, 0)
		_, writeErr := file.Write(buf)
		_ = unix.Flock(int(file.Fd()), unix.LOCK_UN)

		if writeErr != nil {
			file.Close()
			return nil, writeErr
		}
		return file, nil
	}

	if errors.Is(err, os.ErrExist) {
		f, err := os.OpenFile(path, os.O_RDWR, constants.DefaultFileMode)
		if err != nil {
			return nil, err
		}

		_ = unix.Flock(int(f.Fd()), unix.LOCK_SH)
		stat, statErr := f.Stat()
		_ = unix.Flock(int(f.Fd()), unix.LOCK_UN)

		if statErr == nil && stat.Size() >= CounterSizeInBytes {
			return f, nil
		}
		return f, nil
	}

	return nil, fmt.Errorf("failed to open counter file: %w", err)
}
