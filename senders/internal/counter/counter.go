package counter

import (
	"encoding/binary"
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

func Count(counterFile *os.File) (uint64, error) {
	err := unix.Flock(int(counterFile.Fd()), unix.LOCK_EX)
	if err != nil {
		return 0, fmt.Errorf("failed to lock file: %w", err)
	}
	defer unix.Flock(int(counterFile.Fd()), unix.LOCK_UN)

	buf := make([]byte, CounterSizeInBytes)
	var currentCount uint64

	if _, err := counterFile.Seek(SeekStartOffset, 0); err != nil {
		return 0, err
	}
	if _, err := counterFile.Read(buf); err != nil {
		return 0, fmt.Errorf("failed reading counter bytes: %w", err)
	}

	currentCount = binary.LittleEndian.Uint64(buf)
	nextCount := currentCount + 1
	binary.LittleEndian.PutUint64(buf, nextCount)

	if _, err := counterFile.Seek(SeekStartOffset, 0); err != nil {
		return currentCount, err
	}
	if _, err := counterFile.Write(buf); err != nil {
		return currentCount, fmt.Errorf("failed writing counter bytes: %w", err)
	}

	return currentCount, nil
}