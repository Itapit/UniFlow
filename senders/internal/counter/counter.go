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

	buf := make([]byte, 8)
	var currentCount uint64

	counterFile.Seek(0, 0)
	_, err = counterFile.Read(buf)

	binary.Decode(buf, binary.LittleEndian, &currentCount)
	if err != nil {
		return 0, fmt.Errorf("%w", err)
	}
	nextCount := currentCount + 1
	binary.Encode(buf, binary.LittleEndian, nextCount)

	counterFile.Seek(0, 0)
	_, err = counterFile.Write(buf)

	if err != nil {
		return currentCount, fmt.Errorf("%w", err)
	}

	return currentCount, nil
}