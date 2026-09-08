package counter

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
)

func InitCounterFile(path string) (*os.File, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0666)
	if err == nil {
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, 0)
		if _, err := file.Write(buf); err != nil {
			file.Close()
			return nil, err
		}
		return file, nil
	}

	if errors.Is(err, os.ErrExist) {
		return os.OpenFile(path, os.O_RDWR, 0666)
	}

	return nil, fmt.Errorf("failed to open counter file: %w", err)
}