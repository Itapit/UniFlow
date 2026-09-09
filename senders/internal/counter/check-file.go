package counter

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"time"
)

func InitCounterFile(path string) (*os.File, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0666)
	if err == nil {
		buf := make([]byte, 8)
		binary.LittleEndian.PutUint64(buf, 0)
		if _, err := file.Write(buf); err != nil {
			file.Close()
			return nil, err
		}
		return file, nil
	}

	if errors.Is(err, os.ErrExist) {
		f, err := os.OpenFile(path, os.O_RDWR, 0666)
		if err != nil {
			return nil, err
		}

		for i := 0; i < 50; i++ {
			stat, err := f.Stat()
			if err == nil && stat.Size() >= 8 {
				return f, nil
			}
			time.Sleep(2 * time.Millisecond)
		}
		return f, nil
	}
	
	return nil, fmt.Errorf("failed to open counter file: %w", err)
}