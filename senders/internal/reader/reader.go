package reader

import (
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"os"
	"senders/internal/constants"

	"golang.org/x/exp/mmap"
)


type MappedFile struct {
	reader *mmap.ReaderAt
	size   int
}

func OpenFile(path string) (*MappedFile, error) {
	r, err := mmap.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to mmap file %s: %w", path, err)
	}

	return &MappedFile{
		reader: r,
		size:   r.Len(),
	}, nil
}
func (mf *MappedFile) Size() int {
	return mf.size
}

func (mf *MappedFile) ReadChunk(offset int64) ([]byte, error) {
	if offset >= int64(mf.size) {
		return nil, io.EOF
	}

	remaining := int64(mf.size) - offset
	toRead := constants.ChunkSize
	if remaining < int64(toRead) {
		toRead = int(remaining)
	}

	buf := make([]byte, toRead)
	n, err := mf.reader.ReadAt(buf, offset)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("failed reading chunk at offset %d: %w", offset, err)
	}

	return buf[:n], nil
}

func (mf *MappedFile) Close() error {
	return mf.reader.Close()
}
func GenerateFileHash(path string) (uint64, error) {
	info, err := os.Stat(path)
	if err != nil {
		return 0, fmt.Errorf("failed to get file stat for hash: %w", err)
	}

	fingerprint := fmt.Sprintf("%s:%d:%d", info.Name(), info.Size(), info.ModTime().UnixNano())

	h := fnv.New64a()
	h.Write([]byte(fingerprint))
	return h.Sum64(), nil
}

