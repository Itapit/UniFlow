package reader_test

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"senders/internal/constants"
	"senders/internal/reader"
)

func createTempTestFile(t *testing.T, size int) string {
	t.Helper()
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "test_sample.dat")

	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i % 256)
	}

	if err := os.WriteFile(filePath, data, 0644); err != nil {
		t.Fatalf("failed creating temp file: %v", err)
	}
	return filePath
}

func TestMappedFile_OpenAndSize(t *testing.T) {
	expectedSize := 1024 * 64
	path := createTempTestFile(t, expectedSize)

	mf, err := reader.OpenFile(path)
	if err != nil {
		t.Fatalf("OpenFile failed: %v", err)
	}
	defer mf.Close()

	if mf.Size() != expectedSize {
		t.Errorf("expected size %d, got %d", expectedSize, mf.Size())
	}
}

func TestMappedFile_ReadChunk(t *testing.T) {
	fileSize := constants.ChunkSize*2 + 500
	path := createTempTestFile(t, fileSize)

	originalData, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("failed reading original file: %v", err)
	}

	mf, err := reader.OpenFile(path)
	if err != nil {
		t.Fatalf("OpenFile failed: %v", err)
	}
	defer mf.Close()

	// 1. קריאת Chunk ראשון מלא
	chunk1, err := mf.ReadChunk(0)
	if err != nil {
		t.Fatalf("ReadChunk offset 0 failed: %v", err)
	}
	if len(chunk1) != constants.ChunkSize {
		t.Errorf("expected chunk size %d, got %d", constants.ChunkSize, len(chunk1))
	}
	if !bytes.Equal(chunk1, originalData[:constants.ChunkSize]) {
		t.Errorf("chunk1 content mismatch")
	}

	// 2. קריאת Chunk אחרון וחלקי
	lastOffset := int64(constants.ChunkSize * 2)
	chunkLast, err := mf.ReadChunk(lastOffset)
	if err != nil {
		t.Fatalf("ReadChunk offset %d failed: %v", lastOffset, err)
	}
	if len(chunkLast) != 500 {
		t.Errorf("expected last chunk size 500, got %d", len(chunkLast))
	}
	if !bytes.Equal(chunkLast, originalData[lastOffset:]) {
		t.Errorf("last chunk content mismatch")
	}

	// 3. קריאה מעבר לסוף הקובץ (ציפייה ל-EOF)
	_, err = mf.ReadChunk(int64(fileSize))
	if !errors.Is(err, io.EOF) {
		t.Errorf("expected io.EOF at end of file, got %v", err)
	}
}

func TestGenerateFileHash(t *testing.T) {
	path := createTempTestFile(t, 1024)

	// וידוא שהפונקציה עקבית על אותו הקובץ
	hash1, err := reader.GenerateFileHash(path)
	if err != nil {
		t.Fatalf("GenerateFileHash failed: %v", err)
	}
	if hash1 == 0 {
		t.Errorf("expected non-zero hash")
	}

	hash2, err := reader.GenerateFileHash(path)
	if err != nil {
		t.Fatalf("GenerateFileHash second run failed: %v", err)
	}

	if hash1 != hash2 {
		t.Errorf("hash mismatch between runs: %d != %d", hash1, hash2)
	}
}