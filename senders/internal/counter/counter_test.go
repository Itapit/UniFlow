package counter_test

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"senders/internal/counter"
)

func TestInitCounterFile_NewFile(t *testing.T) {
	tmpDir := t.TempDir()
	counterPath := filepath.Join(tmpDir, "test_counter.bin")

	// 1. אתחול קובץ חדש
	f, err := counter.InitCounterFile(counterPath)
	if err != nil {
		t.Fatalf("InitCounterFile failed: %v", err)
	}
	defer f.Close()

	// וידוא שהקובץ בגודל 8 בתים
	stat, err := f.Stat()
	if err != nil {
		t.Fatalf("Stat failed: %v", err)
	}
	if stat.Size() != 8 {
		t.Fatalf("expected size 8, got %d", stat.Size())
	}

	// וידוא שהערך ההתחלתי הוא 0
	buf := make([]byte, 8)
	if _, err := f.ReadAt(buf, 0); err != nil {
		t.Fatalf("ReadAt failed: %v", err)
	}
	val := binary.LittleEndian.Uint64(buf)
	if val != 0 {
		t.Errorf("expected initial counter 0, got %d", val)
	}
}

func TestInitCounterFile_ExistingFile(t *testing.T) {
	tmpDir := t.TempDir()
	counterPath := filepath.Join(tmpDir, "existing_counter.bin")

	// הכנת קובץ קיים עם הערך 42
	initialVal := uint64(42)
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, initialVal)
	if err := os.WriteFile(counterPath, buf, 0666); err != nil {
		t.Fatalf("failed preparing existing file: %v", err)
	}

	// פתיחת הקובץ הקיים דרך InitCounterFile
	f, err := counter.InitCounterFile(counterPath)
	if err != nil {
		t.Fatalf("InitCounterFile on existing file failed: %v", err)
	}
	defer f.Close()

	// וידוא שהערך הקיים לא נדרס על ידי אפסים
	readBuf := make([]byte, 8)
	if _, err := f.ReadAt(readBuf, 0); err != nil {
		t.Fatalf("ReadAt failed: %v", err)
	}
	val := binary.LittleEndian.Uint64(readBuf)
	if val != initialVal {
		t.Errorf("expected counter %d, got %d (file was overwritten)", initialVal, val)
	}
}

func TestCount_Sequential(t *testing.T) {
	tmpDir := t.TempDir()
	counterPath := filepath.Join(tmpDir, "seq_counter.bin")

	f, err := counter.InitCounterFile(counterPath)
	if err != nil {
		t.Fatalf("InitCounterFile failed: %v", err)
	}
	defer f.Close()

	// קידום עוקב 5 פעמים
	for expected := uint64(0); expected < 5; expected++ {
		got, err := counter.Count(f)
		if err != nil {
			t.Fatalf("Count failed at step %d: %v", expected, err)
		}
		if got != expected {
			t.Errorf("expected count %d, got %d", expected, got)
		}
	}
}

func TestCount_Concurrent(t *testing.T) {
	tmpDir := t.TempDir()
	counterPath := filepath.Join(tmpDir, "concurrent_counter.bin")

	// יצירת הקובץ
	initF, err := counter.InitCounterFile(counterPath)
	if err != nil {
		t.Fatalf("InitCounterFile failed: %v", err)
	}
	initF.Close()

	const numWorkers = 10
	const incrementsPerWorker = 20
	const expectedTotal = numWorkers * incrementsPerWorker

	var wg sync.WaitGroup
	claimedIndices := make(chan uint64, expectedTotal)

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			// כל Worker פותח File Descriptor משלו (מדמה תהליכים נפרדים)
			f, err := os.OpenFile(counterPath, os.O_RDWR, 0666)
			if err != nil {
				t.Errorf("worker open failed: %v", err)
				return
			}
			defer f.Close()

			for j := 0; j < incrementsPerWorker; j++ {
				idx, err := counter.Count(f)
				if err != nil {
					t.Errorf("worker Count failed: %v", err)
					return
				}
				claimedIndices <- idx
			}
		}()
	}

	wg.Wait()
	close(claimedIndices)

	// אימות שכל מזהה מ-0 עד 199 נתפס בדיוק פעם אחת (בלי מרוץ זמנים וכפילויות)
	seen := make(map[uint64]bool)
	for idx := range claimedIndices {
		if seen[idx] {
			t.Errorf("duplicate index allocated: %d", idx)
		}
		seen[idx] = true
	}

	if len(seen) != expectedTotal {
		t.Errorf("expected %d unique increments, got %d", expectedTotal, len(seen))
	}
}