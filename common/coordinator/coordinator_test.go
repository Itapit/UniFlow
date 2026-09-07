package coordinator

import (
	"sync"
	"testing"
	"time"

	"common/config"
)

func TestFileClaimNextNoDuplicates(t *testing.T) {
	directory := t.TempDir()
	const workers = 4
	const total = 40
	var mutex sync.Mutex
	seen := make(map[uint32]int)
	var waitGroup sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			// Each worker gets its own instance sharing the same dir,
			// like separate OS processes.
			workerCoordinator := NewFileCoordinator(directory, config.StaleAfter)
			for {
				blockID, release, claimed := workerCoordinator.ClaimNext("ns", total)
				if !claimed {
					return
				}
				mutex.Lock()
				seen[blockID]++
				mutex.Unlock()
				if err := workerCoordinator.MarkDone("ns", blockID); err != nil {
					t.Errorf("mark done: %v", err)
				}
				release()
			}
		}()
	}
	waitGroup.Wait()
	if len(seen) != total {
		t.Fatalf("want %d claimed, got %d", total, len(seen))
	}
	for blockID, count := range seen {
		if count != 1 {
			t.Fatalf("block %d claimed %d times", blockID, count)
		}
	}
}

func TestFileTryClaimExclusive(t *testing.T) {
	directory := t.TempDir()
	firstCoordinator := NewFileCoordinator(directory, config.StaleAfter)
	secondCoordinator := NewFileCoordinator(directory, config.StaleAfter)

	release, claimed := firstCoordinator.TryClaim("ns", 7)
	if !claimed {
		t.Fatal("first claim should win")
	}
	if _, claimed := secondCoordinator.TryClaim("ns", 7); claimed {
		t.Fatal("second claim should lose")
	}
	release()
	if _, claimed := secondCoordinator.TryClaim("ns", 7); !claimed {
		t.Fatal("claim after release should win")
	}
}

func TestFileCoordinatorBasic(t *testing.T) {
	directory := t.TempDir()
	firstCoordinator := NewFileCoordinator(directory, config.StaleAfter)
	secondCoordinator := NewFileCoordinator(directory, config.StaleAfter)

	release, claimed := firstCoordinator.TryClaim("rx-1", 3)
	if !claimed {
		t.Fatal("first coordinator should win claim")
	}
	if _, claimed := secondCoordinator.TryClaim("rx-1", 3); claimed {
		t.Fatal("second coordinator should lose while first holds")
	}
	if firstCoordinator.IsDone("rx-1", 3) {
		t.Fatal("not done yet")
	}
	if err := firstCoordinator.MarkDone("rx-1", 3); err != nil {
		t.Fatal(err)
	}
	release() // no-op after done
	if !secondCoordinator.IsDone("rx-1", 3) {
		t.Fatal("second coordinator should see done")
	}
	if _, claimed := secondCoordinator.TryClaim("rx-1", 3); claimed {
		t.Fatal("done blocks are never reclaimable")
	}
}

func TestFileClaimNextAndStaleReclaim(t *testing.T) {
	directory := t.TempDir()
	firstCoordinator := NewFileCoordinator(directory, 50*time.Millisecond)
	secondCoordinator := NewFileCoordinator(directory, 50*time.Millisecond)

	blockID, release, claimed := firstCoordinator.ClaimNext("tx-1", 4)
	if !claimed || blockID != 0 {
		t.Fatalf("want block 0, got %d claimed=%v", blockID, claimed)
	}
	// Simulate crash: never MarkDone, never release. Just wait for staleness.
	_ = release
	time.Sleep(100 * time.Millisecond)

	// Second coordinator claims the specific stale block directly.
	if _, claimed := secondCoordinator.TryClaim("tx-1", 0); !claimed {
		t.Fatal("stale lock should be reclaimable")
	}
}
