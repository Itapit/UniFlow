// Package coordinator provides the implementation of a shared block-claiming
//
// Processes never know about each other: they only know a namespace
// (e.g. "tx-<filehash>" or "rx-<filehash>") and a shared directory.
// (N anonymous processes, join/leave at runtime, crash-safe via
// stale-lock reclaim).
package coordinator

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"time"
)

type Coordinator interface {
	// TryClaim attempts to claim a specific block. claimed==false means
	// it is already claimed or done. On success the caller owns the
	// block until release is called or MarkDone is called (which
	// implies release).
	TryClaim(namespace string, blockID uint32) (release func(), claimed bool)
	// ClaimNext atomically claims the next unclaimed, not-done block in
	// [0,totalBlocks). claimed==false means nothing left to claim.
	ClaimNext(namespace string, totalBlocks uint32) (blockID uint32, release func(), claimed bool)
	// IsDone reports whether the block was already finished.
	IsDone(namespace string, blockID uint32) bool
	// MarkDone records the block as finished (implies release of the claim).
	MarkDone(namespace string, blockID uint32) error
}

// -------------------------------------------------------------------
// File-backed implementation (N anonymous OS processes).
// -------------------------------------------------------------------

// FileCoordinator is a Coordinator backed by lock/done files under
// directory/<namespace>/.
// Locks older than staleAfter without a done marker are treated as orphaned
// (holder crashed) and reclaimed.
type FileCoordinator struct {
	directory  string
	staleAfter time.Duration
}

// NewFileCoordinator builds a file-backed coordinator over directory,
// shared by all processes. staleAfter reclaims orphaned
// locks; pass config.StaleAfter for the shared default.
func NewFileCoordinator(directory string, staleAfter time.Duration) *FileCoordinator {
	if staleAfter <= 0 {
		staleAfter = 30 * time.Second
	}
	return &FileCoordinator{directory: directory, staleAfter: staleAfter}
}

func (coordinator *FileCoordinator) namespaceDir(namespace string) string {
	return filepath.Join(coordinator.directory, namespace)
}

func (coordinator *FileCoordinator) lockPath(namespace string, blockID uint32) string {
	return filepath.Join(coordinator.namespaceDir(namespace), fmt.Sprintf("lock-%d", blockID))
}

func (coordinator *FileCoordinator) donePath(namespace string, blockID uint32) string {
	return filepath.Join(coordinator.namespaceDir(namespace), fmt.Sprintf("done-%d", blockID))
}

func (coordinator *FileCoordinator) isStale(lockFilePath string) bool {
	fileInfo, err := os.Stat(lockFilePath)
	if err != nil {
		return false
	}
	return time.Since(fileInfo.ModTime()) > coordinator.staleAfter
}

// TryClaim implements Coordinator.
func (coordinator *FileCoordinator) TryClaim(namespace string, blockID uint32) (func(), bool) {
	if coordinator.IsDone(namespace, blockID) {
		return nil, false
	}
	if err := os.MkdirAll(coordinator.namespaceDir(namespace), 0o755); err != nil {
		return nil, false
	}
	lockFilePath := coordinator.lockPath(namespace, blockID)
	lockFile, err := os.OpenFile(lockFilePath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		if !os.IsExist(err) {
			return nil, false
		}
		// Already claimed: reclaim if the holder died.
		if coordinator.isStale(lockFilePath) {
			_ = os.Remove(lockFilePath)
			lockFile, err = os.OpenFile(lockFilePath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
			if err != nil {
				return nil, false
			}
		} else {
			return nil, false
		}
	}
	_, _ = fmt.Fprintf(lockFile, "%d %s\n", os.Getpid(), time.Now().UTC().Format(time.RFC3339))
	_ = lockFile.Close()
	released := false
	return func() {
		if released {
			return
		}
		released = true
		// Do not remove the lock if it already became done.
		if _, err := os.Stat(coordinator.donePath(namespace, blockID)); err == nil {
			return
		}
		_ = os.Remove(lockFilePath)
	}, true
}

// ClaimNext implements Coordinator by scanning forward from a shared
// flock-guarded counter file, claiming the first free block.
func (coordinator *FileCoordinator) ClaimNext(namespace string, totalBlocks uint32) (uint32, func(), bool) {
	if err := os.MkdirAll(coordinator.namespaceDir(namespace), 0o755); err != nil {
		return 0, nil, false
	}
	counterPath := filepath.Join(coordinator.namespaceDir(namespace), "next")
	counterFile, err := os.OpenFile(counterPath, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return 0, nil, false
	}
	defer counterFile.Close()
	if err := syscall.Flock(int(counterFile.Fd()), syscall.LOCK_EX); err != nil {
		return 0, nil, false
	}
	defer syscall.Flock(int(counterFile.Fd()), syscall.LOCK_UN)

	var nextBlockID uint32
	buffer := make([]byte, 32)
	bytesRead, _ := counterFile.ReadAt(buffer, 0)
	if bytesRead > 0 {
		_, _ = fmt.Sscanf(string(buffer[:bytesRead]), "%d", &nextBlockID)
	}
	for blockID := nextBlockID; blockID < totalBlocks; blockID++ {
		release, claimed := coordinator.tryClaimWithCounterLock(namespace, blockID)
		if !claimed {
			continue
		}
		_, _ = counterFile.Seek(0, 0)
		_ = counterFile.Truncate(0)
		_, _ = fmt.Fprintf(counterFile, "%d\n", blockID+1)
		return blockID, release, true
	}
	return 0, nil, false
}

// tryClaimWithCounterLock is TryClaim without counter interaction;
// the caller must hold the namespace counter flock (ClaimNext does).
func (coordinator *FileCoordinator) tryClaimWithCounterLock(namespace string, blockID uint32) (func(), bool) {
	if coordinator.IsDone(namespace, blockID) {
		return nil, false
	}
	lockFilePath := coordinator.lockPath(namespace, blockID)
	lockFile, err := os.OpenFile(lockFilePath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		if !os.IsExist(err) {
			return nil, false
		}
		if coordinator.isStale(lockFilePath) {
			_ = os.Remove(lockFilePath)
			lockFile, err = os.OpenFile(lockFilePath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
			if err != nil {
				return nil, false
			}
		} else {
			return nil, false
		}
	}
	_, _ = fmt.Fprintf(lockFile, "%d %s\n", os.Getpid(), time.Now().UTC().Format(time.RFC3339))
	_ = lockFile.Close()
	released := false
	return func() {
		if released {
			return
		}
		released = true
		if _, err := os.Stat(coordinator.donePath(namespace, blockID)); err == nil {
			return
		}
		_ = os.Remove(lockFilePath)
	}, true
}

// IsDone implements Coordinator.
func (coordinator *FileCoordinator) IsDone(namespace string, blockID uint32) bool {
	_, err := os.Stat(coordinator.donePath(namespace, blockID))
	return err == nil
}

// MarkDone implements Coordinator.
func (coordinator *FileCoordinator) MarkDone(namespace string, blockID uint32) error {
	if err := os.MkdirAll(coordinator.namespaceDir(namespace), 0o755); err != nil {
		return err
	}
	doneFilePath := coordinator.donePath(namespace, blockID)
	doneFile, err := os.OpenFile(doneFilePath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		if os.IsExist(err) {
			_ = os.Remove(coordinator.lockPath(namespace, blockID))
			return nil
		}
		return err
	}
	_ = doneFile.Close()
	_ = os.Remove(coordinator.lockPath(namespace, blockID))
	return nil
}
