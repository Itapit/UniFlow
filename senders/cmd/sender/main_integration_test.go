package main_test

import (
	"bytes"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"senders/internal/constants"
	"senders/internal/counter"
	rs "senders/internal/erasure"
	"senders/internal/pb"
	rd "senders/internal/reader"

	"google.golang.org/protobuf/proto"
)

func TestEndToEndSenderWorkflow(t *testing.T) {
	tmpDir := t.TempDir()

	// 1. יצירת קובץ דמה להעברה (20,000 בתים)
	testFilePath := filepath.Join(tmpDir, "transfer_sample.bin")
	dummyData := make([]byte, 20000)
	for i := range dummyData {
		dummyData[i] = byte(i % 256)
	}
	if err := os.WriteFile(testFilePath, dummyData, 0644); err != nil {
		t.Fatalf("failed to create dummy file: %v", err)
	}

	// 2. הקמת מאזין UDP מקומי לקליטת הפקטות
	udpAddr, err := net.ResolveUDPAddr("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to resolve udp addr: %v", err)
	}
	udpListener, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		t.Fatalf("failed to listen on udp: %v", err)
	}
	defer udpListener.Close()

	// 3. הגדרת משתנה הסטטוס ומשימת העבודה
	var currentState atomic.Int32
	currentState.Store(int32(pb.SenderState_IDLE))

	const fileHash = uint64(0xDEADBEEFCAFEBABE)
	task := &pb.TaskAssignment{
		FilePath: testFilePath,
		FileHash: fileHash,
	}

	// 4. הרצת לוגיקת ה-Sender ב-Goroutine נפרדת (מדמה את לולאת main)
	errChan := make(chan error, 1)
	go func() {
		// אתחול קובץ מונה ייעודי לפי ה-fileHash
		counterPath := filepath.Join(tmpDir, fmt.Sprintf("counter_%d.bin", task.FileHash))
		counterFile, _, err := counter.InitCounterFile(counterPath)
		if err != nil {
			errChan <- fmt.Errorf("init counter file failed: %w", err)
			return
		}
		defer counterFile.Close()

		reader, err := rd.OpenFile(task.FilePath)
		if err != nil {
			errChan <- fmt.Errorf("open file failed: %w", err)
			return
		}
		defer reader.Close()

		readerSize := reader.Size()
		totalBlocks := uint32((readerSize + constants.BlockSize - 1) / constants.BlockSize)

		senderConn, err := net.DialUDP("udp", nil, udpListener.LocalAddr().(*net.UDPAddr))
		if err != nil {
			errChan <- fmt.Errorf("dial udp failed: %w", err)
			return
		}
		defer senderConn.Close()

		currentState.Store(int32(pb.SenderState_WORKING))

		// עיבוד בלוקים
		for {
			count, err := counter.Count(counterFile)
			if err != nil {
				break
			}

			startBlock := uint32(count) * 8
			if startBlock >= totalBlocks {
				break
			}

			endBlock := startBlock + 8
			if endBlock > totalBlocks {
				endBlock = totalBlocks
			}

			for blockIdx := startBlock; blockIdx < endBlock; blockIdx++ {
				offset := int64(blockIdx) * constants.BlockSize
				chunk, err := reader.ReadChunk(offset)
				if err != nil {
					break
				}

				shards, err := rs.EncodeBlock(chunk)
				if err != nil {
					errChan <- fmt.Errorf("encode block failed: %w", err)
					return
				}

				for shardIdx, content := range shards {
					crc := pb.CalculateCRC(
						task.FileHash,
						blockIdx,
						totalBlocks,
						uint32(shardIdx),
						uint32(constants.DefaultDataShrads),
						uint32(constants.DefaultParityShards),
						uint64(readerSize),
						content,
					)

					pktBytes, err := pb.FormatPacket(
						task.FileHash,
						blockIdx,
						totalBlocks,
						uint32(shardIdx),
						uint32(constants.DefaultDataShrads),
						uint32(constants.DefaultParityShards),
						uint64(readerSize),
						content,
						crc,
					)
					if err != nil {
						errChan <- fmt.Errorf("format packet failed: %w", err)
						return
					}

					if _, err := senderConn.Write(pktBytes); err != nil {
						errChan <- fmt.Errorf("write udp failed: %w", err)
						return
					}
				}
			}
		}

		currentState.Store(int32(pb.SenderState_IDLE))
		errChan <- nil
	}()

	// 5. קריאה ואימות של פקטות בצד ה-UDP Receiver
	recvBuf := make([]byte, 65535)
	if err := udpListener.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatalf("failed to set read deadline: %v", err)
	}

	n, _, err := udpListener.ReadFrom(recvBuf)
	if err != nil {
		t.Fatalf("failed receiving UDP packet: %v", err)
	}

	var receivedPacket pb.Packet
	if err := proto.Unmarshal(recvBuf[:n], &receivedPacket); err != nil {
		t.Fatalf("failed unmarshaling received packet: %v", err)
	}

	// 6. אימות נתוני הפקטה וה-CRC
	if receivedPacket.FileHash != fileHash {
		t.Errorf("expected FileHash %d, got %d", fileHash, receivedPacket.FileHash)
	}

	recomputedCRC := pb.CalculateCRC(
		receivedPacket.FileHash,
		receivedPacket.BlockId,
		receivedPacket.TotalBlocks,
		receivedPacket.SymbolId,
		receivedPacket.KSymbols,
		receivedPacket.NSymbols,
		receivedPacket.FileSize,
		receivedPacket.Content,
	)

	if recomputedCRC != receivedPacket.PacketCrc {
		t.Errorf("CRC mismatch on received packet: expected %d, computed %d", receivedPacket.PacketCrc, recomputedCRC)
	}

	// וידוא שהתוכן תואם לתחילת הקובץ
	if !bytes.Equal(receivedPacket.Content[:100], dummyData[:100]) {
		t.Errorf("packet payload does not match source file data")
	}

	// 7. וידוא סיום חלק של ה-Goroutine
	select {
	case err := <-errChan:
		if err != nil {
			t.Fatalf("sender loop error: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for sender routine to complete")
	}

	if currentState.Load() != int32(pb.SenderState_IDLE) {
		t.Errorf("expected state to return to IDLE, got %v", currentState.Load())
	}
}
