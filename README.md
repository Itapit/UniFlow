# System Specification: UniFlow Protocol

UniFlow is an asynchronous, high-throughput, unidirectional-capable file delivery protocol designed for unreliable network environments. By decoupling transmission mechanics from delivery acknowledgement, UniFlow eliminates Round-Trip Time (RTT) bottlenecks, avoiding TCP Head-of-Line blocking and conventional ARQ latency.

---

## 1. Architectural Foundations: The FLUTE Paradigm

UniFlow adapts core architectural concepts from **FLUTE (File Delivery over Unidirectional Transport — RFC 3926 / RFC 6726)**, an established standard for multicast and loss-tolerant file distribution:

* **Asymmetric Unidirectional Delivery:** Transmission pipelines operate without synchronous backchannels or per-packet ACKs.
* **Proactive Loss Recovery:** Instead of re-requesting missing segments, recovery capability is embedded directly into the transmission stream using Application-Level Forward Error Correction (AL-FEC).
* **Deterministic Object Partitioning:** Continuous file byte-streams are decomposed hierarchically into **Source Blocks**, which are further divided into **Encoding Symbols** (packets) of strictly uniform length.
* **In-Band Decoding Metadata:** Every packet carries complete reconstruction headers (block IDs, symbol indices, FEC dimension parameters), allowing stateless reception and out-of-order reassembly across arbitrary loss topologies.

---

## 2. Mathematical Framework: Reed-Solomon Erasure Coding

Loss tolerance is powered by an $(N, K)$ Reed-Solomon Erasure Coding scheme implemented over Galois Field $\text{GF}(2^8)$ (or $\text{GF}(2^{16})$ for larger block bounds):

### 2.1. Parameterization
* $K$ **(Source Symbols):** The number of original data packets segmented from a block.
* $M$ **(Parity / Repair Symbols):** The number of redundant packets generated via generator matrix multiplication.
* $N$ **(Total Transmitted Symbols):** $N = K + M$.
* **Symbol Size ($S$):** The fixed byte length of the payload field across all packets in the block ($S \le 1400\text{ bytes}$ to maintain MTU compliance and avoid IP-level fragmentation).

### 2.2. Matrix Transformation & Erasure Reconstruction
1. **Encoding:** The source symbols $D = [d_0, d_1, \dots, d_{K-1}]^T$ are multiplied by an $N \times K$ Vandermonde or Cauchy distribution matrix $G$:
   $$C = G \cdot D$$
   The resulting vector $C$ yields $K$ systematic data packets (symbols $0$ to $K-1$) and $M$ parity repair packets (symbols $K$ to $N-1$).
2. **Loss Handling & Inversion:** The transport channel introduces packet erasures. If any $K$ distinct packets arrive from the total $N$ transmitted:
   * An auxiliary submatrix $G'$ is formed by keeping the $K$ rows of $G$ corresponding to the received symbol IDs.
   * The original data is recovered via Gaussian elimination / matrix inversion:
     $$D = (G')^{-1} \cdot C'$$
3. **Zero-Padding & Exact Truncation:**
   Because matrix operations require uniform symbol dimensions, any remaining fragment in the final source packet of a block is zero-padded to size $S$. To prevent file corruption and checksum degradation on recovery, the `file_size` parameter explicitly delimits valid payload bounds, instructing the receiver to truncate trailing zero-padding during final disk serialization.

---

## 3. Data Integrity & Verification Pipeline

UniFlow executes integrity verification across two independent boundaries:

* **L1 Packet Verification (CRC32):** Computed over the payload content and evaluated immediately upon UDP socket ingress. Corrupted packets caused by hardware bit flips are discarded prior to matrix ingestion to prevent mathematical poisoning of the FEC decode matrix.
* **L2 Object Verification (File Hash):** A cryptographic hash (e.g., SHA-256 / 64-bit session digest) calculated over the entire unpadded file. Evaluated once all blocks have undergone matrix decoding and disk reassembly.

---

## 4. System Topology & Process Boundaries

The architecture separates responsibilities into specialized runtimes communicating over high-speed Inter-Process Communication (IPC):

```text
+------------------------+                     +------------------------+
|  File Monitor (Python) |                     |    Receivers (Go)      |
|  - Filesystem watch    |                     |    - High-speed UDP    |
|  - Ingestion triggers  |                     |    - CRC32 verification|
+-----------+------------+                     |    - RS Matrix decode  |
            | IPC                              |    - Disk Assembly     |
            v                                  +-----------+------------+
+------------------------+                                 | IPC
| Session Manager (Py)   |                                 v
| - Transfer life cycle  |                     +------------------------+
| - FSM State tracking   |                     | Session Monitor (Py)   |
+-----------+------------+                     | - Ingestion completion |
            | IPC                              | - Status notifications |
            v                                  +------------------------+
+------------------------+
|     Senders (Go)       |
|  - File segmentation   |
|  - RS Matrix encode    |
|  - Protobuf Varint wire|
|  - UDP socket dispatch |
+------------------------+
```

* **Go Engine (Performance Plane):** Manages all high-frequency operations: file disk I/O, Reed-Solomon matrix multiplication, protobuf binary serialization, and raw UDP socket writes/reads with low allocation overhead.
* **Python Runtime (Control Plane):** Manages transfer sessions, file system observation, business logic orchestration, and transfer state machines.
* **IPC Transport:** Local Unix Domain Sockets passing length-delimited Protobuf messages between processes.

---

## 5. Wire Format & Protocol Schema (`packet.proto`)

Packets utilize Protocol Buffers v3 with Varint field packing to minimize wire footprint and CPU deserialization cost:

```protobuf
syntax = "proto3";

package uniflow;
option go_package = "./pb";

message Packet {
  uint64 file_hash    = 1; // Unique file identifier / integrity digest
  uint32 block_id     = 2; // Index of the source block (0-indexed)
  uint32 total_blocks = 3; // Total number of blocks comprising the object
  uint32 symbol_id    = 4; // Symbol index: 0..K-1 (Data), K..N-1 (Parity)
  uint32 k_symbols    = 5; // Count of source symbols required for decoding (K)
  uint32 n_symbols    = 6; // Total symbols transmitted per block (N = K + M)
  uint64 file_size    = 7; // Exact unpadded file size in bytes for truncation
  bytes  content      = 8; // Binary symbol payload (<= 1400 bytes, MTU safe)
  uint32 packet_crc   = 9; // CRC32 integrity check of the content field
}
```

---

## 6. Execution Lifecycle

```text
Sender Processing Pipeline:
[Source File] 
      │
      ▼
[Block Segmentation] (Divide into blocks of K * S bytes)
      │
      ▼
[RS Encoding Matrix] (Generate M parity symbols per block)
      │
      ▼
[Packet Framing]     (Attach FileHash, BlockID, SymbolID, FileSize, CRC32)
      │
      ▼
[Protobuf Varint]    (Serialize to compact binary wire representation)
      │
      ▼
[UDP Dispatch]       (Transmit N packets per block; S <= 1400 bytes)

-------------------------------------------------------------------------

Receiver Processing Pipeline:
[UDP Datagram Read]
      │
      ▼
[Protobuf Parse]     (Extract tags & payload directly from byte buffer)
      │
      ▼
[CRC32 Evaluation]   (Pass -> keep; Fail -> drop immediately)
      │
      ▼
[Block Aggregator]   (Accumulate symbols; check if Count(received) >= K)
      │
      ▼
[RS Matrix Decode]   (Invert G' submatrix to restore K original data symbols)
      │
      ▼
[Block Assembly]     (Order symbols 0..K-1 into sequential block buffer)
      │
      ▼
[Padding Truncation] (Strip trailing zero-padding using file_size boundary)
      │
      ▼
[Final File Write]   (Commit to storage and verify whole-file hash)
```
