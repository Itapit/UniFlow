# System Specification: UniFlow Protocol

UniFlow is an asynchronous, high-throughput, multi-process UDP file transfer protocol engineered for lossy and jitter-prone network channels. By combining Application-Level Forward Error Correction (AL-FEC), inter-process memory concurrency, and packet interleaving across parallel workers, UniFlow guarantees robust data reconstruction without Round-Trip Time (RTT) degradation or transport-level retransmissions.

---

## 1. Architectural Foundations: The FLUTE Paradigm

UniFlow adapts core principles from **FLUTE (File Delivery over Unidirectional Transport — RFC 3926 / RFC 6726)**:

* **Unidirectional Transport Abstraction:** The delivery path operates without per-packet acknowledgments (ACKs) or window renegotiations.
* **Proactive Erasure Recovery:** Instead of reacting to packet loss via Automatic Repeat reQuest (ARQ), resilience is embedded into the stream through mathematical parity generation.
* **Deterministic Object Partitioning:** Large objects are partitioned into discrete **Source Blocks**, which are subdivided into uniform **Encoding Symbols** (packets).
* **Self-Describing Packet Headers:** Every datagram carries complete context (file hash, block identifiers, symbol indices, and FEC dimensions), allowing stateless processing and out-of-order reassembly across arbitrary network paths.

---

## 2. Multi-Process Architecture & Interleaving

The deployment topology distributes computational and network workloads across independent operating system processes interconnected via local IPC and shared memory primitives.

```text
+-----------------------------------------------------------------------------------------------------------------+
|                                              TRANSMISSION TOPOLOGY                                              |
+-----------------------------------------------------------------------------------------------------------------+

       TX (Sender Machine)                                                        RX (Receiver Machine)
+--------------------------------+                                         +--------------------------------+
|                                |                                         |                                |
|  +--------------------------+  |                                         |  +--------------------------+  |
|  |  File Monitor (Process)  |  |                                         |  | Receiver 1 (Process)     |  |
|  |  - Filesystem Watcher    |  |                                         |  | - UDP Ingress Socket     |  |
|  +-------------+------------+  |                                         |  +------------+-------------+  |
|                | IPC           |                                         |               | IPC            |
|                v               |                                         |               v                |
|  +--------------------------+  |          +---------------+              |  +--------------------------+  |
|  | Shared Atomic Counter    |  |          |               |              |  | Receiver 2 (Process)     |  |
|  | (Shared Memory Block)    |  |          |               |              |  | - UDP Ingress Socket     |  |
|  +-------------+------------+  |          |               |              |  +------------+-------------+  |
|                |               |   UDP    |               |    UDP       |               | IPC            |
|    +-----------+-----------+   +--------->|    Router     |------------->|               v                |
|    |           |           |   |          |  (Emulated /  |              |  +--------------------------+  |
|    v           v           v   |          |   Physical    |              |  | Receiver 3 (Process)     |  |
| +------+   +------+   +------+ |          |    Lossy)     |              |  | - UDP Ingress Socket     |  |
| |Sender|   |Sender|   |Sender| |          |               |              |  +------------+-------------+  |
| |  1   |   |  2   |   |  3   | |          +---------------+              |               | IPC            |
| +------+   +------+   +------+ |                                         |               v                |
| (Proc)     (Proc)     (Proc)   |                                         |  +--------------------------+  |
|                                |                                         |  | Session Manager (Process)|  |
|                                |                                         |  | - Reassembly & Hash Check|  |
+--------------------------------+                                         |  +--------------------------+  |
                                                                           |                                |
                                                                           +--------------------------------+
```

### 2.1. Shared-Memory Work Distribution
On the sender host (`TX Machine`), parallel Go processes coordinate work allocations via a **Shared Memory Atomic Counter**:
* Workers concurrently claim block ranges by executing atomic fetch-and-add operations against shared memory.
* Process contention is eliminated at the operating system level, allowing lockless load balancing across CPU cores without IPC scheduling latency.

### 2.2. Block Interleaving Against Burst Losses
Standard sequential transmission renders transmissions vulnerable to **Burst Loss Events** (e.g., periodic router queue drops, buffer exhaustion, or temporary link dropouts lasting 200–500 ms):
* **The Burst Vulnerability:** In sequential transmission, losing 500 ms of bandwidth drops an entire block ($> M$ packets lost), exceeding recovery thresholds and failing the transfer.
* **The UniFlow Interleaving Solution:** Senders skip ahead by block strides and transmit concurrently across multiple distinct blocks:
  $$\text{Sender}_1 \to \text{Block}_0,\quad \text{Sender}_2 \to \text{Block}_1,\quad \text{Sender}_3 \to \text{Block}_2$$
* **Error Distribution:** A transient 500 ms blackout punctures only a small subset of symbols across Block $0$, Block $1$, and Block $2$. Because the losses are distributed uniformly across blocks, the dropped packet count per block remains well within the parity tolerance margin ($\le M$), allowing total erasure recovery.

---

## 3. Mathematical Recovery: Reed-Solomon Erasure Coding

Loss tolerance is governed by an $(N, K)$ Reed-Solomon Erasure Coding matrix over Galois Field $\text{GF}(2^8)$ or $\text{GF}(2^{16})$.

### 3.1. Parameter Definitions
* $K$ **(Source Symbols):** Original payload chunks segmented from a block.
* $M$ **(Parity / Repair Symbols):** Redundant mathematical parity packets generated via generator matrix multiplication.
* $N$ **(Total Symbols):** $N = K + M$.
* $S$ **(Symbol Size):** Fixed byte payload length across all packets ($S \le 1400\text{ bytes}$) to maintain MTU compliance and avoid IP fragmentation.

### 3.2. Matrix Transformation & Erasure Reconstruction
1. **Encoding:** Source vectors $D = [d_0, d_1, \dots, d_{K-1}]^T$ are projected through an $N \times K$ Cauchy or Vandermonde distribution matrix $G$:
   $$C = G \cdot D$$
   The output vector $C$ contains systematic source packets ($0$ to $K-1$) and repair parity packets ($K$ to $N-1$).
2. **Decoding / Inversion:** Let $C'$ denote any subset of $K$ intact symbols received by the receiver workers. An auxiliary submatrix $G'$ is formed using the corresponding $K$ rows of $G$. The original data vector is reconstructed via matrix inversion:
   $$D = (G')^{-1} \cdot C'$$
3. **Zero-Padding & Exact Truncation:**
   Because matrix algebra requires strictly uniform symbol lengths, terminal block fragments are padded with zero-bytes to size $S$. To prevent hash mismatch on completion, the `file_size` parameter instructs the receiver engine to truncate padding bytes during disk output assembly.

---

## 4. Multi-Tier Integrity Verification

Data integrity is validated across two isolated layers:

* **L1 Packet Boundary (CRC32):** Every UDP packet encapsulates an IEEE 802.3 CRC32 checksum in `packet_crc`. Receivers evaluate this checksum upon kernel socket read. Packets altered by physical link bit flips are dropped immediately to prevent mathematical poisoning of the inversion matrix.
* **L2 Object Boundary (Cryptographic Hash):** The entire reconstructed byte sequence is hashed upon completion (SHA-256 / 64-bit digest) and compared against `file_hash` to ensure byte-perfect parity with the source file.

---

## 5. Wire Format Schema (`packet.proto`)

Protobuf v3 with Varint bit-packing minimizes wire footprint and deserialization CPU cost:

```protobuf
syntax = "proto3";

package uniflow;
option go_package = "./pb";

message Packet {
  uint64 file_hash    = 1; // Unique file identifier / session hash
  uint32 block_id     = 2; // Source block identifier (0-indexed)
  uint32 total_blocks = 3; // Total blocks comprising the file
  uint32 symbol_id    = 4; // Symbol index: 0..K-1 (Data), K..N-1 (Parity)
  uint32 k_symbols    = 5; // Source symbols required for decode (K)
  uint32 n_symbols    = 6; // Total symbols transmitted per block (N = K + M)
  uint64 file_size    = 7; // Exact unpadded file size in bytes for truncation
  bytes  content      = 8; // Raw binary payload (<= 1400 bytes, MTU safe)
  uint32 packet_crc   = 9; // CRC32 checksum of content bytes
}
```

---

## 6. End-to-End Processing Workflow

```text
[Source File Detected by File Monitor]
                 │
                 ▼ (File Metadata via IPC)
[Senders Coordinate via Shared Memory Atomic Counter]
                 │
   ┌─────────────┼─────────────┐
   ▼             ▼             ▼
[Sender 1]   [Sender 2]   [Sender 3]  <--- Interleaved Block Slicing
[Block 0]    [Block 1]    [Block 2]
   │             │             │
   └─────────────┼─────────────┘
                 ▼
[RS Matrix Multiplication: Generate M Parity Packets per Block]
                 │
                 ▼
[Protobuf Serialization: Varint Tag Packing]
                 │
                 ▼
[Multi-Process UDP Transmission -> Network / Lossy Router]
                 │
   ┌─────────────┼─────────────┐
   ▼             ▼             ▼
[Receiver 1] [Receiver 2] [Receiver 3]  <--- Parallel Datagram Ingress
   │             │             │
   └─────────────┼─────────────┘
                 ▼
[CRC32 Evaluation: Discard Corrupt Symbols]
                 │
                 ▼
[Buffer by Block ID: Evaluate Condition Count(Symbols) >= K]
                 │
                 ▼
[Gaussian Elimination: Invert G' Submatrix to Recover Data Symbols]
                 │
                 ▼
[Session Manager IPC: Aggregate Blocks & Truncate Trailing Padding]
                 │
                 ▼
[Disk Output & Cryptographic Hash Verification]
```
