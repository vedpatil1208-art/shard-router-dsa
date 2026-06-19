# ShardRoute — Enterprise Database Cluster Shard Router

A smart routing layer for a massive database split across multiple servers. ShardRoute directs every query to the correct server, handles transaction failures, keeps backup servers synchronized, and ensures no single server runs out of space.


---

## Problem Statement

When a database becomes too large for one server, the data is split into fragments called **shards**, each stored on a different physical server. ShardRoute solves 8 core problems that arise in such a distributed system:

- Tracking which server holds which data fragment was slow
- Failed database updates left records in an inconsistent state with no rollback
- Replication tasks ran out of chronological order, causing backup servers to have conflicting data
- No model existed of how database servers connect, so queries took slow network paths

---

## Features

### Feature 1 — Shard-to-Server Mapping
**Data Structure:** `unordered_map<int, ShardInfo>` + `map<string, int>`

Stores and indexes the mapping of data fragments (shards) to their physical server. An `unordered_map` gives O(1) lookup by shard ID. A sorted `map` with `upper_bound()` gives O(log n) range-based query routing — the same model used by CockroachDB's DistSender layer.

```
Key 'user_1234567' → server-0   (shard-0: user_0000000 to user_2500000)
Key 'user_6000000' → server-2   (shard-2: user_5000000 to user_7500000)
Key 'user_8888888' → server-3   (shard-3: user_7500000 to user_9999999)
```

---

### Feature 2 — Transaction Rollback
**Data Structure:** `stack<UndoNode*>` (stack + singly linked list)

Records every step of a database update as a singly-linked list of `UndoNode` entries. If a transaction fails, the stack is popped and the linked list is walked head-to-tail (most recent operation first) to undo each change in reverse order — the same LIFO model used by MongoDB's WiredTiger rollback journal.

```
[TX-1] BEGIN → INSERT Alice → UPDATE Bob→Robert → COMMIT
[TX-2] BEGIN → INSERT Carol → DELETE Robert → FAILURE
[TX] ROLLBACK — undoing 2 operation(s):
  Undo DELETE row=102 → restoring value='Robert'
  Undo INSERT row=201 → removing Carol
```

**Why linked list over vector?** Prepending a new node to a linked list is always O(1). A vector's `push_back` is occasionally O(n) when it needs to resize and copy all elements.

---

### Feature 3 — Chronological Replication Queue
**Data Structure:** `priority_queue` (min-heap on timestamp)

Processes database replication tasks in strict chronological order regardless of the order they are received. A min-heap configured with `greater<ReplicationTask>` always puts the smallest timestamp at the top — analogous to CockroachDB's Raft log application order.

```
Tasks added out of order:  ts=005, ts=001, ts=010, ts=003, ts=008
Tasks processed in order:  ts=001, ts=003, ts=005, ts=008, ts=010
```

**Why order matters:** If a backup server receives DELETE before INSERT for the same row, the row does not exist yet and the server crashes.

---

### Feature 4 — Instant Row Lookup by ID
**Data Structure:** `unordered_map<long long, RowLocation>`

Instantly finds the server and physical byte offset of any data row using its unique ID. O(1) hash-based lookup — the same approach as MongoDB's `_id` hash index.

```
Row 101 → server-0, shard-0, offset=6464 bytes
Row 301 → server-2, shard-2, offset=409600 bytes
Row 777 → NOT FOUND
```

---

### Feature 5 — Write-Frequency Ranking
**Data Structure:** `priority_queue<ShardFrequency>` (max-heap) + `unordered_map<int, long long>`

Ranks shards by how frequently they are written to. Write counts are incremented in O(1) via a flat `unordered_map` during transactions. A max-heap is rebuilt lazily at transaction boundaries for O(log n) hot-shard peek. Used to prioritize caching and replication — the same technique Cassandra uses for hot partition detection.

```
#1  shard-2  server-2  writes=3400   ← hottest, cache first
#2  shard-1  server-1  writes=1200
#3  shard-0  server-0  writes=502
#4  shard-3  server-3  writes=250
```

---

### Feature 6 — Network Topology Map
**Data Structure:** `vector<vector<Edge>>` (adjacency list graph)

Models the network topology of all database servers as a weighted undirected graph. Each server is a node, each network cable is an edge, and latency in milliseconds is the edge weight. Adjacency list is used because the graph is sparse — not every server connects to every other server. Analogous to CockroachDB's gossip-layer topology store.

```
  NYC-DB0 ────(1.2ms)──── NYC-DB1
     |  \                  /
  (78ms) (200ms)       (76ms)
     |      \            /
  LON-DB2 ──(160ms)── SNG-DB3
```

---

### Feature 7 — Minimum-Latency Routing (Dijkstra)
**Data Structure:** `priority_queue` (min-heap) on the topology graph

Calculates the minimum-latency path for query requests across the cluster using Dijkstra's shortest path algorithm. Always explores the lowest-latency unvisited server next using a min-heap.

```
NYC-DB0 → SNG-DB3:  200ms  (direct link is fastest)
NYC-DB0 → LON-DB2:  77.2ms (via NYC-DB1 — faster than direct 78ms link)
LON-DB2 → NYC-DB1:  76ms   (direct link)
```

**Key insight:** The path NYC-DB0 → NYC-DB1 → LON-DB2 (1.2ms + 76ms = 77.2ms) is faster than the direct NYC-DB0 → LON-DB2 link (78ms) — Dijkstra finds this automatically.

**Pseudocode:**
```
dist[src] = 0;  dist[all others] = ∞
push (dist=0, node=src) into min-heap
while heap is not empty:
  pop (d, u) — node with smallest tentative distance
  if d > dist[u]: skip (stale heap entry)
  if u == dst: break
  for each edge (u → v, weight w):
    if dist[u] + w < dist[v]:
      dist[v] = dist[u] + w;  prev[v] = u
      push (dist[v], v) into heap
reconstruct path by following prev[] from dst back to src, then reverse
```

---

### Feature 8 — Smart Data Migration (Load Balancing)
**Data Structure:** `priority_queue<ServerLoad>` (min-heap) + greedy algorithm

Detects overloaded servers and immediately migrates the largest shard to the least-loaded server. A min-heap finds the emptiest server in O(log n). After migration, the row index is updated so all lookups continue to work correctly — analogous to MongoDB's automatic chunk balancer.

```
Before:  server-0: 20%,  server-1: 30%,  server-2: 70%,  server-3: 10%
Action:  MIGRATE shard-2 (7340032 KB) from server-2 → server-3
After:   server-0: 20%,  server-1: 30%,  server-2: 0%,   server-3: 80%
```

---

## Data Structures Used

| Data Structure | Where Used | Time Complexity |
|---|---|---|
| `unordered_map` | Shard map, row index, write frequency, server loads | O(1) lookup |
| `map` (sorted) | Range-based query routing | O(log n) via `upper_bound` |
| `stack` | Transaction undo log (LIFO rollback) | O(1) push/pop |
| Singly linked list (`UndoNode`) | Per-transaction operation log | O(1) prepend |
| `priority_queue` min-heap | Replication queue, Dijkstra, load balancer | O(log n) insert/pop |
| `priority_queue` max-heap | Write-frequency hot shard ranking | O(log n) insert/pop |
| Graph (adjacency list) | Network topology | O(V + E) traversal |
| `vector` | Dynamic arrays for Dijkstra dist/prev/path | O(1) random access |

---

## Space Complexity

| Structure | Space |
|---|---|
| `shardMap` | O(S) — S = number of shards |
| `rangeIndex` | O(S) — one entry per shard end-key |
| `rowIndex` | O(R) — R = number of indexed rows |
| `topology` | O(V + E) — V = servers, E = network links |
| `txStack` | O(T) — T = operations in active transaction |
| `replicationQueue` | O(Q) — Q = pending replication tasks |
| `writeFreqMap/Heap` | O(S) — one entry per shard |
| `serverLoads` | O(V) — one entry per server |

---

## How to Compile and Run

**Requirements:** C++17 or later, g++ compiler

```bash
# Navigate to project folder
cd /path/to/project

# Compile
g++ -std=c++17 ShardRouter.cpp -o ShardRouter

# Run
./ShardRouter
```

**Fix for macOS users (VS Code Code Runner):**

Open VS Code settings JSON (`Cmd + Shift + P` → "Open User Settings JSON") and add:

```json
"code-runner.executorMap": {
    "cpp": "cd $dir && g++ -std=c++17 $fileName -o $fileNameWithoutExt && $dir$fileNameWithoutExt"
}
```

---

## Project Structure

```
ShardRouter.cpp          — complete implementation (all 8 features)
README.md                — this file
```

**Key structs:**

```
ShardInfo         — shard location, key range, size
Operation         — one database change (INSERT/UPDATE/DELETE)
UndoNode          — linked list node for transaction undo log
ReplicationTask   — one backup job with timestamp
RowLocation       — server + shard + byte offset for a row
ShardFrequency    — shard ID + write count for heap ranking
Edge              — network link with latency and bandwidth
ServerLoad        — server storage utilization
ShardRouter       — main class containing all 8 features
```

---

## Real-World Connections

| Feature | MongoDB equivalent | CockroachDB equivalent |
|---|---|---|
| Shard mapping | mongos config server | Range descriptor cache |
| Transaction rollback | WiredTiger journal | MVCC rollback |
| Replication queue | Oplog tailing | Raft log |
| Row lookup | ObjectId → shard routing | KV index |
| Hot shard ranking | Chunk migration trigger | Lease rebalancing |
| Topology graph | Replica set awareness | Gossip network |
| Dijkstra routing | Read preference nearest | Follower reads |
| Load balancer | Chunk balancer | Store rebalancer |

---

## Sample Output

```
╔══════════════════════════════════════════════════════╗
║  ShardRoute — Enterprise Database Cluster Router     ║
╚══════════════════════════════════════════════════════╝

FEATURE 1 — Shard-to-Server Mapping
  Key 'user_1234567' → server-0
  Key 'user_6000000' → server-2

FEATURE 2 — Transaction Rollback (Stack)
  [TX-1] COMMIT — 2 operation(s) made permanent.
  [TX-2] ROLLBACK — undoing 2 operation(s)

FEATURE 7 — Minimum-Latency Routing (Dijkstra)
  NYC-DB0 → LON-DB2: 77.2ms via NYC-DB0 → NYC-DB1 → LON-DB2

All 8 features demonstrated successfully.
```

---

*ShardRoute — Case Study #145 | ITM Skills University | B.Tech CSE 2025–29*
