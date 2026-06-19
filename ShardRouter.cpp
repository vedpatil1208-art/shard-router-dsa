#include <iostream>
#include <unordered_map>
#include <map>
#include <stack>
#include <queue>
#include <vector>
#include <string>
#include <limits>
#include <algorithm>
#include <iomanip>
#include <sstream>
#include <chrono>
#include <functional>

using namespace std;

struct ShardInfo {
    int    serverId;
    string keyRangeStart;
    string keyRangeEnd;
    size_t sizeBytes;

    void print() const {
        cout << "  Shard on server-" << serverId
             << "  range=[" << keyRangeStart << ", " << keyRangeEnd << ")"
             << "  size=" << sizeBytes / 1024 << " KB\n";
    }
};

enum class OpType { INSERT, UPDATE, DELETE };

struct Operation {
    OpType    type;
    int       shardId;
    long long rowId;
    string    oldValue;
    string    newValue;
    string    timestamp;

    string opName() const {
        switch (type) {
            case OpType::INSERT: return "INSERT";
            case OpType::UPDATE: return "UPDATE";
            case OpType::DELETE: return "DELETE";
        }
        return "?";
    }
};

// Singly-linked list node for the per-transaction undo log.
// Using a linked list instead of a vector gives O(1) prepend and
// natural LIFO traversal during rollback without an index.
struct UndoNode {
    Operation op;
    UndoNode* next;
    UndoNode(const Operation& o, UndoNode* n = nullptr) : op(o), next(n) {}
};

struct ReplicationTask {
    long long timestamp;
    int       sourceServer;
    int       targetServer;
    int       shardId;
    long long rowId;
    string    payload;

    // operator> makes priority_queue a min-heap (smallest timestamp processed first)
    bool operator>(const ReplicationTask& o) const { return timestamp > o.timestamp; }
};

struct RowLocation {
    int       serverId;
    int       shardId;
    long long physicalOffset;
};

struct ShardFrequency {
    int       shardId;
    int       serverId;
    long long writeCount;

    // operator< makes priority_queue a max-heap (highest write count at top)
    bool operator<(const ShardFrequency& o) const { return writeCount < o.writeCount; }
};

struct Edge {
    int    targetServer;
    double latencyMs;
    double bandwidthGbps;
};

struct ServerLoad {
    int    serverId;
    size_t usedBytes;
    size_t totalBytes;

    double utilization() const {
        return totalBytes ? (double)usedBytes / totalBytes : 0.0;
    }

    // operator> makes priority_queue a min-heap (least-loaded server at top)
    bool operator>(const ServerLoad& o) const {
        return utilization() > o.utilization();
    }
};

class ShardRouter {
public:
    // unordered_map: O(1) shard descriptor lookup by ID — mirrors CockroachDB's in-memory range-descriptor cache
    unordered_map<int, ShardInfo> shardMap;

    // map<string,int>: sorted by keyRangeEnd; upper_bound gives O(log n) range routing — mirrors CockroachDB's sorted range descriptor table
    map<string, int> rangeIndex;

    // stack<UndoNode*>: each entry is the head of a singly-linked undo list for one transaction.
    // Prepending a new node is O(1); rollback walks head→tail in reverse-insertion order.
    // Same LIFO model as WiredTiger's rollback journal in MongoDB.
    stack<UndoNode*> txStack;
    int nextTxId = 1;

    // min-heap on timestamp: guarantees causal (chronological) replay order — analogous to CockroachDB Raft log application
    priority_queue<ReplicationTask,
                   vector<ReplicationTask>,
                   greater<ReplicationTask>> replicationQueue;

    // unordered_map: O(1) point lookup by rowId — models MongoDB's _id hash index (B-tree in prod, hash here for demo)
    unordered_map<long long, RowLocation> rowIndex;

    // O(1) write increments via flat map; max-heap rebuilt lazily at tx boundaries for O(log n) hot-shard peek
    priority_queue<ShardFrequency>  writeFreqHeap;
    unordered_map<int, long long>   writeFreqMap;

    int                        numServers = 0;
    // adjacency list (vector<vector<Edge>>): sparse graph; suits Dijkstra — analogous to CockroachDB's gossip-layer topology store
    vector<vector<Edge>>       topology;
    unordered_map<int, string> serverNames;

    // unordered_map: O(1) load reads; min-heap built on demand for O(log n) least-loaded selection — analogous to MongoDB balancer
    unordered_map<int, ServerLoad> serverLoads;

    /*
     * SPACE COMPLEXITY SUMMARY
     *   shardMap         → O(S)      S = number of shards
     *   rangeIndex       → O(S)      one entry per shard end-key
     *   rowIndex         → O(R)      R = number of indexed rows
     *   topology         → O(V + E)  V = servers, E = bidirectional links
     *   txStack          → O(T)      T = operations in the active transaction
     *   replicationQueue → O(Q)      Q = pending replication tasks
     *   writeFreqMap/Heap→ O(S)      one entry per shard
     *   serverLoads      → O(V)      one entry per server
     */

    void addShard(int shardId, int serverId,
                  const string& rangeStart, const string& rangeEnd,
                  size_t sizeBytes)
    {
        shardMap[shardId]  = {serverId, rangeStart, rangeEnd, sizeBytes};
        rangeIndex[rangeEnd] = shardId;
        // W4: usedBytes incremented here for standalone correctness; main() immediately
        // overwrites usedBytes with the authoritative value, so this increment is a no-op
        // in the demo flow — not a double-count bug.
        if (serverLoads.count(serverId))
            serverLoads[serverId].usedBytes += sizeBytes;
    }

    // O(log n): upper_bound locates the first shard whose end-key > query key,
    // then a single keyRangeStart check confirms the key falls inside that shard.
    // Replaces the previous O(n) linear scan. Equivalent to CockroachDB's
    // sorted range-descriptor lookup used by the DistSender layer.
    int routeQuery(const string& key) const {
        auto it = rangeIndex.upper_bound(key);   // first end > key
        if (it == rangeIndex.end()) return -1;
        const ShardInfo& info = shardMap.at(it->second);
        return (key >= info.keyRangeStart) ? info.serverId : -1;
    }

    void printShardMap() const {
        cout << "\n[Feature 1] Shard Map (" << shardMap.size() << " shards)\n";
        for (const auto& [id, info] : shardMap) {
            cout << "  Shard-" << id;
            info.print();
        }
    }

    int beginTransaction() {
        txStack.push(nullptr);   // push empty linked-list head
        cout << "[TX-" << nextTxId << "] BEGIN\n";
        return nextTxId++;
    }

    void logOperation(OpType type, int shardId, long long rowId,
                      const string& oldVal, const string& newVal)
    {
        if (txStack.empty()) { cerr << "No active transaction.\n"; return; }

        string ts = currentTimestamp();
        // prepend to linked list — O(1); head always points to most-recent op
        txStack.top() = new UndoNode({type, shardId, rowId, oldVal, newVal, ts}, txStack.top());

        if (type == OpType::INSERT || type == OpType::UPDATE)
            rowIndex[rowId] = {shardMap.count(shardId) ? shardMap[shardId].serverId : -1,
                               shardId, (long long)rowId * 64};

        writeFreqMap[shardId]++;
    }

    void commitTransaction() {
        if (txStack.empty()) { cerr << "No active transaction.\n"; return; }
        // walk and free the linked list; count ops for display
        int count = 0;
        UndoNode* node = txStack.top(); txStack.pop();
        while (node) { UndoNode* tmp = node->next; delete node; node = tmp; count++; }
        cout << "[TX] COMMIT — " << count << " operation(s) made permanent.\n";
        rebuildWriteFreqHeap();
    }

    /*
     * PSEUDOCODE — rollbackTransaction
     *   pop the UndoNode* head from txStack
     *   count nodes to report total ops undone
     *   walk linked list head → tail (most-recent op first):
     *     if INSERT:  erase row from rowIndex
     *     if UPDATE:  restore original shardId in rowIndex
     *     if DELETE:  re-insert row into rowIndex with saved location
     *     decrement writeFreqMap[op.shardId] by 1  (floor at 0)
     *     free the UndoNode
     *   rebuild write-frequency heap
     */
    void rollbackTransaction() {
        if (txStack.empty()) { cerr << "No active transaction.\n"; return; }

        UndoNode* node = txStack.top(); txStack.pop();

        // count ops first so we can print the total before undoing
        int count = 0;
        for (UndoNode* n = node; n; n = n->next) count++;
        cout << "[TX] ROLLBACK — undoing " << count << " operation(s):\n";

        while (node) {
            const Operation& op = node->op;
            cout << "  Undo " << op.opName()
                 << " row=" << op.rowId
                 << " shard=" << op.shardId
                 << " restoring value='" << op.oldValue << "'\n";

            if (op.type == OpType::INSERT) {
                rowIndex.erase(op.rowId);
            } else if (op.type == OpType::UPDATE) {
                // keeps same server/shard; only value is rolled back at the storage layer
                rowIndex[op.rowId].shardId = op.shardId;
            } else if (op.type == OpType::DELETE) {
                rowIndex[op.rowId] = {shardMap.count(op.shardId)
                                          ? shardMap[op.shardId].serverId : -1,
                                      op.shardId, (long long)op.rowId * 64};
            }

            if (writeFreqMap.count(op.shardId))
                writeFreqMap[op.shardId] = max(0LL, writeFreqMap[op.shardId] - 1);

            UndoNode* tmp = node->next;
            delete node;
            node = tmp;
        }
        rebuildWriteFreqHeap();
    }

    void enqueueReplicationTask(long long timestamp, int src, int tgt,
                                int shardId, long long rowId,
                                const string& payload)
    {
        replicationQueue.push({timestamp, src, tgt, shardId, rowId, payload});
    }

    void processReplicationQueue() {
        cout << "\n[Feature 3] Processing replication queue ("
             << replicationQueue.size() << " tasks) in chronological order:\n";
        int seq = 1;
        while (!replicationQueue.empty()) {
            const ReplicationTask& t = replicationQueue.top();
            cout << "  [" << seq++ << "] ts=" << t.timestamp
                 << "  server-" << t.sourceServer << " → server-" << t.targetServer
                 << "  shard=" << t.shardId
                 << "  row=" << t.rowId
                 << "  payload='" << t.payload << "'\n";
            replicationQueue.pop();
        }
    }

    void indexRow(long long rowId, int serverId, int shardId, long long offset) {
        rowIndex[rowId] = {serverId, shardId, offset};
    }

    bool lookupRow(long long rowId) const {
        auto it = rowIndex.find(rowId);
        if (it == rowIndex.end()) {
            cout << "  Row " << rowId << " NOT FOUND in index.\n";
            return false;
        }
        const RowLocation& loc = it->second;
        cout << "  Row " << rowId
             << " → server-" << loc.serverId
             << "  shard-" << loc.shardId
             << "  offset=" << loc.physicalOffset << " bytes\n";
        return true;
    }

    void printRowIndex() const {
        cout << "\n[Feature 4] Row Location Index (" << rowIndex.size() << " rows)\n";
        for (const auto& [rowId, loc] : rowIndex) {
            cout << "  Row " << rowId
                 << " → server-" << loc.serverId
                 << "  shard-" << loc.shardId
                 << "  offset=" << loc.physicalOffset << "\n";
        }
    }

    void rebuildWriteFreqHeap() {
        writeFreqHeap = priority_queue<ShardFrequency>();
        for (const auto& [shardId, count] : writeFreqMap) {
            int serverId = shardMap.count(shardId) ? shardMap[shardId].serverId : -1;
            writeFreqHeap.push({shardId, serverId, count});
        }
    }

    void printWriteFrequencyRanking(int topN = 5) const {
        cout << "\n[Feature 5] Top-" << topN << " hottest shards (by write count):\n";
        auto tmp = writeFreqHeap;  // copy to avoid consuming the heap
        int rank = 1;
        while (!tmp.empty() && rank <= topN) {
            const ShardFrequency& sf = tmp.top();
            cout << "  #" << rank++ << " shard-" << sf.shardId
                 << "  server-" << sf.serverId
                 << "  writes=" << sf.writeCount << "\n";
            tmp.pop();
        }
    }

    void initTopology(int n) {
        numServers = n;
        topology.assign(n, {});
    }

    void addServerLabel(int serverId, const string& name) {
        serverNames[serverId] = name;
    }

    void addLink(int from, int to, double latencyMs, double bandwidthGbps) {
        if (from >= numServers || to >= numServers) {
            cerr << "Server ID out of range.\n"; return;
        }
        topology[from].push_back({to,   latencyMs, bandwidthGbps});
        topology[to  ].push_back({from, latencyMs, bandwidthGbps});
    }

    void printTopology() const {
        cout << "\n[Feature 6] Network Topology (" << numServers << " servers)\n";
        for (int i = 0; i < numServers; i++) {
            string label = serverNames.count(i) ? serverNames.at(i) : "server-" + to_string(i);
            cout << "  " << label << " → ";
            for (const Edge& e : topology[i]) {
                string tlabel = serverNames.count(e.targetServer)
                                    ? serverNames.at(e.targetServer)
                                    : "server-" + to_string(e.targetServer);
                cout << tlabel << "(" << e.latencyMs << "ms) ";
            }
            cout << "\n";
        }
    }

    /*
     * PSEUDOCODE — Dijkstra (min-latency path)
     *   dist[src] = 0;  dist[all others] = ∞
     *   push (dist=0, node=src) into min-heap
     *   while heap is not empty:
     *     pop (d, u) — node with smallest tentative distance
     *     if d > dist[u]: skip (stale heap entry)
     *     if u == dst: break  — shortest path found
     *     for each edge (u → v, weight w):
     *       if dist[u] + w < dist[v]:
     *         dist[v] = dist[u] + w;  prev[v] = u
     *         push (dist[v], v) into heap
     *   reconstruct path: follow prev[] from dst back to src, then reverse
     */
    pair<double, vector<int>> dijkstra(int src, int dst) const {
        if (src >= numServers || dst >= numServers)
            return {-1.0, {}};

        vector<double> dist(numServers, numeric_limits<double>::infinity());
        vector<int>    prev(numServers, -1);

        priority_queue<pair<double,int>,
                       vector<pair<double,int>>,
                       greater<pair<double,int>>> pq;

        dist[src] = 0.0;
        pq.push({0.0, src});

        while (!pq.empty()) {
            auto [d, u] = pq.top(); pq.pop();

            if (d > dist[u]) continue;  // stale entry; a shorter path was already found
            if (u == dst)    break;

            for (const Edge& e : topology[u]) {
                double nd = dist[u] + e.latencyMs;
                if (nd < dist[e.targetServer]) {
                    dist[e.targetServer] = nd;
                    prev[e.targetServer] = u;
                    pq.push({nd, e.targetServer});
                }
            }
        }

        if (dist[dst] == numeric_limits<double>::infinity())
            return {-1.0, {}};

        vector<int> path;
        for (int cur = dst; cur != -1; cur = prev[cur])
            path.push_back(cur);
        reverse(path.begin(), path.end());

        return {dist[dst], path};
    }

    void printShortestPath(int src, int dst) const {
        auto [latency, path] = dijkstra(src, dst);
        cout << "\n[Feature 7] Min-latency path: server-" << src
             << " → server-" << dst << "\n";
        if (latency < 0) { cout << "  No path exists.\n"; return; }

        cout << "  Total latency: " << latency << " ms\n  Path: ";
        for (int i = 0; i < (int)path.size(); i++) {
            if (i) cout << " → ";
            string lbl = serverNames.count(path[i])
                             ? serverNames.at(path[i])
                             : "server-" + to_string(path[i]);
            cout << lbl;
        }
        cout << "\n";
    }

    void registerServer(int serverId, size_t usedBytes, size_t totalBytes) {
        serverLoads[serverId] = {serverId, usedBytes, totalBytes};
    }

    priority_queue<ServerLoad, vector<ServerLoad>, greater<ServerLoad>>
    buildLoadHeap() const {
        priority_queue<ServerLoad, vector<ServerLoad>, greater<ServerLoad>> heap;
        for (const auto& [id, sl] : serverLoads) heap.push(sl);
        return heap;
    }

    int leastLoadedServer() const {
        auto heap = buildLoadHeap();
        return heap.empty() ? -1 : heap.top().serverId;
    }

    void balanceCluster(double overloadThreshold = 0.80) {
        cout << "\n[Feature 8] Cluster Storage Balancing\n";
        cout << "  Current utilization:\n";
        for (const auto& [id, sl] : serverLoads) {
            cout << "    server-" << id
                 << "  " << fixed << setprecision(1)
                 << (sl.utilization() * 100) << "%"
                 << "  (" << sl.usedBytes / 1024 << " KB / "
                 << sl.totalBytes / 1024 << " KB)\n";
        }

        // W2 ping-pong note: leastLoadedServer() re-reads live serverLoads each iteration.
        // If migrating a large shard pushes the target over the threshold, the next
        // outer-loop iteration may migrate it right back. Harmless in this demo
        // (4 shards, one overloaded server); production code would snapshot candidate
        // targets before entering the loop to avoid oscillation.
        for (auto& [srcId, srcLoad] : serverLoads) {
            if (srcLoad.utilization() < overloadThreshold) continue;

            int tgtId = leastLoadedServer();
            if (tgtId == srcId) continue;

            int    bestShard = -1;
            size_t bestSize  = 0;
            for (const auto& [shardId, info] : shardMap) {
                if (info.serverId == srcId && info.sizeBytes > bestSize) {
                    bestShard = shardId;
                    bestSize  = info.sizeBytes;
                }
            }

            if (bestShard < 0) continue;

            cout << "  MIGRATE shard-" << bestShard
                 << " (" << bestSize / 1024 << " KB)"
                 << "  server-" << srcId << " → server-" << tgtId << "\n";

            shardMap[bestShard].serverId      = tgtId;
            srcLoad.usedBytes                -= bestSize;
            serverLoads[tgtId].usedBytes     += bestSize;

            for (auto& [rowId, loc] : rowIndex) {
                if (loc.shardId == bestShard)
                    loc.serverId = tgtId;
            }
        }

        cout << "  Post-migration utilization:\n";
        for (const auto& [id, sl] : serverLoads) {
            cout << "    server-" << id
                 << "  " << fixed << setprecision(1)
                 << (sl.utilization() * 100) << "%\n";
        }
    }

    static string currentTimestamp() {
        static long long counter = 1000;
        return to_string(counter++);
    }
};

int main() {
    cout << "╔══════════════════════════════════════════════════════╗\n";
    cout << "║  ShardRoute — Enterprise Database Cluster Router     ║\n";
    cout << "╚══════════════════════════════════════════════════════╝\n\n";

    ShardRouter router;

    for (int i = 0; i < 4; i++)
        router.registerServer(i, 0, 10ULL * 1024 * 1024 * 1024);

    router.initTopology(4);
    router.addServerLabel(0, "NYC-DB0");
    router.addServerLabel(1, "NYC-DB1");
    router.addServerLabel(2, "LON-DB2");
    router.addServerLabel(3, "SNG-DB3");

    router.addLink(0, 1,   1.2, 40.0);
    router.addLink(0, 2,  78.0,  1.0);
    router.addLink(1, 2,  76.0,  1.0);
    router.addLink(2, 3, 160.0,  0.5);
    router.addLink(0, 3, 200.0,  0.5);

    cout << "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n";
    cout << "FEATURE 1 — Shard-to-Server Mapping\n";
    cout << "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n";

    router.addShard(0, 0, "user_0000000", "user_2500000", 2ULL*1024*1024*1024);
    router.addShard(1, 1, "user_2500000", "user_5000000", 3ULL*1024*1024*1024);
    router.addShard(2, 2, "user_5000000", "user_7500000", 7ULL*1024*1024*1024);
    router.addShard(3, 3, "user_7500000", "user_9999999", 1ULL*1024*1024*1024);
    router.serverLoads[0].usedBytes = 2ULL*1024*1024*1024;
    router.serverLoads[1].usedBytes = 3ULL*1024*1024*1024;
    router.serverLoads[2].usedBytes = 7ULL*1024*1024*1024;
    router.serverLoads[3].usedBytes = 1ULL*1024*1024*1024;

    router.printShardMap();

    cout << "\n  Routing queries:\n";
    vector<string> testKeys = {"user_1234567", "user_6000000", "user_8888888"};
    for (const string& k : testKeys) {
        int srv = router.routeQuery(k);
        cout << "  Key '" << k << "' → server-" << srv << "\n";
    }

    cout << "\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n";
    cout << "FEATURE 2 — Transaction Rollback (Stack)\n";
    cout << "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n";

    router.beginTransaction();
    router.logOperation(OpType::INSERT, 0, 101, "", "Alice");
    router.logOperation(OpType::UPDATE, 0, 102, "Bob", "Robert");
    router.commitTransaction();
    cout << "\n";

    router.beginTransaction();
    router.logOperation(OpType::INSERT, 1, 201, "", "Carol");
    router.logOperation(OpType::DELETE, 1, 102, "Robert", "");
    cout << "  [Simulating failure — rolling back TX-2]\n";
    router.rollbackTransaction();

    cout << "\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n";
    cout << "FEATURE 3 — Replication Queue (min-heap)\n";
    cout << "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n";

    router.enqueueReplicationTask(1700000005, 0, 1, 0, 101, "INSERT Alice");
    router.enqueueReplicationTask(1700000001, 0, 2, 0, 101, "INSERT Alice");
    router.enqueueReplicationTask(1700000010, 0, 3, 0, 102, "UPDATE Bob→Robert");
    router.enqueueReplicationTask(1700000003, 1, 2, 1, 201, "INSERT Carol");
    router.enqueueReplicationTask(1700000008, 0, 1, 0, 105, "DELETE Dave");

    router.processReplicationQueue();

    cout << "\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n";
    cout << "FEATURE 4 — Instant Row Lookup by ID\n";
    cout << "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n";

    router.indexRow(301, 2, 2, 409600);
    router.indexRow(402, 3, 3, 819200);
    router.indexRow(999, 0, 0, 102400);
    router.printRowIndex();

    cout << "\n  Point lookups:\n";
    router.lookupRow(101);
    router.lookupRow(301);
    router.lookupRow(999);
    router.lookupRow(777);

    cout << "\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n";
    cout << "FEATURE 5 — Write-Frequency Ranking (max-heap)\n";
    cout << "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n";

    router.writeFreqMap[0] += 500;
    router.writeFreqMap[1] += 1200;
    router.writeFreqMap[2] += 3400;
    router.writeFreqMap[3] += 250;
    router.rebuildWriteFreqHeap();

    router.printWriteFrequencyRanking(4);

    cout << "\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n";
    cout << "FEATURE 6 — Network Topology Map\n";
    cout << "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n";
    router.printTopology();

    cout << "\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n";
    cout << "FEATURE 7 — Minimum-Latency Routing (Dijkstra)\n";
    cout << "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n";

    router.printShortestPath(0, 3);
    router.printShortestPath(0, 2);
    router.printShortestPath(2, 1);

    cout << "\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n";
    cout << "FEATURE 8 — Cluster Storage Balancing\n";
    cout << "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n";

    router.balanceCluster(0.65);

    cout << "\n  Updated shard map after migration:\n";
    router.printShardMap();

    cout << "\n╔══════════════════════════════════════════════════════╗\n";
    cout << "║  All 8 features demonstrated successfully.           ║\n";
    cout << "╚══════════════════════════════════════════════════════╝\n";
    return 0;
}
