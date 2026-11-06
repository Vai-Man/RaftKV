# RaftKV

A Raft-replicated key-value store in C++17, with a deterministic cluster simulator for fault injection. The consensus code is separate from the network, so a five-node cluster, message loss, partitions and crashes all run in one process and replay exactly from a seed.

## What it does

- **Raft consensus:** leader election, log replication, commit by majority, and the safety rules from the paper (election restriction, current-term-only commit counting, a no-op on election).
- **Key-value state machine:** `Put`, `Get`, `Delete`. Reads go through the log, so they are linearizable. Client `(id, seq)` pairs make retries apply once.
- **Persistence model:** term, vote and log survive a crash; role, commit index and the state machine are rebuilt after a restart.
- **Fault injection:** crash and restart nodes, partition the network into groups, drop messages at a chosen rate, delay them by a random number of ticks.
- **Safety checks after every tick:** election safety, leader completeness, state-machine safety and log matching. A scenario fails if any of them is violated.

## Build and run

```bash
cmake -S . -B build
cmake --build build
./build/raftkv_tests          # unit tests and scenarios across 25 seeds
./build/raftkv_demo [seed]    # prints the scenario table
```

No dependencies beyond a C++17 compiler.

## Failure scenarios

| Scenario | What it checks |
|---|---|
| initial-election | Five nodes elect one leader and replicate ten writes |
| leader-crash | A new leader is elected; the old one catches up when it returns |
| follower-catch-up | A follower misses 20 writes and recovers its log |
| partitioned-leader | An isolated leader cannot commit; the majority side wins after healing |
| no-quorum | With two of five nodes alive nothing commits; progress resumes with a quorum |
| lossy-network | 20% message loss; writes still commit and nodes agree |
| rolling-leader-crashes | The leader is crashed six times in a row; no acknowledged write is lost |
| divergent-logs | A deposed leader's uncommitted entries are overwritten on heal |

## Layout

| Path | Contents |
|---|---|
| `include/raftkv/raftkv.h` | Types, `Node`, `KvStore`, `Cluster` |
| `src/node.cpp` | The Raft state machine |
| `src/cluster.cpp` | Simulated network, fault injection, invariant checks |
| `src/scenarios.cpp` | The scenarios above |
| `tests/test_raftkv.cpp` | Protocol unit tests and scenario sweeps |

## Design notes

- Time is a logical tick counter; election timeouts are randomised per node from the seed. That makes a failing run reproducible by its seed.
- A new leader appends a no-op so it can commit earlier-term entries safely.
- Followers return a hint on a failed append so the leader can back up quickly instead of one entry at a time.
- Log matching is checked every 10 ticks over all node pairs; the other invariants are checked each tick.

## Limitations

- This is a simulator. There are no sockets, no disk, and no real clock.
- No membership changes, log compaction or snapshots, so logs only grow.
- Clients talk to the current leader through the `Cluster` object; there is no request forwarding or client-side leader discovery.

## License

MIT
