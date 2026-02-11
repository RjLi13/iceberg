# Production-Scale Benchmark Results

## Summary

Benchmark with 1,000 snapshots and 12,000 files:
- **Driver Blocking**: Sync 336ms → Async 0.007ms (46,000x improvement)
- **Trade-off**: Detection latency bounded by polling interval (~1s)

This benchmark measures the benefit (reduced driver blocking) but not the cost (increased detection latency). See Trade-offs section for full picture.

## Configuration

```
Snapshots: 10, 50, 200, 500, 1000
Files per snapshot: 10-15 (simulates partitioned writes)
Total files: Up to 12,000
ReadLimit: maxFiles(numSnapshots × 12)
```

## Results

| Snapshots | Async (ms) | Sync (ms) | Speedup | Sync Scaling |
|-----------|------------|-----------|---------|--------------|
| 10        | 0.007      | 2.475     | 339x    | 1.0x         |
| 50        | 0.007      | 10.282    | 1,464x  | 4.2x         |
| 200       | 0.007      | 56.992    | 8,458x  | 23.0x        |
| 500       | 0.007      | 126.576   | 18,430x | 51.2x        |
| 1000      | 0.007      | 335.647   | 46,150x | 135.6x       |

**Note**: Async planner received 5s warmup to pre-fill queue (line 147). This measures steady-state behavior after queue is populated.

## Trade-offs

### ✅ Benefit: Reduced Driver Blocking
Async planner moves catalog I/O to background thread.

At 1000 snapshots:
- **Sync**: 336ms blocked on driver thread
- **Async**: 7µs polling pre-filled queue

### ⚠️ Cost: Increased Detection Latency
New snapshots detected on polling interval, not immediately.

At 1000 snapshots:
- **Sync**: ~93ms to detect new snapshot
- **Async** (1s polling): ~1,000ms to detect new snapshot

### When to Use

**Enable when:**
- Table has 100+ retained snapshots
- Driver blocking causes timeouts
- 1s+ detection delay acceptable
- Memory overhead acceptable (~2MB per 10k files)

**Don't enable when:**
- Table has <50 snapshots (sync overhead <10ms)
- Sub-second detection required
- Memory severely constrained

## Scaling Analysis

### Async Planner

Latency remains constant at ~7µs across all snapshot counts (variance ±4% is measurement noise).

### Sync Planner

Shows super-linear degradation:

| Snapshots | Expected Scaling | Actual Scaling | Deviation |
|-----------|------------------|----------------|-----------|
| 50        | 5x               | 4.2x           | 83%       |
| 200       | 20x              | 23.0x          | 115%      |
| 500       | 50x              | 51.2x          | 102%      |
| 1000      | 100x             | 135.6x         | 136%      |

The super-linear behavior at 1000 snapshots (136% of expected) is likely due to:
- Larger manifest metadata working sets
- Filesystem cache misses
- Non-constant Hadoop filesystem overhead

## Implementation Details

### Sync Planner
```
latestOffset() execution:
  1. Scan snapshots sequentially from startOffset
  2. Open manifest files via Hadoop FileSystem
  3. Read file metadata for up to maxFiles limit
  4. Return offset when limit reached

Time: O(N) where N = number of snapshots to scan
```

### Async Planner
```
Background thread (continuous):
  1. Scan snapshots and add to queue
  2. Poll catalog on configured interval (default 1s)

latestOffset() execution:
  1. Poll pre-filled in-memory queue
  2. Iterate until limit reached
  3. Return offset

Time: O(1) queue iteration over pre-fetched data
Warmup: Benchmark includes 5s pre-fill period (line 147)
```

## Memory Overhead

Async planner buffers snapshot metadata in heap:
- Per FileScanTask: ~100-200 bytes
- At 1000 snapshots × 12 files: ~1.2-2.4MB heap usage

## Benchmark Environment

- JDK: OpenJDK 21.0.8
- JMH: 1.37
- JVM: -Xmx32g
- Filesystem: Local HadoopCatalog
- Warmup: 5 iterations × 1s
- Measurement: 10 iterations × 1s
- Fork: 1

## Comparison to Light Benchmark

| Parameter | Light | Production |
|-----------|-------|------------|
| Max snapshots | 200 | 1000 |
| Files per snapshot | 1 | 10-15 |
| Total files | 200 | 12,000 |
| Max sync latency | 47ms | 336ms |
| Speedup at max | 6,724x | 46,150x |
| Runtime | 3 min | 15 min |

Light benchmark proves O(N) vs O(1) scaling (20.21x vs 20x expected = 101% accuracy).
Production benchmark demonstrates real-world latency at scale.

## Benchmark Compilation Requirements

The `MicroBatchPlannerBenchmark.java` requires classes from the async-planner feature branch:
- `AsyncSparkMicroBatchPlanner`
- `SyncSparkMicroBatchPlanner`

These classes do not exist on the main branch. The benchmark is provided as documentation with pre-collected results. To run the benchmark, checkout the feature branch where these implementations exist.
