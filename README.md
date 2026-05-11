# atomic-log

`atomic-log` is a Rust library for sharing a recent rolling window of values from one
writer to many readers with very little coordination.

It is built for in-process streaming workloads where readers care more about a stable,
recent view than about guaranteed delivery of every historical update. The log is
append-only, segmented, zero-copy on the read side, and uses atomics for publication.

## At a glance

- Single writer, many readers
- Low-coordination publication and atomics-only observation on the read path
- No reader registration
- Generic over `T`
- Fixed-size segmented storage
- Zero-copy snapshots backed by stable segment allocations
- Automatic reclamation of old segments
- Reclaimable write access after a writer is dropped
- Flat iteration and per-segment chunk iteration

## When to use it

`atomic-log` is a good fit when:

- one thread publishes and many threads observe
- readers mainly care about the latest retained state
- stale history may be dropped
- readers should not block the writer
- zero-copy reads are valuable
- bounded retention matters more than full replay

Typical examples:

- market data
- telemetry
- service health and status propagation
- real-time monitoring
- replicated in-memory state views

## When not to use it

Do not use `atomic-log` when:

- every message must be delivered
- readers must never miss an update
- commands or events must be processed exactly once
- multiple writers need to append concurrently without external synchronization
- data must survive restarts

For those cases, a channel, queue, or durable log is usually the right tool.

## Model

The writer appends values into a fixed-capacity head segment. When that segment fills, a
new head segment is allocated and published. Readers build `Snapshot<T>` values from the
current head and iterate over immutable published prefixes of the retained segments.
The log owns the retained segment chain; a `Writer<T>` is an exclusive append capability
that can be dropped and later reacquired from the log.

Snapshots are stable:

- readers only observe fully published values
- published values are never mutated again
- holding a snapshot keeps its backing segments alive
- readers access `&T` directly from segment storage without copying
- dropping a writer does not discard retained history

Refreshing a snapshot replaces it with a newer captured view. If a reader falls behind
beyond the retained history, continuity across refreshes may be lost.

## Retention semantics

The constructor takes:

- `retained_capacity`: target logical retention in elements
- `segment_capacity`: fixed elements per segment

The current implementation retains whole segments, not exact element counts, so the
visible live window is rounded to segment boundaries and may exceed `retained_capacity`.
That tradeoff keeps publication and reclamation simple while preserving stable zero-copy
snapshots.

## Example

```rust
use atomic_log::AtomicLog;

let (mut writer, log) = AtomicLog::new_claimed(8, 4);

for value in 0..6 {
    writer.append(value);
}

let mut snapshot = log.snapshot();
assert_eq!(
    snapshot.iter().copied().collect::<Vec<_>>(),
    vec![0, 1, 2, 3, 4, 5]
);

writer.append(6);
writer.append(7);
snapshot.refresh();

assert_eq!(
    snapshot.iter().copied().collect::<Vec<_>>(),
    vec![0, 1, 2, 3, 4, 5, 6, 7]
);

let chunks: Vec<_> = snapshot
    .chunks()
    .map(|chunk| (chunk.sequence(), chunk.values().len()))
    .collect();

assert_eq!(chunks, vec![(0, 4), (1, 4)]);
```

## API summary

- `AtomicLog::new(retained_capacity, segment_capacity)` creates an unclaimed log
- `AtomicLog::new_claimed(retained_capacity, segment_capacity)` creates a writer and read handle
- `AtomicLog::try_claim_writer()` recreates a writer if no writer currently exists
- `Writer::append(value)` publishes one value
- `AtomicLog::snapshot()` captures a stable read view
- `Snapshot::refresh()` updates an existing snapshot in place
- `Snapshot::iter()` yields `&T`
- `Snapshot::chunks()` yields per-segment slices plus sequence numbers

## License

Licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or <http://www.apache.org/licenses/LICENSE-2.0>)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or <http://opensource.org/licenses/MIT>)

at your option.

## Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted for
inclusion in the work by you, as defined in the Apache-2.0 license, shall be dual
licensed as above, without any additional terms or conditions.
