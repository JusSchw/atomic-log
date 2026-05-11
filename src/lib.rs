//! A segmented, append-only, zero-copy rolling log for in-process fan-out.
//!
//! `atomic-log` provides [`AtomicLog`] and [`Writer`], a low-coordination primitive for
//! publishing values from one producer to many readers. The intended use case is
//! state-oriented streaming inside a process: readers care primarily about a recent,
//! stable view of published data, not guaranteed delivery of every historical update.
//!
//! The log is split into fixed-capacity segments. The writer appends values into the
//! current head segment, publishes them with atomics, and rolls to a new segment when
//! the head fills. Readers take [`Snapshot`]s, which hold `Arc`s to the backing segments
//! and expose zero-copy access through flat iteration or per-segment chunk iteration.
//!
//! # What This Crate Optimizes For
//!
//! - Single-writer, many-reader fan-out
//! - Low-coordination publication and atomics-only observation on the read path
//! - Stable snapshots that do not block the writer
//! - Zero-copy reads of immutable published values
//! - Bounded retention through automatic segment reclamation
//! - Reclaimable write access while the log owns retained history
//!
//! # What It Does Not Provide
//!
//! - Multi-writer coordination
//! - Delivery guarantees for every historical value
//! - Backpressure from readers to the writer
//! - Persistence or durability
//! - Exactly-once or must-not-miss event delivery
//!
//! If every update matters, use a channel, queue, or durable log instead.
//!
//! # Snapshot Semantics
//!
//! A [`Snapshot`] is a stable captured view of the currently retained prefix reachable
//! from the current head at the moment the snapshot is built or refreshed.
//!
//! - Readers only observe fully published values.
//! - Published values are immutable after publication.
//! - Holding a snapshot keeps its backing segments alive.
//! - Refresh replaces the snapshot contents with a newer captured view.
//! - Slow readers may lose continuity across refreshes if older segments have already
//!   been reclaimed.
//! - Dropping a writer does not discard the log's retained segments.
//!
//! The important distinction is that a single snapshot is internally stable, while
//! continuity across time is best-effort.
//!
//! # Retention Model
//!
//! The constructor takes a logical `retained_capacity` and a fixed `segment_capacity`.
//! The current implementation retains whole segments, so the live window is rounded to
//! segment boundaries rather than truncated element-by-element. In practice that means
//! the visible retained history can exceed `retained_capacity` by up to roughly one
//! extra segment of historical data plus the current head segment.
//!
//! # Example
//!
//! ```
//! use atomic_log::AtomicLog;
//!
//! let (mut writer, log) = AtomicLog::new_claimed(8, 4);
//!
//! for value in 0..6 {
//!     writer.append(value);
//! }
//!
//! let mut snapshot = log.snapshot();
//! let initial: Vec<_> = snapshot.iter().copied().collect();
//! assert_eq!(initial, vec![0, 1, 2, 3, 4, 5]);
//!
//! writer.append(6);
//! writer.append(7);
//! snapshot.refresh();
//!
//! let refreshed: Vec<_> = snapshot.iter().copied().collect();
//! assert_eq!(refreshed, vec![0, 1, 2, 3, 4, 5, 6, 7]);
//! ```
//!
//! # Reading Patterns
//!
//! [`Snapshot::iter`] yields a flat `&T` stream across the captured segments.
//! [`Snapshot::chunks`] yields [`SegmentSlice`] values for consumers that care about
//! segment-local slices or segment sequence numbers.
//! [`AtomicLog::try_claim_writer`] recreates a writer after the previous writer has
//! been dropped.
mod claim;
mod log;
mod segment;
mod snapshot;

pub use log::{AtomicLog, Writer};
pub use snapshot::{Chunks, Iter, SegmentSlice, Snapshot};

#[cfg(test)]
mod tests {
    use crate::Snapshot;
    use crate::log::AtomicLog;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::thread;

    #[test]
    fn empty_snapshot_is_empty() {
        let log = AtomicLog::<usize>::new(4, 2);

        assert!(!log.is_writer_claimed());

        let snapshot = log.snapshot();

        assert!(snapshot.is_empty());
        assert_eq!(snapshot.len(), 0);
        assert_eq!(snapshot.iter().count(), 0);
    }

    #[test]
    fn new_starts_without_claimed_writer() {
        let log = AtomicLog::<usize>::new(8, 2);

        assert!(!log.is_writer_claimed());
        let writer = log.try_claim_writer();
        assert!(writer.is_some());
        assert!(log.is_writer_claimed());
    }

    #[test]
    fn snapshot_returns_full_retained_view() {
        let (mut writer, log) = AtomicLog::new_claimed(5, 2);

        for value in 0..8 {
            writer.append(value);
        }

        let snapshot = log.snapshot();
        let values: Vec<_> = snapshot.iter().copied().collect();

        assert_eq!(values, vec![0, 1, 2, 3, 4, 5, 6, 7]);
    }

    #[test]
    fn snapshot_captures_full_retained_view() {
        let (mut writer, log) = AtomicLog::new_claimed(8, 3);

        for value in 0..7 {
            writer.append(value);
        }

        let snapshot = log.snapshot();
        let values: Vec<_> = snapshot.iter().copied().collect();

        assert_eq!(values, vec![0, 1, 2, 3, 4, 5, 6]);
    }

    #[test]
    fn chunk_iteration_exposes_segment_sequences() {
        let (mut writer, log) = AtomicLog::new_claimed(6, 2);

        for value in 0..5 {
            writer.append(value);
        }

        let chunks: Vec<_> = log
            .snapshot()
            .chunks()
            .map(|chunk| (chunk.sequence(), chunk.values().to_vec()))
            .collect();

        assert_eq!(chunks, vec![(0, vec![0, 1]), (1, vec![2, 3]), (2, vec![4])]);
    }

    #[test]
    fn held_snapshot_remains_stable_after_reclamation() {
        let (mut writer, log) = AtomicLog::new_claimed(3, 1);
        for value in 0..3 {
            writer.append(value);
        }
        let snapshot = log.snapshot();

        for value in 3..20 {
            writer.append(value);
        }

        let old_values: Vec<_> = snapshot.iter().copied().collect();
        let fresh_values: Vec<_> = log.snapshot().iter().copied().collect();

        assert_eq!(old_values, vec![0, 1, 2]);
        assert_eq!(fresh_values, vec![16, 17, 18, 19]);
    }

    #[test]
    fn refresh_replaces_snapshot_with_latest_view() {
        let (mut writer, log) = AtomicLog::new_claimed(4, 2);
        for value in 0..4 {
            writer.append(value);
        }
        let mut snapshot = log.snapshot();

        for value in 4..9 {
            writer.append(value);
        }
        snapshot.refresh();

        let values: Vec<_> = snapshot.iter().copied().collect();
        assert_eq!(values, vec![0, 1, 2, 3, 4, 5, 6, 7, 8]);
    }

    #[test]
    fn snapshot_refresh_extends_same_head_without_rebuild() {
        let (mut writer, log) = AtomicLog::new_claimed(4, 8);
        writer.append(0);
        writer.append(1);
        let mut snapshot = log.snapshot();

        writer.append(2);
        writer.append(3);
        snapshot.refresh();

        let values: Vec<_> = snapshot.iter().copied().collect();
        assert_eq!(values, vec![0, 1, 2, 3]);
        assert_eq!(snapshot.chunks().count(), 1);
    }

    #[test]
    fn snapshot_refresh_appends_new_segments_when_continuous() {
        let (mut writer, log) = AtomicLog::new_claimed(5, 2);
        for value in 0..3 {
            writer.append(value);
        }
        let mut snapshot = log.snapshot();

        for value in 3..6 {
            writer.append(value);
        }
        snapshot.refresh();

        let values: Vec<_> = snapshot.iter().copied().collect();
        assert_eq!(values, vec![0, 1, 2, 3, 4, 5]);
        assert_eq!(snapshot.chunks().count(), 3);
    }

    #[test]
    fn writer_drop_preserves_retained_segments_for_refresh() {
        let (mut writer, log) = AtomicLog::new_claimed(8, 2);
        for value in 0..3 {
            writer.append(value);
        }
        let mut snapshot = log.snapshot();

        for value in 3..8 {
            writer.append(value);
        }
        drop(writer);

        snapshot.refresh();

        let values: Vec<_> = snapshot.iter().copied().collect();
        assert_eq!(values, vec![0, 1, 2, 3, 4, 5, 6, 7]);
    }

    #[test]
    fn writer_can_be_reclaimed_after_drop() {
        let (mut writer, log) = AtomicLog::new_claimed(8, 2);
        writer.append(1);
        assert!(log.is_writer_claimed());
        drop(writer);
        assert!(!log.is_writer_claimed());

        let mut writer = log
            .try_claim_writer()
            .expect("writer claim should be released");
        assert!(log.is_writer_claimed());
        writer.append(2);

        let values: Vec<_> = log.snapshot().iter().copied().collect();
        assert_eq!(values, vec![1, 2]);
    }

    #[test]
    fn writer_cannot_be_reclaimed_while_existing_writer_lives() {
        let (_writer, log) = AtomicLog::<usize>::new_claimed(8, 2);

        assert!(log.is_writer_claimed());
        assert!(log.try_claim_writer().is_none());
    }

    #[test]
    fn drops_only_initialized_values() {
        static DROPS: AtomicUsize = AtomicUsize::new(0);

        struct CountDrop;
        impl Drop for CountDrop {
            fn drop(&mut self) {
                DROPS.fetch_add(1, Ordering::Relaxed);
            }
        }

        {
            let (mut writer, _log) = AtomicLog::new_claimed(10, 8);
            for _ in 0..3 {
                writer.append(CountDrop);
            }
        }

        assert_eq!(DROPS.load(Ordering::Relaxed), 3);
    }

    #[test]
    fn many_readers_can_snapshot_while_writer_appends() {
        let (mut writer, log) = AtomicLog::new_claimed(64, 8);
        let log = Arc::new(log);
        let stop = Arc::new(AtomicUsize::new(0));
        let mut readers = Vec::new();

        for _ in 0..4 {
            let log = Arc::clone(&log);
            let stop = Arc::clone(&stop);
            readers.push(thread::spawn(move || {
                while stop.load(Ordering::Acquire) == 0 {
                    let values: Vec<_> = log.snapshot().iter().copied().collect();
                    assert!(values.windows(2).all(|pair| pair[0] + 1 == pair[1]));
                }
            }));
        }

        for value in 0..1000 {
            writer.append(value);
        }
        stop.store(1, Ordering::Release);

        for reader in readers {
            reader.join().unwrap();
        }
    }

    #[test]
    fn writer_can_be_shared_through_a_lock_when_requested() {
        let (writer, log) = AtomicLog::new_claimed(8, 2);
        let writer = std::sync::Arc::new(std::sync::Mutex::new(writer));

        let first = {
            let writer = std::sync::Arc::clone(&writer);
            thread::spawn(move || writer.lock().unwrap().append(1))
        };
        let second = {
            let writer = std::sync::Arc::clone(&writer);
            thread::spawn(move || writer.lock().unwrap().append(2))
        };

        first.join().unwrap();
        second.join().unwrap();

        let values: Vec<_> = log.snapshot().iter().copied().collect();
        assert_eq!(values.len(), 2);
        assert!(values.contains(&1));
        assert!(values.contains(&2));
    }

    #[test]
    fn log_snapshot_and_writer_conversions_round_trip() {
        let (mut writer, log) = AtomicLog::new_claimed(8, 2);
        for value in 0..5 {
            writer.append(value);
        }

        let log_from_writer = writer.log();
        let snapshot = Snapshot::from(log_from_writer.clone());
        let log_from_snapshot = AtomicLog::from(snapshot);

        let values: Vec<_> = log_from_snapshot.snapshot().iter().copied().collect();
        assert_eq!(values, vec![0, 1, 2, 3, 4]);

        let snapshot = log.snapshot();
        let cloned_log = snapshot.log();
        let values: Vec<_> = cloned_log.snapshot().iter().copied().collect();
        assert_eq!(values, vec![0, 1, 2, 3, 4]);
    }
}
