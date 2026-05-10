mod log;
mod segment;
mod snapshot;

pub use log::{AtomicLog, Writer};
pub use snapshot::{Chunks, Iter, SegmentSlice, Snapshot};

#[cfg(test)]
mod tests {
    use crate::log::AtomicLog;
    use crate::Snapshot;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::thread;

    #[test]
    fn empty_snapshot_is_empty() {
        let (_writer, log) = AtomicLog::<usize>::new(4, 2);

        let snapshot = log.snapshot();

        assert!(snapshot.is_empty());
        assert_eq!(snapshot.len(), 0);
        assert_eq!(snapshot.iter().count(), 0);
    }

    #[test]
    fn snapshot_returns_full_retained_view() {
        let (mut writer, log) = AtomicLog::new(5, 2);

        for value in 0..8 {
            writer.append(value);
        }

        let snapshot = log.snapshot();
        let values: Vec<_> = snapshot.iter().copied().collect();

        assert_eq!(values, vec![0, 1, 2, 3, 4, 5, 6, 7]);
    }

    #[test]
    fn snapshot_captures_full_retained_view() {
        let (mut writer, log) = AtomicLog::new(8, 3);

        for value in 0..7 {
            writer.append(value);
        }

        let snapshot = log.snapshot();
        let values: Vec<_> = snapshot.iter().copied().collect();

        assert_eq!(values, vec![0, 1, 2, 3, 4, 5, 6]);
    }

    #[test]
    fn chunk_iteration_exposes_segment_sequences() {
        let (mut writer, log) = AtomicLog::new(6, 2);

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
        let (mut writer, log) = AtomicLog::new(3, 1);
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
        let (mut writer, log) = AtomicLog::new(4, 2);
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
        let (mut writer, log) = AtomicLog::new(4, 8);
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
        let (mut writer, log) = AtomicLog::new(5, 2);
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
    fn drops_only_initialized_values() {
        static DROPS: AtomicUsize = AtomicUsize::new(0);

        struct CountDrop;
        impl Drop for CountDrop {
            fn drop(&mut self) {
                DROPS.fetch_add(1, Ordering::Relaxed);
            }
        }

        {
            let (mut writer, _log) = AtomicLog::new(10, 8);
            for _ in 0..3 {
                writer.append(CountDrop);
            }
        }

        assert_eq!(DROPS.load(Ordering::Relaxed), 3);
    }

    #[test]
    fn many_readers_can_snapshot_while_writer_appends() {
        let (mut writer, log) = AtomicLog::new(64, 8);
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
        let (writer, log) = AtomicLog::new(8, 2);
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
        let (mut writer, log) = AtomicLog::new(8, 2);
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
