use std::cell::UnsafeCell;
use std::cmp::min;
use std::collections::VecDeque;
use std::mem::MaybeUninit;
use std::ops::Range;
use std::ptr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Weak};

use arc_swap::ArcSwap;

#[derive(Clone)]
pub struct AtomicLog<T> {
    shared: Arc<Shared<T>>,
}

pub struct Writer<T> {
    shared: Arc<Shared<T>>,
    state: WriterState<T>,
}

struct Shared<T> {
    retained_capacity: usize,
    segment_capacity: usize,
    head: ArcSwap<Segment<T>>,
}

pub struct Snapshot<T> {
    shared: Arc<Shared<T>>,
    target_len: usize,
    len: usize,
    chunks: VecDeque<SnapshotChunk<T>>,
}

pub struct SegmentSlice<'a, T> {
    sequence: u64,
    values: &'a [T],
}

struct SnapshotChunk<T> {
    segment: Arc<Segment<T>>,
    range: Range<usize>,
}

struct WriterState<T> {
    head: Arc<Segment<T>>,
    retained: VecDeque<Arc<Segment<T>>>,
    retained_segments: usize,
}

struct Segment<T> {
    sequence: u64,
    previous: Weak<Segment<T>>,
    published: AtomicUsize,
    storage: Box<[UnsafeCell<MaybeUninit<T>>]>,
}

pub struct Iter<'a, T> {
    chunks: std::collections::vec_deque::Iter<'a, SnapshotChunk<T>>,
    current: Option<std::slice::Iter<'a, T>>,
}

pub struct Chunks<'a, T> {
    chunks: std::collections::vec_deque::Iter<'a, SnapshotChunk<T>>,
}

impl<T> AtomicLog<T> {
    pub fn new(retained_capacity: usize, segment_capacity: usize) -> (Writer<T>, Self) {
        assert!(retained_capacity > 0, "retained capacity must be non-zero");
        assert!(segment_capacity > 0, "segment capacity must be non-zero");

        let retained_segments = retained_capacity.div_ceil(segment_capacity) + 1;
        let head = Segment::new(0, Weak::new(), segment_capacity);
        let mut retained = VecDeque::with_capacity(retained_segments);
        retained.push_back(Arc::clone(&head));

        let shared = Arc::new(Shared {
            retained_capacity,
            segment_capacity,
            head: ArcSwap::from(Arc::clone(&head)),
        });

        let writer = Writer {
            shared: Arc::clone(&shared),
            state: WriterState {
                head,
                retained,
                retained_segments,
            },
        };
        let log = Self { shared };

        (writer, log)
    }

    pub fn retained_capacity(&self) -> usize {
        self.shared.retained_capacity
    }

    pub fn segment_capacity(&self) -> usize {
        self.shared.segment_capacity
    }

    pub fn snapshot(&self, max_len: usize) -> Snapshot<T> {
        Snapshot::new(
            Arc::clone(&self.shared),
            min(max_len, self.shared.retained_capacity),
        )
    }

    pub fn refresh(&self, snapshot: &mut Snapshot<T>) {
        snapshot.refresh();
    }
}

impl<T> Writer<T> {
    pub fn log(&self) -> AtomicLog<T> {
        AtomicLog {
            shared: Arc::clone(&self.shared),
        }
    }

    pub fn retained_capacity(&self) -> usize {
        self.shared.retained_capacity
    }

    pub fn segment_capacity(&self) -> usize {
        self.shared.segment_capacity
    }

    pub fn append(&mut self, value: T) {
        if self.state.head.published_len() == self.shared.segment_capacity {
            let next = Segment::new(
                self.state.head.sequence + 1,
                Arc::downgrade(&self.state.head),
                self.shared.segment_capacity,
            );
            self.state.head = Arc::clone(&next);
            self.state.retained.push_back(Arc::clone(&next));
            while self.state.retained.len() > self.state.retained_segments {
                self.state.retained.pop_front();
            }
            self.shared.head.store(next);
        }

        self.state.head.push(value);
    }
}

impl<T> Snapshot<T> {
    fn new(shared: Arc<Shared<T>>, target_len: usize) -> Self {
        let mut snapshot = Self {
            shared,
            target_len,
            len: 0,
            chunks: VecDeque::new(),
        };
        snapshot.rebuild();
        snapshot
    }

    fn rebuild(&mut self) {
        let head = self.shared.head.load_full();
        let mut remaining = self.target_len;
        let mut reversed = Vec::new();
        let mut cursor = Some(head);

        while remaining > 0 {
            let Some(segment) = cursor else {
                break;
            };
            let published = segment.published_len();
            if published > 0 {
                let take = min(published, remaining);
                reversed.push(SnapshotChunk {
                    segment: Arc::clone(&segment),
                    range: published - take..published,
                });
                remaining -= take;
            }
            cursor = segment.previous.upgrade();
        }

        reversed.reverse();
        self.chunks.clear();
        self.chunks.extend(reversed);
        self.len = self
            .chunks
            .iter()
            .map(|chunk| chunk.range.end - chunk.range.start)
            .sum();
    }

    pub fn refresh(&mut self) {
        let head = self.shared.head.load_full();
        if self.refresh_same_head(&head) {
            return;
        }
        if self.refresh_incremental(&head) {
            return;
        }

        self.rebuild();
    }

    fn refresh_same_head(&mut self, head: &Arc<Segment<T>>) -> bool {
        let Some(last) = self.chunks.back_mut() else {
            return head.published_len() == 0;
        };
        if !Arc::ptr_eq(&last.segment, head) {
            return false;
        }

        let published = head.published_len();
        if published <= last.range.end {
            return true;
        }

        let added = published - last.range.end;
        last.range.end = published;
        self.len += added;
        self.trim_front_to_target();
        true
    }

    fn refresh_incremental(&mut self, head: &Arc<Segment<T>>) -> bool {
        let Some(last) = self.chunks.back_mut() else {
            return false;
        };

        let mut cursor = Some(Arc::clone(head));
        let mut new_segments: Vec<Arc<Segment<T>>> = Vec::new();
        while let Some(segment) = cursor {
            if Arc::ptr_eq(&segment, &last.segment) {
                let published = segment.published_len();
                if published > last.range.end {
                    let added = published - last.range.end;
                    last.range.end = published;
                    self.len += added;
                }

                for segment in new_segments.into_iter().rev() {
                    let published = segment.published_len();
                    if published > 0 {
                        self.len += published;
                        self.chunks.push_back(SnapshotChunk {
                            segment,
                            range: 0..published,
                        });
                    }
                }
                self.trim_front_to_target();
                return true;
            }

            cursor = segment.previous.upgrade();
            new_segments.push(segment);
        }

        false
    }

    fn trim_front_to_target(&mut self) {
        while self.len > self.target_len {
            let excess = self.len - self.target_len;
            let Some(front) = self.chunks.front_mut() else {
                self.len = 0;
                break;
            };

            let front_len = front.range.end - front.range.start;
            if excess < front_len {
                front.range.start += excess;
                self.len -= excess;
                break;
            }

            self.len -= front_len;
            self.chunks.pop_front();
        }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn iter(&self) -> Iter<'_, T> {
        Iter {
            chunks: self.chunks.iter(),
            current: None,
        }
    }

    pub fn chunks(&self) -> Chunks<'_, T> {
        Chunks {
            chunks: self.chunks.iter(),
        }
    }
}

impl<'a, T> SegmentSlice<'a, T> {
    pub fn sequence(&self) -> u64 {
        self.sequence
    }

    pub fn values(&self) -> &'a [T] {
        self.values
    }
}

impl<'a, T> Iterator for Iter<'a, T> {
    type Item = &'a T;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(current) = &mut self.current
                && let Some(value) = current.next()
            {
                return Some(value);
            }

            let chunk = self.chunks.next()?;
            self.current = Some(chunk.as_slice().iter());
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let current = self.current.as_ref().map_or(0, ExactSizeIterator::len);
        let rest: usize = self
            .chunks
            .clone()
            .map(|chunk| chunk.range.end - chunk.range.start)
            .sum();
        let total = current + rest;
        (total, Some(total))
    }
}

impl<T> ExactSizeIterator for Iter<'_, T> {}

impl<'a, T> Iterator for Chunks<'a, T> {
    type Item = SegmentSlice<'a, T>;

    fn next(&mut self) -> Option<Self::Item> {
        let chunk = self.chunks.next()?;
        Some(SegmentSlice {
            sequence: chunk.segment.sequence,
            values: chunk.as_slice(),
        })
    }
}

impl<T> SnapshotChunk<T> {
    fn as_slice(&self) -> &[T] {
        self.segment.slice(self.range.clone())
    }
}

impl<T> Segment<T> {
    fn new(sequence: u64, previous: Weak<Segment<T>>, capacity: usize) -> Arc<Self> {
        let mut storage = Vec::with_capacity(capacity);
        storage.resize_with(capacity, || UnsafeCell::new(MaybeUninit::uninit()));

        Arc::new(Self {
            sequence,
            previous,
            published: AtomicUsize::new(0),
            storage: storage.into_boxed_slice(),
        })
    }

    fn published_len(&self) -> usize {
        self.published.load(Ordering::Acquire)
    }

    fn push(&self, value: T) {
        let index = self.published.load(Ordering::Relaxed);
        assert!(index < self.storage.len(), "segment is full");

        unsafe {
            (*self.storage[index].get()).write(value);
        }
        self.published.store(index + 1, Ordering::Release);
    }

    fn slice(&self, range: Range<usize>) -> &[T] {
        debug_assert!(range.start <= range.end);
        debug_assert!(range.end <= self.published_len());

        unsafe {
            std::slice::from_raw_parts(
                self.storage[range.start].get().cast::<T>(),
                range.end - range.start,
            )
        }
    }
}

impl<T> Drop for Segment<T> {
    fn drop(&mut self) {
        let initialized = self.published.load(Ordering::Acquire);
        for slot in &mut self.storage[..initialized] {
            unsafe {
                ptr::drop_in_place((*slot.get()).as_mut_ptr());
            }
        }
    }
}

unsafe impl<T: Send + Sync> Send for Shared<T> {}
unsafe impl<T: Send + Sync> Sync for Shared<T> {}
unsafe impl<T: Send + Sync> Send for Segment<T> {}
unsafe impl<T: Send + Sync> Sync for Segment<T> {}

impl<T> std::fmt::Debug for Snapshot<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Snapshot")
            .field("target_len", &self.target_len)
            .field("len", &self.len)
            .field("chunks", &self.chunks.len())
            .finish()
    }
}

impl<'a, T> IntoIterator for &'a Snapshot<T> {
    type Item = &'a T;
    type IntoIter = Iter<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::thread;

    #[test]
    fn empty_snapshot_is_empty() {
        let (_writer, log) = AtomicLog::<usize>::new(4, 2);

        let snapshot = log.snapshot(10);

        assert!(snapshot.is_empty());
        assert_eq!(snapshot.len(), 0);
        assert_eq!(snapshot.iter().count(), 0);
    }

    #[test]
    fn snapshot_returns_latest_contiguous_suffix() {
        let (mut writer, log) = AtomicLog::new(5, 2);

        for value in 0..8 {
            writer.append(value);
        }

        let snapshot = log.snapshot(10);
        let values: Vec<_> = snapshot.iter().copied().collect();

        assert_eq!(values, vec![3, 4, 5, 6, 7]);
    }

    #[test]
    fn snapshot_can_request_less_than_retained_capacity() {
        let (mut writer, log) = AtomicLog::new(8, 3);

        for value in 0..7 {
            writer.append(value);
        }

        let snapshot = log.snapshot(4);
        let values: Vec<_> = snapshot.iter().copied().collect();

        assert_eq!(values, vec![3, 4, 5, 6]);
    }

    #[test]
    fn chunk_iteration_exposes_segment_sequences() {
        let (mut writer, log) = AtomicLog::new(6, 2);

        for value in 0..5 {
            writer.append(value);
        }

        let chunks: Vec<_> = log
            .snapshot(6)
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
        let snapshot = log.snapshot(3);

        for value in 3..20 {
            writer.append(value);
        }

        let old_values: Vec<_> = snapshot.iter().copied().collect();
        let fresh_values: Vec<_> = log.snapshot(3).iter().copied().collect();

        assert_eq!(old_values, vec![0, 1, 2]);
        assert_eq!(fresh_values, vec![17, 18, 19]);
    }

    #[test]
    fn refresh_replaces_snapshot_with_latest_view() {
        let (mut writer, log) = AtomicLog::new(4, 2);
        for value in 0..4 {
            writer.append(value);
        }
        let mut snapshot = log.snapshot(3);

        for value in 4..9 {
            writer.append(value);
        }
        log.refresh(&mut snapshot);

        let values: Vec<_> = snapshot.iter().copied().collect();
        assert_eq!(values, vec![6, 7, 8]);
    }

    #[test]
    fn snapshot_refresh_extends_same_head_without_rebuild() {
        let (mut writer, log) = AtomicLog::new(4, 8);
        writer.append(0);
        writer.append(1);
        let mut snapshot = log.snapshot(3);

        writer.append(2);
        writer.append(3);
        snapshot.refresh();

        let values: Vec<_> = snapshot.iter().copied().collect();
        assert_eq!(values, vec![1, 2, 3]);
        assert_eq!(snapshot.chunks().count(), 1);
    }

    #[test]
    fn snapshot_refresh_appends_new_segments_when_continuous() {
        let (mut writer, log) = AtomicLog::new(5, 2);
        for value in 0..3 {
            writer.append(value);
        }
        let mut snapshot = log.snapshot(5);

        for value in 3..6 {
            writer.append(value);
        }
        snapshot.refresh();

        let values: Vec<_> = snapshot.iter().copied().collect();
        assert_eq!(values, vec![1, 2, 3, 4, 5]);
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
                    let values: Vec<_> = log.snapshot(32).iter().copied().collect();
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

        let values: Vec<_> = log.snapshot(8).iter().copied().collect();
        assert_eq!(values.len(), 2);
        assert!(values.contains(&1));
        assert!(values.contains(&2));
    }
}
