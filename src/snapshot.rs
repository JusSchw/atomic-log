use std::collections::VecDeque;
use std::ops::Range;
use std::sync::Arc;

use crate::log::Shared;
use crate::segment::Segment;

pub struct Snapshot<T> {
    pub(crate) shared: Arc<Shared<T>>,
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

pub struct Iter<'a, T> {
    chunks: std::collections::vec_deque::Iter<'a, SnapshotChunk<T>>,
    current: Option<std::slice::Iter<'a, T>>,
}

pub struct Chunks<'a, T> {
    chunks: std::collections::vec_deque::Iter<'a, SnapshotChunk<T>>,
}

impl<T> Snapshot<T> {
    pub(crate) fn new(shared: Arc<Shared<T>>) -> Self {
        let mut snapshot = Self {
            shared,
            len: 0,
            chunks: VecDeque::new(),
        };
        snapshot.rebuild();
        snapshot
    }

    fn rebuild(&mut self) {
        let head = self.shared.head.load_full();
        let mut reversed = Vec::new();
        let mut cursor = Some(head);

        while let Some(segment) = cursor {
            let published = segment.published_len();
            if published > 0 {
                reversed.push(SnapshotChunk {
                    segment: Arc::clone(&segment),
                    range: 0..published,
                });
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
                return true;
            }

            cursor = segment.previous.upgrade();
            new_segments.push(segment);
        }

        false
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

    pub fn log(&self) -> crate::log::AtomicLog<T> {
        crate::log::AtomicLog {
            shared: Arc::clone(&self.shared),
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

impl<T> std::fmt::Debug for Snapshot<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Snapshot")
            .field("len", &self.len)
            .field("chunks", &self.chunks.len())
            .finish()
    }
}

impl<T> From<crate::log::AtomicLog<T>> for Snapshot<T> {
    fn from(log: crate::log::AtomicLog<T>) -> Self {
        log.snapshot()
    }
}

impl<T> From<Snapshot<T>> for crate::log::AtomicLog<T> {
    fn from(snapshot: Snapshot<T>) -> Self {
        snapshot.log()
    }
}

impl<'a, T> IntoIterator for &'a Snapshot<T> {
    type Item = &'a T;
    type IntoIter = Iter<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}
