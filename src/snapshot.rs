use std::cmp::min;
use std::collections::VecDeque;
use std::ops::Range;
use std::sync::Arc;

use crate::log::Shared;
use crate::segment::Segment;

pub struct Snapshot<T> {
    pub(crate) shared: Arc<Shared<T>>,
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

pub struct Iter<'a, T> {
    chunks: std::collections::vec_deque::Iter<'a, SnapshotChunk<T>>,
    current: Option<std::slice::Iter<'a, T>>,
}

pub struct Chunks<'a, T> {
    chunks: std::collections::vec_deque::Iter<'a, SnapshotChunk<T>>,
}

impl<T> Snapshot<T> {
    pub(crate) fn new(shared: Arc<Shared<T>>, target_len: usize) -> Self {
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
