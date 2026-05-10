use std::collections::VecDeque;
use std::sync::Arc;

use arc_swap::ArcSwap;

use crate::segment::Segment;
use crate::snapshot::Snapshot;

pub struct AtomicLog<T> {
    pub(crate) shared: Arc<Shared<T>>,
}

pub struct Writer<T> {
    pub(crate) shared: Arc<Shared<T>>,
    pub(crate) state: WriterState<T>,
}

pub(crate) struct Shared<T> {
    pub(crate) retained_capacity: usize,
    pub(crate) segment_capacity: usize,
    pub(crate) head: ArcSwap<Segment<T>>,
}

pub(crate) struct WriterState<T> {
    pub(crate) head: Arc<Segment<T>>,
    pub(crate) retained: VecDeque<Arc<Segment<T>>>,
    pub(crate) retained_segments: usize,
}

impl<T> Clone for AtomicLog<T> {
    fn clone(&self) -> Self {
        Self {
            shared: Arc::clone(&self.shared),
        }
    }
}

impl<T> From<Writer<T>> for AtomicLog<T> {
    fn from(writer: Writer<T>) -> Self {
        writer.log()
    }
}

impl<T> AtomicLog<T> {
    pub fn new(retained_capacity: usize, segment_capacity: usize) -> (Writer<T>, Self) {
        assert!(retained_capacity > 0, "retained capacity must be non-zero");
        assert!(segment_capacity > 0, "segment capacity must be non-zero");

        let retained_segments = retained_capacity.div_ceil(segment_capacity) + 1;
        let head = Segment::new(0, std::sync::Weak::new(), segment_capacity);
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

    pub fn snapshot(&self) -> Snapshot<T> {
        Snapshot::new(Arc::clone(&self.shared))
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
