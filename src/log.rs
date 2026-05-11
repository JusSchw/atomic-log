use std::collections::VecDeque;
use std::sync::Arc;

use arc_swap::ArcSwap;

use crate::claim::Claimed;
use crate::segment::Segment;
use crate::snapshot::Snapshot;

/// Shared read handle for a segmented rolling log.
///
/// `AtomicLog<T>` is cheaply clonable and can be sent to many reader threads. Readers use
/// [`snapshot`](Self::snapshot) to capture a stable, zero-copy view of the currently
/// retained data.
pub struct AtomicLog<T> {
    pub(crate) shared: Arc<Shared<T>>,
}

/// Single-writer append handle for an [`AtomicLog`].
///
/// The core design assumes exactly one active writer. If a caller wants to share write
/// access across threads, it must add its own external synchronization.
pub struct Writer<T> {
    pub(crate) shared: Arc<Shared<T>>,
}

pub(crate) struct Shared<T> {
    pub(crate) retained_capacity: usize,
    pub(crate) segment_capacity: usize,
    pub(crate) head: ArcSwap<Segment<T>>,
    writer_state: Claimed<WriterState<T>>,
}

struct WriterState<T> {
    head: Arc<Segment<T>>,
    retained: VecDeque<Arc<Segment<T>>>,
    retained_segments: usize,
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
    /// Creates a new log and its corresponding writer.
    ///
    /// `retained_capacity` is the target logical retention size in elements. The current
    /// implementation retains whole segments, so the observed window may exceed this value.
    ///
    /// `segment_capacity` is the number of elements stored in each segment and remains fixed
    /// for the lifetime of the log.
    ///
    /// # Panics
    ///
    /// Panics if either capacity is zero.
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
            writer_state: Claimed::new_claimed(WriterState {
                head,
                retained,
                retained_segments,
            }),
        });

        let writer = Writer {
            shared: Arc::clone(&shared),
        };
        let log = Self { shared };

        (writer, log)
    }

    /// Returns the configured logical retained capacity, in elements.
    #[inline]
    pub fn retained_capacity(&self) -> usize {
        self.shared.retained_capacity
    }

    /// Returns the fixed segment size, in elements.
    #[inline]
    pub fn segment_capacity(&self) -> usize {
        self.shared.segment_capacity
    }

    /// Returns `true` if this log currently has a claimed writer.
    #[inline]
    pub fn is_writer_claimed(&self) -> bool {
        self.shared.writer_state.is_claimed()
    }

    /// Captures a stable snapshot of the currently retained data.
    ///
    /// The snapshot borrows no locks and keeps its backing segments alive through `Arc`
    /// ownership, so readers can iterate over the result without copying values out of the
    /// log.
    #[inline]
    pub fn snapshot(&self) -> Snapshot<T> {
        Snapshot::new(Arc::clone(&self.shared))
    }

    /// Attempts to claim exclusive write access to this log.
    ///
    /// Returns `None` if another [`Writer`] currently exists. Dropping the returned writer
    /// releases the claim without discarding the log's retained segment state.
    pub fn try_claim_writer(&self) -> Option<Writer<T>> {
        self.shared.writer_state.try_claim().then(|| Writer {
            shared: Arc::clone(&self.shared),
        })
    }
}

impl<T> Writer<T> {
    /// Returns a clonable read handle for this writer's log.
    #[inline]
    pub fn log(&self) -> AtomicLog<T> {
        AtomicLog {
            shared: Arc::clone(&self.shared),
        }
    }

    /// Returns the configured logical retained capacity, in elements.
    #[inline]
    pub fn retained_capacity(&self) -> usize {
        self.shared.retained_capacity
    }

    /// Returns the fixed segment size, in elements.
    #[inline]
    pub fn segment_capacity(&self) -> usize {
        self.shared.segment_capacity
    }

    /// Appends one value to the log and publishes it for readers.
    ///
    /// Values are written into the current head segment. If that segment is full, the writer
    /// allocates a new head segment, publishes it, and continues there.
    pub fn append(&mut self, value: T) {
        unsafe {
            self.shared.writer_state.with_claimed_mut(|state| {
                if state.head.published_len() == self.shared.segment_capacity {
                    let next = Segment::new(
                        state.head.sequence + 1,
                        Arc::downgrade(&state.head),
                        self.shared.segment_capacity,
                    );
                    state.retained.push_back(Arc::clone(&next));
                    while state.retained.len() > state.retained_segments {
                        state.retained.pop_front();
                    }
                    state.head = Arc::clone(&next);
                    self.shared.head.store(next);
                }

                state.head.push(value);
            });
        }
    }
}

impl<T> Drop for Writer<T> {
    fn drop(&mut self) {
        self.shared.writer_state.release();
    }
}
