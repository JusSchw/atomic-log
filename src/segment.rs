use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::ops::Range;
use std::ptr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Weak};

pub(crate) struct Segment<T> {
    pub(crate) sequence: u64,
    pub(crate) previous: Weak<Segment<T>>,
    published: AtomicUsize,
    storage: Box<[UnsafeCell<MaybeUninit<T>>]>,
}

impl<T> Segment<T> {
    pub(crate) fn new(sequence: u64, previous: Weak<Segment<T>>, capacity: usize) -> Arc<Self> {
        let mut storage = Vec::with_capacity(capacity);
        storage.resize_with(capacity, || UnsafeCell::new(MaybeUninit::uninit()));

        Arc::new(Self {
            sequence,
            previous,
            published: AtomicUsize::new(0),
            storage: storage.into_boxed_slice(),
        })
    }

    pub(crate) fn published_len(&self) -> usize {
        self.published.load(Ordering::Acquire)
    }

    pub(crate) fn push(&self, value: T) {
        let index = self.published.load(Ordering::Relaxed);
        assert!(index < self.storage.len(), "segment is full");

        unsafe {
            (*self.storage[index].get()).write(value);
        }
        self.published.store(index + 1, Ordering::Release);
    }

    pub(crate) fn slice(&self, range: Range<usize>) -> &[T] {
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

unsafe impl<T: Send + Sync> Send for Segment<T> {}
unsafe impl<T: Send + Sync> Sync for Segment<T> {}
