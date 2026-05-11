use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicBool, Ordering};

pub(crate) struct Claimed<T> {
    claimed: AtomicBool,
    value: UnsafeCell<T>,
}

impl<T> Claimed<T> {
    pub(crate) fn new_claimed(value: T) -> Self {
        Self {
            claimed: AtomicBool::new(true),
            value: UnsafeCell::new(value),
        }
    }

    pub(crate) fn try_claim(&self) -> bool {
        self.claimed
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    pub(crate) fn is_claimed(&self) -> bool {
        self.claimed.load(Ordering::Acquire)
    }

    pub(crate) fn release(&self) {
        self.claimed.store(false, Ordering::Release);
    }

    pub(crate) unsafe fn with_claimed_mut<R>(&self, f: impl FnOnce(&mut T) -> R) -> R {
        // SAFETY: callers may only use this while holding the exclusive claim tracked by
        // `claimed`, which prevents any other mutable access to `value`.
        f(unsafe { &mut *self.value.get() })
    }
}

unsafe impl<T: Send> Sync for Claimed<T> {}
