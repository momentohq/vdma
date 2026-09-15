//! A one-shot answer from the fabric worker to a waiting caller.

use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};

enum State<T> {
    Pending,
    Answered(T),
    /// The worker dropped its end without answering: it exited mid-request.
    Abandoned,
}

struct Slot<T> {
    state: Mutex<State<T>>,
    ready: Condvar,
}

impl<T> Slot<T> {
    fn lock(&self) -> MutexGuard<'_, State<T>> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn settle(&self, state: State<T>) {
        *self.lock() = state;
        self.ready.notify_one();
    }
}

/// The worker's end. Dropped unanswered, it wakes the caller with `None`, so a worker that dies
/// mid-request never leaves its caller blocked.
pub(crate) struct Answer<T>(Arc<Slot<T>>);

/// The caller's end.
pub(crate) struct Waiting<T>(Arc<Slot<T>>);

pub(crate) fn reply<T>() -> (Answer<T>, Waiting<T>) {
    let slot = Arc::new(Slot {
        state: Mutex::new(State::Pending),
        ready: Condvar::new(),
    });
    (Answer(Arc::clone(&slot)), Waiting(slot))
}

impl<T> Answer<T> {
    pub(crate) fn send(self, value: T) {
        self.0.settle(State::Answered(value));
    }
}

impl<T> Drop for Answer<T> {
    fn drop(&mut self) {
        let mut state = self.0.lock();
        if matches!(*state, State::Pending) {
            *state = State::Abandoned;
            self.0.ready.notify_one();
        }
    }
}

impl<T> Waiting<T> {
    /// Block until the worker answers, or `None` if it went away first.
    pub(crate) fn wait(self) -> Option<T> {
        let mut state = self.0.lock();
        loop {
            match std::mem::replace(&mut *state, State::Pending) {
                State::Answered(value) => return Some(value),
                State::Abandoned => return None,
                State::Pending => {
                    state = self
                        .0
                        .ready
                        .wait(state)
                        .unwrap_or_else(PoisonError::into_inner);
                }
            }
        }
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::reply;

    #[test]
    fn an_answer_arrives() {
        let (answer, waiting) = reply();
        std::thread::spawn(move || answer.send(7));
        std::thread::sleep(std::time::Duration::from_millis(1));
        assert_eq!(Some(7), waiting.wait());
    }

    #[test]
    fn an_answer_arrives_out_of_order() {
        let (answer, waiting) = reply();
        let handle = std::thread::spawn(move || waiting.wait());
        std::thread::sleep(std::time::Duration::from_millis(1));
        std::thread::spawn(move || answer.send(7));
        assert_eq!(Some(7), handle.join().expect("join worked"));
    }

    /// The failure this exists to make impossible: a worker that exits before answering must not
    /// leave the caller blocked forever.
    #[test]
    fn a_dropped_answer_wakes_the_caller() {
        let (answer, waiting) = reply::<u8>();
        std::thread::spawn(move || drop(answer));
        assert_eq!(None, waiting.wait());
    }
}
