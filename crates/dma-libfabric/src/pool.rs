//! A small fixed thread pool, `f-pool-nn`, running per-transfer completion work off the fabric
//! worker: the CRC and reply of checksummed transfers, which would otherwise stall the worker's
//! post-and-reap loop while it hashes the payload.
//!
//! Each worker owns an mpsc channel that [`Pool::submit`] round-robins jobs across, taking the next
//! job, running it, then blocking for the next. A panicking job is caught so it kills only itself:
//! losing the worker would silently strand every later job on its channel, hanging the blocked
//! clients they would reply to.

use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{Sender, channel};
use std::thread::JoinHandle;

/// A unit of completion work handed to a pool worker.
pub type Job = Box<dyn FnOnce() + Send + 'static>;

/// A fixed set of worker threads, each draining its own channel.
#[derive(Debug)]
pub struct Pool {
    senders: Vec<Sender<Job>>,
    next: AtomicUsize,
    workers: Vec<JoinHandle<()>>,
}

impl Pool {
    /// Spawn at least one worker named `f-pool-nn`, each consuming its own channel.
    pub fn new(threads: usize) -> std::io::Result<Self> {
        let threads = threads.max(1);
        let mut senders = Vec::with_capacity(threads);
        let mut workers = Vec::with_capacity(threads);
        for index in 0..threads {
            let (sender, receiver) = channel::<Job>();
            let worker = std::thread::Builder::new()
                .name(format!("f-pool-{index:02}"))
                .spawn(move || {
                    // Ends when the channel closes.
                    while let Ok(job) = receiver.recv() {
                        // Isolate a panicking job so it doesn't take the worker and its queue.
                        let _ = catch_unwind(AssertUnwindSafe(job));
                    }
                })?;
            senders.push(sender);
            workers.push(worker);
        }
        Ok(Self {
            senders,
            next: AtomicUsize::new(0),
            workers,
        })
    }

    /// Submit a job, round-robined across the workers. Dropped silently if the pool is shutting down.
    pub fn submit(&self, job: Job) {
        let index = self.next.fetch_add(1, Ordering::Relaxed) % self.senders.len();
        let _ = self.senders[index].send(job);
    }
}

impl Drop for Pool {
    fn drop(&mut self) {
        // Closing every channel lets the workers drain what's queued and exit.
        self.senders.clear();
        for worker in self.workers.drain(..) {
            let _ = worker.join();
        }
    }
}
