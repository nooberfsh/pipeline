extern crate pipeline;

use std::sync::{mpsc, Arc};
use std::sync::atomic::{AtomicBool, ATOMIC_BOOL_INIT};
use std::sync::atomic::Ordering::SeqCst;

use pipeline::{Component, ExctCallBack, Executor, PipelineBuilder, Task};

#[derive(Debug)]
struct SimpleTask {
    id: usize,
    is_finished: AtomicBool,
    is_fetched: AtomicBool,
    is_computed: AtomicBool,
    is_stored: AtomicBool,
}

impl SimpleTask {
    fn new(id: usize) -> Self {
        SimpleTask {
            id: id,
            is_finished: ATOMIC_BOOL_INIT,
            is_fetched: ATOMIC_BOOL_INIT,
            is_computed: ATOMIC_BOOL_INIT,
            is_stored: ATOMIC_BOOL_INIT,
        }
    }

    fn set_fetch(&self) {
        self.is_fetched.store(true, SeqCst);
    }

    fn set_compute(&self) {
        self.is_computed.store(true, SeqCst);
    }

    fn set_store(&self) {
        self.is_stored.store(true, SeqCst);
    }

    fn set_finish(&self) {
        self.is_finished.store(true, SeqCst);
    }

    fn is_fetched(&self) -> bool {
        self.is_fetched.load(SeqCst)
    }

    fn is_computed(&self) -> bool {
        self.is_computed.load(SeqCst)
    }

    fn is_stored(&self) -> bool {
        self.is_stored.load(SeqCst)
    }
}

impl Task for SimpleTask {
    type Id = usize;
    fn get_id(&self) -> usize {
        self.id
    }
    fn is_finished(&self) -> bool {
        self.is_finished.load(SeqCst)
    }
}

macro_rules! executor {
    ($exct: ident, $f: expr) => (
        struct $exct {
            cb: Option<ExctCallBack<SimpleTask>>,
        }

        impl $exct {
            fn new() -> Self {
                $exct {
                    cb: None,
                }
            }

            fn cb(&self, task: Arc<SimpleTask>) {
                let cb = self.cb.as_ref().unwrap();
                cb(task)
            }
        }

        impl Executor<SimpleTask> for $exct {
            fn register_cb(&mut self, cb: ExctCallBack<SimpleTask>) {
                self.cb = Some(cb);
            }
            fn execute(&mut self, task: Arc<SimpleTask>) {
                ($f)(self, &task);
                self.cb(task);
            }
            fn concurrency(&self) -> usize {
                1
            }
        }
    )
}

fn fetch(_exct: &mut Fetch, task: &Arc<SimpleTask>) {
    task.set_fetch();
}

fn compute(_exct: &mut Compute, task: &Arc<SimpleTask>) {
    task.set_compute();
}

fn store(_exct: &mut Store, task: &Arc<SimpleTask>) {
    task.set_store();
    task.set_finish();
}

executor!(Fetch, fetch);
executor!(Compute, compute);
executor!(Store, store);

fn main() {
    let (tx, rx) = mpsc::channel();
    let cb = move |task| tx.send(task).unwrap();
    let fetch = Component::new("fetch", 1, Fetch::new());
    let compute = Component::new("compute", 1, Compute::new());
    let store = Component::new("store", 1, Store::new());
    let mut pipeline = PipelineBuilder::new(cb)
        .add_comp(fetch)
        .add_comp(compute)
        .add_comp(store)
        .build()
        .unwrap();
    pipeline.run();
    let task = SimpleTask::new(1);
    pipeline.accept_task(Arc::new(task)).unwrap();

    let task = rx.recv().unwrap();
    assert!(task.is_fetched());
    assert!(task.is_computed());
    assert!(task.is_stored());
    assert!(task.is_finished());
}
