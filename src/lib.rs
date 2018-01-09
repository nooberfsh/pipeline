#![feature(clone_closures)]

#[macro_use]
extern crate log;
extern crate uuid;

use std::collections::{HashMap, VecDeque};
use std::ops::Index;
use std::fmt;
use std::thread::{self, JoinHandle};
use std::hash::Hash;
use std::sync::{Arc, Mutex};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::atomic::{AtomicUsize, ATOMIC_USIZE_INIT};
use std::sync::atomic::Ordering::SeqCst;

use uuid::Uuid;

pub trait Task: Send + Sync + 'static + fmt::Debug {
    type Id: Hash + Eq + Ord + Send;

    fn get_id(&self) -> Self::Id;
    fn is_finished(&self) -> bool;
    fn abandom(&self) {}
}

pub trait Component<T: Task>: Send + Sync + 'static {
    fn get_id(&self) -> Uuid;
    fn accept_task(&self, task: Arc<T>) -> Result<(), Arc<T>>;
    fn register_cb(&self, cb: Box<Fn(Uuid, Arc<T>)>);
    fn concurrent_num(&self) -> usize {
        1
    }
}

pub struct Buidler<T: Task> {
    cap: usize,
    buf_cap: usize,

    cb: Option<Box<Fn(Arc<T>) + Send + Sync>>,
    // len must >= 1
    comps: Vec<Arc<Component<T>>>,
}

#[derive(Debug)]
pub struct NoComponent;

pub struct Pipeline<T: Task> {
    tx: Sender<Message<T>>,
    rx: Option<Receiver<Message<T>>>,
    inner: Arc<Inner<T>>,
    thread_handle: Option<JoinHandle<()>>,
}

trait AssertKinds: Send + Sync {}

impl<T: Task> AssertKinds for Inner<T> {}

struct Inner<T: Task> {
    waiting_tasks: TaskQueue<T>,
    processing_tasks: ProcessingTasks<T>,
    cap: usize,
    cb: Box<Fn(Arc<T>) + Send + Sync>,

    comps: BufferedCompQueue<T>,
}

struct TaskQueue<T: Task> {
    tasks: Mutex<VecDeque<Arc<T>>>,
}

struct ProcessingTasks<T: Task> {
    tasks: Mutex<HashMap<T::Id, Arc<T>>>,
}

struct BufferedComp<T: Task> {
    buffed_tasks: TaskQueue<T>,
    buf_cap: usize,
    comp: Arc<Component<T>>,
    processing_num: AtomicUsize,
}

struct BufferedCompQueue<T: Task> {
    indices: HashMap<Uuid, usize>,
    comps: Vec<BufferedComp<T>>,
}

enum Message<T: Task> {
    NewTask(Arc<T>),
    Intermediate(Uuid, Arc<T>),
    Stop,
}

impl<T: Task> Buidler<T> {
    pub fn new() -> Self {
        Buidler {
            cap: 64,
            buf_cap: 4,
            cb: None,
            comps: vec![],
        }
    }

    pub fn cap(mut self, cap: usize) -> Self {
        self.cap = cap;
        self
    }

    pub fn buf_cap(mut self, buf_cap: usize) -> Self {
        self.buf_cap = buf_cap;
        self
    }

    pub fn cb<F: Fn(Arc<T>) + Send + Sync + 'static>(mut self, cb: F) -> Self {
        self.cb = Some(Box::new(cb));
        self
    }

    pub fn add_comp<C: Component<T> + 'static>(mut self, c: C) -> Self {
        self.comps.push(Arc::new(c));
        self
    }

    pub fn build(self) -> Result<Pipeline<T>, NoComponent> {
        if self.cb.is_none() {
            return Err(NoComponent);
        }
        let (tx, rx) = mpsc::channel();
        let sender = tx.clone();
        let f = move |uuid, task| {
            let msg = Message::Intermediate(uuid, task);
            if sender.send(msg).is_err() {
                info!("pipeline was dropped");
            }
        };
        self.comps
            .iter()
            .for_each(|comp| comp.register_cb(Box::new(f.clone())));
        let inner = Arc::new(Inner {
            waiting_tasks: TaskQueue::new(),
            processing_tasks: ProcessingTasks::new(),
            cap: self.cap,
            cb: self.cb.unwrap(),
            comps: BufferedCompQueue::new(self.buf_cap, self.comps),
        });

        let pipeline = Pipeline {
            tx: tx,
            rx: Some(rx),
            inner: inner,
            thread_handle: None,
        };
        Ok(pipeline)
    }
}

fn poll<T: Task>(inner: Arc<Inner<T>>, rx: Receiver<Message<T>>) {
    loop {
        match rx.recv().unwrap() {
            Message::NewTask(task) => inner.accept_new_task(task),
            Message::Intermediate(uuid, task) => inner.accept_intermediate_task(uuid, task),
            Message::Stop => break,
        }
    }
    info!("poll finished");
}

impl<T: Task> Pipeline<T> {
    pub fn new<F: Fn(Arc<T>) + Send + Sync + 'static>(cb: F) -> Self {
        Buidler::new().cb(cb).build().unwrap()
    }

    pub fn run(&mut self) {
        let rx = self.rx.take().unwrap();
        let inner = Arc::clone(&self.inner);
        let handle = thread::Builder::new()
            .name("pipeline".into())
            .spawn(move || poll(inner, rx))
            .unwrap();
        self.thread_handle = Some(handle);
    }

    pub fn accept_task(&self, task: Arc<T>) -> Result<(), Arc<T>> {
        if self.total_task_num() < self.capacity() {
            let msg = Message::NewTask(task);
            self.tx.send(msg).unwrap();
            Ok(())
        } else {
            Err(task)
        }
    }

    pub fn total_task_num(&self) -> usize {
        self.waiting_task_num() + self.processing_task_num()
    }

    pub fn waiting_task_num(&self) -> usize {
        self.inner.waiting_tasks.len()
    }

    pub fn processing_task_num(&self) -> usize {
        self.inner.processing_tasks.len()
    }

    pub fn capacity(&self) -> usize {
        self.inner.cap
    }
}

impl<T: Task> Drop for Pipeline<T> {
    fn drop(&mut self) {
        if let Some(handle) = self.thread_handle.take() {
            self.tx.send(Message::Stop).unwrap();
            handle.join().unwrap();
        }
    }
}

impl<T: Task> Inner<T> {
    fn accept_new_task(&self, task: Arc<T>) {
        if self.comps[0].buf_is_full() {
            self.waiting_tasks.push(task);
        } else {
            self.comps[0].accept_task(task.clone());
            let res = self.processing_tasks.insert(task);
            assert!(res.is_none());
        }
    }

    fn accept_intermediate_task(&self, comp_id: Uuid, task: Arc<T>) {
        let current_comp = self.comps.current_comp(&comp_id);
        current_comp.dec_processing();

        let next_comp = self.comps.next_comp(&comp_id);
        if task.is_finished() || next_comp.is_none() {
            let res = self.processing_tasks.remove(&task.get_id());
            assert!(res.is_some());
            (self.cb)(task);
        } else {
            let next = next_comp.clone().unwrap();
            next.accept_task(task);
        }

        let mut iter = Some(current_comp);
        while let Some(comp) = iter {
            let id = comp.comp.get_id();
            self.adjust_component(&id);
            iter = self.comps.prev_comp(&id);
        }
    }

    fn adjust_component(&self, comp_id: &Uuid) {
        let next_vcant = if let Some(next) = self.comps.next_comp(comp_id) {
            self.comps.vcant_num(&next.comp.get_id())
        } else {
            // if this is the top most component;
            std::usize::MAX
        };

        let current = self.comps.current_comp(comp_id);
        assert!(current.current_processing() <= next_vcant);

        let vcant_processing = current.vcant_processing();
        let for_next_vcant_processing = next_vcant - current.current_processing();
        let vcant_processing = vcant_processing.min(for_next_vcant_processing);
        current.pop_to_run(vcant_processing);

        if *comp_id == self.comps[0].comp.get_id() {
            // handle the first component.
            let vcant = self.comps.vcant_num(comp_id);
            for _ in 0..vcant {
                if let Some(task) = self.waiting_tasks.pop() {
                    current.accept_task(task);
                } else {
                    return;
                }
            }
        }
    }
}

impl<T: Task> BufferedComp<T> {
    fn new(buf_cap: usize, comp: Arc<Component<T>>) -> Self {
        BufferedComp {
            buffed_tasks: TaskQueue::new(),
            buf_cap: buf_cap,
            comp: comp,
            processing_num: ATOMIC_USIZE_INIT,
        }
    }

    fn accept_task(&self, task: Arc<T>) {
        let buffed_num = self.buffed_tasks.len();
        assert!(buffed_num < self.buf_cap);

        if self.processing_num.load(SeqCst) == self.comp.concurrent_num() {
            self.buffed_tasks.push(task);
        } else if self.buffed_tasks.is_empty() {
            self.comp.accept_task(task).unwrap();
            self.processing_num.fetch_add(1, SeqCst);
        } else {
            // it is possible that the component did not reach it's maximum concurent num while
            // the buffer is not empty when the next comonent's has no vcant entry.
            self.buffed_tasks.push(task);
        }
    }

    fn buf_is_full(&self) -> bool {
        self.buffed_tasks.len() == self.buf_cap
    }

    fn dec_processing(&self) {
        self.processing_num.fetch_sub(1, SeqCst);
    }

    fn current_processing(&self) -> usize {
        self.processing_num.load(SeqCst)
    }

    fn vcant_processing(&self) -> usize {
        self.comp.concurrent_num() - self.current_processing()
    }

    fn pop_to_run(&self, num: usize) {
        for _ in 0..num {
            if let Some(task) = self.buffed_tasks.pop() {
                self.comp.accept_task(task).unwrap();
            } else {
                return;
            }
        }
    }
}

impl<T: Task> BufferedCompQueue<T> {
    fn new(buf_cap: usize, comps: Vec<Arc<Component<T>>>) -> Self {
        let indices = comps
            .iter()
            .enumerate()
            .map(|(i, comp)| (comp.get_id(), i))
            .collect();

        let comps = comps
            .into_iter()
            .map(|comp| BufferedComp::new(buf_cap, comp))
            .collect();
        BufferedCompQueue {
            indices: indices,
            comps: comps,
        }
    }

    fn current_comp(&self, comp_id: &Uuid) -> &BufferedComp<T> {
        let current = self.indices[comp_id];
        &self.comps[current]
    }

    fn next_comp(&self, comp_id: &Uuid) -> Option<&BufferedComp<T>> {
        let current = self.indices[comp_id];
        if current == self.comps.len() - 1 {
            None
        } else {
            Some(&self.comps[current + 1])
        }
    }

    fn prev_comp(&self, comp_id: &Uuid) -> Option<&BufferedComp<T>> {
        let current = self.indices[comp_id];
        if current == 0 {
            None
        } else {
            Some(&self.comps[current - 1])
        }
    }

    fn vcant_num(&self, comp_id: &Uuid) -> usize {
        // fast path
        let comp = self.current_comp(comp_id);
        let len = comp.buffed_tasks.len();
        if len != 0 {
            return comp.buf_cap - len;
        }

        if let Some(comp) = self.next_comp(comp_id) {
            // next component's vcant num
            let vcant_num = self.vcant_num(&comp.comp.get_id());
            let processing_num = comp.processing_num.load(SeqCst);
            let left = comp.comp.concurrent_num() - processing_num;
            assert!(vcant_num >= processing_num);
            let vcant_num = left.min(vcant_num - processing_num);
            return vcant_num + comp.buf_cap;
        } else {
            // top most component.
            if comp.comp.concurrent_num() == comp.processing_num.load(SeqCst) {
                return comp.buf_cap - comp.buffed_tasks.len();
            } else {
                // it is only suitable for top most component.
                assert!(comp.buffed_tasks.is_empty());
                return comp.buf_cap + comp.comp.concurrent_num() - comp.processing_num.load(SeqCst);
            }
        }
    }
}

impl<T: Task> Index<usize> for BufferedCompQueue<T> {
    type Output = BufferedComp<T>;
    fn index(&self, index: usize) -> &BufferedComp<T> {
        &self.comps[index]
    }
}

impl<T: Task> TaskQueue<T> {
    fn new() -> Self {
        TaskQueue {
            tasks: Default::default(),
        }
    }

    fn push(&self, task: Arc<T>) {
        let mut lock = self.tasks.lock().unwrap();
        lock.push_back(task);
    }

    fn pop(&self) -> Option<Arc<T>> {
        let mut lock = self.tasks.lock().unwrap();
        lock.pop_front()
    }

    fn is_empty(&self) -> bool {
        let lock = self.tasks.lock().unwrap();
        lock.is_empty()
    }

    fn len(&self) -> usize {
        let lock = self.tasks.lock().unwrap();
        lock.len()
    }
}

impl<T: Task> ProcessingTasks<T> {
    fn new() -> Self {
        ProcessingTasks {
            tasks: Default::default(),
        }
    }

    fn insert(&self, task: Arc<T>) -> Option<Arc<T>> {
        let mut lock = self.tasks.lock().unwrap();
        lock.insert(task.get_id(), task)
    }

    fn remove(&self, id: &T::Id) -> Option<Arc<T>> {
        let mut lock = self.tasks.lock().unwrap();
        lock.remove(id)
    }

    fn len(&self) -> usize {
        let lock = self.tasks.lock().unwrap();
        lock.len()
    }
}
