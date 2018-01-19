#![feature(clone_closures, unboxed_closures, fn_traits)]

extern crate futures;
#[macro_use]
extern crate log;
extern crate uuid;

use std::collections::{HashMap, VecDeque};
use std::ops::{Index, IndexMut};
use std::thread::{self, JoinHandle};
use std::hash::Hash;
use std::sync::Arc;
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::atomic::{AtomicUsize, ATOMIC_USIZE_INIT};
use std::sync::atomic::Ordering::SeqCst;

use uuid::Uuid;
use futures::Future;
use futures::sync::oneshot;

#[derive(Debug)]
pub struct NoComponent;

pub trait Task: Send + Sync + 'static {
    type Id: Hash + Eq + Ord + Send;

    fn get_id(&self) -> Self::Id;
    fn is_finished(&self) -> bool;
    fn abandon(&self) {}
}

pub struct CompCallBack<T: Task> {
    tx: Sender<Message<T>>,
}

impl<T: Task> FnOnce<(Uuid, Arc<T>)> for CompCallBack<T> {
    type Output = ();
    extern "rust-call" fn call_once(self, args: (Uuid, Arc<T>)) {
        self.call(args);
    }
}

impl<T: Task> FnMut<(Uuid, Arc<T>)> for CompCallBack<T> {
    extern "rust-call" fn call_mut(&mut self, args: (Uuid, Arc<T>)) {
        self.call(args)
    }
}

impl<T: Task> Fn<(Uuid, Arc<T>)> for CompCallBack<T> {
    extern "rust-call" fn call(&self, args: (Uuid, Arc<T>)) {
        let msg = Message::Intermediate(args.0, args.1);
        if self.tx.send(msg).is_err() {
            info!("pipeline was dropped");
        }
    }
}

impl<T: Task> Clone for CompCallBack<T> {
    fn clone(&self) -> Self {
        CompCallBack {
            tx: self.tx.clone(),
        }
    }
}

pub trait Component<T: Task>: Send + 'static {
    fn get_id(&self) -> Uuid;
    fn accept_task(&mut self, task: Arc<T>) -> Result<(), Arc<T>>;
    fn register_cb(&mut self, cb: CompCallBack<T>);
    fn concurrent_num(&self) -> usize {
        1
    }
    fn run(&mut self) {}
}

pub struct Buidler<T: Task> {
    cap: usize,
    buf_cap: usize,

    cb: Option<Box<Fn(Arc<T>) + Send>>,
    // len must >= 1
    comps: Vec<Box<Component<T>>>,
}

pub struct Pipeline<T: Task> {
    cap: usize,
    buf_cap: usize,
    tx: Sender<Message<T>>,
    rx: Option<Receiver<Message<T>>>,
    inner: Option<PipelineImpl<T>>,
    handle: Option<JoinHandle<()>>,
}

struct PipelineImpl<T: Task> {
    waiting_tasks: TaskQueue<T>,
    processing_tasks: ProcessingTasks<T>,
    cb: Box<Fn(Arc<T>) + Send>,
    comps: BufferedCompQueue<T>,
}

type Indices = HashMap<Uuid, usize>;

struct BufferedCompQueue<T: Task> {
    indices: Arc<Indices>,
    comps: Vec<BufferedComp<T>>,
    table: Arc<ViewTable>,
}

struct BufferedComp<T: Task> {
    buffered_tasks: TaskQueue<T>,
    comp: Box<Component<T>>,
    table: Arc<ViewTable>,
}

#[derive(Clone, Debug)]
struct ViewTable {
    indices: Arc<Indices>,
    views: Vec<BufferedCompView>,
}

#[derive(Debug)]
struct BufferedCompView {
    id: Uuid,
    buf_cap: usize,
    concurrent: usize,
    processing: AtomicUsize,
    buf_vcant: AtomicUsize,
}

struct TaskQueue<T: Task> {
    tasks: VecDeque<Arc<T>>,
}

struct ProcessingTasks<T: Task> {
    tasks: HashMap<T::Id, Arc<T>>,
}

enum Message<T: Task> {
    NewTask(Arc<T>),
    Intermediate(Uuid, Arc<T>),
    Query(oneshot::Sender<QueryResult>, QueryRequest),
    Stop,
}

#[derive(Debug)]
enum QueryRequest {
    TotalNum,
    ProcessingNum,
    WaitingNum,
    ViewTable,
}

#[derive(Debug)]
enum QueryResult {
    TotalNum(usize),
    ProcessingNum(usize),
    WaitingNum(usize),
    ViewTable(ViewTable),
}

macro_rules! query {
    ($req: expr, $res: path, $tx: expr) => {
        {
            let (tx, rx) = oneshot::channel();
            let msg = Message::Query(tx, $req);
            $tx.send(msg).unwrap();
            match rx.wait().unwrap() {
                $res(res) => res,
                _ => panic!("invalid query result"),
            }
        }
    };
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

    pub fn cb<F: Fn(Arc<T>) + Send + 'static>(mut self, cb: F) -> Self {
        self.cb = Some(Box::new(cb));
        self
    }

    pub fn add_comp<C: Component<T>>(mut self, c: C) -> Self {
        assert!(c.concurrent_num() >= 1);
        self.comps.push(Box::new(c));
        self
    }

    pub fn build(mut self) -> Result<Pipeline<T>, NoComponent> {
        if self.cb.is_none() {
            return Err(NoComponent);
        }
        let (tx, rx) = mpsc::channel();
        let cb = CompCallBack { tx: tx.clone() };

        let mut views = Vec::new();
        for comp in &mut self.comps {
            comp.register_cb(cb.clone());
            let id = comp.get_id();
            let view = BufferedCompView::new(id, self.buf_cap, comp.concurrent_num());
            views.push(view);
        }
        let table = Arc::new(ViewTable::new(views));

        let comps = self.comps
            .into_iter()
            .map(|comp| BufferedComp::new(comp, Arc::clone(&table)))
            .collect();

        let queue = BufferedCompQueue {
            indices: Arc::clone(&table.indices),
            comps: comps,
            table: table,
        };

        let inner = PipelineImpl {
            waiting_tasks: TaskQueue::new(),
            processing_tasks: ProcessingTasks::new(),
            cb: self.cb.unwrap(),
            comps: queue,
        };

        let pipeline = Pipeline {
            cap: self.cap,
            buf_cap: self.buf_cap,
            tx: tx,
            rx: Some(rx),
            inner: Some(inner),
            handle: None,
        };
        Ok(pipeline)
    }
}

impl<T: Task> Pipeline<T> {
    pub fn run(&mut self) {
        let rx = self.rx.take().unwrap();
        let mut inner = self.inner.take().unwrap();
        let handle = thread::Builder::new()
            .name("pipeline".into())
            .spawn(move || inner.run(&rx))
            .unwrap();
        self.handle = Some(handle);
    }

    pub fn accept_task(&self, task: Arc<T>) -> Result<(), Arc<T>> {
        if self.total_num() < self.cap {
            self.tx.send(Message::NewTask(task)).unwrap();
            Ok(())
        } else {
            Err(task)
        }
    }

    pub fn capacity(&self) -> usize {
        self.cap
    }

    pub fn buf_cap(&self) -> usize {
        self.buf_cap
    }

    pub fn total_num(&self) -> usize {
        query!(
            QueryRequest::TotalNum,
            QueryResult::TotalNum,
            self.tx.clone()
        )
    }

    pub fn processing_num(&self) -> usize {
        query!(
            QueryRequest::ProcessingNum,
            QueryResult::ProcessingNum,
            self.tx.clone()
        )
    }

    pub fn waiting_num(&self) -> usize {
        query!(
            QueryRequest::WaitingNum,
            QueryResult::WaitingNum,
            self.tx.clone()
        )
    }

    // used for debuging
    #[allow(dead_code)]
    fn view_table(&self) -> ViewTable {
        query!(
            QueryRequest::ViewTable,
            QueryResult::ViewTable,
            self.tx.clone()
        )
    }
}

impl<T: Task> Drop for Pipeline<T> {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.take() {
            self.tx.send(Message::Stop).unwrap();
            handle.join().unwrap();
        }
    }
}

impl<T: Task> PipelineImpl<T> {
    fn run(&mut self, rx: &Receiver<Message<T>>) {
        for comp in &mut self.comps.comps {
            comp.comp.run();
        }
        loop {
            match rx.recv().unwrap() {
                Message::NewTask(task) => self.new_task(task),
                Message::Intermediate(uuid, task) => self.intermediate_task(uuid, task),
                Message::Query(sender, request) => self.query(sender, request),
                Message::Stop => break,
            }
        }

        info!("abandom all tasks in the pipeline");
        while let Some(task) = self.waiting_tasks.pop() {
            task.abandon();
        }
        self.processing_tasks
            .drain_to_vec()
            .into_iter()
            .for_each(|t| t.abandon());

        assert_eq!(self.total_num(), 0);
        assert_eq!(self.processing_tasks.len(), 0);
        assert_eq!(self.waiting_tasks.len(), 0);

        info!("pipeline finished");
    }

    fn total_num(&self) -> usize {
        self.processing_tasks.len() + self.waiting_tasks.len()
    }

    fn new_task(&mut self, task: Arc<T>) {
        let id = self.comps[0].get_id();
        let num = self.comps.table.get_view(&id).buf_vcant_num();
        if num == 0 {
            self.waiting_tasks.push(task);
        } else {
            let res = self.processing_tasks.insert(Arc::clone(&task));
            assert!(res.is_none());
            self.comps[0].accept_task(task);
        }
    }

    fn intermediate_task(&mut self, id: Uuid, task: Arc<T>) {
        {
            let view = self.comps.table.get_view(&id);
            view.dec_processing();
        }
        {
            let next_comp = self.comps.get_next_comp_mut(&id);
            if task.is_finished() || next_comp.is_none() {
                let res = self.processing_tasks.remove(&task.get_id());
                assert!(res.is_some());
                (self.cb)(task);
            } else {
                let next_comp = next_comp.unwrap();
                next_comp.accept_task(task);
            }
        }
        for i in (0..self.comps.get_index(&id) + 1).rev() {
            let num = self.comps[i].pop_to_run();
            assert!(num <= 1);
            if i == 0 {
                let vcant = self.comps.table.vcant_num(&id);
                for _ in 0..vcant {
                    if let Some(task) = self.waiting_tasks.pop() {
                        let res = self.processing_tasks.insert(Arc::clone(&task));
                        assert!(res.is_none());
                        self.comps[0].accept_task(task);
                    } else {
                        break;
                    }
                }
            }
        }
    }

    fn query(&self, sender: oneshot::Sender<QueryResult>, req: QueryRequest) {
        match req {
            QueryRequest::TotalNum => {
                let num = self.total_num();
                sender.send(QueryResult::TotalNum(num)).unwrap();
            }
            QueryRequest::ProcessingNum => {
                let num = self.processing_tasks.len();
                sender.send(QueryResult::ProcessingNum(num)).unwrap();
            }
            QueryRequest::WaitingNum => {
                let num = self.waiting_tasks.len();
                sender.send(QueryResult::WaitingNum(num)).unwrap();
            }
            QueryRequest::ViewTable => {
                let table = (*self.comps.table).clone();
                sender.send(QueryResult::ViewTable(table)).unwrap();
            }
        }
    }
}

impl<T: Task> BufferedCompQueue<T> {
    fn get_next_comp_mut(&mut self, id: &Uuid) -> Option<&mut BufferedComp<T>> {
        let current = self.indices[id];
        if current == self.comps.len() - 1 {
            None
        } else {
            Some(&mut self.comps[current + 1])
        }
    }

    #[allow(dead_code)]
    fn get_prev_comp_mut(&mut self, id: &Uuid) -> Option<&mut BufferedComp<T>> {
        let current = self.indices[id];
        if current == 0 {
            None
        } else {
            Some(&mut self.comps[current - 1])
        }
    }

    fn get_index(&self, id: &Uuid) -> usize {
        self.indices[id]
    }
}

impl<T: Task> Index<usize> for BufferedCompQueue<T> {
    type Output = BufferedComp<T>;
    fn index(&self, index: usize) -> &BufferedComp<T> {
        &self.comps[index]
    }
}

impl<T: Task> IndexMut<usize> for BufferedCompQueue<T> {
    fn index_mut(&mut self, index: usize) -> &mut BufferedComp<T> {
        &mut self.comps[index]
    }
}

impl<T: Task> BufferedComp<T> {
    fn new(comp: Box<Component<T>>, table: Arc<ViewTable>) -> Self {
        BufferedComp {
            buffered_tasks: TaskQueue::new(),
            comp: comp,
            table: table,
        }
    }

    fn get_id(&self) -> Uuid {
        self.comp.get_id()
    }

    fn get_view(&self) -> &BufferedCompView {
        self.table.get_view(&self.get_id())
    }

    // there must at least one vcant entry in this component.
    fn accept_task(&mut self, task: Arc<T>) {
        let id = self.get_id();
        let view = self.table.get_view(&id);
        let buf_vcant = view.buf_vcant_num();
        assert!(buf_vcant > 0);
        // fast path. this means there are some tasks in the buffer.
        if buf_vcant != view.buf_cap {
            self.buffered_tasks.push(task);
            view.dec_buf_vcant();
            return;
        }

        // here means there is no task in the buffer, we should determine where to
        // put this task, the buffer or the componet.
        let num = self.table.real_comp_vcant(&id);
        if num == 0 {
            // this means there is no vcant entry in the component.
            self.buffered_tasks.push(task);
            view.dec_buf_vcant();
        } else {
            if self.comp.accept_task(task).is_err() {
                panic!("pipeline should never overfeed the component");
            }
            view.inc_processing();
        }
    }

    fn pop_to_run(&mut self) -> usize {
        let id = self.comp.get_id();
        let num = self.table.real_comp_vcant(&id);
        let mut res = 0;
        for _ in 0..num {
            if let Some(task) = self.buffered_tasks.pop() {
                res += 1;
                if self.comp.accept_task(task).is_err() {
                    panic!("pipeline should never overfeed the component");
                }
            } else {
                break;
            }
        }
        let view = self.get_view();
        for _ in 0..res {
            view.inc_buf_vcant();
            view.inc_processing();
        }
        res
    }
}

impl ViewTable {
    fn new(views: Vec<BufferedCompView>) -> Self {
        let mut indices = HashMap::new();
        for (i, view) in views.iter().enumerate() {
            indices.insert(view.id, i);
        }
        ViewTable {
            indices: Arc::new(indices),
            views: views,
        }
    }

    /// indicate that how many tasks the buffered component can handle in the
    /// current status.
    fn vcant_num(&self, id: &Uuid) -> usize {
        let view = self.get_view(id);
        let buf_vcant = view.buf_vcant_num();
        // fast path. this means there are some tasks in the buffer.
        if buf_vcant != view.buf_cap {
            return buf_vcant;
        }

        if let Some(next) = self.get_next_view(id) {
            let next_vcant_num = self.vcant_num(&next.id);
            let rhs = next_vcant_num - view.processing_num();
            let vcant_num = view.comp_vcant_num().min(rhs);
            view.buf_vcant_num() + vcant_num
        } else {
            // this is the last view
            view.buf_vcant_num() + view.comp_vcant_num()
        }
    }

    /// indicate that how many tasks the user's component can handle in the current
    /// status.
    fn real_comp_vcant(&self, id: &Uuid) -> usize {
        let view = self.get_view(id);
        if let Some(next) = self.get_next_view(id) {
            let next_vcant_num = self.vcant_num(&next.id);
            let rhs = next_vcant_num - view.processing_num();
            view.comp_vcant_num().min(rhs)
        } else {
            view.comp_vcant_num()
        }
    }

    fn get_view(&self, id: &Uuid) -> &BufferedCompView {
        let index = self.indices[id];
        &self.views[index]
    }

    fn get_next_view(&self, id: &Uuid) -> Option<&BufferedCompView> {
        let index = self.indices[id];
        if index == self.views.len() - 1 {
            None
        } else {
            Some(&self.views[index + 1])
        }
    }

    #[allow(dead_code)]
    fn get_prev_view(&self, id: &Uuid) -> Option<&BufferedCompView> {
        let index = self.indices[id];
        if index == 0 {
            None
        } else {
            Some(&self.views[index - 1])
        }
    }
}

impl Index<usize> for ViewTable {
    type Output = BufferedCompView;
    fn index(&self, index: usize) -> &BufferedCompView {
        &self.views[index]
    }
}

impl BufferedCompView {
    fn new(id: Uuid, buf_cap: usize, concurrent: usize) -> Self {
        BufferedCompView {
            id: id,
            buf_cap: buf_cap,
            concurrent: concurrent,
            processing: ATOMIC_USIZE_INIT,
            buf_vcant: AtomicUsize::new(buf_cap),
        }
    }

    fn processing_num(&self) -> usize {
        self.processing.load(SeqCst)
    }

    #[allow(dead_code)]
    fn buffered_num(&self) -> usize {
        self.buf_cap - self.buf_vcant_num()
    }

    fn buf_vcant_num(&self) -> usize {
        self.buf_vcant.load(SeqCst)
    }

    fn comp_vcant_num(&self) -> usize {
        self.concurrent - self.processing_num()
    }

    fn dec_processing(&self) {
        self.processing.fetch_sub(1, SeqCst);
    }

    fn inc_processing(&self) {
        self.processing.fetch_add(1, SeqCst);
    }

    fn dec_buf_vcant(&self) {
        self.buf_vcant.fetch_sub(1, SeqCst);
    }

    fn inc_buf_vcant(&self) {
        self.buf_vcant.fetch_add(1, SeqCst);
    }
}

impl Clone for BufferedCompView {
    fn clone(&self) -> Self {
        BufferedCompView {
            id: self.id,
            buf_cap: self.buf_cap,
            concurrent: self.concurrent,
            processing: AtomicUsize::new(self.processing.load(SeqCst)),
            buf_vcant: AtomicUsize::new(self.buf_vcant.load(SeqCst)),
        }
    }
}

impl<T: Task> TaskQueue<T> {
    fn new() -> Self {
        TaskQueue {
            tasks: Default::default(),
        }
    }

    fn push(&mut self, task: Arc<T>) {
        self.tasks.push_back(task);
    }

    fn pop(&mut self) -> Option<Arc<T>> {
        self.tasks.pop_front()
    }

    fn len(&self) -> usize {
        self.tasks.len()
    }
}

impl<T: Task> ProcessingTasks<T> {
    fn new() -> Self {
        ProcessingTasks {
            tasks: Default::default(),
        }
    }

    fn insert(&mut self, task: Arc<T>) -> Option<Arc<T>> {
        self.tasks.insert(task.get_id(), task)
    }

    fn remove(&mut self, id: &T::Id) -> Option<Arc<T>> {
        self.tasks.remove(id)
    }

    fn len(&self) -> usize {
        self.tasks.len()
    }

    fn drain_to_vec(&mut self) -> Vec<Arc<T>> {
        self.tasks.drain().map(|(_, v)| v).collect()
    }
}

#[cfg(test)]
mod test;
