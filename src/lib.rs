#![feature(clone_closures, unboxed_closures, fn_traits)]

extern crate futures;
#[macro_use]
extern crate log;
extern crate worker;

use std::collections::{HashMap};
use std::ops::{Index};
use std::thread::{self, JoinHandle};
use std::hash::Hash;
use std::sync::Arc;
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::atomic::{AtomicUsize, ATOMIC_USIZE_INIT};
use std::sync::atomic::Ordering::SeqCst;

use futures::Future;
use futures::sync::oneshot;
use worker::general::{Runner, Worker};


mod fifo;

use self::fifo::Fifo;

#[derive(Debug)]
pub struct NoComponent;

#[derive(Debug)]
pub enum Error {
    NoComponent,
    NoTaskFinishHandle,
}

pub trait Task: Send + Sync + 'static {
    type Id: Hash + Eq + Ord + Send;

    fn get_id(&self) -> Self::Id;
    fn is_finished(&self) -> bool;
    fn abandon(&self) {}
}

pub trait Executor<T: Task>: Send + 'static {
    fn register_cb(&mut self, cb: ExctCallBack<T>);
    fn execute(&mut self, task: Arc<T>);
    fn concurrency(&self) -> usize { 1 }
    fn run(&mut self) {}
}

pub struct ExctCallBack<T: Task> {
    tx: Sender<Message<T>>,
    comp_id: usize,
    worker_id: usize,
}

pub struct PipelineBuilder<T: Task> {
    comps: Vec<Component<T>>,
    cb: Option<Box<Fn(Arc<T>) + Send>>,
}

pub struct Pipeline<T: Task> {
    cap: usize,
    tx: Sender<Message<T>>,
    rx: Option<Receiver<Message<T>>>,
    inner: Option<PipelineImpl<T>>,
    handle: Option<JoinHandle<()>>,
}

struct PipelineImpl<T: Task> {
    processing_tasks: HashMap<T::Id, Arc<T>>,
    waiting_tasks: Fifo<Arc<T>>,
    cb: Box<Fn(Arc<T>) + Send>,
    comps: Vec<CompImpl<T>>,
    table: Arc<ViewTable>,
}

impl<T: Task> PipelineBuilder<T> {
    pub fn new() -> Self {
        PipelineBuilder {
            comps: vec![],
            cb: None,
        }
    }

    pub fn cb<F: Fn(Arc<T>) + Send + 'static>(mut self, cb: F) -> Self {
        self.cb = Some(Box::new(cb));
        self
    }

    pub fn add_comp(mut self, comp: Component<T>) -> Self {
        self.comps.push(comp);
        self
    }

    pub fn build(self) -> Result<Pipeline<T>, Error> {
        if self.cb.is_none() {
            return Err(Error::NoTaskFinishHandle);
        }
        if self.comps.is_empty() {
            return Err(Error::NoComponent);
        }

        let (tx, rx) = mpsc::channel();

        let mut table = vec![];
        for (i, comp) in self.comps.iter().enumerate() {
            let exct_views = comp.exct_views();
            let view = CompView::new(i, comp.buf_cap, exct_views);
            table.push(view);
        }
        let table = Arc::new(ViewTable {views: table});
        let cap = table.capacity();
        
        let mut comps = vec![];
        for (i, comp) in self.comps.into_iter().enumerate() {
            let comp_impl = comp.into_comp_impl(i, tx.clone(), Arc::clone(&table));
            comps.push(comp_impl);
        }
        
        let inner = PipelineImpl {
            processing_tasks: HashMap::new(),
            waiting_tasks: Fifo::new(),
            cb: self.cb.unwrap(),
            comps: comps,
            table: table,
        };

        let ret = Pipeline {
            cap: cap,
            tx: tx,
            rx: Some(rx),
            inner: Some(inner),
            handle: None,
        };
        Ok(ret)
    }
}

pub struct Component<T: Task> {
    name: String,
    buf_cap: usize,
    executors: Vec<Box<Executor<T>>>,
}

impl<T: Task> Component<T> {
    pub fn new<N: Into<String>, E: Executor<T>>(name: N, buf_cap: usize, e: E) -> Self {
        assert!(e.concurrency() >= 1);
        Component { 
            name: name.into(),
            buf_cap: buf_cap,
            executors: vec![Box::new(e)],
        }
    }

    pub fn new_with_multi_executor<N: Into<String>, E: Executor<T>>(name: N, buf_cap: usize, exs: Vec<E>) -> Self {
        assert!(exs.len() >0 );
        exs.iter().for_each(|e| assert!(e.concurrency() > 0));
        
        Component {
            name: name.into(),
            buf_cap: buf_cap,
            executors: exs.into_iter().map(|e| Box::new(e) as Box<Executor<_>>).collect(),
        }
    }

    fn exct_views(&self) -> Vec<ExctView> {
        let mut ret  = Vec::with_capacity(self.executors.len());
        for (i, exct) in self.executors.iter().enumerate() {
            let v = ExctView { id: i, concurrency: exct.concurrency(), processing: ATOMIC_USIZE_INIT};
            ret.push(v)
        }
        ret
    }

    fn into_comp_impl(self, id: usize, tx: Sender<Message<T>>, table: Arc<ViewTable>) -> CompImpl<T> {
        let mut workers = vec![];
        for (j, mut e) in self.executors.into_iter().enumerate() {
            let cb = ExctCallBack {
                tx: tx.clone(),
                comp_id: id,
                worker_id: j,
            };
            e.register_cb(cb.clone());
            e.run();
            let name = self.name.clone() + &format!("_{}", j);
            let worker = Worker::new(name, CompRunner { executor: e });
            workers.push(worker);
        }
        
        CompImpl::new(id, workers, table)
    }
}

pub struct CompImpl<T: Task> {
    id: usize,
    buffered_tasks: Fifo<Arc<T>>,
    workers: Vec<Worker<Arc<T>>>,
    table: Arc<ViewTable>,
}

struct CompRunner<T: Task> {
    executor: Box<Executor<T>>,
}

impl<T: Task> Runner<Arc<T>> for CompRunner<T> {
    fn run(&mut self, task: Arc<T>) {
        self.executor.execute(task)
    }
}

#[derive(Debug)]
struct ViewTable {
    views: Vec<CompView>,
}

#[derive(Debug)]
struct CompView {
    id: usize,
    buf_cap: usize,
    buffered: AtomicUsize,
    concurrency: usize,
    processing: AtomicUsize,
    exct_views: Vec<ExctView>,
}

#[derive(Debug)]
struct ExctView {
    id: usize,
    concurrency: usize,
    processing: AtomicUsize,
}

enum Message<T: Task> {
    NewTask(Arc<T>),
    Intermediate(usize, usize, Arc<T>),
    Query(oneshot::Sender<QueryResult>, QueryRequest),
    Stop,
}

#[derive(Debug)]
enum QueryRequest {
    TotalNum,
    ProcessingNum,
    WaitingNum,
}

#[derive(Debug)]
enum QueryResult {
    TotalNum(usize),
    ProcessingNum(usize),
    WaitingNum(usize),
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
        loop {
            match rx.recv().unwrap() {
                Message::NewTask(task) => self.new_task(task),
                Message::Intermediate(comp_id, worker_id, task) => self.intermediate_task(comp_id, worker_id, task),
                Message::Query(sender, request) => self.query(sender, request),
                Message::Stop => break,
            }
        }

        info!("abandom all tasks in the pipeline");
        while let Some(task) = self.waiting_tasks.pop() {
            task.abandon();
        }

        self.processing_tasks
            .drain()
            .into_iter()
            .for_each(|(_, t)| t.abandon());

        assert_eq!(self.total_num(), 0);
        assert_eq!(self.processing_tasks.len(), 0);
        assert_eq!(self.waiting_tasks.len(), 0);

        info!("pipeline finished");
    }

    fn total_num(&self) -> usize {
        self.processing_tasks.len() + self.waiting_tasks.len()
    }

    fn new_task(&mut self, task: Arc<T>) {
        if self.table.vcant_num(0) == 0 {
            self.waiting_tasks.push(task);
        } else {
            self.comps[0].accept_task(Arc::clone(&task));
            let res = self.processing_tasks.insert(task.get_id(), task);
            assert!(res.is_none());
        }
    }

    fn intermediate_task(&mut self, comp_id: usize, worker_id: usize, task: Arc<T>) {
        self.table[comp_id].dec_processing(worker_id);
        if task.is_finished() ||  self.table.is_last(comp_id) {
            let res = self.processing_tasks.remove(&task.get_id());
            assert!(res.is_some());
            (self.cb)(task);
        } else {
            self.comps[comp_id + 1].accept_task(task);
        }

        for i in (0..self.comps.len()).rev() {
            let num = self.comps[i].transfer_to_worker();
            assert!(num <= 1);
            if i == 0 {
                let vcant = self.table.vcant_num(0);
                for _ in 0..vcant {
                    if let Some(task) = self.waiting_tasks.pop() {
                        self.comps[0].accept_task(Arc::clone(&task));
                        let res = self.processing_tasks.insert(task.get_id(), task);
                        assert!(res.is_none());
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
        }
    }   
}

impl<T: Task> CompImpl<T> {
    fn new(id: usize, workers: Vec<Worker<Arc<T>>>, table: Arc<ViewTable>) -> Self {
        assert!(!workers.is_empty());
        CompImpl {
            id: id,
            buffered_tasks: Fifo::new(),
            workers: workers,
            table: table,
        }
    }

    fn accept_task(&mut self, task: Arc<T>) {
        let view = &self.table[self.id];
        let buffered = view.buffered_num();
        assert_ne!(buffered, view.buf_cap);
        
        if buffered != 0 {
            self.buffered_tasks.push(task);
            view.inc_buffered();
            return;
        }

        if self.table.real_comp_vcant(self.id) == 0 {
            self.buffered_tasks.push(task);
            view.inc_buffered();
        } else {
            let idx = view.pick_one().unwrap();
            self.workers[idx].schedule(task);
            view.inc_processing(idx);
        }
    }

    fn transfer_to_worker(&mut self) -> usize {
        let view = &self.table[self.id];
        let rcv = self.table.real_comp_vcant(self.id);
        let mut res = 0;
        for _ in 0..rcv {
            if let Some(task) = self.buffered_tasks.pop() {
                view.dec_buffered();
                let idx = view.pick_one().unwrap();
                self.workers[idx].schedule(task);
                view.inc_processing(idx);
                res += 1;
            } else {
                break;
            }
        }
        res
    }
}

impl ViewTable {
    fn capacity(&self) -> usize {
        let mut ret = self.views[self.views.len() - 1].concurrency;
        for v in &self.views {
            ret += v.buf_cap;
        }
        ret
    }

    fn vcant_num(&self, id: usize) -> usize {
        let view = &self.views[id];
        if view.buffered_num() != 0 {
            return view.buf_vcant_num()
        }

        view.buf_cap + self.real_comp_vcant(id)
    }

    fn real_comp_vcant(&self, id: usize) -> usize {
        let view = &self.views[id];
        if !self.is_last(id) {
            let next_vcant_num = self.vcant_num(id + 1);
            let rhs = next_vcant_num - view.processing_num();
            view.comp_vcant_num().min(rhs)
        } else {
            // this is the last view
            view.comp_vcant_num()
        }
    }

    fn is_last(&self, id: usize) -> bool {
        self.views.len() - 1 == id
    }
}

impl CompView {
    fn new(id: usize, buf_cap: usize, exct_views: Vec<ExctView>) -> Self {
        let concurrency = exct_views.iter().map(|e|e.concurrency).sum();
        CompView {
            id: id,
            buf_cap: buf_cap,
            buffered: ATOMIC_USIZE_INIT,
            concurrency: concurrency,
            processing: ATOMIC_USIZE_INIT,
            exct_views: exct_views,
        }
    }

    fn processing_num(&self) -> usize {
        self.processing.load(SeqCst)
    }

    fn buffered_num(&self) -> usize {
        self.buffered.load(SeqCst)
    }

    fn buf_vcant_num(&self) -> usize {
        self.buf_cap - self.buffered_num()
    }

    fn comp_vcant_num(&self) -> usize {
        self.concurrency - self.processing_num()
    }

    fn dec_processing(&self, id: usize) {
        self.exct_views[id].dec();
        self.processing.fetch_sub(1, SeqCst);
    }

    fn inc_processing(&self, id: usize) {
        self.exct_views[id].inc();
        self.processing.fetch_add(1, SeqCst);
    }

    fn dec_buffered(&self) {
        self.buffered.fetch_sub(1, SeqCst);
    }

    fn inc_buffered(&self) {
        self.buffered.fetch_add(1, SeqCst);
    }

    fn pick_one(&self) -> Option<usize> {
        let mut max = 0;
        let mut ret = None;
        for (i, v) in self.exct_views.iter().enumerate() {
            let vcant = v.vcant_num();
            if vcant > max {
                max = vcant;
                ret = Some(i);
            }
        }
        ret
    }
}

impl Index<usize> for ViewTable {
    type Output = CompView;
    fn index(&self, index: usize) -> &CompView {
        &self.views[index]
    }
}

impl ExctView {
    fn dec(&self) {
        self.processing.fetch_sub(1, SeqCst);
    }
    
    fn inc(&self) {
        self.processing.fetch_add(1, SeqCst);
    }

    fn vcant_num(&self) -> usize {
        self.concurrency - self.processing.load(SeqCst)
    }
}

impl<T: Task> FnOnce<(Arc<T>, )> for ExctCallBack<T> {
    type Output = ();
    extern "rust-call" fn call_once(self, args: (Arc<T>, )) {
        self.call(args);
    }
}

impl<T: Task> FnMut<(Arc<T>, )> for ExctCallBack<T> {
    extern "rust-call" fn call_mut(&mut self, args: (Arc<T>, )) {
        self.call(args)
    }
}

impl<T: Task> Fn<(Arc<T>, )> for ExctCallBack<T> {
    extern "rust-call" fn call(&self, args: (Arc<T>, )) {
        let msg = Message::Intermediate(self.comp_id, self.worker_id, args.0);
        if self.tx.send(msg).is_err() {
            info!("pipeline was dropped");
        }
    }
}

impl<T: Task> Clone for ExctCallBack<T> {
    fn clone(&self) -> Self {
        ExctCallBack {
            tx: self.tx.clone(),
            comp_id: self.comp_id,
            worker_id: self.worker_id,
        }
    }
}
