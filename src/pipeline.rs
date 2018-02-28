use std::collections::HashMap;
use std::thread::{self, JoinHandle};
use std::hash::Hash;
use std::sync::Arc;
use std::sync::mpsc::{self, Receiver, Sender};

use futures::Future;
use futures::sync::oneshot;
use worker::general::{Runner, Worker};

use fifo::Fifo;
use view::{CompView, ExctView, ViewTable};
use NoComponent;

pub trait Task: Send + Sync + 'static {
    type Id: Hash + Eq + Ord + Send;

    fn get_id(&self) -> Self::Id;
    fn is_finished(&self) -> bool;
    fn abandon(&self) {}
}

pub trait Executor<T: Task>: Send + 'static {
    fn register_cb(&mut self, cb: ExctCallBack<T>);
    fn execute(&mut self, task: Arc<T>);
    fn concurrency(&self) -> usize {
        1
    }
    fn run(&mut self) {}
}

pub struct ExctCallBack<T: Task> {
    tx: Sender<Message<T>>,
    comp_id: usize,
    worker_id: usize,
}

pub struct PipelineBuilder<T: Task> {
    comps: Vec<Component<T>>,
    cb: Box<Fn(Arc<T>) + Send>,
}

pub struct Pipeline<T: Task> {
    cap: usize,
    tx: Sender<Message<T>>,
    inner: Option<PipelineImpl<T>>,
    handle: Option<JoinHandle<()>>,
}

struct PipelineImpl<T: Task> {
    processing_tasks: HashMap<T::Id, Arc<T>>,
    waiting_tasks: Fifo<Arc<T>>,
    rx: Receiver<Message<T>>,
    cb: Box<Fn(Arc<T>) + Send>,
    comps: Vec<CompImpl<T>>,
    table: Arc<ViewTable>,
}

impl<T: Task> PipelineBuilder<T> {
    pub fn new<F: Fn(Arc<T>) + Send + 'static>(cb: F) -> Self {
        PipelineBuilder {
            comps: vec![],
            cb: Box::new(cb),
        }
    }

    pub fn add_comp(mut self, comp: Component<T>) -> Self {
        self.comps.push(comp);
        self
    }

    pub fn build(self) -> Result<Pipeline<T>, NoComponent> {
        if self.comps.is_empty() {
            return Err(NoComponent);
        }

        let (tx, rx) = mpsc::channel();

        let mut views = vec![];
        for (i, comp) in self.comps.iter().enumerate() {
            let exct_views = comp.exct_views();
            let view = CompView::new(i, comp.buf_cap, exct_views);
            views.push(view);
        }
        let table = Arc::new(ViewTable::new(views));
        let cap = table.capacity();

        let mut comps = vec![];
        for (i, comp) in self.comps.into_iter().enumerate() {
            let comp_impl = comp.into_comp_impl(i, &tx, Arc::clone(&table));
            comps.push(comp_impl);
        }

        let inner = PipelineImpl::new(rx, self.cb, comps, table);
        let ret = Pipeline {
            cap: cap,
            tx: tx,
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

    pub fn new_with_multi_executor<N: Into<String>, E: Executor<T>>(
        name: N,
        buf_cap: usize,
        exs: Vec<E>,
    ) -> Self {
        assert!(exs.is_empty());
        exs.iter().for_each(|e| assert!(e.concurrency() > 0));

        Component {
            name: name.into(),
            buf_cap: buf_cap,
            executors: exs.into_iter()
                .map(|e| Box::new(e) as Box<Executor<_>>)
                .collect(),
        }
    }

    fn exct_views(&self) -> Vec<ExctView> {
        let mut ret = Vec::with_capacity(self.executors.len());
        for (i, exct) in self.executors.iter().enumerate() {
            let v = ExctView::new(i, exct.concurrency());
            ret.push(v)
        }
        ret
    }

    fn into_comp_impl(
        self,
        id: usize,
        tx: &Sender<Message<T>>,
        table: Arc<ViewTable>,
    ) -> CompImpl<T> {
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

enum Message<T: Task> {
    NewTask(Arc<T>),
    Intermediate(usize, usize, Arc<T>),
    Query(oneshot::Sender<QueryResult>, QueryRequest),
    Stop,
}

#[derive(Copy, Clone, Debug)]
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

impl<T: Task> Pipeline<T> {
    pub fn run(&mut self) {
        let mut inner = self.inner.take().unwrap();
        let handle = thread::Builder::new()
            .name("pipeline".into())
            .spawn(move || inner.run())
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
    fn new(
        rx: Receiver<Message<T>>,
        cb: Box<Fn(Arc<T>) + Send>,
        comps: Vec<CompImpl<T>>,
        table: Arc<ViewTable>,
    ) -> Self {
        assert_eq!(comps.len(), table.len());
        for i in 0..comps.len() {
            assert_eq!(comps[i].id, i);
            assert_eq!(comps[i].id, table[i].id);
        }

        PipelineImpl {
            processing_tasks: HashMap::new(),
            waiting_tasks: Fifo::new(),
            rx: rx,
            cb: cb,
            comps: comps,
            table: table,
        }
    }

    fn run(&mut self) {
        loop {
            match self.rx.recv().unwrap() {
                Message::NewTask(task) => self.new_task(task),
                Message::Intermediate(comp_id, worker_id, task) => {
                    self.intermediate_task(comp_id, worker_id, task)
                }
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
        if task.is_finished() || self.table.is_last(comp_id) {
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
            QueryRequest::ViewTable => {
                let table = (*self.table).clone();
                sender.send(QueryResult::ViewTable(table)).unwrap();
            }
        }
    }
}

impl<T: Task> CompImpl<T> {
    fn new(id: usize, workers: Vec<Worker<Arc<T>>>, table: Arc<ViewTable>) -> Self {
        assert!(!workers.is_empty());
        assert!(id < table.len());
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
            let idx = view.pick_min().unwrap();
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
                let idx = view.pick_min().unwrap();
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

impl<T: Task> FnOnce<(Arc<T>,)> for ExctCallBack<T> {
    type Output = ();
    extern "rust-call" fn call_once(self, args: (Arc<T>,)) {
        self.call(args);
    }
}

impl<T: Task> FnMut<(Arc<T>,)> for ExctCallBack<T> {
    extern "rust-call" fn call_mut(&mut self, args: (Arc<T>,)) {
        self.call(args)
    }
}

impl<T: Task> Fn<(Arc<T>,)> for ExctCallBack<T> {
    extern "rust-call" fn call(&self, args: (Arc<T>,)) {
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

#[cfg(test)]
mod tests {
    extern crate env_logger;

    use super::*;
    use tests_util::*;

    use std::sync::atomic::{AtomicBool, AtomicUsize, ATOMIC_BOOL_INIT, ATOMIC_USIZE_INIT};
    use std::sync::atomic::Ordering::SeqCst;
    use std::sync::Mutex;

    #[test]
    fn test_pipeline() {
        let _ = env_logger::init();

        const FETCH_ID: usize = 0;
        const COMPUTE_ID: usize = 1;
        const STORE_ID: usize = 2;

        let (exct_tx, exct_rx) = mpsc::channel();
        let fetch = SimpleExecutor::new(FETCH_ID, 2, exct_tx.clone());
        let compute = SimpleExecutor::new(COMPUTE_ID, 2, exct_tx.clone());
        let store = SimpleExecutor::new(STORE_ID, 2, exct_tx);

        let fetch_comp = Component::new("fetch", 2, fetch.clone());
        let compute_comp = Component::new("compute", 2, compute.clone());
        let store_comp = Component::new("store", 2, store.clone());

        let (tx, rx) = mpsc::channel();
        let cb = move |t| tx.send(t).unwrap();

        let mut pipeline = PipelineBuilder::new(cb)
            .add_comp(fetch_comp)
            .add_comp(compute_comp)
            .add_comp(store_comp)
            .build()
            .unwrap();
        pipeline.run();
        pipeline.accept_task(simple_task()).unwrap();
        assert_eq!(exct_rx.recv().unwrap(), FETCH_ID);

        let table = pipeline.view_table();
        assert_eq!(table.len(), 3);
        check_view(&table[0], 0, 1);
        check_view(&table[1], 0, 0);
        check_view(&table[2], 0, 0);

        fetch.handle_one();
        assert_eq!(exct_rx.recv().unwrap(), COMPUTE_ID);
        let table = pipeline.view_table();
        check_view(&table[0], 0, 0);
        check_view(&table[1], 0, 1);
        check_view(&table[2], 0, 0);

        compute.handle_one();
        assert_eq!(exct_rx.recv().unwrap(), STORE_ID);
        let table = pipeline.view_table();
        check_view(&table[0], 0, 0);
        check_view(&table[1], 0, 0);
        check_view(&table[2], 0, 1);

        store.handle_one();
        let table = pipeline.view_table();
        check_view(&table[0], 0, 0);
        check_view(&table[1], 0, 0);
        check_view(&table[2], 0, 0);

        let _ = rx.recv().unwrap();

        //test three tasks
        (1..4).for_each(|_| pipeline.accept_task(simple_task()).unwrap());

        let table = pipeline.view_table();
        assert_eq!(exct_rx.recv().unwrap(), FETCH_ID);
        assert_eq!(exct_rx.recv().unwrap(), FETCH_ID);
        check_view(&table[0], 1, 2);
        check_view(&table[1], 0, 0);
        check_view(&table[2], 0, 0);

        fetch.handle_one();
        let table = pipeline.view_table();
        let a = exct_rx.recv().unwrap();
        let b = exct_rx.recv().unwrap();
        let mut v = vec![a, b];
        v.sort();
        assert_eq!(v, vec![FETCH_ID, COMPUTE_ID]);
        check_view(&table[0], 0, 2);
        check_view(&table[1], 0, 1);
        check_view(&table[2], 0, 0);

        compute.handle_one();
        let table = pipeline.view_table();
        assert_eq!(exct_rx.recv().unwrap(), STORE_ID);
        check_view(&table[0], 0, 2);
        check_view(&table[1], 0, 0);
        check_view(&table[2], 0, 1);

        fetch.handle_one();
        fetch.handle_one();
        let table = pipeline.view_table();
        assert_eq!(exct_rx.recv().unwrap(), COMPUTE_ID);
        assert_eq!(exct_rx.recv().unwrap(), COMPUTE_ID);
        check_view(&table[0], 0, 0);
        check_view(&table[1], 0, 2);
        check_view(&table[2], 0, 1);

        compute.handle_one();
        compute.handle_one();
        let table = pipeline.view_table();
        assert_eq!(exct_rx.recv().unwrap(), STORE_ID);
        check_view(&table[0], 0, 0);
        check_view(&table[1], 0, 0);
        check_view(&table[2], 1, 2);

        store.handle_one();
        let table = pipeline.view_table();
        assert_eq!(exct_rx.recv().unwrap(), STORE_ID);
        check_view(&table[0], 0, 0);
        check_view(&table[1], 0, 0);
        check_view(&table[2], 0, 2);

        store.handle_one();
        store.handle_one();
        let table = pipeline.view_table();
        check_view(&table[0], 0, 0);
        check_view(&table[1], 0, 0);
        check_view(&table[2], 0, 0);

        for _ in 1..4 {
            let _ = rx.recv().unwrap();
        }
    }

    fn check_view(v: &CompView, buffered: usize, processing: usize) {
        assert_eq!(v.buffered_num(), buffered);
        assert_eq!(v.processing_num(), processing);
    }

    #[test]
    fn test_comp_impl() {
        let _ = env_logger::init();
        // l0's concurrncy < l1's vcant_num
        let vt = create_table(vec![[2, 2], [2, 2]]);
        check_comp_impl(vt);

        // l0's concurrncy = l1's vcant_num
        let vt = create_table(vec![[2, 4], [2, 2]]);
        check_comp_impl(vt);

        // l0's concurrncy > l1's vcant_num
        let vt = create_table(vec![[2, 8], [2, 2]]);
        check_comp_impl(vt);
    }

    fn check_comp_impl(table: ViewTable) {
        let idx = table.len() - 1;
        let vts = table_permutation(table, idx);
        for vt in vts {
            for i in 0..vt.len() - 1 {
                check_accept_task(Arc::new(vt.clone()), i);
                check_transfer_to_worker(Arc::new(vt.clone()), i);
            }
        }
    }

    fn check_accept_task(table: Arc<ViewTable>, idx: usize) {
        let view = &table[idx];
        let w = Worker::new("check_accpet_task", SimpleRunner);
        let mut comp = CompImpl::new(idx, vec![w], Arc::clone(&table));
        while view.buffered_num() != view.buf_cap {
            let r = table.real_comp_vcant(idx);
            let b = view.buffered_num();
            let p = view.processing_num();
            comp.accept_task(simple_task());
            if r == 0 {
                assert_eq!(view.buffered_num(), b + 1);
                assert_eq!(view.processing_num(), p);
            } else {
                assert_eq!(view.buffered_num(), b);
                assert_eq!(view.processing_num(), p + 1);
            }
        }
    }

    fn check_transfer_to_worker(table: Arc<ViewTable>, idx: usize) {
        let mut comp = simple_component("check_transfer_to_worker", idx, Arc::clone(&table));
        assert_eq!(comp.transfer_to_worker(), 0);

        let view = &table[idx];
        let b = view.buffered_num();
        let p = view.processing_num();
        if p == 0 {
            return;
        }
        view.dec_processing(0);

        if !table.is_last(idx) {
            let mut comp =
                simple_component("check_transfer_to_worker", idx + 1, Arc::clone(&table));
            comp.accept_task(simple_task());
        }

        let vcant = table.real_comp_vcant(idx);
        let transfered = comp.transfer_to_worker();
        if vcant == 0 {
            assert_eq!(transfered, 0);
            assert_eq!(view.buffered_num(), b);
            assert_eq!(view.processing_num(), p - 1);
        } else {
            if b != 0 {
                assert_eq!(transfered, 1);
                assert_eq!(view.buffered_num(), b - 1);
                assert_eq!(view.processing_num(), p);
            } else {
                assert_eq!(transfered, 0);
                assert_eq!(view.buffered_num(), 0);
                assert_eq!(view.processing_num(), p - 1);
            }
        }
    }

    fn simple_component(name: &str, idx: usize, table: Arc<ViewTable>) -> CompImpl<SimpleTask> {
        let w = Worker::new(name, SimpleRunner);
        let mut comp = CompImpl::new(idx, vec![w], Arc::clone(&table));
        for _ in 0..table[idx].buffered_num() {
            comp.buffered_tasks.push(simple_task());
        }
        comp
    }

    fn simple_task() -> Arc<SimpleTask> {
        static ID: AtomicUsize = ATOMIC_USIZE_INIT;
        Arc::new(SimpleTask::new(ID.fetch_add(1, SeqCst)))
    }

    struct SimpleRunner;

    impl Runner<Arc<SimpleTask>> for SimpleRunner {
        fn run(&mut self, _: Arc<SimpleTask>) {}
    }

    #[derive(Debug)]
    struct SimpleTask {
        id: usize,
        is_finished: AtomicBool,
        is_abandoned: AtomicBool,
    }

    impl SimpleTask {
        fn new(id: usize) -> Self {
            SimpleTask {
                id: id,
                is_finished: ATOMIC_BOOL_INIT,
                is_abandoned: ATOMIC_BOOL_INIT,
            }
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
        fn abandon(&self) {
            self.is_abandoned.store(true, SeqCst);
        }
    }

    struct SimpleExecutor {
        id: usize,
        concurrency: usize,
        cb: Arc<Mutex<Option<ExctCallBack<SimpleTask>>>>,
        pending: Arc<Mutex<Fifo<Arc<SimpleTask>>>>,
        finished: Arc<Mutex<Fifo<Arc<SimpleTask>>>>,
        is_runing: AtomicBool,
        // indicate receive a task
        tx: Sender<usize>,
    }

    impl SimpleExecutor {
        fn new(id: usize, con: usize, tx: Sender<usize>) -> Self {
            SimpleExecutor {
                id: id,
                concurrency: con,
                cb: Arc::default(),
                pending: Arc::default(),
                finished: Arc::default(),
                is_runing: ATOMIC_BOOL_INIT,
                tx: tx,
            }
        }

        fn handle_one(&self) {
            let task = {
                let mut lock = self.pending.lock().unwrap();
                let task = lock.pop();
                assert!(task.is_some());
                task.unwrap()
            };
            {
                let mut lock = self.finished.lock().unwrap();
                lock.push(Arc::clone(&task));
            }
            let lock = self.cb.lock().unwrap();
            lock.as_ref().unwrap()(task);
        }
    }

    impl Executor<SimpleTask> for SimpleExecutor {
        fn register_cb(&mut self, cb: ExctCallBack<SimpleTask>) {
            let mut lock = self.cb.lock().unwrap();
            *lock = Some(cb);
        }
        fn execute(&mut self, task: Arc<SimpleTask>) {
            let mut lock = self.pending.lock().unwrap();
            lock.push(task);
            drop(lock);
            self.tx.send(self.id).unwrap();
        }
        fn concurrency(&self) -> usize {
            self.concurrency
        }
        fn run(&mut self) {
            self.is_runing.store(true, SeqCst);
        }
    }

    impl Clone for SimpleExecutor {
        fn clone(&self) -> Self {
            SimpleExecutor {
                id: self.id,
                cb: self.cb.clone(),
                concurrency: self.concurrency,
                pending: self.pending.clone(),
                finished: self.finished.clone(),
                is_runing: AtomicBool::new(self.is_runing.load(SeqCst)),
                tx: self.tx.clone(),
            }
        }
    }
}
