
extern crate env_logger;

use super::*;

use std::sync::atomic::{AtomicBool, ATOMIC_BOOL_INIT};
use std::ops::Range;
use std::sync::Mutex;

macro_rules! impl_comp {
        ($t: ident) => (
            #[derive(Clone)]
            struct $t {
                id: Uuid,
                tasks: Arc<Mutex<Vec<Arc<MyTask>>>>,
                cb: Arc<Mutex<Option<CompCallBack<MyTask>>>>,
                concurrent: usize,
                product: usize,
            }

            impl $t {
                fn new(id: Uuid, concurrent: usize, product: usize) -> Self {
                    $t {
                        id: id,
                        tasks: Default::default(),
                        cb: Default::default(),
                        concurrent: concurrent,
                        product: product,
                    }
                }

                #[allow(dead_code)]
                fn handle_one(&self) {
                    let mut lock = self.tasks.lock().unwrap();
                    let task = lock.pop().unwrap();
                    task.add_product(self.product);
                    drop(lock);
                    let lock = self.cb.lock().unwrap();
                    let cb = lock.as_ref().unwrap();
                    cb(self.id, task);
                }
            }

            impl Component<MyTask> for $t {
                fn get_id(&self) -> Uuid { self.id }
                fn accept_task(&mut self, task: Arc<MyTask>) -> Result<(), Arc<MyTask>> {
                    let mut lock = self.tasks.lock().unwrap();
                    if lock.len() < self.concurrent {
                        Ok(lock.push(task))
                    } else {
                        Err(task)
                    }
                }
                fn register_cb(&mut self, cb: CompCallBack<MyTask>) {
                    let mut lock = self.cb.lock().unwrap();
                    *lock = Some(cb);
                }
                fn concurrent_num(&self) -> usize {self.concurrent}
            }
        );
    }

fn check_view(view: &BufferedCompView, buffered: usize, processing: usize) {
    assert_eq!(view.processing.load(SeqCst), processing);
    assert_eq!(view.buf_cap - view.buf_vcant.load(SeqCst), buffered);
}

fn check_product(task: &Arc<MyTask>, product: usize) {
    let t = task.product.load(SeqCst);
    let r = t & product;
    assert_ne!(r, 0);
}

#[derive(Debug)]
struct MyTask {
    id: usize,
    is_finished: AtomicBool,
    product: AtomicUsize,
    is_abandoned: AtomicBool,
}

impl_comp!(FetchComp);
impl_comp!(ExecuteComp);
impl_comp!(WriteComp);
impl_comp!(FakeComp);

const FETCH_PRODUCT: usize = 0x1;
const EXECUTE_PRODUCT: usize = 0x10;
const WRITE_PRODUCT: usize = 0x100;
const FAKE_PRODUCT: usize = 0x1000;

impl Task for MyTask {
    type Id = usize;
    fn get_id(&self) -> usize {
        self.id
    }
    fn is_finished(&self) -> bool {
        self.is_finished.load(SeqCst)
    }
    fn abandon(&self) {
        self.is_abandoned.store(true, SeqCst)
    }
}

impl MyTask {
    fn new(id: usize) -> Arc<MyTask> {
        let t = MyTask {
            id: id,
            is_finished: ATOMIC_BOOL_INIT,
            product: ATOMIC_USIZE_INIT,
            is_abandoned: ATOMIC_BOOL_INIT,
        };
        Arc::new(t)
    }

    fn add_product(&self, product: usize) {
        let p = self.product.load(SeqCst);
        self.product.store(p + product, SeqCst);
    }
}

impl BufferedCompView {
    fn set_buffered_num(&self, num: usize) {
        assert!(self.buf_cap >= num);
        self.buf_vcant.store(self.buf_cap - num, SeqCst);
    }

    fn set_processing_num(&self, num: usize) {
        assert!(self.concurrent >= num);
        self.processing.store(num, SeqCst);
    }

    fn is_full(&self) -> bool {
        self.buf_vcant_num() == 0
    }
}

macro_rules! init {
        ($pp: ident, $config: expr, $rx: ident, $fc: ident, $ec: ident, $wc: ident, $con: expr) => {
            let _ = env_logger::init();
            let $fc = FetchComp::new(Uuid::new_v4(), $con[0], FETCH_PRODUCT);
            let $ec = ExecuteComp::new(Uuid::new_v4(), $con[1], EXECUTE_PRODUCT);
            let $wc = WriteComp::new(Uuid::new_v4(), $con[2], WRITE_PRODUCT);

            let (tx, $rx) = mpsc::channel();
            let f = move |t| tx.send(t).unwrap();
            let mut $pp = Buidler::new()
                .cb(f)
                .cap($config[0])
                .buf_cap($config[1])
                .add_comp($fc.clone())
                .add_comp($ec.clone())
                .add_comp($wc.clone())
                .build()
                .unwrap();
            $pp.run();
        }
    }

#[test]
fn smoke() {
    init!(
        pipeline,
        [64, 4],
        g_rx,
        fetch_comp,
        execute_comp,
        write_comp,
        [2, 2, 2]
    );

    // test single task
    let task = MyTask::new(0);
    pipeline.accept_task(task).unwrap();

    let table = pipeline.view_table();
    assert_eq!(table.views.len(), 3);
    check_view(&table[0], 0, 1);
    check_view(&table[1], 0, 0);
    check_view(&table[2], 0, 0);

    fetch_comp.handle_one();
    let table = pipeline.view_table();
    check_view(&table[0], 0, 0);
    check_view(&table[1], 0, 1);
    check_view(&table[2], 0, 0);

    execute_comp.handle_one();
    let table = pipeline.view_table();
    check_view(&table[0], 0, 0);
    check_view(&table[1], 0, 0);
    check_view(&table[2], 0, 1);

    write_comp.handle_one();
    let table = pipeline.view_table();
    check_view(&table[0], 0, 0);
    check_view(&table[1], 0, 0);
    check_view(&table[2], 0, 0);

    let task = g_rx.recv().unwrap();
    check_product(&task, FETCH_PRODUCT);
    check_product(&task, EXECUTE_PRODUCT);
    check_product(&task, WRITE_PRODUCT);

    // test three tasks
    (1..4).for_each(|id| pipeline.accept_task(MyTask::new(id)).unwrap());

    let table = pipeline.view_table();
    assert_eq!(table.views.len(), 3);
    check_view(&table[0], 1, 2);
    check_view(&table[1], 0, 0);
    check_view(&table[2], 0, 0);

    fetch_comp.handle_one();
    let table = pipeline.view_table();
    check_view(&table[0], 0, 2);
    check_view(&table[1], 0, 1);
    check_view(&table[2], 0, 0);

    execute_comp.handle_one();
    let table = pipeline.view_table();
    check_view(&table[0], 0, 2);
    check_view(&table[1], 0, 0);
    check_view(&table[2], 0, 1);

    fetch_comp.handle_one();
    fetch_comp.handle_one();
    let table = pipeline.view_table();
    check_view(&table[0], 0, 0);
    check_view(&table[1], 0, 2);
    check_view(&table[2], 0, 1);

    execute_comp.handle_one();
    execute_comp.handle_one();
    let table = pipeline.view_table();
    check_view(&table[0], 0, 0);
    check_view(&table[1], 0, 0);
    check_view(&table[2], 1, 2);

    write_comp.handle_one();
    let table = pipeline.view_table();
    check_view(&table[0], 0, 0);
    check_view(&table[1], 0, 0);
    check_view(&table[2], 0, 2);

    write_comp.handle_one();
    write_comp.handle_one();
    let table = pipeline.view_table();
    check_view(&table[0], 0, 0);
    check_view(&table[1], 0, 0);
    check_view(&table[2], 0, 0);

    for _ in 1..4 {
        let task = g_rx.recv().unwrap();
        check_product(&task, FETCH_PRODUCT);
        check_product(&task, EXECUTE_PRODUCT);
        check_product(&task, WRITE_PRODUCT);
    }
}

// generate [buffered tasks num, processing tasks num] pairs
fn gen_bp_pairs(buf_cap: usize, max_processing: usize) -> Vec<[usize; 2]> {
    let mut ret = vec![];
    for i in 0..max_processing + 1 {
        ret.push([0, i]);
    }
    for i in 1..buf_cap + 1 {
        ret.push([i, max_processing]);
    }
    ret
}

fn check_vcant(table: &ViewTable, i: usize, cons: &[usize], next_vcant: usize) {
    let view = &table[i];
    // the max num tasks can the current component processing.
    let max = next_vcant.min(cons[i]);
    for bp in gen_bp_pairs(view.buf_cap, max) {
        view.set_buffered_num(bp[0]);
        view.set_processing_num(bp[1]);
        let v = table.vcant_num(&view.id);
        let r = table.real_comp_vcant(&view.id);
        let p = view.processing_num();
        assert_eq!(v, max - p + view.buf_vcant_num());
        assert_eq!(r, max - p);
        if i != 0 {
            check_vcant(table, i - 1, cons, v)
        }
    }
}

fn new_view_table(buf_cap: usize, concurrent_nums: &[usize]) -> ViewTable {
    assert!(!concurrent_nums.is_empty());

    let mut views = vec![];
    for con in concurrent_nums {
        let id = Uuid::new_v4();
        let view = BufferedCompView::new(id, buf_cap, *con);
        views.push(view);
    }
    ViewTable::new(views)
}

fn concurrent_permutation(levels: usize, range: Range<usize>) -> Vec<Vec<usize>> {
    assert_ne!(levels, 0);
    if levels == 1 {
        return range.map(|i| vec![i]).collect();
    }
    let mut ret = vec![];
    let rhs = concurrent_permutation(levels - 1, range.clone());
    for tup in rhs {
        for i in range.clone() {
            let mut e = tup.clone();
            e.push(i);
            ret.push(e)
        }
    }
    ret
}

#[test]
fn test_view_table() {
    let levels = 3;
    let buf_cap = 4;
    let cons_p = concurrent_permutation(levels, 1..12);
    for cons in cons_p {
        let table = new_view_table(buf_cap, &cons);
        check_vcant(&table, levels - 1, &cons, !0);
    }
}

fn new_buffered_fake_comp(
    table: &Arc<ViewTable>,
    index: usize,
    con: usize,
) -> BufferedComp<MyTask> {
    assert!(index < table.views.len());
    let view = &table[index];
    let mut fake_comp = FakeComp::new(view.id, con, FAKE_PRODUCT);
    for i in 0..view.processing_num() {
        fake_comp.accept_task(MyTask::new(i)).unwrap();
    }
    let mut ret = BufferedComp::new(Box::new(fake_comp), Arc::clone(table));
    for i in 0..view.buffered_num() {
        let id = i + view.processing_num();
        let _ = ret.buffered_tasks.push(MyTask::new(id));
    }
    ret
}

fn check_accept_task(table: Arc<ViewTable>, i: usize, cons: &[usize], next_vcant: usize) {
    let view = &table[i];
    // the max num tasks can the current component processing.
    let max = next_vcant.min(cons[i]);
    for bp in gen_bp_pairs(view.buf_cap, max) {
        view.set_buffered_num(bp[0]);
        view.set_processing_num(bp[1]);
        let mut comp = new_buffered_fake_comp(&table, i, cons[i]);
        let mut id = !0;
        while !view.is_full() {
            let vcant_num = table.vcant_num(&view.id);
            let real_comp_vcant = table.real_comp_vcant(&view.id);
            let buffered = view.buffered_num();
            let processing = view.processing_num();
            let task = MyTask::new(id);
            id -= 1;
            comp.accept_task(task);
            if real_comp_vcant == 0 {
                assert_eq!(buffered + 1, view.buffered_num());
                assert_eq!(processing, view.processing_num());
            } else {
                assert_eq!(buffered, view.buffered_num());
                assert_eq!(processing + 1, view.processing_num());
            }
            assert_eq!(vcant_num - 1, table.vcant_num(&view.id));
        }
    }
}

fn check_pop_to_run(table: Arc<ViewTable>, i: usize, cons: &[usize], next_vcant: usize) {
    let view = &table[i];
    // the max num tasks can the current component processing.
    let max = next_vcant.min(cons[i]);
    for bp in gen_bp_pairs(view.buf_cap, max) {
        view.set_buffered_num(bp[0]);
        view.set_processing_num(bp[1]);
        let mut comp = new_buffered_fake_comp(&table, i, cons[i]);
        let buffered = view.buffered_num();
        let processing = view.processing_num();
        let real_comp_vcant = table.real_comp_vcant(&view.id);

        let pop_to_run_num = comp.pop_to_run();
        if buffered <= real_comp_vcant {
            assert_eq!(0, view.buffered_num());
            assert_eq!(processing + buffered, view.processing_num());
            assert_eq!(pop_to_run_num, buffered);
        } else {
            assert_eq!(buffered - real_comp_vcant, view.buffered_num());
            assert_eq!(processing + real_comp_vcant, view.processing_num());
            assert_eq!(pop_to_run_num, real_comp_vcant);
        }
    }
}

#[test]
fn test_buffered_comp() {
    let levels = 1;
    let buf_cap = 4;
    let cons_p = concurrent_permutation(levels, 1..9);
    for cons in cons_p {
        let table = new_view_table(buf_cap, &cons);
        check_accept_task(Arc::new(table.clone()), levels - 1, &cons, !0);
        check_pop_to_run(Arc::new(table), levels - 1, &cons, !0);
    }
}
