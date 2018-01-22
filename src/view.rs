use std::ops::Index;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::atomic::{AtomicUsize, ATOMIC_USIZE_INIT};

#[derive(Debug)]
pub struct ViewTable {
    views: Vec<CompView>,
}

#[derive(Debug)]
pub struct CompView {
    pub id: usize,
    pub buf_cap: usize,
    buffered: AtomicUsize,
    concurrency: usize,
    processing: AtomicUsize,
    exct_views: Vec<ExctView>,
}

#[derive(Debug)]
pub struct ExctView {
    id: usize,
    concurrency: usize,
    processing: AtomicUsize,
}

impl ViewTable {
    pub fn new(views: Vec<CompView>) -> Self {
        ViewTable { views: views }
    }

    pub fn capacity(&self) -> usize {
        let mut ret = self.views[self.views.len() - 1].concurrency;
        for v in &self.views {
            ret += v.buf_cap;
        }
        ret
    }

    pub fn vcant_num(&self, id: usize) -> usize {
        let view = &self.views[id];
        if view.buffered_num() != 0 {
            return view.buf_vcant_num();
        }

        view.buf_cap + self.real_comp_vcant(id)
    }

    pub fn real_comp_vcant(&self, id: usize) -> usize {
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

    pub fn is_last(&self, id: usize) -> bool {
        self.views.len() - 1 == id
    }
}

impl CompView {
    pub fn new(id: usize, buf_cap: usize, exct_views: Vec<ExctView>) -> Self {
        let concurrency = exct_views.iter().map(|e| e.concurrency).sum();
        CompView {
            id: id,
            buf_cap: buf_cap,
            buffered: ATOMIC_USIZE_INIT,
            concurrency: concurrency,
            processing: ATOMIC_USIZE_INIT,
            exct_views: exct_views,
        }
    }

    pub fn processing_num(&self) -> usize {
        self.processing.load(SeqCst)
    }

    pub fn buffered_num(&self) -> usize {
        self.buffered.load(SeqCst)
    }

    pub fn buf_vcant_num(&self) -> usize {
        self.buf_cap - self.buffered_num()
    }

    pub fn comp_vcant_num(&self) -> usize {
        self.concurrency - self.processing_num()
    }

    pub fn dec_processing(&self, id: usize) {
        self.exct_views[id].dec();
        self.processing.fetch_sub(1, SeqCst);
    }

    pub fn inc_processing(&self, id: usize) {
        self.exct_views[id].inc();
        self.processing.fetch_add(1, SeqCst);
    }

    pub fn dec_buffered(&self) {
        self.buffered.fetch_sub(1, SeqCst);
    }

    pub fn inc_buffered(&self) {
        self.buffered.fetch_add(1, SeqCst);
    }

    pub fn pick_one(&self) -> Option<usize> {
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
    pub fn new(id: usize, concurrency: usize) -> Self {
        ExctView {
            id: id,
            concurrency: concurrency,
            processing: ATOMIC_USIZE_INIT,
        }
    }

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
