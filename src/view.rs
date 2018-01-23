use std::ops::Index;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::atomic::{AtomicUsize, ATOMIC_USIZE_INIT};

#[derive(Debug)]
pub struct ViewTable {
    views: Vec<CompView>,
}

/// component view
#[derive(Debug)]
pub struct CompView {
    pub id: usize,
    pub buf_cap: usize,
    buffered: AtomicUsize,
    concurrency: usize,
    processing: AtomicUsize,
    exct_views: Vec<ExctView>,
}

/// executor view
#[derive(Debug)]
pub struct ExctView {
    id: usize,
    concurrency: usize,
    processing: AtomicUsize,
}

impl ViewTable {
    pub fn new(views: Vec<CompView>) -> Self {
        views
            .iter()
            .enumerate()
            .for_each(|(i, v)| assert_eq!(i, v.id));

        ViewTable { views: views }
    }

    pub fn capacity(&self) -> usize {
        let mut ret = self.views[self.views.len() - 1].concurrency;
        self.views.iter().for_each(|v| ret += v.buf_cap);
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
        exct_views
            .iter()
            .enumerate()
            .for_each(|(i, v)| assert_eq!(i, v.id));
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

    /// pick a worker with the minimum load
    pub fn pick_min(&self) -> Option<usize> {
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
        assert_ne!(concurrency, 0);
        ExctView {
            id: id,
            concurrency: concurrency,
            processing: ATOMIC_USIZE_INIT,
        }
    }

    fn dec(&self) {
        assert_ne!(self.processing.load(SeqCst), 0);
        self.processing.fetch_sub(1, SeqCst);
    }

    fn inc(&self) {
        self.processing.fetch_add(1, SeqCst);
    }

    fn vcant_num(&self) -> usize {
        self.concurrency - self.processing.load(SeqCst)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_table() {
        // l0's concurrncy < l1's vcant_num
        let vt = simple_table([2, 2], [2, 2]);
        assert_eq!(vt.capacity(), 6);
        check_table(&vt, 1, !0);

        // l0's concurrncy = l1's vcant_num
        let vt = simple_table([2, 4], [2, 2]);
        assert_eq!(vt.capacity(), 6);
        check_table(&vt, 1, !0);

        // l0's concurrncy > l1's vcant_num
        let vt = simple_table([2, 8], [2, 2]);
        assert_eq!(vt.capacity(), 6);
        check_table(&vt, 1, !0);
    }

    fn check_table(table: &ViewTable, idx: usize, next_vcant: usize) {
        let view = &table[idx];
        let max = next_vcant.min(view.concurrency);
        for bp in gen_bp_pairs(view.buf_cap, max) {
            view.set_buffered_num(bp[0]);
            view.set_processing_num(bp[1]);
            let v = table.vcant_num(idx);
            let r = table.real_comp_vcant(idx);
            let p = view.processing_num();
            assert_eq!(v, max - p + view.buf_vcant_num());
            assert_eq!(r, max - p);
            if idx != 0 {
                check_table(table, idx - 1, v)
            }
        }
    }

    impl CompView {
        fn set_buffered_num(&self, num: usize) {
            assert!(self.buf_cap >= num);
            self.buffered.store(num, SeqCst);
        }

        fn set_processing_num(&self, num: usize) {
            assert!(self.concurrency >= num);
            self.processing.store(num, SeqCst);
        }
    }

    /// Generate [buffered tasks num, processing tasks num] pairs
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

    /// Construct 2 level table,
    ///
    /// Parameter is [buf_cap, concurrency] pair
    fn simple_table(l0: [usize; 2], l1: [usize; 2]) -> ViewTable {
        let mut views = vec![];
        views.push(simple_comp_view(0, l0[0], l0[1]));
        views.push(simple_comp_view(1, l1[0], l1[1]));
        ViewTable::new(views)
    }

    fn simple_comp_view(id: usize, buf_cap: usize, concurrency: usize) -> CompView {
        let mut exct_views = vec![];
        exct_views.push(ExctView::new(0, concurrency));
        CompView::new(id, buf_cap, exct_views)
    }

    #[test]
    fn test_comp_view() {
        let mut exct_views = vec![];
        exct_views.push(ExctView::new(0, 2));
        exct_views.push(ExctView::new(1, 2));
        let view = CompView::new(0, 2, exct_views);

        view.inc_processing(0);
        assert_eq!(view.pick_min(), Some(1));

        view.inc_processing(1);
        view.inc_processing(1);
        assert_eq!(view.pick_min(), Some(0));

        view.inc_processing(0);
        assert_eq!(view.pick_min(), None);
    }
}
