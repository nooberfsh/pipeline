use std::ops::Index;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::atomic::{AtomicUsize, ATOMIC_USIZE_INIT};

/// Component view table.
#[derive(Clone, Debug)]
pub struct ViewTable {
    views: Vec<CompView>,
}

/// Component view
#[derive(Debug)]
pub struct CompView {
    /// The index into the view table.
    pub id: usize,
    /// The component's buffer capacity.
    pub buf_cap: usize,
    /// The max count of the tasks the component can execute concurrently.
    pub concurrency: usize,
    /// Executor view table.
    pub exct_views: Vec<ExctView>,
    /// Buffered tasks count.
    buffered: AtomicUsize,
    /// Processing tasks count.
    processing: AtomicUsize,
}

/// Executor view
#[derive(Debug)]
pub struct ExctView {
    /// The index into the executor table.
    pub id: usize,
    /// The max count of the tasks the executor can run concurrently.
    pub concurrency: usize,
    /// Processing tasks count.
    processing: AtomicUsize,
}

impl ViewTable {
    /// Create a compoent view table.
    pub fn new(views: Vec<CompView>) -> Self {
        views
            .iter()
            .enumerate()
            .for_each(|(i, v)| assert_eq!(i, v.id));

        ViewTable { views: views }
    }

    /// Get the pipeline's capacity.
    pub fn capacity(&self) -> usize {
        let mut ret = self.views[self.views.len() - 1].concurrency;
        self.views.iter().for_each(|v| ret += v.buf_cap);
        ret
    }

    /// Get vcant count of the corresponding pipeline's component.
    pub fn vcant_num(&self, id: usize) -> usize {
        let view = &self.views[id];
        if view.buffered_num() != 0 {
            return view.buf_vcant_num();
        }

        view.buf_cap + self.real_comp_vcant(id)
    }

    /// Get vcant count of the corresponding user's component.
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

    pub fn len(&self) -> usize {
        self.views.len()
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

    #[cfg(test)]
    pub fn set_buffered_num(&self, num: usize) {
        assert!(self.buf_cap >= num);
        self.buffered.store(num, SeqCst);
    }

    #[cfg(test)]
    pub fn set_processing_num(&self, num: usize) {
        assert!(self.concurrency >= num);
        self.processing.store(num, SeqCst);
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

    #[cfg(test)]
    pub fn set_processing_num(&self, num: usize) {
        assert!(self.concurrency >= num);
        self.processing.store(num, SeqCst);
    }
}

impl Index<usize> for ViewTable {
    type Output = CompView;
    fn index(&self, index: usize) -> &CompView {
        &self.views[index]
    }
}

impl Index<usize> for CompView {
    type Output = ExctView;
    fn index(&self, index: usize) -> &ExctView {
        &self.exct_views[index]
    }
}

impl Clone for CompView {
    fn clone(&self) -> Self {
        CompView {
            id: self.id,
            buf_cap: self.buf_cap,
            buffered: AtomicUsize::new(self.buffered_num()),
            concurrency: self.concurrency,
            processing: AtomicUsize::new(self.processing_num()),
            exct_views: self.exct_views.clone(),
        }
    }
}

impl Clone for ExctView {
    fn clone(&self) -> Self {
        ExctView {
            id: self.id,
            concurrency: self.concurrency,
            processing: AtomicUsize::new(self.processing.load(SeqCst)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tests_util::*;

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

    #[test]
    fn test_table() {
        // l0's concurrncy < l1's vcant_num
        let vt = create_table(vec![[2, 2], [2, 2]]);
        assert_eq!(vt.capacity(), 6);
        check_table(&vt, 1, !0);

        // l0's concurrncy = l1's vcant_num
        let vt = create_table(vec![[2, 4], [2, 2]]);
        assert_eq!(vt.capacity(), 6);
        check_table(&vt, 1, !0);

        // l0's concurrncy > l1's vcant_num
        let vt = create_table(vec![[2, 8], [2, 2]]);
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
}
