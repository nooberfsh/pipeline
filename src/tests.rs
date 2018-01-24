use view::*;

/// Generate [buffered tasks num, processing tasks num] pairs
pub fn gen_bp_pairs(buf_cap: usize, max_processing: usize) -> Vec<[usize; 2]> {
    let mut ret = vec![];
    for i in 0..max_processing + 1 {
        ret.push([0, i]);
    }
    for i in 1..buf_cap + 1 {
        ret.push([i, max_processing]);
    }
    ret
}

/// Construct n level table,
///
/// Parameter is [buf_cap, concurrency] pair
pub fn create_table(levels: Vec<[usize; 2]>) -> ViewTable {
    assert!(!levels.is_empty());
    let mut views = vec![];
    for (i, l) in levels.into_iter().enumerate() {
        views.push(simple_comp_view(i, l[0], l[1]));
    }
    ViewTable::new(views)
}

pub fn table_permutation(table: ViewTable, idx: usize) -> Vec<ViewTable> {
    assert!(table.len() > 0);
    assert!(table.len() > idx);

    let mut ret = vec![];
    let next_vcant = if table.is_last(idx) {
        !0
    } else {
        table.vcant_num(idx + 1)
    };

    let max = next_vcant.min(table[idx].concurrency);
    for bp in gen_bp_pairs(table[idx].buf_cap, max) {
        let table = table.clone();
        table[idx].set_buffered_num(bp[0]);
        table[idx].set_processing_num(bp[1]);
        table[idx][0].set_processing_num(bp[1]);
        ret.push(table);
    }
    if idx != 0 {
        ret = ret.into_iter()
            .map(|t| table_permutation(t, idx - 1))
            .collect::<Vec<_>>()
            .concat();
    }
    ret
}

pub fn simple_comp_view(id: usize, buf_cap: usize, concurrency: usize) -> CompView {
    let mut exct_views = vec![];
    exct_views.push(ExctView::new(0, concurrency));
    CompView::new(id, buf_cap, exct_views)
}
