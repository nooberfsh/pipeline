use std::collections::VecDeque;

#[derive(Debug)]
pub struct Fifo<T> {
    q: VecDeque<T>,
}

impl<T> Fifo<T> {
    pub fn new() -> Self {
        Fifo {
            q: VecDeque::new(),
        }
    }

    pub fn push(&mut self, t: T) {
        self.q.push_back(t);
    }

    pub fn pop(&mut self) -> Option<T> {
        self.q.pop_front()
    }

    pub fn len(&self) -> usize {
        self.q.len()
    }
}
