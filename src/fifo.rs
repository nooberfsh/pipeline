use std::collections::VecDeque;

/// A `First In, First Out` queue.
#[derive(Debug)]
pub struct Fifo<T> {
    q: VecDeque<T>,
}

impl<T> Fifo<T> {
    pub fn new() -> Self {
        Fifo { q: VecDeque::new() }
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

impl<T> Default for Fifo<T> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::Fifo;

    #[test]
    fn test_fifo() {
        let mut fifo = Fifo::new();
        assert_eq!(fifo.len(), 0);

        fifo.push(1);
        fifo.push(2);
        assert_eq!(fifo.len(), 2);

        assert_eq!(fifo.pop(), Some(1));
        assert_eq!(fifo.pop(), Some(2));
        assert_eq!(fifo.pop(), None);
    }
}
