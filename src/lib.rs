#![feature(clone_closures, unboxed_closures, fn_traits)]

extern crate futures;
#[macro_use]
extern crate log;
extern crate worker;

mod fifo;
mod view;
mod pipeline;

pub use pipeline::*;

#[derive(Debug)]
pub struct NoComponent;

#[cfg(test)]
mod tests;
