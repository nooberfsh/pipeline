#![feature(clone_closures, unboxed_closures, fn_traits)]

extern crate futures;
#[macro_use]
extern crate log;
extern crate worker;

mod fifo;
mod view;
mod pipeline;

pub use pipeline::*;

/// An error indicate that there is no component.
#[derive(Debug)]
pub struct NoComponent;

#[cfg(test)]
mod tests_util;
