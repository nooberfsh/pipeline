#![feature(clone_closures, unboxed_closures, fn_traits)]

extern crate futures;
#[macro_use]
extern crate log;
extern crate worker;

mod fifo;
mod view;
pub mod pipeline;

#[derive(Debug)]
pub enum Error {
    NoComponent,
    NoTaskFinishHandle,
}
