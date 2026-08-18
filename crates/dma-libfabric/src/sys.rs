//! Raw bindgen FFI bindings to libfabric, including wrappers for its `static inline` functions.
#![allow(non_upper_case_globals, non_camel_case_types, non_snake_case)]
#![allow(dead_code, unsafe_op_in_unsafe_fn, clippy::all)]

include!(concat!(env!("OUT_DIR"), "/bindings.rs"));

#[cfg(test)]
mod tests;
