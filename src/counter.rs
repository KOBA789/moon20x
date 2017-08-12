use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

pub struct Counter {
  number: AtomicUsize,
}