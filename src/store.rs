use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Instant;

pub enum RedisValue {
    Str(String, Option<Instant>),
    List(Vec<String>),
}

pub struct Inner {
    pub map: HashMap<String, RedisValue>,
    pub waiters: HashMap<String, VecDeque<u64>>, // FIFO tickets per key
    pub next_ticket: u64,
}

pub struct Db {
    pub inner: Mutex<Inner>,
    pub on_push: Condvar,
}

pub type Store = Arc<Db>;

pub fn new_store() -> Store {
    Arc::new(Db {
        inner: Mutex::new(Inner {
            map: HashMap::new(),
            waiters: HashMap::new(),
            next_ticket: 0
        }),
        on_push: Condvar::new()
    })
}