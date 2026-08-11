use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Instant;

pub struct StreamEntry {
    pub id: String,                     // e.g. "1526919030474-0"
    pub fields: Vec<(String, String)>,
}

pub enum RedisValue {
    Str(String, Option<Instant>),
    List(Vec<String>),
    Stream(Vec<StreamEntry>),
}

pub struct Inner {
    pub map: HashMap<String, RedisValue>,
    pub waiters: HashMap<String, VecDeque<u64>>, // FIFO tickets per key
    pub next_ticket: u64,
}

impl Inner {
    /// Remove a specific ticket from a key's waiter queue (used on timeout).
    pub fn remove_ticket(&mut self, key: &str, ticket: u64) {
        if let Some(q) = self.waiters.get_mut(key) {
            q.retain(|&t| t != ticket); // remove my ticket wherever it is
            if q.is_empty() {
                self.waiters.remove(key);
            }
        }
    }
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