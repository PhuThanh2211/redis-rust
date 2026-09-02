use std::collections::{HashMap, VecDeque};
use std::net::TcpStream;
use std::sync::{Arc, Condvar, Mutex};
use std::sync::atomic::AtomicUsize;
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

pub struct ReplicaConn {
    pub stream: TcpStream,  // write handle (propagation + GETACK)
    pub ack: usize,         // latest offset this replica has acknowledged
}

pub struct Inner {
    pub map: HashMap<String, RedisValue>,
    pub waiters: HashMap<String, VecDeque<u64>>, // FIFO tickets per key
    pub next_ticket: u64,
    pub versions: HashMap<String, u64>, // bumped whenever a key is modified
}

impl Inner {
    pub fn touch (&mut self, key: &str) {
        *self.versions.entry(key.to_string()).or_insert(0) += 1;
    }
    pub fn version_of(&self, key: &str) -> u64 {
        self.versions.get(key).copied().unwrap_or(0)
    }
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
    pub replica_of: Option<(String, u16)>, // Some((host, port)) if this is a replica
    pub master_repl_id: String,
    pub replicas: Mutex<Vec<ReplicaConn>>, // write handles to connected replicas
    pub master_offset: AtomicUsize,     // bytes propagated on the repl stream
    pub ack_cv: Condvar,                // notified when a replica ACKs
}

impl Db {
    pub fn is_replica(&self) -> bool {
        self.replica_of.is_some()
    }
}

pub type Store = Arc<Db>;

pub fn new_store(replica_of: Option<(String, u16)>) -> Store {
    Arc::new(Db {
        inner: Mutex::new(Inner {
            map: HashMap::new(),
            waiters: HashMap::new(),
            next_ticket: 0,
            versions: HashMap::new(),
        }),
        on_push: Condvar::new(),
        replica_of,
        master_repl_id: "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".to_string(),
        replicas: Mutex::new(Vec::new()),
        master_offset: AtomicUsize::new(0),
        ack_cv: Condvar::new(),
    })
}