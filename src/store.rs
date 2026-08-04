use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;

pub enum RedisValue {
    Str(String, Option<Instant>),
    List(Vec<String>),
}

pub type Store = Arc<Mutex<HashMap<String, RedisValue>>>;

pub fn new_store() -> Store {
    Arc::new(Mutex::new(HashMap::new()))
}