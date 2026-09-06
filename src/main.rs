#![allow(unused_imports)]

mod resp;
mod store;
mod commands;
mod connection;
mod replication;
mod rdb;
mod config;

use std::net::TcpListener;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use crate::config::Config;
use crate::store::{new_store, RedisValue, Store};

fn main() {
    let config = Config::from_args();
    println!("Redis Server listening here with port {}!!!", config.port);
    let addr = format!("127.0.0.1:{}", config.port);

    if config.appendonly == "yes" {
        if let Err(e) = std::fs::create_dir_all(config.aof_dir()) {
            println!("Failed to create AOF directory: {e}");
        }
        if let Err(e) = std::fs::File::create(config.aof_file()) {
            println!("Failed to create AOF file: {e}");
        }
        let manifest_line = format!("file {}.1.incr.aof seq 1 type i\n", config.appendfilename);
        if let Err(e) = std::fs::write(config.aof_manifest(), manifest_line) {
            println!("Failed to create manifest file: {e}");
        }
    }

    let store = new_store(config);
    load_rdb(&store);

    // If we're a replica, connect to the master and start the handshake.
    replication::start_handshake(store.clone(), store.config.port);

    let listener = TcpListener::bind(&addr).unwrap();
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let store = store.clone();
                thread::spawn(move || {
                    if let Err(e) = connection::handle(stream, store) {
                        println!("Connection Error: {e}");
                    }
                });
            }
            Err(e) => println!("error: {e}")
        }
    }
}

/// Load keys from the RDB file into the store, skipping already-expired ones.
fn load_rdb(store: &Store) {
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;

    for (key, value, expiry) in rdb::load(&store.config.dir, &store.config.dbfilename) {
        let deadline = match expiry {
            Some(exp_ms) if exp_ms <= now_ms => continue, // already expired
            Some(exp_ms) => Some(Instant::now() + Duration::from_millis(exp_ms - now_ms)),
            None => None,
        };
        let mut guard = store.inner.lock().unwrap();
        guard.map.insert(key, RedisValue::Str(value, deadline));
    }
}
