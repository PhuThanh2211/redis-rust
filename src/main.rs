#![allow(unused_imports)]

mod resp;
mod store;
mod commands;
mod connection;
mod replication;
mod rdb;

use std::net::TcpListener;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use crate::store::{new_store, RedisValue};

fn main() {
    let (port, replica_of, dir, dbfilename,
        appendonly, appenddirname, appendfilename, appendfsync) = parse_port();
    println!("Redis Server listening here with port {port}!!!");
    let addr = format!("127.0.0.1:{port}");

    let store = new_store(replica_of, dir.clone(), dbfilename.clone(), appendonly, appenddirname, appendfilename, appendfsync);
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    // Load existing RDB data (if any) into the store.
    for (key, value, expiry) in rdb::load(&dir, &dbfilename) {
        let deadline = match expiry {
            Some(exp_ms) => {
                if exp_ms <= now_ms {
                    continue; // already expired -> don't load it at all
                }
                Some(Instant::now() + Duration::from_millis(exp_ms - now_ms))
            }
            None => None,
        };

        let mut guard = store.inner.lock().unwrap();
        guard.map.insert(key, RedisValue::Str(value, deadline));
    }

    // If we're a replica, connect to the master and start the handshake.
    replication::start_handshake(store.clone(), port);

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

fn parse_port() -> (u16, Option<(String, u16)>, String, String, String, String, String, String) {
    // Master Server: cargo run
    // Slave Server: cargo run -- --port <PORT> --replicaof "<MASTER_HOST> <MASTER_PORT>"
    // Client: redis-cli -p <PORT> INFO replication
    let args: Vec<String> = std::env::args().collect();
    let mut port = 6379; // default port
    let mut replica_of: Option<(String, u16)> = None;
    let mut dir = String::new();
    let mut dbfilename = String::new();

    // AOF options pre-loaded with defaults:
    let mut appendonly = "no".to_string();
    let mut appenddirname = "appendonlydir".to_string();
    let mut appendfilename = "appendonly.aof".to_string();
    let mut appendfsync = "everysec".to_string();


    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--port" if i + 1 < args.len() => {
                if let Ok(p) = args[i + 1].parse() {
                    port = p;
                }

                i += 2;
            }
            "--replicaof" if i + 1 < args.len() => {
                let mut parts = args[i + 1].split_whitespace();
                if let (Some(h), Some(p)) = (parts.next(), parts.next()) {
                    if let Ok(port_num) = p.parse() {
                        replica_of = Some((h.to_string(), port_num));
                    }
                }

                i += 2;
            }
            "--dir" if i + 1 < args.len()            => { dir = args[i+1].clone(); i += 2; }
            "--dbfilename" if i + 1 < args.len()     => { dbfilename = args[i+1].clone(); i += 2; }
            "--appendonly" if i + 1 < args.len()     => { appendonly = args[i+1].clone(); i += 2; }
            "--appenddirname" if i + 1 < args.len()  => { appenddirname = args[i+1].clone(); i += 2; }
            "--appendfilename" if i + 1 < args.len() => { appendfilename = args[i+1].clone(); i += 2; }
            "--appendfsync" if i + 1 < args.len()    => { appendfsync = args[i+1].clone(); i += 2; }
            _ => i += 1,
        }
    }

    if dir.is_empty() {
        dir = std::env::current_dir()
            .map(|p| p.to_string_lossy().into_owned())
            .unwrap_or_default();
    }

    (port, replica_of, dir, dbfilename, appendonly, appenddirname, appendfilename, appendfsync)
}
