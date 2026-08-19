#![allow(unused_imports)]

mod resp;
mod store;
mod commands;
mod connection;

use std::net::TcpListener;
use std::thread;

use crate::store::new_store;

fn main() {
    let port = parse_port();
    println!("Redis Server listening here with port {port}!!!");
    let addr = format!("127.0.0.1:{port}");

    let listener = TcpListener::bind(&addr).unwrap();
    let store = new_store();

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

fn parse_port() -> u16 {
    let args: Vec<String> = std::env::args().collect();
    let mut port = 6379; // default port
    let mut i = 1;
    while i < args.len() {
        if args[i] == "--port" && i + 1 < args.len() {
            if let Ok(p) = args[i + 1].parse() {
                port = p
            }
            i += 2;
        } else {
            i += 1;
        }
    }
    port
}
