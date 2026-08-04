#![allow(unused_imports)]

mod resp;
mod store;
mod commands;
mod connection;

use std::net::TcpListener;
use std::thread;

use crate::store::new_store;

fn main() {
    println!("Redis Server listening here!!!");

    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
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
