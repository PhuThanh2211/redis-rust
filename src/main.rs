#![allow(unused_imports)]

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Redis Server listening here!!!");

    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    let store: Arc<Mutex<HashMap<String, (String, Option<Instant>)>>> = Arc::new(Mutex::new(HashMap::new()));

    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                let store = Arc::clone(&store);
                thread::spawn(move || {
                    println!("Accept new connection");

                    let mut buf = [0u8; 512];
                    loop {
                        let bytes_read = stream.read(&mut buf).unwrap();
                        if bytes_read == 0 {
                            break; // client closed the connection (EOF)
                        }

                        let received = String::from_utf8_lossy(&buf[..bytes_read]);
                        let parts: Vec<&str> = received.split("\r\n").collect();

                        let args: Vec<&str> = parts.iter()
                            .skip(1)
                            .filter(|s| !s.starts_with('$') && !s.is_empty())
                            .cloned()
                            .collect();

                        let command = args[0].to_uppercase();
                        match command.as_str() {
                            "PING" => {
                                stream.write_all(b"+PONG\r\n").unwrap();
                            }
                            "ECHO" => {
                                let argument = args[1];
                                let response = format!("${}\r\n{}\r\n", argument.len(), argument);
                                stream.write_all(response.as_bytes()).unwrap();
                            }
                            "SET" => {
                                let key = args[1].to_string();
                                let value = args[2].to_string();

                                let mut expiry: Option<Instant> = None;
                                if args.len() >= 5 && args[3].to_uppercase() == "PX" {
                                    let ms: u64 = args[4].parse().unwrap();
                                    expiry = Some(Instant::now() + Duration::from_millis(ms));
                                }

                                store.lock().unwrap().insert(key, (value, expiry));
                                stream.write_all(b"+OK\r\n").unwrap();
                            }
                            "GET" => {
                                let key = args[1];
                                let mut map = store.lock().unwrap();

                                let result = match map.get(key) {
                                    Some((value, Some(deadline)))
                                    if Instant::now() >= *deadline => {
                                        map.remove(key);        // lazily delete expired key
                                        None
                                    }
                                    Some((value, _)) => Some(value.clone()),
                                    None => None,
                                };

                                match result {
                                    Some(v) => {
                                        let response = format!("${}\r\n{}\r\n", v.len(), v);
                                        stream.write_all(response.as_bytes()).unwrap();
                                    }
                                    None => {
                                        // Null bulk string
                                        stream.write_all(b"$-1\r\n").unwrap();
                                    }
                                }
                            }
                            _ => {
                                stream.write_all(b"-ERR unknow command\r\n").unwrap();
                            }
                        }


                    }
                });
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
