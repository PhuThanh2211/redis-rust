#![allow(unused_imports)]

use std::collections::HashMap;
use std::fmt::format;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

enum RedisValue {
    Str(String, Option<Instant>),
    List(Vec<String>),
}

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Redis Server listening here!!!");

    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    let store: Arc<Mutex<HashMap<String, RedisValue>>> = Arc::new(Mutex::new(HashMap::new()));

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
                                if args.len() >= 5 {
                                    if args[3].to_uppercase() == "PX" {
                                        let ms: u64 = args[4].parse().unwrap();
                                        expiry = Some(Instant::now() + Duration::from_millis(ms));
                                    } else if args[3].to_uppercase() == "EX" {
                                        let secs: u64 = args[4].parse().unwrap();
                                        expiry = Some(Instant::now() + Duration::from_secs(secs));
                                    }
                                }

                                store.lock().unwrap().insert(key, RedisValue::Str(value, expiry));
                                stream.write_all(b"+OK\r\n").unwrap();
                            }
                            "GET" => {
                                let key = args[1];
                                let mut map = store.lock().unwrap();

                                let result = match map.get(key) {
                                    Some(RedisValue::Str(value, Some(deadline)))
                                    if Instant::now() >= *deadline => {
                                        map.remove(key);        // lazily delete expired key
                                        None
                                    }
                                    Some(RedisValue::Str(value, _)) => Some(value.clone()),
                                    Some(RedisValue::List(_)) => {
                                        stream.write_all(b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n").unwrap();
                                        continue;
                                    },
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
                            "RPUSH" => {
                                let key = args[1].to_string();

                                let mut map = store.lock().unwrap();

                                match map.entry(key).or_insert_with(|| RedisValue::List(Vec::new())) {
                                    RedisValue::List(list) => {
                                        for element in &args[2..] {
                                            list.push(element.to_string());
                                        }
                                        stream.write_all(format!(":{}\r\n", list.len()).as_bytes()).unwrap();
                                    }
                                    _ => {
                                        stream.write_all(b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n").unwrap();
                                    }
                                }
                            }
                            "LRANGE" => {
                                let key = args[1];
                                let start_idx: usize = args[2].parse().unwrap();
                                let mut stop_idx: usize = args[3].parse().unwrap();

                                let map = store.lock().unwrap();

                                let elements: Vec<String> = match map.get(key) {
                                    Some(RedisValue::List(list)) => {
                                        let len = list.len();
                                        if stop_idx >= len {
                                            stop_idx = len - 1;
                                        }

                                        if start_idx >= len || start_idx > stop_idx {
                                            Vec::new()
                                        } else {
                                            list[start_idx..=stop_idx].to_vec()
                                        }
                                    }
                                    _ => Vec::new()
                                };

                                drop(map);
                                let mut response = format!("*{}\r\n", elements.len());
                                for e in &elements {
                                    response.push_str(&format!("${}\r\n{}\r\n", e.len(), e));
                                }
                                stream.write_all(response.as_bytes()).unwrap();
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
