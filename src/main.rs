#![allow(unused_imports)]
use std::io::{Read, Write};
use std::net::TcpListener;
use std::thread;

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Redis Server listening here!!!");

    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
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
