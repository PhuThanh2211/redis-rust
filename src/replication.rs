use std::io::{Read, Write};
use std::net::TcpStream;
use crate::store::Store;

/// Connect to the master and perform the replication handshake
pub fn start_handshake(store: Store, my_port: u16) {
    let (host, port) = match &store.replica_of {
        Some(hp) => hp.clone(),
        None => return, // not a replica -> nothing to do
    };

    std::thread::spawn(move || {
        let addr = format!("{host}:{port}");
        match TcpStream::connect(&addr) {
            Ok(mut stream) => {
                if let Err(e) = handshake(&mut stream, my_port) {
                    println!("Replication handshake error: {e}");
                }
            }
            Err(e) => println!("Failed to connect to master {addr}: {e}"),
        }
    });

    fn handshake(stream: &mut TcpStream, my_port: u16) -> std::io::Result<()> {
        // Step 1: PING
        send_command(stream, &["PING"])?;
        let _ = read_reply(stream)?; // expect +PONG

        // Step 2: REPLCONF (twice)
        let port_str = my_port.to_string();
        send_command(stream, &["REPLCONF", "listening-port", &port_str])?;
        let _ = read_reply(stream)?; // expect +OK

        send_command(stream, &["REPLCONF", "capa", "psync2"])?;
        let _ = read_reply(stream)?; // expect +OK

        // Step 3: PSYNC
        send_command(stream, &["PSYNC", "?", "-1"])?;
        let _ = read_reply(stream)?; // expect +FULLRESYNC <REPL_ID> 0

        Ok(())
    }

    /// Encode args as a RESP array of bulk strings and send.
    fn send_command(stream: &mut TcpStream, args: &[&str]) -> std::io::Result<()> {
        let mut out = format!("*{}\r\n", args.len());
        for a in args {
            out.push_str(&format!("${}\r\n{}\r\n", a.len(), a));
        }

        stream.write_all(out.as_bytes())
    }

    fn read_reply(stream: &mut TcpStream) -> std::io::Result<String> {
        let mut buf = [0u8; 512];
        let n = stream.read(&mut buf)?;
        Ok(String::from_utf8_lossy(&buf[..n]).into_owned())
    }
}