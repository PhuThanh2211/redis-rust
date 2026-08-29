use std::io::{BufReader, BufRead, Read, Write};
use std::net::TcpStream;
use crate::store::Store;
use crate::resp::read_command;
use crate::commands::dispatch;

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
                if let Err(e) = run(stream, store, my_port) {
                    println!("Replication handshake error: {e}");
                }
            }
            Err(e) => println!("Failed to connect to master {addr}: {e}"),
        }
    });
}

fn run(stream: TcpStream, store: Store, my_port: u16) -> std::io::Result<()> {
    let mut writer = stream.try_clone()?;
    let mut reader = BufReader::new(stream);

    // ---- Handshake ----
    send_command(&mut writer, &["PING"])?;
    read_line(&mut reader)?;                // +PONG

    let port_str= my_port.to_string();
    send_command(&mut writer, &["REPLCONF", "listening-port", &port_str])?;
    read_line(&mut reader)?;                // +OK

    send_command(&mut writer, &["REPLCONF", "capa", "psync2"])?;
    read_line(&mut reader)?;                // +OK

    send_command(&mut writer, &["PSYNC", "?", "-1"])?;
    read_line(&mut reader)?;                // +FULLRESYNC <id> 0

    // ---- Read the RDB snapshot: $<len>\r\n<bytes>  (NO trailing CRLF) ----
    read_rdb(&mut reader)?;

    // ---- Process propagated commands forever, WITHOUT replying ----
    loop {
        match read_command(&mut reader)? {
            Some(args) => {
                let cmd = String::from_utf8_lossy(&args[0]).to_uppercase();

                if cmd == "REPLCONF" {
                    let sub = args.get(1)
                        .map(|a| String::from_utf8_lossy(a).to_uppercase())
                        .unwrap_or_default();

                    if sub == "GETACK" {
                        send_command(&mut writer, &["REPLCONF", "ACK", "0"])?;
                    }

                } else {
                    // Normal propagated write -> apply, DON'T reply.
                    let _ = dispatch(&args, &store);
                }
            }
            None => break, // master closed the connection
        }
    }

    Ok(())
}

fn send_command(writer: &mut TcpStream, args: &[&str]) -> std::io::Result<()> {
    let mut out = format!("*{}\r\n", args.len());
    for a in args {
        out.push_str(&format!("${}\r\n{}\r\n", a.len(), a));
    }
    writer.write_all(out.as_bytes())
}

fn read_line<R: BufRead>(reader: &mut R) -> std::io::Result<String> {
    let mut line = String::new();
    reader.read_line(&mut line)?;
    Ok(line)
}

fn read_rdb<R: BufRead>(reader: &mut R) -> std::io::Result<Vec<u8>> {
    let mut header = String::new();
    reader.read_line(&mut header)?;        // "$88\r\n"
    let len: usize = header.trim_end()[1..]       // strip \r\n, drop '$'
        .parse()
        .map_err(|_| std::io::Error::new(
            std::io::ErrorKind::InvalidData, "bad rdb len"))?;

    let mut buf = vec![0u8; len];
    reader.read_exact(&mut buf)?;       // exactly <len> bytes, no CRLF after
    Ok(buf)
}