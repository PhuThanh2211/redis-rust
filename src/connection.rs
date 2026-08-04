use std::io::{BufReader, Write};
use std::net::TcpStream;

use crate::commands::dispatch;
use crate::resp::read_command;
use crate::store::Store;

pub fn handle(stream: TcpStream, store: Store) -> std::io::Result<()> {
    println!("Accept New Connection");
    let mut writer = stream.try_clone()?;
    let mut reader = BufReader::new(stream);

    loop {
        match read_command(&mut reader)? {
            Some(args) => {
                let reply = dispatch(&args, &store);
                writer.write_all(&reply.encode());
            },
            None => break, // Client closed the connection (EOF)
        }
    }

    Ok(())
}