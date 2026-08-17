use std::io::{BufReader, Write};
use std::net::TcpStream;

use crate::commands::dispatch;
use crate::resp::{read_command, Resp};
use crate::store::Store;

struct ConnState {
    in_multi: bool,
}

pub fn handle(stream: TcpStream, store: Store) -> std::io::Result<()> {
    println!("Accept New Connection");
    let mut writer = stream.try_clone()?;
    let mut reader = BufReader::new(stream);

    let mut state = ConnState { in_multi: false };

    loop {
        match read_command(&mut reader)? {
            Some(args) => {
                let reply = handle_command(&args, &store, &mut state);
                writer.write_all(&reply.encode())?;
            },
            None => break, // Client closed the connection (EOF)
        }
    }

    Ok(())
}

fn handle_command(args: &[Vec<u8>], store: &Store, state: &mut ConnState) -> Resp {
    if args.is_empty() {
        return Resp::Error("ERR empty command".into());
    }

    let cmd = String::from_utf8_lossy(&args[0]).to_uppercase();

    match cmd.as_str() {
        "MULTI" => {
            state.in_multi = true;
            Resp::Simple("OK".into())
        },
        "EXEC" => {
            if !state.in_multi {
                return Resp::Error("ERR EXEC without MULTI".into());
            }

            state.in_multi = false;
            Resp::Array(vec![])
        }
        // All non-transaction commands go to the stateless dispatcher.
        _ => dispatch(args, store),
    }
}