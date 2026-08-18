use std::io::{BufReader, Write};
use std::net::TcpStream;

use crate::commands::dispatch;
use crate::resp::{read_command, Resp};
use crate::store::Store;

struct ConnState {
    in_multi: bool,
    queue: Vec<Vec<Vec<u8>>>,
    watched: Vec<String>, // keys being watched
}

pub fn handle(stream: TcpStream, store: Store) -> std::io::Result<()> {
    println!("Accept New Connection");
    let mut writer = stream.try_clone()?;
    let mut reader = BufReader::new(stream);

    let mut state = ConnState {
        in_multi: false,
        queue: Vec::new(),
        watched: Vec::new(),
    };

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
        }
        "WATCH" => {
            if state.in_multi {
                return Resp::Error("ERR WATCH inside MULTI is not allowed".into());
            }

            if args.len() < 2 {
                return Resp::Error("ERR wrong number of arguments for 'watch' command".into());
            }

            for k in &args[1..] {
                state.watched.push(String::from_utf8_lossy(k).into_owned());
            }

            Resp::Simple("OK".into())
        }
        "EXEC" => {
            if !state.in_multi {
                return Resp::Error("ERR EXEC without MULTI".into());
            }

            state.in_multi = false;
            let queued = std::mem::take(&mut state.queue);
            let mut replies: Vec<Resp> = Vec::with_capacity(queued.len());

            for cmd_args in &queued {
                replies.push(dispatch(cmd_args, store));
            }

            Resp::Array(replies)
        }
        "DISCARD" => {
            if !state.in_multi {
                return Resp::Error("ERR DISCARD without MULTI".into());
            }

            state.in_multi = false;
            state.queue.clear();
            Resp::Simple("OK".into())
        }
        _ if state.in_multi => {
            // queue the raw command; don't execute or touch the DB
            state.queue.push(args.to_vec());
            Resp::Simple("QUEUED".into())
        }
        // All non-transaction commands go to the stateless dispatcher.
        _ => dispatch(args, store),
    }
}