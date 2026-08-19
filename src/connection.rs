use std::io::{BufReader, Write};
use std::net::TcpStream;

use crate::commands::dispatch;
use crate::resp::{read_command, Resp};
use crate::store::Store;

struct ConnState {
    in_multi: bool,
    queue: Vec<Vec<Vec<u8>>>,
    watched: Vec<(String, u64)>, // keys being watched
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

            let guard = store.inner.lock().unwrap();

            for k in &args[1..] {
                let key = String::from_utf8_lossy(k).into_owned();
                let ver = guard.version_of(&key);
                state.watched.push((key, ver));
            }

            Resp::Simple("OK".into())
        }
        "UNWATCH" => {
            if args.len() > 1 {
                return Resp::Error("ERR wrong number of arguments for 'unwatch' command".into());
            }

            if !state.watched.is_empty() {
                state.watched.clear();
            }

            Resp::Simple("OK".into())
        }
        "EXEC" => {
            if !state.in_multi {
                return Resp::Error("ERR EXEC without MULTI".into());
            }

            state.in_multi = false;

            // Optimistic-locking check:
            let dirty = {
                let guard = store.inner.lock().unwrap();
                state.watched.iter().any(|(k, v)| guard.version_of(k) != *v)
            };

            let queued = std::mem::take(&mut state.queue);
            state.watched.clear(); // clear watch state regardless of outcome

            if dirty {
                return Resp::NullArray; // aborted -> *-1\r\n, queue discarded
            }

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