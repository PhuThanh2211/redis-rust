use std::io::{BufReader, Write};
use std::net::TcpStream;
use std::sync::atomic::Ordering;
use crate::commands::dispatch;
use crate::resp::{read_command, Resp};
use crate::store::{ReplicaConn, Store};

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

    let mut my_replica_index: Option<usize> = None;

    loop {
        match read_command(&mut reader)? {
            Some(args) => {
                let cmd = String::from_utf8_lossy(&args[0]).to_uppercase();
                let sub = args.get(1)
                    .map(|a| String::from_utf8_lossy(a).to_uppercase())
                    .unwrap_or_default();

                // A replica reporting its offset: record it, send NO reply
                if cmd == "REPLCONF" && sub == "ACK" {
                    if let (Some(i), Some(off_byte)) = (my_replica_index, args.get(2)) {
                        if let Ok(off) = String::from_utf8_lossy(off_byte).parse::<usize>() {
                            let mut reps = store.replicas.lock().unwrap();
                            if let Some(r) = reps.get_mut(i) {
                                r.ack = off;
                            }

                            drop(reps);
                            store.ack_cv.notify_all();
                        }
                    }
                    continue; // no reply, no propagation
                }

                let reply = handle_command(&args, &store, &mut state);
                writer.write_all(&reply.encode())?;

                // After PSYNC + RDB, this connection becomes a replica link
                if cmd == "PSYNC" {
                    let mut reps = store.replicas.lock().unwrap();
                    my_replica_index = Some(reps.len());
                    reps.push(ReplicaConn{
                        stream: writer.try_clone()?,
                        ack: 0
                    });
                }

                // Propagate writes to all replicas and advance the master offset.
                if is_write_command(&cmd) {
                    let encoded = encode_command(&args);
                    {
                        let mut reps = store.replicas.lock().unwrap();
                        for r in reps.iter_mut() {
                            let _ = r.stream.write_all(&encoded);
                        }
                    }
                    store.master_offset.fetch_add(encoded.len(), Ordering::SeqCst);
                }

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

            state.watched.clear();
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
            state.watched.clear();
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

fn is_write_command(cmd: &str) -> bool {
    matches!(cmd, "SET" | "DEL" | "INCR" | "RPUSH" | "LPUSH" | "LPOP" | "XADD")
}

fn encode_command(args: &[Vec<u8>]) -> Vec<u8> {
    Resp::Array(args.iter().map(|a| Resp::Bulk(Some(a.clone()))).collect()).encode()
}