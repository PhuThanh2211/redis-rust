use std::collections::hash_map::Entry;
use std::time::{Duration, Instant};

use crate::resp::Resp;
use crate::store::{RedisValue, Store};

pub fn dispatch(args: &[Vec<u8>], store: &Store) -> Resp {
    if args.is_empty() {
        return Resp::Error("ERR empty command".into());
    }

    let cmd = String::from_utf8_lossy(&args[0]).to_uppercase();
    match cmd.as_str() {
        "PING" => Resp::Simple("PONG".into()),
        "ECHO" => match args.get(1) {
            Some(a) => Resp::Bulk(Some(a.clone())),
            None => wrong_args("echo"),
        }
        "SET" => cmd_set(args, store),
        "GET" => cmd_get(args, store),
        "RPUSH" => cmd_rpush(args, store),
        "LPUSH" => cmd_lpush(args, store),
        "LRANGE" => cmd_lrange(args, store),
        "LLEN" => cmd_llen(args, store),
        other => Resp::Error(format!("ERR unknown command '{other}'")),
    }
}

fn cmd_set(args: &[Vec<u8>], store: &Store) -> Resp {
    if args.len() < 3 {
        return wrong_args("set");
    }

    let key = as_str(&args[1]);
    let value = as_str(&args[2]);

    let mut expiry: Option<Instant> = None;
    if args.len() >= 5 {
        let opt = as_str(&args[3]).to_uppercase();
        let amount: Result<u64, _> = as_str(&args[4]).parse();
        match (opt.as_str(), amount) {
            ("PX", Ok(ms)) => expiry = Some(Instant::now() + Duration::from_millis(ms)),
            ("EX", Ok(secs)) => expiry = Some(Instant::now() + Duration::from_secs(secs)),
            _ => return Resp::Error("ERR invalid expire option".into()),
        }
    }

    store.lock().unwrap().insert(key, RedisValue::Str(value, expiry));

    Resp::Simple("OK".into())
}

fn cmd_get(args: &[Vec<u8>], store: &Store) -> Resp {
    if args.len() < 2 {
        return wrong_args("get");
    }

    let key = as_str(&args[1]);
    let mut map = store.lock().unwrap();

    match map.get(&key) {
        Some(RedisValue::Str(_, Some(deadline))) if Instant::now() >= *deadline => {
            map.remove(&key); // Lazily delete expired key
            Resp::Bulk(None)
        }
        Some(RedisValue::Str(value, _)) => Resp::Bulk(Some(value.clone().into_bytes())),
        Some(RedisValue::List(_)) => wrong_type(),
        None => Resp::Bulk(None),
    }
}

fn cmd_rpush(args: &[Vec<u8>], store: &Store) -> Resp {
    if args.len() < 3 {
        return wrong_args("rpush");
    }

    let key = as_str(&args[1]);
    let mut map = store.lock().unwrap();

    match map.entry(key).or_insert_with(|| RedisValue::List(Vec::new())) {
        RedisValue::List(list) => {
            for element in &args[2..] {
                list.push(as_str(element));
            }
            Resp::Integer(list.len() as i64)
        }
        _ => wrong_type(),
    }
}

fn cmd_lpush(args: &[Vec<u8>], store: &Store) -> Resp {
    if args.len() < 3 {
        return wrong_args("lpush");
    }

    let key = as_str(&args[1]);
    let mut map = store.lock().unwrap();

    match map.entry(key).or_insert_with(|| RedisValue::List(Vec::new())) {
        RedisValue::List(list) => {
            for element in &args[2..] {
                list.insert(0, as_str(element)); // prepend -> reverses input order
            }
            Resp::Integer(list.len() as i64)
        }
        _ => wrong_type(),
    }
}

fn cmd_lrange(args: &[Vec<u8>], store: &Store) -> Resp {
    if args.len() < 4 {
        return wrong_args("lrange");
    }

    let key = as_str(&args[1]);
    let start: i64 = match as_str(&args[2]).parse() {
        Ok(n) => n,
        Err(_) => return Resp::Error("ERR value is not an integer or out of range".into())
    };

    let stop: i64 = match as_str(&args[3]).parse() {
        Ok(n) => n,
        Err(_) => return Resp::Error("ERR value is not an integer or out of range".into())
    };

    let map = store.lock().unwrap();
    match map.get(&key) {
        Some(RedisValue::List(list)) => {
            let len = list.len() as i64;
            let start = normalize(start, len);
            let stop = normalize(stop, len).min(len - 1);

            if len == 0 || start > stop || start >= len {
                return Resp::Array(vec![]);
            }

            let slice = &list[start as usize..=stop as usize];
            Resp::Array(
                slice.iter().map(|s| Resp::Bulk(Some(s.clone().into_bytes()))).collect(),
            )
        },
        Some(RedisValue::Str(_, _)) => wrong_type(),
        None => Resp::Array(vec![]),
    }
}

fn cmd_llen(args: &[Vec<u8>], store: &Store) -> Resp {
    if args.len() < 2 {
        return wrong_args("llen");
    }

    let key = as_str(&args[1]);
    let map = store.lock().unwrap();

    match map.get(&key) {
        Some(RedisValue::List(list)) => Resp::Integer(list.len() as i64),
        _ => Resp::Integer(0),
    }
}

/// Clamp a possibly-negative index into `[0, len]`.
fn normalize(idx: i64, len: i64) -> i64 {
    if idx < 0 {
        (len + idx).max(0)
    } else {
        idx
    }
}

fn wrong_args(cmd: &str) -> Resp {
    Resp::Error(format!("ERR wrong number of arguments for '{cmd}' command"))
}

fn as_str(b: &[u8]) -> String {
    String::from_utf8_lossy(b).into_owned()
}

fn wrong_type() -> Resp {
    Resp::Error("WRONGTYPE Operation against a key holding the wrong kind of value".into())
}