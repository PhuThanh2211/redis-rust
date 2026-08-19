use std::collections::hash_map::Entry;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::resp::Resp;
use crate::store::{RedisValue, Store, StreamEntry};

enum IdSpec {
    Explicit(u64, u64), // ms-seq
    AutoSeq(u64),       // ms-*
    AutoAll,            // *
}

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
        "LPOP" => cmd_lpop(args, store),
        "BLPOP" => cmd_blpop(args, store),
        "TYPE" => cmd_type(args, store),
        "XADD" => cmd_xadd(args, store),
        "XRANGE" => cmd_xrange(args, store),
        "XREAD" => cmd_xread(args, store),
        "INCR" => cmd_incr(args, store),
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

    let mut guard = store.inner.lock().unwrap();
    guard.map.insert(key.clone(), RedisValue::Str(value, expiry));
    guard.touch(&key);

    Resp::Simple("OK".into())
}

fn cmd_get(args: &[Vec<u8>], store: &Store) -> Resp {
    if args.len() < 2 {
        return wrong_args("get");
    }

    let key = as_str(&args[1]);
    let mut guard = store.inner.lock().unwrap();

    match guard.map.get(&key) {
        Some(RedisValue::Str(_, Some(deadline))) if Instant::now() >= *deadline => {
            guard.map.remove(&key); // Lazily delete expired key
            Resp::Bulk(None)
        }
        Some(RedisValue::Str(value, _)) => Resp::Bulk(Some(value.clone().into_bytes())),
        Some(RedisValue::List(_)) | Some(RedisValue::Stream(_)) => wrong_type(),
        None => Resp::Bulk(None),
    }
}

fn cmd_rpush(args: &[Vec<u8>], store: &Store) -> Resp {
    if args.len() < 3 {
        return wrong_args("rpush");
    }

    let key = as_str(&args[1]);
    let mut guard = store.inner.lock().unwrap();

    let len = match guard.map.entry(key).or_insert_with(|| RedisValue::List(Vec::new())) {
        RedisValue::List(list) => {
            for element in &args[2..] {
                list.push(as_str(element));
            }
            list.len() as i64
        }
        _ => return wrong_type(),
    };

    // `list` borrow ends here; safe to signal blocked BLPOP waiters.
    store.on_push.notify_all();

    Resp::Integer(len)
}

fn cmd_lpush(args: &[Vec<u8>], store: &Store) -> Resp {
    if args.len() < 3 {
        return wrong_args("lpush");
    }

    let key = as_str(&args[1]);
    let mut guard = store.inner.lock().unwrap();

    let len = match guard.map.entry(key).or_insert_with(|| RedisValue::List(Vec::new())) {
        RedisValue::List(list) => {
            for element in &args[2..] {
                list.insert(0, as_str(element)); // prepend -> reverses input order
            }
            list.len() as i64
        }
        _ => return wrong_type(),
    };

    store.on_push.notify_all();

    Resp::Integer(len)
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

    let guard = store.inner.lock().unwrap();
    match guard.map.get(&key) {
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
        Some(RedisValue::Str(_, _)) | Some(RedisValue::Stream(_)) => wrong_type(),
        None => Resp::Array(vec![]),
    }
}

fn cmd_llen(args: &[Vec<u8>], store: &Store) -> Resp {
    if args.len() < 2 {
        return wrong_args("llen");
    }

    let key = as_str(&args[1]);
    let guard = store.inner.lock().unwrap();

    match guard.map.get(&key) {
        Some(RedisValue::List(list)) => Resp::Integer(list.len() as i64),
        _ => Resp::Integer(0),
    }
}

fn cmd_lpop(args: &[Vec<u8>], store: &Store) -> Resp {
    if args.len() < 2 {
        return wrong_args("lpop");
    }

    let key = as_str(&args[1]);

    // Optional count argument: LPOP key [count]
    let count: Option<usize> = match args.get(2) {
        Some(raw) => match as_str(raw).parse::<i64>() {
            Ok(n) if n >= 0 => Some(n as usize),
            _ => return Resp::Error("ERR value is out of range, must be positive".into()),
        },
        None => None
    };

    let mut guard = store.inner.lock().unwrap();

    let result = match guard.map.get_mut(&key) {
        Some(RedisValue::List(list)) => {
            if list.is_empty() {
                return match count {
                    Some(_) => Resp::Array(vec![]),
                    None => Resp::Bulk(None),
                };
            }

            match count {
                None => {
                    // single-element form
                    Resp::Bulk(Some(list.remove(0).into_bytes()))
                }
                Some(n) => {
                    let n = n.min(list.len());
                    let popped: Vec<Resp> = list
                        .drain(0..n)
                        .map(|s| Resp::Bulk(Some(s.into_bytes())))
                        .collect();
                    Resp::Array(popped)
                }
            }
        }
        Some(RedisValue::Str(_, _)) | Some(RedisValue::Stream(_)) => return wrong_type(),
        None => {
            return match count {
                Some(_) => Resp::Array(vec![]),
                None => Resp::Bulk(None),
            };
        }
    };

    // borrow of `list` is over here, so we can touch `map` again
    if matches!(guard.map.get(&key), Some(RedisValue::List(l)) if l.is_empty()) {
        guard.map.remove(&key);
    }

    result
}

fn cmd_blpop(args: &[Vec<u8>], store: &Store) -> Resp {
    if args.len() < 3 {
        return wrong_args("blpop");
    }

    let key = as_str(&args[1]);
    // Timeout is always "0" (block forever) in this stage; parse to validate.
    let timeout_secs: f64 = match as_str(&args[2]).parse() {
        Ok(t) if t >= 0.0 => t,
        _ => return Resp::Error("ERR timeout is not a float or out of range, must be positive".into()),
    };

    let deadline: Option<Instant> = if timeout_secs == 0.0 {
        None
    } else {
        Some(Instant::now() + Duration::from_secs_f64(timeout_secs))
    };

    let mut guard = store.inner.lock().unwrap();

    // Take a FIFO ticket so the longest-waiting client is served first.
    let ticket = guard.next_ticket;
    guard.next_ticket += 1;
    guard.waiters.entry(key.clone()).or_default().push_back(ticket);

    loop {
        let inner = &mut *guard; // reborrow so map & waiters are disjoint fields

        let is_front = inner.waiters.get(&key)
            .and_then(|q| q.front())
            .map_or(false, |&t| t == ticket);

        if is_front {
            if let Some(RedisValue::List(list)) = inner.map.get_mut(&key) {
                if !list.is_empty() {
                    let elem = list.remove(0);

                    if list.is_empty() {
                        inner.map.remove(&key);
                    }
                    if let Some(q) = inner.waiters.get_mut(&key) {
                        q.pop_front();
                        if q.is_empty() {
                            inner.waiters.remove(&key);
                        }
                    }

                    // Let the next waiter re-check (more elements may remain).
                    store.on_push.notify_all();
                    return Resp::Array(vec![
                        Resp::Bulk(Some(key.into_bytes())),
                        Resp::Bulk(Some(elem.into_bytes())),
                    ]);
                }
            }
        }

        // Not my turn / no data yet -> release lock and sleep until a push.
        match deadline {
            None => {
                // block forever (timeout 0)
                guard = store.on_push.wait(guard).unwrap();
            }
            Some(dl) => {
                let now = Instant::now();
                if now >= dl {
                    // timed out: clean up my ticket, return null array
                    guard.remove_ticket(&key, ticket);
                    store.on_push.notify_all();
                    return Resp::NullArray;
                }

                let remaining = dl - now;
                let (g, wait_result) = store.on_push.wait_timeout(guard, remaining).unwrap();
                guard = g;
                if wait_result.timed_out() {
                    if Instant::now() >= dl {
                        guard.remove_ticket(&key, ticket);
                        store.on_push.notify_all();
                        return Resp::NullArray;
                    }
                }
            }
        }
    }
}

fn cmd_type(args: &[Vec<u8>], store: &Store) -> Resp {
    if args.len() < 2 {
        return wrong_args("type");
    }

    let key = as_str(&args[1]);
    let mut guard = store.inner.lock().unwrap();

    let type_name = match guard.map.get(&key) {
        Some(RedisValue::Str(_, Some(deadline))) if Instant::now() >= *deadline => {
            guard.map.remove(&key);
            "none"
        }
        Some(RedisValue::Str(_, _)) => "string",
        Some(RedisValue::List(_)) => "list",
        Some(RedisValue::Stream(_)) => "stream",
        None => "none",
    };

    Resp::Simple(type_name.into())
}

fn cmd_xadd(args: &[Vec<u8>], store: &Store) -> Resp {
    // Ex: redis-cli XADD stream_key 1526919030474-0 temperature 36 humidity 95
    if args.len() < 5 || args.len() % 2 != 1 {
        return wrong_args("xadd");
    }

    let key = as_str(&args[1]);
    let id_args = as_str(&args[2]);

    let spec = match parse_entry_spec(&id_args) {
        Some(s) => s,
        None => return Resp::Error("ERR Invalid stream ID specified as stream command argument".into()),
    };

    let mut fields: Vec<(String, String)> = Vec::new();
    let mut i = 3;

    while i + 1 < args.len() {
        fields.push((as_str(&args[i]), as_str(&args[i + 1])));
        i += 2;
    }

    let mut guard = store.inner.lock().unwrap();

    match guard.map.entry(key).or_insert_with(|| RedisValue::Stream(Vec::new())) {
        RedisValue::Stream(entries) => {
            let (ms, seq) = match spec {
                IdSpec::Explicit(ms, seq) => (ms, seq),
                IdSpec::AutoSeq(ms) => (ms, resolve_seq(ms, entries)),
                IdSpec::AutoAll => {
                    let ms = now_ms();
                    (ms, resolve_seq(ms, entries))
                }
            };

            if ms == 0 && seq == 0 {
                return Resp::Error("ERR The ID specified in XADD must be greater than 0-0".into());
            }

            if let Some(last) = entries.last() {
                if let Some((last_ms, last_seq)) = parse_entry_id(&last.id) {
                    if (ms, seq) <= (last_ms, last_seq) {
                        return Resp::Error("ERR The ID specified in XADD is equal or smaller than the target stream top item".into());
                    }
                }
            }

            let id = format!("{ms}-{seq}");
            entries.push(StreamEntry {id: id.clone(), fields});
            store.on_push.notify_all();
            Resp::Bulk(Some(id.into_bytes()))
        }
        _ => wrong_type(),
    }
}

fn cmd_xrange(args: &[Vec<u8>], store: &Store) -> Resp {
    // Ex: redis-cli XRANGE some_key 1526985054069 1526985054079
    if args.len() < 4 {
        return wrong_args("xrange");
    }

    let key = as_str(&args[1]);
    let start = match parse_entry_id_range(&as_str(&args[2]), true) {
        Some(s) => s,
        None => return Resp::Error("ERR Invalid stream ID specified as stream command argument".into()),
    };

    let end = match parse_entry_id_range(&as_str(&args[3]), false) {
        Some(s) => s,
        None => return Resp::Error("ERR Invalid stream ID specified as stream command argument".into()),
    };

    let guard = store.inner.lock().unwrap();
    let entries = match guard.map.get(&key) {
        Some(RedisValue::Stream(entries)) => entries,
        Some(_) => return wrong_type(),
        None => return Resp::Array(vec![]), // no stream => empty array
    };

    let mut result: Vec<Resp> = Vec::new();
    for entry in entries {
        let id = match parse_entry_id(&entry.id) {
            Some(id) => id,
            None => continue,
        };
        if id >= start && id <= end {
            result.push(entry_to_resp(entry));
        }
    }

    Resp::Array(result)
}

fn cmd_xread(args: &[Vec<u8>], store: &Store) -> Resp {
    // Ex1: redis-cli XREAD STREAMS key1 key2 ... id1 id2 ...
    // Ex2: redis-cli XREAD BLOCK<ms> STREAMS key1 key2 ... id1 id2 ...
    if args.len() < 4 {
        return wrong_args("xread");
    }

    // Optional BLOCK <ms> prefix
    let mut idx = 1;
    let mut block: Option<u64> = None;

    if as_str(&args[idx]).eq_ignore_ascii_case("block") {
        let ms: u64 = match as_str(&args[idx + 1]).parse() {
            Ok(v) => v,
            Err(_) => return Resp::Error("ERR timeout is not an integer or out of range".into()),
        };
        block = Some(ms);
        idx += 2;
    }

    if !as_str(&args[idx]).eq_ignore_ascii_case("streams") {
        return Resp::Error("ERR syntax error".into());
    }

    // Tokens after STREAMS: N keys then N ids -> must be even, split in half.
    let rest = &args[idx + 1..];
    if rest.is_empty() || rest.len() % 2 != 0 {
        return Resp::Error(
            "ERR unbalanced XREAD list of streams: for each stream key an Id or '$' must be specified".into(),
        );
    }

    let n = rest.len() / 2;
    let keys: Vec<String> = rest[..n].iter().map(|k| as_str(k)).collect();
    let id_args: Vec<String> = rest[n..].iter().map(|i| as_str(i)).collect();

    let mut guard = store.inner.lock().unwrap();

    let mut afters: Vec<(u64, u64)> = Vec::with_capacity(n);
    for (key, id_arg) in keys.iter().zip(&id_args) {
        let after = if id_arg == "$" {
            // current top of the stream, or (0,0) if empty/missing
            match guard.map.get(key) {
                Some(RedisValue::Stream(entries)) => entries
                    .last().and_then(|e| parse_entry_id(&e.id))
                    .unwrap_or((0, 0)),
                _ => (0, 0),
            }
        } else {
            match parse_entry_id(id_arg) {
                Some(id) => id,
                None => return Resp::Error(
                    "ERR Invalid stream ID specified as stream command argument".into(),
                )
            }
        };

        afters.push(after);
    }

    // Try immediately first.
    let found = collect_streams(&guard, &keys, &afters);
    if !found.is_empty() {
        return Resp::Array(found);
    }

    let block_ms = match block {
        Some(ms) => ms,
        None => return Resp::NullArray,
    };

    let deadline: Option<Instant> = if block_ms == 0 {
        None
    } else {
        Some(Instant::now() + Duration::from_millis(block_ms))
    };

    loop {
        match deadline {
            None => {
                guard = store.on_push.wait(guard).unwrap();
            },
            Some(dl) => {
                let now = Instant::now();
                if now >= dl {
                    return Resp::NullArray;
                }

                let (g, res) = store.on_push.wait_timeout(guard, dl - now).unwrap();
                guard = g;

                if res.timed_out() && Instant::now() >= dl {
                    // One last check before giving up
                    let found = collect_streams(&guard, &keys, &afters);
                    return if found.is_empty() {
                        Resp::NullArray
                    } else {
                        Resp::Array(found)
                    };
                }
            }
        }
        let found = collect_streams(&guard, &keys, &afters);
        if !found.is_empty() {
            return Resp::Array(found);
        }
    }
}

fn cmd_incr(args: &[Vec<u8>], store: &Store) -> Resp {
    // Ex: redis-cli INCR foo
    if args.len() < 2 {
        return wrong_args("incr");
    }

    let key = as_str(&args[1]);
    let mut guard = store.inner.lock().unwrap();

    match guard.map.get_mut(&key) {
        Some(RedisValue::Str(value, expired)) => {
            let n: i64 = match value.parse() {
                Ok(n) => n,
                Err(_) => return Resp::Error("ERR value is not an integer or out of range".into())
            };

            let new = n + 1;
            *value = new.to_string();
            Resp::Integer(new)
        },
        Some(_) => wrong_type(),
        None => {
            // Key doesn't exist -> set to 1.
            guard.map.insert(key, RedisValue::Str("1".to_string(), None));
            Resp::Integer(1)
        }
    }
}

fn collect_streams(guard: &std::sync::MutexGuard<'_, crate::store::Inner>,
                   keys: &[String], afters: &[(u64, u64)]) -> Vec<Resp> {
    let mut out = Vec::new();
    for (key, &after) in keys.iter().zip(afters) {
        if let Some(RedisValue::Stream(entries)) = guard.map.get(key) {
            let mut matched = Vec::new();
            for entry in entries {
                if let Some(id) = parse_entry_id(&entry.id) {
                    if id > after {
                        matched.push(entry_to_resp(entry));
                    }
                }
            }

            if !matched.is_empty() {
                out.push(Resp::Array(vec![
                    Resp::Bulk(Some(key.clone().into_bytes())),
                    Resp::Array(matched),
                ]))
            }
        }
    }
    out
}

/// Clamp a possibly-negative index into `[0, len]`.
fn normalize(idx: i64, len: i64) -> i64 {
    if idx < 0 {
        (len + idx).max(0)
    } else {
        idx
    }
}

fn entry_to_resp(entry: &StreamEntry) -> Resp {
    let mut fv: Vec<Resp> = Vec::new();
    for (f, v) in &entry.fields {
        fv.push(Resp::Bulk(Some(f.clone().into_bytes())));
        fv.push(Resp::Bulk(Some(v.clone().into_bytes())));
    }

    Resp::Array(vec![
        Resp::Bulk(Some(entry.id.clone().into_bytes())),
        Resp::Array(fv),
    ])
}

/// Parse an explicit stream ID "millis-seq" into (millis, seq).
fn parse_entry_spec(id: &str) -> Option<IdSpec> {
    if id == "*" {
        return Some(IdSpec::AutoAll);
    }

    let (ms, seq) = id.split_once('-')?;
    let ms: u64 = ms.parse().ok()?;
    if seq == "*" {
       Some(IdSpec::AutoSeq(ms))
    } else {
        Some(IdSpec::Explicit(ms, seq.parse().ok()?))
    }
}

fn parse_entry_id(id: &str) -> Option<(u64, u64)> {
    match id.split_once('-') {
        Some((ms, seq)) => Some((ms.parse().ok()?, seq.parse().ok()?)),
        None => None
    }
}

fn parse_entry_id_range(id: &str, is_start: bool) -> Option<(u64, u64)> {
    if id == "-" {
        return Some((0, 1));
    }

    if id == "+" {
        return Some((u64::MAX, u64::MAX));
    }

    match id.split_once('-') {
        Some((ms, seq)) => Some((ms.parse().ok()?, seq.parse().ok()?)),
        None => {
            let ms: u64 = id.parse().ok()?;
            // start: seq default to 0; end: seq defaults to max.
            Some((ms, if is_start { 0 } else { u64::MAX }))
        }
    }
}

fn resolve_seq(ms: u64, entries: &[StreamEntry]) -> u64 {
    match entries.last().and_then(|e| parse_entry_id(&e.id)) {
        Some((last_ms, last_seq)) if last_ms == ms => last_seq + 1,
        _ if ms == 0 => 1,
        _ => 0
    }
}

fn now_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64
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