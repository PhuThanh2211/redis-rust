[![progress-banner](https://backend.codecrafters.io/progress/redis/65b3692a-5148-4803-adb3-5a1187cd2057)](https://app.codecrafters.io/users/PhuThanh2211?r=2qF)

# Build Your Own Redis in Rust

A feature-complete, multithreaded Redis server built from scratch in Rust, adhering to the official [Redis Serialization Protocol (RESP)](https://redis.io/docs/reference/protocol-spec/).

Built as part of the [CodeCrafters Redis Challenge](https://codecrafters.io/challenges/redis).

---

## 🚀 Implemented Features

### 1. Core & Strings
- **RESP Framing & Parser**: Decodes commands and encodes data types (Simple Strings, Errors, Integers, Bulk Strings, Arrays, Null Arrays).
- **String Commands**: `PING`, `ECHO`, `SET` (with `EX` and `PX` TTL options), `GET`, `INCR`.
- **Key Expiry**: Lazy deletion on key access when expired.

### 2. Lists & Blocking Operations
- `LPUSH`, `RPUSH`, `LLEN`, `LRANGE`, `LPOP` (with optional count).
- `BLPOP`: Blocking list pop using FIFO ticket queues and condition variables (`std::sync::Condvar`), supporting configurable timeouts and thread wakeups.

### 3. Streams
- `XADD`: Supports explicit IDs, auto-generated sequence numbers (`<time>-*`), and fully automatic IDs (`*`), with strict monotonic validation.
- `XRANGE`: Query stream ranges using inclusive boundaries and `-` / `+` min/max sentinels.
- `XREAD`: Multi-stream reads with blocking (`BLOCK <ms>`) using condition variable signaling.

### 4. Transactions & Optimistic Locking
- `MULTI`, `EXEC`, `DISCARD`.
- `WATCH` / `UNWATCH`: Optimistic concurrency control via per-key monotonic version counters (`inner.touch()`). Automatically aborts transaction with a null array (`*-1\r\n`) if watched keys change prior to `EXEC`.

### 5. Replication (Master & Replica)
- **Role Detection**: Starts as master or replica via `--port` and `--replicaof <host> <port>`.
- **Handshake Flow**: 3-step synchronization (`PING` -> `REPLCONF listening-port` / `capa` -> `PSYNC ? -1`).
- **RDB Snapshot Transfer**: Full resynchronization sending an empty RDB dump (`+FULLRESYNC`).
- **Command Propagation**: Propagates write commands from master to all connected replica streams.
- **Offset Tracking & ACKs**: Master tracks propagated byte offsets; replicas consume and reply to `REPLCONF GETACK *` with current byte offset.
- `WAIT <num_replicas> <timeout>`: Synchronously blocks until the required number of replicas acknowledge writes or timeout expires.

### 6. Persistence
- **RDB Persistence**:
  - CLI flags: `--dir` and `--dbfilename`.
  - Custom binary cursor parsing RDB files (opcodes `0xFA`, `0xFE`, `0xFB`, `0xFC`, `0xFD`, `0x00`, `0xFF`), length encodings (6-bit, 14-bit, 32-bit, integer-encoded strings), and millisecond/second expiration timestamps.
  - `CONFIG GET` and `KEYS *`.
- **AOF (Append-Only File) Persistence**:
  - CLI flags: `--appendonly`, `--appenddirname`, `--appendfilename`, `--appendfsync`.
  - Creates append directory, incremental AOF file (`.1.incr.aof`), and `.manifest` on startup.
  - Intercepts writes and appends RESP-encoded commands before client acknowledgment (`sync_all` on `appendfsync always`).
  - Startup replay: Parses active incremental AOF to restore database state.

### 7. Pub/Sub Messaging
- `SUBSCRIBE` / `UNSUBSCRIBE`: Multi-channel subscriptions with per-client channel counts and global subscriber registry (`Arc<Mutex<TcpStream>>`).
- **Subscribed Mode Enforcement**: Rejects unallowed commands when subscribed, keeping only `SUBSCRIBE`, `UNSUBSCRIBE`, `PSUBSCRIBE`, `PUNSUBSCRIBE`, `PING`, `QUIT`, `RESET`.
- Specialized `PING` response in subscribed mode (`["pong", ""]`).
- `PUBLISH`: Broadcasts payload to all channel subscribers and returns recipient count.
- Automatic subscriber cleanup on unexpected client disconnect.

### 8. Sorted Sets (ZSet)
- `ZADD`, `ZRANK`, `ZRANGE`, `ZCARD`, `ZSCORE`, `ZREM`.
- Stored as ordered elements sorted by `score` ascending, tie-broken lexicographically by member name.

### 9. Geospatial Indices (GEO)
- `GEOADD`: Validates latitude (`[-85.05112878, 85.05112878]`) and longitude (`[-180, 180]`).
- **Geohash Encoding/Decoding**: 52-bit Morton code / bit-interleaving between 26-bit normalized latitude and longitude grids.
- `GEOPOS`: Decodes Morton scores back to coordinate approximations.
- `GEODIST`: Calculates great-circle distance between locations using Haversine's formula in meters.
- `GEOSEARCH`: Searches points within radius (`FROMLONLAT ... BYRADIUS <r> <unit>`) supporting `m`, `km`, `mi`, `ft`.

### 10. Bitmaps
- `SETBIT`: Manipulates individual bits (left-to-right, MSB at offset 0), automatically growing the underlying byte storage.
- `GETBIT`: Reads bit at specified offset without mutation (returns 0 for unallocated or out-of-range offsets).
- `STRLEN`: Returns string buffer size in bytes.
- `BITCOUNT`: Counts set bits (`u8::count_ones()`) across entire string or specified byte ranges with clamp/normalization support.
- `BITOP`: Supports bitwise operations across multiple keys (`AND`, `OR`, `XOR`, `NOT`).

### 11. Security & ACL
- `ACL GETUSER`: Queries user properties including `flags` and `passwords`.
- `ACL SETUSER`: Configures user passwords with `>password` syntax, computing and storing SHA-256 hex digests while clearing `nopass`.
- `AUTH <username> <password>`: Verifies credentials against SHA-256 hashes.
- Connection authentication gating: Rejects unauthorized requests with `NOAUTH` when a password is set.

---

## 🛠 Project Structure

```text
src/
├── main.rs          # Startup, CLI arg parsing, RDB/AOF loading, accept loop
├── config.rs        # Configuration management (CLI flags & path resolvers)
├── store.rs         # Shared memory state (Db, Inner, RedisValue, Condvars)
├── resp.rs          # RESP wire protocol encoder & stream parser
├── commands.rs      # Command execution and dispatching
├── connection.rs    # Per-connection worker thread loop, auth, & transactions
├── replication.rs   # Master/replica handshake, offset tracking, RDB transfer
├── rdb.rs           # Binary RDB parser & deserializer
└── geo.rs           # Morton bit interleaving, coordinate conversions, & Haversine formula
```

---

## 🚦 Getting Started

### Prerequisites
- [Rust](https://www.rust-lang.org/) (edition 2024 / Rust 1.85+)

### Running Locally

Start the Redis server on default port `6379`:
```sh
cargo run
```

Start on a custom port with AOF persistence enabled:
```sh
cargo run -- --port 6380 --dir ./data --appendonly yes --appendfsync always
```

Start as a replica of a master instance:
```sh
cargo run -- --port 6381 --replicaof "127.0.0.1 6379"
```

Connect using standard `redis-cli`:
```sh
redis-cli -p 6379
```
