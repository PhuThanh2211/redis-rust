import socket
import time

HOST = "127.0.0.1"
PORT = 6379


def enc(*parts):
    out = f"*{len(parts)}\r\n"
    for p in parts:
        out += f"${len(p)}\r\n{p}\r\n"
    return out.encode()


def read_reply(sock):
    # naive line read up to \r\n (enough for +PONG, +OK, +FULLRESYNC ...)
    data = b""
    while not data.endswith(b"\r\n"):
        data += sock.recv(1)
    return data


# ---- 1. Replica side: full handshake ----
replica = socket.create_connection((HOST, PORT))

replica.sendall(enc("PING"))
print("PING ->", read_reply(replica))

replica.sendall(enc("REPLCONF", "listening-port", "6380"))
print("REPLCONF listening-port ->", read_reply(replica))

replica.sendall(enc("REPLCONF", "capa", "psync2"))
print("REPLCONF capa ->", read_reply(replica))

replica.sendall(enc("PSYNC", "?", "-1"))
print("PSYNC (+FULLRESYNC) ->", read_reply(replica))

# ---- 2. Read the RDB file: $<len>\r\n<bytes> (NO trailing CRLF) ----
header = b""
while not header.endswith(b"\r\n"):
    header += replica.recv(1)
assert header.startswith(b"$"), f"expected RDB header, got {header!r}"
rdb_len = int(header[1:-2])
rdb = b""
while len(rdb) < rdb_len:
    rdb += replica.recv(rdb_len - len(rdb))
print(f"RDB received: {rdb_len} bytes, starts with {rdb[:9]!r}")

# ---- 3. A separate client sends write commands ----
client = socket.create_connection((HOST, PORT))
for kv in [("foo", "1"), ("bar", "2"), ("baz", "3")]:
    client.sendall(enc("SET", *kv))
    print(f"client SET {kv} ->", read_reply(client))

# ---- 4. Assert the replica received them, propagated as RESP arrays ----
time.sleep(0.3)
replica.setblocking(False)
propagated = b""
try:
    while True:
        chunk = replica.recv(4096)
        if not chunk:
            break
        propagated += chunk
except BlockingIOError:
    pass

print("\n--- Propagated bytes to replica ---")
print(propagated)

expected = (
    enc("SET", "foo", "1")
    + enc("SET", "bar", "2")
    + enc("SET", "baz", "3")
)
assert propagated == expected, f"\nEXPECTED:\n{expected}\nGOT:\n{propagated}"
print("\n PASS: all 3 SET commands propagated in order.")

replica.close()
client.close()
