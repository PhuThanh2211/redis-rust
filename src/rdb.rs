use std::fs;
use std::path::Path;

/// Load keys/values from the RDB file. Missing/invalid file -> empty vec.
pub fn load(dir: &str, dbfilename: &str) -> Vec<(String, String, Option<u64>)> {
    if dir.is_empty() || dbfilename.is_empty() {
        return Vec::new();
    }
    let path = Path::new(dir).join(dbfilename);
    let bytes = match fs::read(&path) {
        Ok(b) => b,
        Err(_) => return Vec::new(), // file doesn't exist -> empty DB
    };
    parse(&bytes).unwrap_or_default()
}

struct Cursor<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn byte(&mut self) -> Option<u8> {
        let b = *self.data.get(self.pos)?;
        self.pos += 1;
        Some(b)
    }
    fn take(&mut self, n: usize) -> Option<&'a [u8]> {
        let s = self.data.get(self.pos..self.pos + n)?;
        self.pos += n;
        Some(s)
    }
}

/// A length-encoded value. Either a real length, or a special "this is an
/// integer-encoded string" marker (the 0b11 case).
enum Len {
    Length(usize),
    IntStr(u8), // 0xC0/0xC1/0xC2 -> 8/16/32-bit int follows
}

fn read_len(c: &mut Cursor) -> Option<Len> {
    let first = c.byte()?;
    match first >> 6 {              // top two bits
        0b00 => Some(Len::Length((first & 0x3F) as usize)),
        0b01 => {
            let second = c.byte()?;
            let len = (((first & 0x3F) as usize) << 8) | second as usize;
            Some(Len::Length(len))
        }
        0b10 => {
            let b = c.take(4)?;
            let len = u32::from_be_bytes([b[0], b[1], b[2], b[3]]) as usize;
            Some(Len::Length(len))
        }
        _ => Some(Len::IntStr(first & 0x3F)), // 0b11 -> special string
    }
}

fn read_string(c: &mut Cursor) -> Option<String> {
    match read_len(c)? {
        Len::Length(n) => {
            let bytes = c.take(n)?;
            Some(String::from_utf8_lossy(bytes).into_owned())
        }
        Len::IntStr(kind) => {
            let val: i64 = match kind {
                0 => c.byte()? as i8 as i64,                          // C0: 8-bit
                1 => {
                    let b = c.take(2)?;
                    i16::from_le_bytes([b[0], b[1]]) as i64           // C1: 16-bit LE
                }
                2 => {
                    let b = c.take(4)?;
                    i32::from_le_bytes([b[0], b[1], b[2], b[3]]) as i64 // C2: 32-bit LE
                }
                _ => return None, // C3 = LZF, not in this challenge
            };
            Some(val.to_string())
        }
    }
}

fn parse(data: &[u8]) -> Option<Vec<(String, String, Option<u64>)>> {
    let mut c = Cursor { data, pos: 0 };

    // Header: "REDIS0011" (9 bytes)
    let header = c.take(9)?;
    if &header[..5] != b"REDIS" {
        return None;
    }

    let mut out = Vec::new();

    loop {
        let op = c.byte()?;
        match op {
            0xFA => { // metadata subsection: name + value strings, skip
                read_string(&mut c)?;
                read_string(&mut c)?;
            }
            0xFE => { // database selector: db index (size encoded)
                read_len(&mut c)?;
            }
            0xFB => { // hash table sizes: two length-encoded numbers
                read_len(&mut c)?;
                read_len(&mut c)?;
            }
            0xFC => { // expire in ms: 8-byte little-endian timestamp
                let b = c.take(8)?;
                let ms = u64::from_le_bytes([b[0],b[1],b[2],b[3],b[4],b[5],b[6],b[7]]);
                read_kv(&mut c, &mut out, Some(ms))?;
            }
            0xFD => { // expire in seconds: 4-byte little-endian timestamp
                let b = c.take(4)?;
                let secs = u32::from_le_bytes([b[0],b[1],b[2],b[3]]) as u64;
                read_kv(&mut c, &mut out, Some(secs * 1000))?; // normalize to ms
            }
            0x00 => { // no expiry
                let key = read_string(&mut c)?;
                let value = read_string(&mut c)?;
                out.push((key, value, None));
            }
            0xFF => break, // end of file (checksum follows, ignore)
            _ => return None, // unknown opcode
        }
    }

    Some(out)
}

// After an expire opcode we've consumed the timestamp; next is the value-type
// flag, then key + value.
fn read_kv(
    c: &mut Cursor,
    out: &mut Vec<(String, String, Option<u64>)>,
    expiry: Option<u64>,
) -> Option<()> {
    let _type_flag = c.byte()?;
    let key = read_string(c)?;
    let value = read_string(c)?;
    out.push((key, value, expiry));
    Some(())
}