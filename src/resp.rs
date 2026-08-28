use std::io::{BufRead, Read};

#[derive(Debug, Clone)]
pub enum Resp {
    Simple(String),
    Error(String),
    Integer(i64),
    Bulk(Option<Vec<u8>>), // None = null bulk string ($-1)
    Array(Vec<Resp>),
    NullArray, // *-1\r\n
    Raw(Vec<u8>), // written verbatim, no framing added
}

impl Resp {
    pub fn encode(&self) -> Vec<u8> {
        match self {
            Resp::Simple(s) => format!("+{s}\r\n").into_bytes(),
            Resp::Error(s) => format!("-{s}\r\n").into_bytes(),
            Resp::Integer(n) => format!(":{n}\r\n").into_bytes(),
            Resp::Bulk(None) => b"$-1\r\n".to_vec(),
            Resp::Bulk(Some(b)) => {
                let mut out = format!("${}\r\n", b.len()).into_bytes();
                out.extend_from_slice(b);
                out.extend_from_slice(b"\r\n");
                out
            },
            Resp::Array(items) => {
                let mut out = format!("*{}\r\n", items.len()).into_bytes();
                for i in items {
                    out.extend(i.encode());
                }
                out
            },
            Resp::NullArray => b"*-1\r\n".to_vec(),
            Resp::Raw(bytes) => bytes.clone(),
        }
    }
}

/// Reads one CRLF-terminated line (without the trailing \r\n).
fn read_line<R: BufRead>(reader: &mut R) -> std::io::Result<Option<String>> {
    let mut line = String::new();
    let n = reader.read_line(&mut line)?;
    if n == 0 {
        return Ok(None); // EOF
    }

    while line.ends_with('\n') || line.ends_with('\r') {
        line.pop();
    }

    Ok(Some(line))
}

/// Parses one client command as an array of bulk strings.
/// Returns Ok(None) on clean EOF.
pub fn read_command<R: BufRead>(reader: &mut R) -> std::io::Result<Option<Vec<Vec<u8>>>> {
    let header = match read_line(reader)? {
        Some(h) => h,
        None => return Ok(None),
    };
    if !header.starts_with('*') {
        return Err(io_error("expected array header"));
    }
    let count = header[1..].parse().map_err(|_| io_error("bad array len"))?;
    
    let mut args = Vec::with_capacity(count);
    for _ in 0..count {
        let len_line = read_line(reader)?.ok_or_else(|| io_error("unexpected EOF"))?;
        if !len_line.starts_with('$') {
            return Err(io_error("expected bulk header")); 
        }
        let len: usize = len_line[1..].parse().map_err(|_| io_error("bad bulk len"))?;
        
        let mut buf = vec![0u8; len];
        reader.read_exact(&mut buf)?;
        let mut crlf = [0u8; 2];
        reader.read_exact(&mut crlf)?; // Consume trailing \r\n
        args.push(buf);
    }
    
    Ok(Some(args))

}

fn io_error(msg: &str) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, msg)
}