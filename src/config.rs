use std::path::{Path, PathBuf};

#[derive(Clone)]
pub struct Config {
    pub port: u16,
    pub replica_of: Option<(String, u16)>,
    pub dir: String,
    pub dbfilename: String,
    pub appendonly: String,
    pub appenddirname: String,
    pub appendfilename: String,
    pub appendfsync: String,
}

impl Config {
    /// Parse argv into a Config, starting from sensible defaults.
    pub fn from_args() -> Config {
        let mut cfg = Config {
            port: 6379,
            replica_of: None,
            dir: String::new(),
            dbfilename: String::new(),
            appendonly: "no".into(),
            appenddirname: "appendonlydir".into(),
            appendfilename: "appendonly.aof".into(),
            appendfsync: "everysec".into(),
        };

        let args: Vec<String> = std::env::args().collect();
        let mut i = 1;
        while i < args.len() {
            let has_value = i + 1 < args.len();
            match args[i].as_str() {
                "--port" if has_value => {
                    if let Ok(p) = args[i+1].parse() {
                        cfg.port = p;
                    }
                    i += 2;
                }
                "--replicaof" if has_value => {
                    let mut parts = args[i+1].split_whitespace();
                    if let (Some(h), Some(p)) = (parts.next(), parts.next()) {
                        if let Ok(pn) = p.parse() { cfg.replica_of = Some((h.to_string(), pn)); }
                    }
                    i += 2;
                }
                "--dir"            if has_value => { cfg.dir = args[i+1].clone(); i += 2; }
                "--dbfilename"     if has_value => { cfg.dbfilename = args[i+1].clone(); i += 2; }
                "--appendonly"     if has_value => { cfg.appendonly = args[i+1].clone(); i += 2; }
                "--appenddirname"  if has_value => { cfg.appenddirname = args[i+1].clone(); i += 2; }
                "--appendfilename" if has_value => { cfg.appendfilename = args[i+1].clone(); i += 2; }
                "--appendfsync"    if has_value => { cfg.appendfsync = args[i+1].clone(); i += 2; }
                _ => i += 1,
            }
        }

        // dir defaults to the current working directory
        if cfg.dir.is_empty() {
            cfg.dir = std::env::current_dir()
                .map(|p| p.to_string_lossy().into_owned())
                .unwrap_or_default();
        }

        cfg
    }

    pub fn default_manifest_line(&self) -> String {
        format!("file {}.1.incr.aof seq 1 type i\n", self.appendfilename)
    }

    pub fn aof_enable(&self) -> bool {
        self.appendonly == "yes"
    }

    pub fn aof_dir(&self) -> PathBuf {
        Path::new(&self.dir).join(&self.appenddirname)
    }

    pub fn aof_file(&self) -> PathBuf {
        self.aof_dir().join(format!("{}.1.incr.aof", &self.appendfilename))
    }

    pub fn aof_manifest(&self) -> PathBuf {
        self.aof_dir().join(format!("{}.manifest", &self.appendfilename))
    }

    pub fn active_aof_file(&self) -> Option<PathBuf> {
        let manifest = std::fs::read_to_string(self.aof_manifest()).ok()?;
        let mut active = None;

        for line in manifest.lines() {
            // Each line is: file <name> seq <n> type <t>
            let tokens: Vec<&str> = line.split_whitespace().collect();
            let mut file_name = None;
            let mut is_incr = false;

            let mut i = 0;
            while i + 1 < tokens.len() {
                match tokens[i] {
                    "file" => file_name = Some(tokens[i + 1]),
                    "type" => is_incr = tokens[i + 1] == "i",
                    _ => {}
                }
                i += 2;
            }

            if is_incr {
                if let Some(name) = file_name {
                    active = Some(self.aof_dir().join(name));
                }
            }
        }
        active
    }
}