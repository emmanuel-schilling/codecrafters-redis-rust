use clap::builder::{PossibleValuesParser, TypedValueParser};
use clap::Parser;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;

/// A simple Redis-clone server implementation.
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Config {
    /// The address to listen on.
    #[arg(long, default_value_t = IpAddr::V4(Ipv4Addr::LOCALHOST))]
    pub bind: IpAddr,

    /// The port number to listen on.
    #[arg(long, default_value_t = 6379)]
    pub port: u16,

    /// The log level to use.
    #[arg(long, default_value = "info")]
    pub log_level: String,

    /// The directory where data files are stored; by default the current
    /// directory at startup.
    #[arg(long, default_value_os_t = current_dir())]
    pub dir: PathBuf,

    /// The name of the RDB file.
    #[arg(long, default_value = "dump.rdb")]
    pub dbfilename: String,

    /// The address of the master to replicate from, e.g., "localhost 6379".
    #[arg(long)]
    pub replicaof: Option<String>,

    /// Whether AOF persistence is enabled. Values are case-insensitive, as in
    /// Redis, and stored in lower case.
    #[arg(long, default_value = "no", value_parser = one_of(&["yes", "no"]), ignore_case = true)]
    pub appendonly: String,

    /// The subdirectory of `dir` that holds the AOF files.
    #[arg(long, default_value = "appendonlydir", value_parser = plain_name)]
    pub appenddirname: String,

    /// The name of the append-only file.
    #[arg(long, default_value = "appendonly.aof", value_parser = plain_name)]
    pub appendfilename: String,

    /// How often writes to the AOF file are flushed to disk. Values are
    /// case-insensitive and stored in lower case.
    #[arg(
        long,
        default_value = "everysec",
        value_parser = one_of(&["always", "everysec", "no"]),
        ignore_case = true
    )]
    pub appendfsync: String,
}

/// Accepts one of `values`, ignoring case, and gives it in lower case.
fn one_of(values: &'static [&'static str]) -> impl TypedValueParser<Value = String> {
    PossibleValuesParser::new(values).map(|value| value.to_lowercase())
}

/// Accepts a file or directory name that isn't empty and isn't a path. As
/// in Redis, both `/` and `\` count as separators, on every platform.
fn plain_name(name: &str) -> Result<String, String> {
    if name.is_empty() || name.contains(['/', '\\']) {
        Err("must be a name, not a path".into())
    } else {
        Ok(name.to_string())
    }
}

/// The current directory, or "." if it can't be read.
fn current_dir() -> PathBuf {
    std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."))
}

impl Config {
    /// Returns the socket address to listen on.
    pub fn listen_addr(&self) -> SocketAddr {
        SocketAddr::new(self.bind, self.port)
    }

    /// Returns the full path to the RDB file.
    pub fn rdb_file_path(&self) -> PathBuf {
        self.dir.join(&self.dbfilename)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_listen_on_localhost_and_read_dump_rdb_from_the_current_dir() {
        let config = Config::parse_from(["redis"]);
        assert_eq!(
            config.listen_addr(),
            "127.0.0.1:6379".parse().expect("addr")
        );
        let cwd = std::env::current_dir().expect("current dir");
        assert_eq!(config.rdb_file_path(), cwd.join("dump.rdb"));
    }

    #[test]
    fn aof_options_have_defaults_and_can_be_set() {
        let defaults = Config::parse_from(["redis"]);
        assert_eq!(
            [
                defaults.appendonly,
                defaults.appenddirname,
                defaults.appendfilename,
                defaults.appendfsync,
            ],
            ["no", "appendonlydir", "appendonly.aof", "everysec"]
        );

        let config = Config::parse_from([
            "redis",
            "--appendonly",
            "yes",
            "--appenddirname",
            "aofdir",
            "--appendfilename",
            "x.aof",
            "--appendfsync",
            "always",
        ]);
        assert_eq!(
            [
                config.appendonly,
                config.appenddirname,
                config.appendfilename,
                config.appendfsync,
            ],
            ["yes", "aofdir", "x.aof", "always"]
        );
    }

    #[test]
    fn bind_dir_and_dbfilename_are_configurable() {
        let config = Config::parse_from([
            "redis",
            "--bind",
            "0.0.0.0",
            "--port",
            "7000",
            "--dir",
            "data",
            "--dbfilename",
            "x.rdb",
        ]);
        assert_eq!(config.listen_addr(), "0.0.0.0:7000".parse().expect("addr"));
        assert_eq!(config.rdb_file_path(), PathBuf::from("data").join("x.rdb"));
    }

    #[test]
    fn bad_aof_values_are_rejected() {
        let bad: [&[&str]; 6] = [
            &["--appendonly", "maybe"],
            &["--appendfsync", "sometimes"],
            &["--appenddirname", ""],
            &["--appenddirname", "a/b"],
            &["--appendfilename", "a\\b.aof"],
            &["--appendfilename", "/x.aof"],
        ];
        for args in bad {
            let parsed =
                Config::try_parse_from(std::iter::once("redis").chain(args.iter().copied()));
            assert!(parsed.is_err(), "{args:?} was accepted");
        }
        let fsync = |value| {
            Config::try_parse_from(["redis", "--appendfsync", value])
                .expect("valid")
                .appendfsync
        };
        assert_eq!([fsync("always"), fsync("No")], ["always", "no"]);
        let appendonly = Config::try_parse_from(["redis", "--appendonly", "YES"])
            .expect("valid")
            .appendonly;
        assert_eq!(appendonly, "yes");
    }
}
