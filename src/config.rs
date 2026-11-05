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

    /// The directory where RDB files are stored.
    #[arg(long, default_value = ".")]
    pub dir: PathBuf,

    /// The name of the RDB file.
    #[arg(long, default_value = "dump.rdb")]
    pub dbfilename: String,

    /// The address of the master to replicate from, e.g., "localhost 6379".
    #[arg(long)]
    pub replicaof: Option<String>,
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
        assert_eq!(config.rdb_file_path(), PathBuf::from(".").join("dump.rdb"));
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
}
