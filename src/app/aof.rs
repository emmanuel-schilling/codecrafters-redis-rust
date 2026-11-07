//! The append-only file (AOF) and its manifest, in `<dir>/<appenddirname>`.

use crate::config::Config;
use std::fs::{self, OpenOptions};
use std::io::{self, Write};

/// The name of the first incremental AOF file.
fn incr_file_name(config: &Config) -> String {
    format!("{}.1.incr.aof", config.appendfilename)
}

/// The manifest, which lists the AOF files in the order they are replayed.
fn manifest(config: &Config) -> String {
    format!("file {} seq 1 type i\n", incr_file_name(config))
}

/// With AOF enabled, creates the AOF directory, an empty AOF file and the
/// manifest, keeping any that already exist.
pub fn prepare(config: &Config) -> io::Result<()> {
    if config.appendonly != "yes" {
        return Ok(());
    }
    let dir = config.dir.join(&config.appenddirname);
    fs::create_dir_all(&dir)?;
    OpenOptions::new()
        .append(true)
        .create(true)
        .open(dir.join(incr_file_name(config)))?;
    match OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(dir.join(format!("{}.manifest", config.appendfilename)))
    {
        Ok(mut file) => file.write_all(manifest(config).as_bytes()),
        Err(e) if e.kind() == io::ErrorKind::AlreadyExists => Ok(()),
        Err(e) => Err(e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;
    use std::ffi::OsString;
    use std::path::PathBuf;

    /// A new, empty directory for one test, removed when dropped.
    struct TempDir(PathBuf);

    impl TempDir {
        fn new(name: &str) -> Self {
            let dir = std::env::temp_dir().join(format!("aof-{name}-{}", std::process::id()));
            let _ = fs::remove_dir_all(&dir);
            fs::create_dir_all(&dir).expect("create temp dir");
            Self(dir)
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn config(dir: &TempDir, appendonly: &str) -> Config {
        let mut args: Vec<OsString> = ["redis", "--dir"].map(OsString::from).into();
        args.push(dir.0.clone().into());
        args.extend(
            [
                "--appendonly",
                appendonly,
                "--appenddirname",
                "aofdir",
                "--appendfilename",
                "x.aof",
            ]
            .map(OsString::from),
        );
        Config::parse_from(args)
    }

    #[test]
    fn creates_the_directory_an_empty_aof_file_and_the_manifest() {
        let tmp = TempDir::new("create");
        prepare(&config(&tmp, "yes")).expect("prepare");
        let dir = tmp.0.join("aofdir");
        assert_eq!(
            fs::read(dir.join("x.aof.1.incr.aof")).expect("aof file"),
            b""
        );
        assert_eq!(
            fs::read_to_string(dir.join("x.aof.manifest")).expect("manifest"),
            "file x.aof.1.incr.aof seq 1 type i\n"
        );
    }

    #[test]
    fn keeps_existing_files() {
        let tmp = TempDir::new("keep");
        let dir = tmp.0.join("aofdir");
        fs::create_dir_all(&dir).expect("create aof dir");
        fs::write(dir.join("x.aof.1.incr.aof"), "*1\r\n$4\r\nPING\r\n").expect("write aof");
        fs::write(dir.join("x.aof.manifest"), "existing\n").expect("write manifest");

        prepare(&config(&tmp, "yes")).expect("prepare");
        assert_eq!(
            fs::read(dir.join("x.aof.1.incr.aof")).expect("aof file"),
            b"*1\r\n$4\r\nPING\r\n"
        );
        assert_eq!(
            fs::read_to_string(dir.join("x.aof.manifest")).expect("manifest"),
            "existing\n"
        );
    }

    #[test]
    fn creates_nothing_when_aof_is_disabled() {
        let tmp = TempDir::new("disabled");
        prepare(&config(&tmp, "no")).expect("prepare");
        assert!(!tmp.0.join("aofdir").exists());
    }
}
