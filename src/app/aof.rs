//! The append-only file (AOF) and its manifest, in `<dir>/<appenddirname>`.

use crate::app::protocol::RespValue;
use crate::config::Config;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::Path;
use std::sync::{Mutex, PoisonError};
use tracing::error;

/// The name of the first incremental AOF file.
fn incr_file_name(config: &Config) -> String {
    format!("{}.1.incr.aof", config.appendfilename)
}

/// The manifest, which lists the AOF files in the order they are replayed.
fn manifest(config: &Config) -> String {
    format!("file {} seq 1 type i\n", incr_file_name(config))
}

/// The name of the incremental AOF file (`type i`) that `manifest` lists.
fn incr_file_in(manifest: &str) -> Option<&str> {
    manifest.lines().find_map(|line| {
        let fields: Vec<&str> = line.split_whitespace().collect();
        let value = |key| {
            fields
                .as_chunks::<2>()
                .0
                .iter()
                .find(|[name, _]| *name == key)
                .map(|[_, value]| *value)
        };
        (value("type") == Some("i"))
            .then(|| value("file"))
            .flatten()
    })
}

/// With AOF enabled, makes sure the AOF directory exists, and creates an
/// empty AOF file and a manifest naming it unless a manifest already exists.
pub fn prepare(config: &Config) -> io::Result<()> {
    if config.appendonly != "yes" {
        return Ok(());
    }
    let dir = config.dir.join(&config.appenddirname);
    fs::create_dir_all(&dir)?;
    let manifest_path = dir.join(format!("{}.manifest", config.appendfilename));
    if manifest_path.exists() {
        return Ok(());
    }
    OpenOptions::new()
        .append(true)
        .create(true)
        .open(dir.join(incr_file_name(config)))?;
    fs::write(manifest_path, manifest(config))
}

/// The open incremental AOF file that applied writes are appended to.
#[derive(Debug)]
pub struct Aof {
    file: Mutex<File>,
    /// Whether each append is flushed to disk before the client gets its reply.
    fsync_always: bool,
}

impl Aof {
    /// With AOF enabled, prepares the AOF files and opens the incremental file
    /// the manifest names. Returns `None` with AOF disabled.
    pub fn open(config: &Config) -> io::Result<Option<Self>> {
        prepare(config)?;
        if config.appendonly != "yes" {
            return Ok(None);
        }
        let dir = config.dir.join(&config.appenddirname);
        let manifest_path = dir.join(format!("{}.manifest", config.appendfilename));
        let manifest = fs::read_to_string(&manifest_path)?;
        let name = incr_file_in(&manifest).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("{} lists no incremental AOF file", manifest_path.display()),
            )
        })?;
        Ok(Some(Self::append_to(
            &dir.join(name),
            config.appendfsync == "always",
        )?))
    }

    fn append_to(path: &Path, fsync_always: bool) -> io::Result<Self> {
        let file = OpenOptions::new().append(true).create(true).open(path)?;
        Ok(Self {
            file: Mutex::new(file),
            fsync_always,
        })
    }

    /// Appends `commands` in RESP form and, with `appendfsync always`, flushes
    /// them to disk. As in Redis, if that fails with `appendfsync always` the
    /// server exits, so no client is told a write succeeded that wasn't saved;
    /// with other policies the failure is logged.
    pub fn append(&self, commands: &[RespValue]) {
        if commands.is_empty() {
            return;
        }
        if let Err(e) = self.write(commands) {
            error!("failed to append to the AOF file: {e}");
            if self.fsync_always {
                error!("Can't recover from AOF write error when the AOF fsync policy is 'always'. Exiting...");
                std::process::exit(1);
            }
        }
    }

    fn write(&self, commands: &[RespValue]) -> io::Result<()> {
        let bytes: Vec<u8> = commands
            .iter()
            .flat_map(|command| command.encode_to_bytes())
            .collect();
        let mut file = self.file.lock().unwrap_or_else(PoisonError::into_inner);
        file.write_all(&bytes)?;
        if self.fsync_always {
            file.sync_data()?;
        }
        Ok(())
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
    fn an_existing_manifest_is_kept_and_no_default_file_is_created() {
        let tmp = TempDir::new("keep");
        let dir = tmp.0.join("aofdir");
        fs::create_dir_all(&dir).expect("create aof dir");
        fs::write(
            dir.join("x.aof.manifest"),
            "file other.1.incr.aof seq 1 type i\n",
        )
        .expect("write manifest");

        prepare(&config(&tmp, "yes")).expect("prepare");
        assert_eq!(
            fs::read_to_string(dir.join("x.aof.manifest")).expect("manifest"),
            "file other.1.incr.aof seq 1 type i\n"
        );
        assert!(!dir.join("x.aof.1.incr.aof").exists());
    }

    #[test]
    fn the_manifest_names_the_incremental_file() {
        assert_eq!(
            incr_file_in("file a.1.base.rdb seq 1 type b\nfile a.1.incr.aof seq 1 type i\n"),
            Some("a.1.incr.aof")
        );
        assert_eq!(incr_file_in("file a.1.base.rdb seq 1 type b\n"), None);
        assert_eq!(incr_file_in(""), None);
    }

    #[tokio::test]
    async fn only_applied_writes_are_appended_to_the_file_the_manifest_names() {
        let tmp = TempDir::new("append");
        let dir = tmp.0.join("aofdir");
        fs::create_dir_all(&dir).expect("create aof dir");
        fs::write(
            dir.join("x.aof.manifest"),
            "file random.1.incr.aof seq 1 type i\n",
        )
        .expect("write manifest");
        fs::write(dir.join("random.1.incr.aof"), "").expect("write aof");
        let mut args: Vec<OsString> = ["redis", "--dir"].map(OsString::from).into();
        args.push(tmp.0.clone().into());
        args.extend(
            [
                "--appendonly",
                "yes",
                "--appenddirname",
                "aofdir",
                "--appendfilename",
                "x.aof",
                "--appendfsync",
                "always",
            ]
            .map(OsString::from),
        );
        let config = Config::parse_from(args);
        let aof = Aof::open(&config).expect("open").expect("enabled");
        let ctx = crate::app::test_context_with_aof(aof);

        for parts in [
            &["SET", "foo", "1"][..],
            &["GET", "foo"],
            &["PING"],
            &["ECHO", "hello"],
            &["CONFIG", "GET", "dir"],
            &["LPOP", "missing"],
            &["SET", "bar", "2"],
        ] {
            crate::app::run_test(parts, &ctx).await;
        }
        assert_eq!(
            fs::read(dir.join("random.1.incr.aof")).expect("aof file"),
            b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$1\r\n1\r\n*3\r\n$3\r\nSET\r\n$3\r\nbar\r\n$1\r\n2\r\n"
        );
        assert!(!dir.join("x.aof.1.incr.aof").exists());
    }

    #[test]
    fn creates_nothing_when_aof_is_disabled() {
        let tmp = TempDir::new("disabled");
        prepare(&config(&tmp, "no")).expect("prepare");
        assert!(!tmp.0.join("aofdir").exists());
    }

    #[test]
    fn a_failed_write_is_reported() {
        let tmp = TempDir::new("fail");
        let path = tmp.0.join("read-only.aof");
        fs::write(&path, "").expect("create file");
        // Opened for reading only, so every write fails.
        let aof = Aof {
            file: Mutex::new(File::open(&path).expect("open")),
            fsync_always: true,
        };
        let set = crate::app::test_command(&["SET", "k", "v"]).into_resp_array();
        assert!(aof.write(&[set]).is_err());
    }
}
