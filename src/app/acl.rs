//! The ACL user connections are authenticated as: `default`.

use sha2::{Digest, Sha256};
use std::sync::{Mutex, MutexGuard, PoisonError};

#[derive(Debug)]
struct User {
    /// Whether any password (or none) authenticates as this user.
    nopass: bool,
    /// The SHA-256 hashes of the user's passwords, as lowercase hex, in the
    /// order they were added.
    passwords: Vec<String>,
}

/// The `default` user, which starts with `nopass` and no passwords.
#[derive(Debug)]
pub struct Acl {
    default: Mutex<User>,
}

impl Default for Acl {
    fn default() -> Self {
        Self {
            default: Mutex::new(User {
                nopass: true,
                passwords: Vec::new(),
            }),
        }
    }
}

impl Acl {
    pub fn new() -> Self {
        Self::default()
    }

    /// Locks the user, recovering it if another task panicked while holding it.
    fn user(&self) -> MutexGuard<'_, User> {
        self.default.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Adds a password for the default user, which clears its `nopass` flag.
    /// A password it already has isn't added twice.
    pub fn add_password(&self, password: &[u8]) {
        let hash = sha256_hex(password);
        let mut user = self.user();
        user.nopass = false;
        if !user.passwords.contains(&hash) {
            user.passwords.push(hash);
        }
    }

    /// The default user's flags and password hashes, as ACL GETUSER lists them.
    pub fn flags_and_passwords(&self) -> (Vec<&'static str>, Vec<String>) {
        let user = self.user();
        let flags = if user.nopass { vec!["nopass"] } else { vec![] };
        (flags, user.passwords.clone())
    }
}

/// The SHA-256 hash of `bytes`, as lowercase hex.
fn sha256_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hashes_passwords_as_lowercase_sha256_hex() {
        assert_eq!(
            sha256_hex(b"mypassword"),
            "89e01536ac207279409d4de1e5253e01f4a1769e696db0d6062ca9b8f56767c8"
        );
    }
}
