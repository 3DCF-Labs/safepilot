use anyhow::{anyhow, Result};
use std::path::PathBuf;

#[derive(Clone, Debug)]
pub struct SecretSpec {
    pub name: &'static str,
    pub envs: Vec<&'static str>,
    pub file_envs: Vec<&'static str>,
}

impl SecretSpec {
    pub fn new(name: &'static str, envs: &[&'static str], file_envs: &[&'static str]) -> Self {
        Self {
            name,
            envs: envs.to_vec(),
            file_envs: file_envs.to_vec(),
        }
    }

    pub fn is_configured(&self) -> bool {
        for k in &self.envs {
            if let Ok(v) = std::env::var(k) {
                if !v.trim().is_empty() {
                    return true;
                }
            }
        }
        for k in &self.file_envs {
            if let Ok(v) = std::env::var(k) {
                if !v.trim().is_empty() {
                    return true;
                }
            }
        }
        false
    }

    pub fn load(&self) -> Result<String> {
        self.load_with_crypto(None)
    }

    pub fn load_with_crypto(&self, crypto: Option<&crate::crypto::Crypto>) -> Result<String> {
        let raw = self.load_raw()?;

        if let Some(c) = crypto {
            if crate::crypto::Crypto::is_encrypted(&raw) {
                return c
                    .decrypt_str(&raw)?
                    .ok_or_else(|| anyhow!("Decryption returned None for encrypted value"));
            }
        }

        Ok(raw)
    }

    fn load_raw(&self) -> Result<String> {
        for k in &self.envs {
            if let Ok(v) = std::env::var(k) {
                let s = v.trim().to_string();
                if !s.is_empty() {
                    return Ok(s);
                }
            }
        }

        for k in &self.file_envs {
            if let Ok(v) = std::env::var(k) {
                let p = v.trim();
                if p.is_empty() {
                    continue;
                }
                let path = PathBuf::from(p);
                #[cfg(unix)]
                {
                    use std::os::unix::fs::PermissionsExt;
                    if let Ok(md) = std::fs::metadata(&path) {
                        let mode = md.permissions().mode() & 0o777;
                        if (mode & 0o077) != 0 {
                            let strict = std::env::var("STRICT_SECRET_FILE_PERMS")
                                .ok()
                                .map(|s| s.trim().to_ascii_lowercase())
                                .map(|s| !matches!(s.as_str(), "0" | "false" | "no" | "off"))
                                .unwrap_or(true); // default: strict
                            if strict {
                                return Err(anyhow!(
                                    "Insecure permissions on {} (mode {:o}). Fix with: chmod 600 {}",
                                    path.display(),
                                    mode,
                                    path.display()
                                ));
                            }
                            tracing::warn!(
                                path = %path.display(),
                                mode,
                                "Secret file permissions are too broad (recommended: 600). Set STRICT_SECRET_FILE_PERMS=0 to allow."
                            );
                        }
                    }
                }
                let bytes = std::fs::read(&path).map_err(|e| {
                    anyhow!("Failed to read {} from {}: {e}", self.name, path.display())
                })?;
                let s = String::from_utf8_lossy(&bytes).trim().to_string();
                if s.is_empty() {
                    return Err(anyhow!(
                        "{} loaded from {} is empty",
                        self.name,
                        path.display()
                    ));
                }
                return Ok(s);
            }
        }

        Err(anyhow!(
            "{} is not configured (set one of {:?} or {:?})",
            self.name,
            self.envs,
            self.file_envs
        ))
    }
}

fn is_strict_secret_perms() -> bool {
    std::env::var("STRICT_SECRET_FILE_PERMS")
        .ok()
        .map(|s| s.trim().to_ascii_lowercase())
        .map(|s| !matches!(s.as_str(), "0" | "false" | "no" | "off"))
        .unwrap_or(true)
}

pub fn load_secret_from_file_path(path: &str) -> Result<String> {
    let path = PathBuf::from(path.trim());
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if let Ok(md) = std::fs::metadata(&path) {
            let mode = md.permissions().mode() & 0o777;
            if (mode & 0o077) != 0 {
                if is_strict_secret_perms() {
                    return Err(anyhow!(
                        "Insecure permissions on {} (mode {:o}). Fix with: chmod 600 {}",
                        path.display(),
                        mode,
                        path.display()
                    ));
                }
                tracing::warn!(
                    path = %path.display(),
                    mode,
                    "Secret file permissions are too broad (recommended: 600). Set STRICT_SECRET_FILE_PERMS=0 to allow."
                );
            }
        }
    }
    let bytes = std::fs::read(&path)
        .map_err(|e| anyhow!("Failed to read secret from {}: {e}", path.display()))?;
    let value = String::from_utf8_lossy(&bytes).trim().to_string();
    if value.is_empty() {
        return Err(anyhow!("Secret loaded from {} is empty", path.display()));
    }
    Ok(value)
}

pub fn resolve_secret_reference_or_literal(
    raw: &str,
    crypto: Option<&crate::crypto::Crypto>,
) -> Result<String> {
    let value = raw.trim();
    if let Some(env_key) = value.strip_prefix("env:") {
        let key = env_key.trim();
        if key.is_empty() {
            return Err(anyhow!("Invalid env: secret reference"));
        }
        let v = std::env::var(key)
            .map_err(|_| anyhow!("Environment secret `{}` is not set", key))?
            .trim()
            .to_string();
        if v.is_empty() {
            return Err(anyhow!("Environment secret `{}` is empty", key));
        }
        if let Some(c) = crypto {
            if crate::crypto::Crypto::is_encrypted(&v) {
                return c
                    .decrypt_str(&v)?
                    .ok_or_else(|| anyhow!("Decryption returned None for encrypted env secret"));
            }
        }
        return Ok(v);
    }
    if let Some(file_path) = value.strip_prefix("file:") {
        let file_value = load_secret_from_file_path(file_path)?;
        if let Some(c) = crypto {
            if crate::crypto::Crypto::is_encrypted(&file_value) {
                return c
                    .decrypt_str(&file_value)?
                    .ok_or_else(|| anyhow!("Decryption returned None for encrypted file secret"));
            }
        }
        return Ok(file_value);
    }
    if let Some(c) = crypto {
        if crate::crypto::Crypto::is_encrypted(value) {
            return c
                .decrypt_str(value)?
                .ok_or_else(|| anyhow!("Decryption returned None for encrypted literal"));
        }
    }
    Ok(value.to_string())
}
