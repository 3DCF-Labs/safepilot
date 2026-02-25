use anyhow::{anyhow, Result};
use base64::Engine;
use chacha20poly1305::aead::{Aead, AeadCore, KeyInit, OsRng};
use chacha20poly1305::{ChaCha20Poly1305, Nonce};

const PREFIX: &str = "enc:v1:";

#[derive(Clone)]
pub struct Crypto {
    cipher: ChaCha20Poly1305,
}

impl std::fmt::Debug for Crypto {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Crypto(..)")
    }
}

impl Crypto {
    pub fn from_key_str(key: &str) -> Result<Self> {
        let key = key.trim();
        if key.is_empty() {
            return Err(anyhow!("Empty ORCH_MASTER_KEY"));
        }

        let bytes = if key.len() == 64 && key.chars().all(|c| c.is_ascii_hexdigit()) {
            hex::decode(key).map_err(|e| anyhow!("Invalid ORCH_MASTER_KEY hex: {e}"))?
        } else {
            base64::engine::general_purpose::STANDARD
                .decode(key)
                .map_err(|e| anyhow!("Invalid ORCH_MASTER_KEY base64: {e}"))?
        };

        if bytes.len() != 32 {
            return Err(anyhow!(
                "ORCH_MASTER_KEY must decode to 32 bytes (got {})",
                bytes.len()
            ));
        }

        let cipher = ChaCha20Poly1305::new_from_slice(&bytes)
            .map_err(|_| anyhow!("Invalid ORCH_MASTER_KEY length"))?;
        Ok(Self { cipher })
    }

    pub fn encrypt_str(&self, plaintext: &str) -> Result<String> {
        let nonce_bytes = ChaCha20Poly1305::generate_nonce(&mut OsRng);
        let nonce = Nonce::from_slice(&nonce_bytes);
        let ciphertext = self
            .cipher
            .encrypt(nonce, plaintext.as_bytes())
            .map_err(|_| anyhow!("Encrypt failed"))?;

        let mut out = Vec::with_capacity(nonce_bytes.len() + ciphertext.len());
        out.extend_from_slice(&nonce_bytes);
        out.extend_from_slice(&ciphertext);

        Ok(format!(
            "{}{}",
            PREFIX,
            base64::engine::general_purpose::STANDARD.encode(out)
        ))
    }

    pub fn decrypt_str(&self, maybe: &str) -> Result<Option<String>> {
        let s = maybe.trim();
        if !s.starts_with(PREFIX) {
            return Ok(None);
        }
        let b64 = &s[PREFIX.len()..];
        let data = base64::engine::general_purpose::STANDARD
            .decode(b64)
            .map_err(|e| anyhow!("Invalid ciphertext base64: {e}"))?;
        if data.len() < 12 {
            return Err(anyhow!("Ciphertext too short"));
        }
        let (nonce_bytes, ciphertext) = data.split_at(12);
        let nonce = Nonce::from_slice(nonce_bytes);
        let pt = self
            .cipher
            .decrypt(nonce, ciphertext)
            .map_err(|_| anyhow!("Decrypt failed (wrong key?)"))?;
        let text = String::from_utf8(pt).map_err(|_| anyhow!("Decrypted value is not UTF-8"))?;
        Ok(Some(text))
    }

    pub fn is_encrypted(s: &str) -> bool {
        s.trim_start().starts_with(PREFIX)
    }
}
