//! Encryption for answers kept in the ledger.
//!
//! A completion's answer is stored with its reservation so that a repeat under
//! the same `Idempotency-Key` can be served without calling the provider
//! again. That answer is model output about the tenant's data, so it is sealed
//! before it reaches the database:
//!
//! * **AES-256-GCM**, a fresh random 96-bit nonce per answer.
//! * **Bound to its row.** The tenant and reservation id are authenticated
//!   data, so a sealed answer copied into another row, or another tenant's
//!   row, fails to open instead of being served to the wrong caller.
//! * **Keys come from the environment**, never from source or config files:
//!   `kid:base64key[,kid:base64key...]`. The first key seals; every key
//!   opens, so a key can be rotated by putting the new one first and keeping
//!   the old one until its answers expire.
//! * **Fail closed.** An answer that does not open (unknown key id, tampered
//!   bytes, wrong row) is never served; the caller gets `duplicate_request`
//!   instead. A malformed key setting stops the gateway starting.
//!
//! Stored form: `v1:<kid>:<base64(nonce || ciphertext || tag)>`.

use std::collections::HashMap;

use aes_gcm::{
    Aes256Gcm, Nonce,
    aead::{Aead, AeadCore, KeyInit, OsRng, Payload},
};
use base64::{Engine, engine::general_purpose::STANDARD};

const VERSION: &str = "v1";
const NONCE_LEN: usize = 12;

pub struct ResponseSealer {
    current: String,
    keys: HashMap<String, Aes256Gcm>,
}

/// Key material never reaches a log line, even through `{:?}`.
impl std::fmt::Debug for ResponseSealer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut ids: Vec<&String> = self.keys.keys().collect();
        ids.sort();
        f.debug_struct("ResponseSealer")
            .field("current", &self.current)
            .field("key_ids", &ids)
            .finish()
    }
}

fn valid_key_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 32
        && id.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
}

/// What a sealed answer is bound to: its tenant and its reservation.
pub fn binding(tenant_id: &str, reservation: uuid::Uuid) -> Vec<u8> {
    let mut aad = Vec::with_capacity(tenant_id.len() + 17);
    aad.extend_from_slice(tenant_id.as_bytes());
    aad.push(0);
    aad.extend_from_slice(reservation.as_bytes());
    aad
}

impl ResponseSealer {
    /// Parse `kid:base64key[,kid:base64key...]`. Each key must decode to
    /// exactly 32 bytes. Errors name the problem, never the key material.
    pub fn from_keys(spec: &str) -> Result<Self, String> {
        let mut current = None;
        let mut keys = HashMap::new();
        for (position, entry) in spec.split(',').map(str::trim).filter(|e| !e.is_empty()).enumerate() {
            let (id, encoded) = entry
                .split_once(':')
                .ok_or_else(|| format!("response key #{} must be `kid:base64key`", position + 1))?;
            if !valid_key_id(id) {
                return Err(format!(
                    "response key #{} has an invalid id: use 1 to 32 letters, digits, `_` or `-`",
                    position + 1
                ));
            }
            let bytes = STANDARD
                .decode(encoded)
                .map_err(|_| format!("response key `{id}` is not valid base64"))?;
            if bytes.len() != 32 {
                return Err(format!(
                    "response key `{id}` must decode to 32 bytes for AES-256, got {}",
                    bytes.len()
                ));
            }
            let cipher = Aes256Gcm::new_from_slice(&bytes)
                .map_err(|_| format!("response key `{id}` was rejected by AES-256-GCM"))?;
            if keys.insert(id.to_string(), cipher).is_some() {
                return Err(format!("response key id `{id}` appears twice"));
            }
            current.get_or_insert_with(|| id.to_string());
        }
        let current = current.ok_or("no response keys given")?;
        Ok(Self { current, keys })
    }

    /// Seal an answer for storage in the row named by `binding`.
    pub fn seal(&self, binding: &[u8], answer: &str) -> String {
        let cipher = &self.keys[&self.current];
        let nonce = Aes256Gcm::generate_nonce(&mut OsRng);
        // Encryption with a valid key and a fresh nonce cannot fail for
        // inputs of this size; a failure would be a bug, not a condition.
        let sealed = cipher
            .encrypt(&nonce, Payload { msg: answer.as_bytes(), aad: binding })
            .expect("AES-256-GCM encryption");
        let mut blob = Vec::with_capacity(NONCE_LEN + sealed.len());
        blob.extend_from_slice(&nonce);
        blob.extend_from_slice(&sealed);
        format!("{VERSION}:{}:{}", self.current, STANDARD.encode(blob))
    }

    /// Open an answer stored in the row named by `binding`. `None` for
    /// anything that does not verify: never serve bytes that did not.
    pub fn open(&self, binding: &[u8], stored: &str) -> Option<String> {
        let mut parts = stored.splitn(3, ':');
        let (version, id, encoded) = (parts.next()?, parts.next()?, parts.next()?);
        if version != VERSION {
            return None;
        }
        let cipher = self.keys.get(id)?;
        let blob = STANDARD.decode(encoded).ok()?;
        if blob.len() <= NONCE_LEN {
            return None;
        }
        let (nonce, sealed) = blob.split_at(NONCE_LEN);
        let plain = cipher
            .decrypt(Nonce::from_slice(nonce), Payload { msg: sealed, aad: binding })
            .ok()?;
        String::from_utf8(plain).ok()
    }

    pub fn current_key_id(&self) -> &str {
        &self.current
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    // Test keys only: 32 bytes each, base64.
    const K1: &str = "AQIDBAUGBwgJCgsMDQ4PEBESExQVFhcYGRobHB0eHyA=";
    const K2: &str = "ICEiIyQlJicoKSorLC0uLzAxMjM0NTY3ODk6Ozw9Pj8=";

    fn sealer(spec: &str) -> ResponseSealer {
        ResponseSealer::from_keys(spec).unwrap()
    }

    #[test]
    fn an_answer_round_trips_and_is_not_stored_in_the_clear() {
        let s = sealer(&format!("k1:{K1}"));
        let row = binding("acme", Uuid::new_v4());
        let stored = s.seal(&row, "the secret answer");
        assert!(stored.starts_with("v1:k1:"), "{stored}");
        assert!(!stored.contains("secret"), "the stored form must not contain the answer");
        assert_eq!(s.open(&row, &stored).as_deref(), Some("the secret answer"));
    }

    #[test]
    fn each_seal_uses_a_fresh_nonce() {
        let s = sealer(&format!("k1:{K1}"));
        let row = binding("acme", Uuid::new_v4());
        assert_ne!(s.seal(&row, "same"), s.seal(&row, "same"));
    }

    #[test]
    fn a_sealed_answer_only_opens_in_its_own_row() {
        let s = sealer(&format!("k1:{K1}"));
        let id = Uuid::new_v4();
        let stored = s.seal(&binding("acme", id), "answer");
        assert!(s.open(&binding("acme", Uuid::new_v4()), &stored).is_none(), "another request");
        assert!(s.open(&binding("other-tenant", id), &stored).is_none(), "another tenant");
    }

    #[test]
    fn tampering_is_detected_not_served() {
        let s = sealer(&format!("k1:{K1}"));
        let row = binding("acme", Uuid::new_v4());
        let stored = s.seal(&row, "answer");
        let (prefix, encoded) = stored.rsplit_once(':').unwrap();
        let mut blob = STANDARD.decode(encoded).unwrap();
        let last = blob.len() - 1;
        blob[last] ^= 1;
        let tampered = format!("{prefix}:{}", STANDARD.encode(blob));
        assert!(s.open(&row, &tampered).is_none());
        assert!(s.open(&row, "v1:k1:").is_none());
        assert!(s.open(&row, "v2:k1:AAAA").is_none());
        assert!(s.open(&row, "garbage").is_none());
    }

    #[test]
    fn rotation_seals_with_the_first_key_and_opens_with_any() {
        let row = binding("acme", Uuid::new_v4());
        let old = sealer(&format!("k1:{K1}"));
        let sealed_before = old.seal(&row, "before rotation");

        let rotated = sealer(&format!("k2:{K2},k1:{K1}"));
        assert_eq!(rotated.current_key_id(), "k2");
        assert_eq!(rotated.open(&row, &sealed_before).as_deref(), Some("before rotation"));
        let sealed_after = rotated.seal(&row, "after rotation");
        assert!(sealed_after.starts_with("v1:k2:"));

        // Retiring the old key makes its answers unreadable, not wrong.
        let retired = sealer(&format!("k2:{K2}"));
        assert!(retired.open(&row, &sealed_before).is_none());
    }

    #[test]
    fn malformed_key_settings_are_refused_without_echoing_key_material() {
        for (spec, fragment) in [
            ("", "no response keys"),
            ("k1", "kid:base64key"),
            ("bad id:AAAA", "invalid id"),
            ("k1:not-base64!!", "not valid base64"),
            ("k1:AAAA", "32 bytes"),
            (&format!("k1:{K1},k1:{K2}"), "twice"),
        ] {
            let err = ResponseSealer::from_keys(spec).unwrap_err();
            assert!(err.contains(fragment), "{spec:?} -> {err}");
            assert!(!err.contains(K1) && !err.contains(K2), "key material in error: {err}");
        }
    }

    #[test]
    fn debug_output_names_key_ids_only() {
        let rendered = format!("{:?}", sealer(&format!("k2:{K2},k1:{K1}")));
        assert!(rendered.contains("k1") && rendered.contains("k2"));
        assert!(!rendered.contains(K1) && !rendered.contains(K2));
    }
}
