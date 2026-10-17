use anyhow::{Result, ensure};

#[derive(Clone, Default)]
pub struct EncryptionClient;

impl EncryptionClient {
    pub async fn encrypt(&self, secret_key_id: Option<i64>, data: Vec<u8>) -> Result<Vec<u8>> {
        // Never silently store plaintext when the table requires encryption.
        ensure!(
            secret_key_id.is_none(),
            "Encryption service is not configured"
        );
        Ok(data)
    }

    pub async fn decrypt(&self, secret_key_id: Option<i64>, data: Vec<u8>) -> Result<Vec<u8>> {
        ensure!(
            secret_key_id.is_none(),
            "Encryption service is not configured"
        );
        Ok(data)
    }
}
