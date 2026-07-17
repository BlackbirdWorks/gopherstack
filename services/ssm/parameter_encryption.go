package ssm

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"io"
)

// aes256KeyLen is the byte length of an AES-256 key.
const aes256KeyLen = 32

// newInstanceGCM generates a random AES-256 key and returns a GCM cipher for
// it. Each InMemoryBackend instance calls this once so that different instances
// have distinct keys and their ciphertexts are not interchangeable.
func newInstanceGCM() cipher.AEAD {
	key := make([]byte, aes256KeyLen)
	if _, err := io.ReadFull(rand.Reader, key); err != nil {
		panic("ssm: failed to generate instance KMS key: " + err.Error())
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		panic("ssm: failed to create AES cipher: " + err.Error())
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		panic("ssm: failed to create GCM: " + err.Error())
	}

	return gcm
}

// encryptValue encrypts a value using the provided AES-GCM cipher.
func encryptValue(gcm cipher.AEAD, plaintext string) (string, error) {
	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return "", err
	}

	ciphertext := gcm.Seal(nonce, nonce, []byte(plaintext), nil)

	return base64.StdEncoding.EncodeToString(ciphertext), nil
}

// decryptValue decrypts a value encrypted with encryptValue using the same cipher.
func decryptValue(gcm cipher.AEAD, ciphertext string) (string, error) {
	ciphertextBytes, err := base64.StdEncoding.DecodeString(ciphertext)
	if err != nil {
		return "", err
	}

	nonceSize := gcm.NonceSize()
	if len(ciphertextBytes) < nonceSize {
		return "", ErrCiphertextTooShort
	}

	nonce, ciphertextOnly := ciphertextBytes[:nonceSize], ciphertextBytes[nonceSize:]

	plaintext, err := gcm.Open(nil, nonce, ciphertextOnly, nil)
	if err != nil {
		return "", err
	}

	return string(plaintext), nil
}

// encryptSSMValue encrypts a SecureString value using KMS (when keyID is set
// and a KMSEncryptor is wired) or the built-in mock GCM cipher.
// Returns base64-encoded ciphertext for storage.
func (b *InMemoryBackend) encryptSSMValue(keyID, plaintext string) (string, error) {
	if keyID != "" && b.kms != nil {
		ct, err := b.kms.EncryptSSM(keyID, []byte(plaintext))
		if err != nil {
			return "", fmt.Errorf("%w: %w", ErrInvalidKeyID, err)
		}

		return base64.StdEncoding.EncodeToString(ct), nil
	}

	return encryptValue(b.gcm, plaintext)
}

// decryptSSMValue decrypts a stored SecureString value.  When the value was
// encrypted with KMS (detected by attempting KMS decrypt when a backend is
// available), the KMS path is used; otherwise falls back to the instance cipher.
func (b *InMemoryBackend) decryptSSMValue(keyID, ciphertext string) (string, error) {
	if keyID != "" && b.kms != nil {
		ct, err := base64.StdEncoding.DecodeString(ciphertext)
		if err != nil {
			return "", err
		}
		pt, err := b.kms.DecryptSSM(ct)
		if err != nil {
			return "", err
		}

		return string(pt), nil
	}

	return decryptValue(b.gcm, ciphertext)
}
