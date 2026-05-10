package s3

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
)

// encryptWithSSE applies envelope-style AES-256-GCM encryption to the supplied
// plaintext when the request specified server-side encryption. Returns the
// ciphertext along with the data key (DEK) and nonce that must be persisted
// on the object version so decryption can round-trip on a later GET.
//
// Modes implemented:
//
//   - SSE-S3 (AES256): generates a random 256-bit DEK. The DEK is held in
//     memory next to the version, which mirrors envelope encryption at the
//     bit-pattern level even though we don't actually wrap it under a CMK.
//
//   - SSE-KMS (aws:kms / aws:kms:dsse): generates a random 256-bit DEK, same
//     as SSE-S3. The KMS key ID is recorded on the version for the
//     subsequent x-amz-server-side-encryption-aws-kms-key-id header.
//
//   - SSE-C: derives the DEK from the customer-supplied key bytes (base64-
//     encoded in the X-Amz-Server-Side-Encryption-Customer-Key header).
//     Returns nil DEK so callers know NOT to persist it — SSE-C requires
//     the customer to re-supply the key on every GET.
//
// Real AWS computes ETag = MD5(plaintext) for SSE-S3, an opaque value for
// SSE-KMS/SSE-C. For tests we keep ETag = MD5(plaintext) across the board so
// existing checksum-based assertions still match.
func encryptWithSSE(plaintext []byte, sse sseInfo, customerKeyB64 string) (
	ciphertext, dek, nonce []byte, err error,
) {
	if sse.Algorithm == "" && sse.SSECAlgorithm == "" {
		return plaintext, nil, nil, nil
	}

	switch {
	case sse.SSECAlgorithm != "":
		key, decodeErr := base64.StdEncoding.DecodeString(customerKeyB64)
		if decodeErr != nil || len(key) != 32 {
			return nil, nil, nil, errors.New("SSE-C key must decode to 32 bytes")
		}
		ct, n, encErr := aesGCMEncrypt(key, plaintext)
		if encErr != nil {
			return nil, nil, nil, encErr
		}

		return ct, nil, n, nil

	case sse.Algorithm != "":
		generatedDEK := make([]byte, 32)
		if _, randErr := rand.Read(generatedDEK); randErr != nil {
			return nil, nil, nil, fmt.Errorf("generating DEK: %w", randErr)
		}
		ct, n, encErr := aesGCMEncrypt(generatedDEK, plaintext)
		if encErr != nil {
			return nil, nil, nil, encErr
		}

		return ct, generatedDEK, n, nil
	}

	return plaintext, nil, nil, nil
}

// decryptWithSSE reverses encryptWithSSE using the DEK + nonce stored on the
// version (or, for SSE-C, the customer-supplied key from the GET request).
//
// Returns the original plaintext when the stored algorithm was empty (no SSE
// was applied at PUT-time), so it's safe to call unconditionally.
func decryptWithSSE(ciphertext []byte, sseAlg, sseCAlg string, dek, nonce []byte, customerKeyB64 string) ([]byte, error) {
	if sseAlg == "" && sseCAlg == "" {
		return ciphertext, nil
	}

	if sseCAlg != "" {
		key, err := base64.StdEncoding.DecodeString(customerKeyB64)
		if err != nil || len(key) != 32 {
			return nil, errors.New("SSE-C key must decode to 32 bytes")
		}

		return aesGCMDecrypt(key, nonce, ciphertext)
	}

	if len(dek) != 32 {
		return nil, errors.New("stored DEK is missing or wrong size")
	}

	return aesGCMDecrypt(dek, nonce, ciphertext)
}

// aesGCMEncrypt seals plaintext under AES-256-GCM using a fresh 12-byte nonce.
// Returns the ciphertext (which includes the 16-byte auth tag appended by GCM)
// and the nonce. Both must be stored to support decrypt later.
func aesGCMEncrypt(key, plaintext []byte) ([]byte, []byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, nil, fmt.Errorf("aes.NewCipher: %w", err)
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, nil, fmt.Errorf("cipher.NewGCM: %w", err)
	}

	nonce := make([]byte, gcm.NonceSize())
	if _, randErr := rand.Read(nonce); randErr != nil {
		return nil, nil, fmt.Errorf("generating nonce: %w", randErr)
	}

	return gcm.Seal(nil, nonce, plaintext, nil), nonce, nil
}

// aesGCMDecrypt opens a ciphertext previously produced by aesGCMEncrypt with
// the same key + nonce. Returns an authentication failure when the nonce
// doesn't match or the ciphertext was tampered.
func aesGCMDecrypt(key, nonce, ciphertext []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("aes.NewCipher: %w", err)
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("cipher.NewGCM: %w", err)
	}

	plaintext, err := gcm.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return nil, fmt.Errorf("ciphertext authentication failed: %w", err)
	}

	return plaintext, nil
}
