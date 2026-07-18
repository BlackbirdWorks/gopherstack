package s3

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/md5" //nolint:gosec // G501: MD5 is required by the SSE-C key validation specification
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
)

// Static SSE errors. The Go-style "wrapped static" pattern keeps err113 happy
// and lets callers `errors.Is` for branch-specific handling.
var (
	errSSECKeySize   = errors.New("SSE-C key must decode to 32 bytes")
	errSSEMissingDEK = errors.New("stored DEK is missing or wrong size")
)

// dataKeySize is the byte length of an AES-256 data encryption key.
const dataKeySize = 32

// headerSSEAlgorithm is the request header that names the SSE algorithm.
const headerSSEAlgorithm = "X-Amz-Server-Side-Encryption"

// headerSSEKMSKeyID is the request header for the KMS master key ID.
const headerSSEKMSKeyID = "X-Amz-Server-Side-Encryption-Aws-Kms-Key-Id"

// headerSSECAlgorithm is the request header for SSE-C customer algorithm.
const headerSSECAlgorithm = "X-Amz-Server-Side-Encryption-Customer-Algorithm"

// headerSSECKey is the request header for the base64-encoded SSE-C customer key.
const headerSSECKey = "X-Amz-Server-Side-Encryption-Customer-Key"

// headerSSECKeyMD5 is the request header for the MD5 of the SSE-C customer key.
const headerSSECKeyMD5 = "X-Amz-Server-Side-Encryption-Customer-Key-Md5"

// sseInfo captures SSE parameters extracted from an HTTP request.
type sseInfo struct {
	// Algorithm is one of "AES256", "aws:kms", "aws:kms:dsse", or "" (none).
	Algorithm string
	// KMSKeyID is the KMS key ID, populated when Algorithm is aws:kms/dsse.
	KMSKeyID string
	// SSECAlgorithm is "AES256" when SSE-C is requested.
	SSECAlgorithm string
	// SSECKeyMD5 is the base64-encoded MD5 of the customer-supplied key.
	SSECKeyMD5 string
	// SSECKeyB64 is the base64-encoded raw customer key. Kept on the
	// request-scoped sseInfo only — not persisted (json:"-") — so the backend
	// can encrypt the body on PUT and the GET handler can decrypt when the
	// caller re-supplies it.
	SSECKeyB64 string `json:"-"`
}

// extractSSEInfo reads SSE-* request headers and validates SSE-C when present.
// Returns an error when the SSE-C key MD5 does not match the supplied key.
func extractSSEInfo(r *http.Request) (sseInfo, error) {
	info := sseInfo{
		Algorithm:     r.Header.Get(headerSSEAlgorithm),
		KMSKeyID:      r.Header.Get(headerSSEKMSKeyID),
		SSECAlgorithm: r.Header.Get(headerSSECAlgorithm),
		SSECKeyMD5:    r.Header.Get(headerSSECKeyMD5),
		SSECKeyB64:    r.Header.Get(headerSSECKey),
	}

	rawKey := r.Header.Get(headerSSECKey)
	if rawKey == "" {
		return info, nil
	}

	if err := validateSSECKey(rawKey, info.SSECKeyMD5); err != nil {
		return sseInfo{}, err
	}

	return info, nil
}

// validateSSECKey decodes the base64 key and verifies the supplied MD5.
func validateSSECKey(rawKeyB64, suppliedMD5B64 string) error {
	keyBytes, err := base64.StdEncoding.DecodeString(rawKeyB64)
	if err != nil {
		return ErrInvalidArgument
	}

	if suppliedMD5B64 == "" {
		return ErrInvalidArgument
	}

	sum := md5.Sum(keyBytes) //nolint:gosec // MD5 required by SSE-C specification
	computedMD5 := base64.StdEncoding.EncodeToString(sum[:])

	if computedMD5 != suppliedMD5B64 {
		return ErrBadChecksum
	}

	return nil
}

// setSSEResponseHeaders writes the appropriate SSE response headers.
func setSSEResponseHeaders(w http.ResponseWriter, info sseInfo) {
	if info.Algorithm != "" {
		w.Header().Set(headerSSEAlgorithm, info.Algorithm)
	}

	if info.KMSKeyID != "" {
		w.Header().Set(headerSSEKMSKeyID, info.KMSKeyID)
	}

	if info.SSECAlgorithm != "" {
		w.Header().Set(headerSSECAlgorithm, info.SSECAlgorithm)
	}

	if info.SSECKeyMD5 != "" {
		w.Header().Set(headerSSECKeyMD5, info.SSECKeyMD5)
	}
}

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
func encryptWithSSE(
	plaintext []byte,
	sse sseInfo,
	customerKeyB64 string,
) ([]byte, []byte, []byte, error) {
	if sse.Algorithm == "" && sse.SSECAlgorithm == "" {
		return plaintext, nil, nil, nil
	}

	switch {
	case sse.SSECAlgorithm != "":
		key, decodeErr := base64.StdEncoding.DecodeString(customerKeyB64)
		if decodeErr != nil || len(key) != 32 {
			return nil, nil, nil, errSSECKeySize
		}
		ct, n, encErr := aesGCMEncrypt(key, plaintext)
		if encErr != nil {
			return nil, nil, nil, encErr
		}

		return ct, nil, n, nil

	case sse.Algorithm != "":
		generatedDEK := make([]byte, dataKeySize)
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
func decryptWithSSE(
	ciphertext []byte,
	sseAlg, sseCAlg string,
	dek, nonce []byte,
	customerKeyB64 string,
) ([]byte, error) {
	if sseAlg == "" && sseCAlg == "" {
		return ciphertext, nil
	}

	if sseCAlg != "" {
		key, err := base64.StdEncoding.DecodeString(customerKeyB64)
		if err != nil || len(key) != dataKeySize {
			return nil, errSSECKeySize
		}

		return aesGCMDecrypt(key, nonce, ciphertext)
	}

	if len(dek) != dataKeySize {
		return nil, errSSEMissingDEK
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
