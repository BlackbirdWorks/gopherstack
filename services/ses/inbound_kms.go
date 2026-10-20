package ses

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
)

const (
	envelopeKeyBytes = 32
	envelopeTagBits  = "128"
)

var errInboundNoKMS = errors.New("ses inbound: no KMS backend wired")

// InboundKMS issues KMS data keys for S3 receipt action envelope encryption.
type InboundKMS interface {
	GenerateDataKey(ctx context.Context, keyID string, encCtx map[string]string) (plain, wrapped []byte, err error)
}

// InboundMetadataStore is an InboundObjectStore that also stores user metadata.
type InboundMetadataStore interface {
	PutObjectWithMetadata(ctx context.Context, bucket, key string, body []byte, meta map[string]string) error
}

// SetInboundKMS wires the KMS backend used by S3 receipt actions with a KmsKeyArn.
func (b *InMemoryBackend) SetInboundKMS(k InboundKMS) {
	b.mu.Lock("SetInboundKMS")
	defer b.mu.Unlock()

	b.inboundKMS = k
}

// sealEnvelope encrypts body under a fresh KMS data key and returns the ciphertext plus the
// S3 encryption client v2 envelope metadata (x-amz-key-v2, x-amz-iv, x-amz-matdesc, ...).
func sealEnvelope(
	ctx context.Context, k InboundKMS, keyARN string, encCtx map[string]string, body []byte,
) ([]byte, map[string]string, error) {
	plain, wrapped, err := k.GenerateDataKey(ctx, keyARN, encCtx)
	if err != nil {
		return nil, nil, fmt.Errorf("kms generate data key: %w", err)
	}

	block, err := aes.NewCipher(plain[:min(len(plain), envelopeKeyBytes)])
	if err != nil {
		return nil, nil, fmt.Errorf("aes key: %w", err)
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, nil, fmt.Errorf("gcm: %w", err)
	}

	iv := make([]byte, gcm.NonceSize())
	if _, err = rand.Read(iv); err != nil {
		return nil, nil, fmt.Errorf("iv: %w", err)
	}

	matdesc, err := json.Marshal(encCtx)
	if err != nil {
		return nil, nil, fmt.Errorf("matdesc: %w", err)
	}

	meta := map[string]string{
		"x-amz-key-v2":   base64.StdEncoding.EncodeToString(wrapped),
		"x-amz-iv":       base64.StdEncoding.EncodeToString(iv),
		"x-amz-matdesc":  string(matdesc),
		"x-amz-cek-alg":  "AES/GCM/NoPadding",
		"x-amz-wrap-alg": "kms",
		"x-amz-tag-len":  envelopeTagBits,
	}

	return gcm.Seal(nil, iv, body, nil), meta, nil
}
