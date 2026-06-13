package kms_test

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kms"
)

func TestEncrypt_RejectsOversizeEncryptionContext(t *testing.T) {
	t.Parallel()

	b := kms.NewInMemoryBackend()
	create, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{})
	require.NoError(t, err)

	oversizedValue := strings.Repeat("v", 5000)

	_, err = b.Encrypt(context.Background(), &kms.EncryptInput{
		KeyID:             create.KeyMetadata.KeyID,
		Plaintext:         []byte("hello"),
		EncryptionContext: map[string]string{"k": oversizedValue},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "EncryptionContext")
}

func TestDecrypt_RejectsOversizeEncryptionContext(t *testing.T) {
	t.Parallel()

	b := kms.NewInMemoryBackend()

	oversizedValue := strings.Repeat("v", 5000)

	_, err := b.Decrypt(context.Background(), &kms.DecryptInput{
		CiphertextBlob:    make([]byte, 64),
		EncryptionContext: map[string]string{"k": oversizedValue},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "EncryptionContext")
}
