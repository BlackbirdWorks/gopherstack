package kms_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kms"
)

// TestKeyMaterialHistory_CappedAtMax verifies that rotating a key more than
// maxKeyMaterialHistoryEntries times does not grow the history slice beyond the
// cap, preventing unbounded memory growth on long-lived mock instances.
func TestKeyMaterialHistory_CappedAtMax(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		rotations int
	}{
		{name: "at cap", rotations: kms.MaxKeyMaterialHistoryEntriesForTest},
		{name: "one over cap", rotations: kms.MaxKeyMaterialHistoryEntriesForTest + 1},
		{name: "well over cap", rotations: kms.MaxKeyMaterialHistoryEntriesForTest + 20},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			b := kms.NewInMemoryBackend()

			out, err := b.CreateKey(ctx, &kms.CreateKeyInput{})
			require.NoError(t, err)
			keyID := out.KeyMetadata.KeyID

			var rotErr error
			for i := range tc.rotations {
				rotErr = b.ForceRotateForTest(keyID)
				require.NoError(t, rotErr, "rotation %d must succeed", i)
			}

			histLen := b.KeyMaterialHistoryLenForTest(keyID)
			require.LessOrEqual(t, histLen, kms.MaxKeyMaterialHistoryEntriesForTest,
				"key material history must not exceed the cap after %d rotations", tc.rotations)
		})
	}
}

// TestKeyMaterialHistory_OldestMaterialDropped verifies that when the history
// cap is reached, the oldest entries are dropped (not the newest), so that
// decrypt operations against recently-rotated keys still work.
func TestKeyMaterialHistory_OldestMaterialDropped(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	b := kms.NewInMemoryBackend()

	out, err := b.CreateKey(ctx, &kms.CreateKeyInput{})
	require.NoError(t, err)
	keyID := out.KeyMetadata.KeyID

	// Rotate past the cap.
	extra := 5
	for i := range kms.MaxKeyMaterialHistoryEntriesForTest + extra {
		require.NoError(t, b.ForceRotateForTest(keyID), "rotation %d", i)
	}

	// Encryption with the current key must still work after excess rotations.
	enc, err := b.Encrypt(ctx, &kms.EncryptInput{
		KeyID:     keyID,
		Plaintext: []byte("hello"),
	})
	require.NoError(t, err)

	_, err = b.Decrypt(ctx, &kms.DecryptInput{
		CiphertextBlob: enc.CiphertextBlob,
	})
	require.NoError(t, err, "decrypt must succeed — current key material must remain accessible")
}
