package ssm_test

// parity_batch7_test.go — §3 Single-service B (#21 SSM SecureString KMS)

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kms"
	"github.com/blackbirdworks/gopherstack/services/ssm"
)

// testKMSAdapter adapts kms.InMemoryBackend to ssm.KMSEncryptor.
type testKMSAdapter struct {
	b *kms.InMemoryBackend
}

func (a *testKMSAdapter) EncryptSSM(keyID string, plaintext []byte) ([]byte, error) {
	out, err := a.b.Encrypt(context.Background(), &kms.EncryptInput{KeyID: keyID, Plaintext: plaintext})
	if err != nil {
		return nil, err
	}

	return out.CiphertextBlob, nil
}

func (a *testKMSAdapter) DecryptSSM(ciphertext []byte) ([]byte, error) {
	out, err := a.b.Decrypt(context.Background(), &kms.DecryptInput{CiphertextBlob: ciphertext})
	if err != nil {
		return nil, err
	}

	return out.Plaintext, nil
}

// newSSMWithKMS creates an SSM backend wired to a real KMS backend.
// Returns the SSM backend and the created key ID.
func newSSMWithKMS(t *testing.T) (*ssm.InMemoryBackend, string) {
	t.Helper()
	kmsBackend := kms.NewInMemoryBackend()
	keyOut, err := kmsBackend.CreateKey(t.Context(), &kms.CreateKeyInput{Description: "test key"})
	require.NoError(t, err)
	keyID := keyOut.KeyMetadata.KeyID

	ssmBackend := ssm.NewInMemoryBackend()
	ssmBackend.WithKMS(&testKMSAdapter{b: kmsBackend})

	return ssmBackend, keyID
}

// ---------------------------------------------------------------------------
// #21 — SSM SecureString with real KMS encrypt/decrypt
// ---------------------------------------------------------------------------

func TestSSMSecureStringKMS_EncryptedAtRest(t *testing.T) {
	t.Parallel()

	ssmBackend, keyID := newSSMWithKMS(t)

	// PutParameter — value should be KMS-encrypted.
	_, err := ssmBackend.PutParameter(context.TODO(), &ssm.PutParameterInput{
		Name:  "/test/secret",
		Type:  "SecureString",
		Value: "my-plaintext-password",
		KeyID: keyID,
	})
	require.NoError(t, err)

	// Read without decryption — must not be plaintext.
	raw, err := ssmBackend.GetParameter(context.TODO(), &ssm.GetParameterInput{
		Name:           "/test/secret",
		WithDecryption: false,
	})
	require.NoError(t, err)
	assert.NotEqual(t, "my-plaintext-password", raw.Parameter.Value, "plaintext must not be stored as-is")
}

func TestSSMSecureStringKMS_DecryptOnGet(t *testing.T) {
	t.Parallel()

	ssmBackend, keyID := newSSMWithKMS(t)

	_, err := ssmBackend.PutParameter(context.TODO(), &ssm.PutParameterInput{
		Name:  "/test/db-pass",
		Type:  "SecureString",
		Value: "super-secret-db-password",
		KeyID: keyID,
	})
	require.NoError(t, err)

	out, err := ssmBackend.GetParameter(context.TODO(), &ssm.GetParameterInput{
		Name:           "/test/db-pass",
		WithDecryption: true,
	})
	require.NoError(t, err)
	assert.Equal(t, "super-secret-db-password", out.Parameter.Value)
}

func TestSSMSecureStringKMS_MultipleParams(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		keyValue string
		pname    string
	}{
		{"alpha", "password-alpha", "/multi/alpha"},
		{"beta", "password-beta", "/multi/beta"},
		{"gamma", "password-gamma", "/multi/gamma"},
	}

	ssmBackend, keyID := newSSMWithKMS(t)

	for _, tt := range tests {
		_, err := ssmBackend.PutParameter(context.TODO(), &ssm.PutParameterInput{
			Name:  tt.pname,
			Type:  "SecureString",
			Value: tt.keyValue,
			KeyID: keyID,
		})
		require.NoError(t, err, "param %s", tt.pname)
	}

	for _, tt := range tests {
		out, err := ssmBackend.GetParameter(context.TODO(), &ssm.GetParameterInput{
			Name:           tt.pname,
			WithDecryption: true,
		})
		require.NoError(t, err, "get %s", tt.pname)
		assert.Equal(t, tt.keyValue, out.Parameter.Value, "decrypted value mismatch for %s", tt.pname)
	}
}

func TestSSMSecureStringKMS_GetParameters(t *testing.T) {
	t.Parallel()

	ssmBackend, keyID := newSSMWithKMS(t)

	for _, p := range []struct{ name, val string }{
		{"/batch/x", "val-x"},
		{"/batch/y", "val-y"},
	} {
		_, err := ssmBackend.PutParameter(context.TODO(), &ssm.PutParameterInput{
			Name:  p.name,
			Type:  "SecureString",
			Value: p.val,
			KeyID: keyID,
		})
		require.NoError(t, err)
	}

	out, err := ssmBackend.GetParameters(context.TODO(), &ssm.GetParametersInput{
		Names:          []string{"/batch/x", "/batch/y"},
		WithDecryption: true,
	})
	require.NoError(t, err)
	require.Len(t, out.Parameters, 2)
	vals := map[string]string{}
	for _, p := range out.Parameters {
		vals[p.Name] = p.Value
	}
	assert.Equal(t, "val-x", vals["/batch/x"])
	assert.Equal(t, "val-y", vals["/batch/y"])
}

func TestSSMSecureStringKMS_GetParametersByPath(t *testing.T) {
	t.Parallel()

	ssmBackend, keyID := newSSMWithKMS(t)

	for _, p := range []struct{ name, val string }{
		{"/path/a", "v-a"},
		{"/path/b", "v-b"},
	} {
		_, err := ssmBackend.PutParameter(context.TODO(), &ssm.PutParameterInput{
			Name:  p.name,
			Type:  "SecureString",
			Value: p.val,
			KeyID: keyID,
		})
		require.NoError(t, err)
	}

	out, err := ssmBackend.GetParametersByPath(context.TODO(), &ssm.GetParametersByPathInput{
		Path:           "/path/",
		WithDecryption: true,
	})
	require.NoError(t, err)
	require.Len(t, out.Parameters, 2)
	vals := map[string]string{}
	for _, p := range out.Parameters {
		vals[p.Name] = p.Value
	}
	assert.Equal(t, "v-a", vals["/path/a"])
	assert.Equal(t, "v-b", vals["/path/b"])
}

func TestSSMSecureStringKMS_History(t *testing.T) {
	t.Parallel()

	ssmBackend, keyID := newSSMWithKMS(t)

	for _, v := range []string{"v1", "v2"} {
		_, err := ssmBackend.PutParameter(context.TODO(), &ssm.PutParameterInput{
			Name:      "/hist/key",
			Type:      "SecureString",
			Value:     v,
			KeyID:     keyID,
			Overwrite: true,
		})
		require.NoError(t, err)
	}

	hist, err := ssmBackend.GetParameterHistory(context.TODO(), &ssm.GetParameterHistoryInput{
		Name:           "/hist/key",
		WithDecryption: true,
	})
	require.NoError(t, err)
	require.Len(t, hist.Parameters, 2)
	vals := map[string]bool{}
	for _, p := range hist.Parameters {
		vals[p.Value] = true
	}
	assert.True(t, vals["v1"], "history should contain v1")
	assert.True(t, vals["v2"], "history should contain v2")
}

func TestSSMSecureStringKMS_MockFallback(t *testing.T) {
	t.Parallel()

	// Without KMS wired, SSM uses built-in mock cipher.
	backend := ssm.NewInMemoryBackend()

	_, err := backend.PutParameter(context.TODO(), &ssm.PutParameterInput{
		Name:  "/no-kms/secret",
		Type:  "SecureString",
		Value: "mock-plaintext",
	})
	require.NoError(t, err)

	out, err := backend.GetParameter(context.TODO(), &ssm.GetParameterInput{
		Name:           "/no-kms/secret",
		WithDecryption: true,
	})
	require.NoError(t, err)
	assert.Equal(t, "mock-plaintext", out.Parameter.Value)
}
