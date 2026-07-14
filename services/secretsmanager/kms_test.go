package secretsmanager_test

import (
	"context"
	"encoding/base64"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	sm "github.com/blackbirdworks/gopherstack/services/secretsmanager"
)

// fakeKMSEncryptor is a test double for sm.KMSEncryptor that performs a
// reversible, clearly-not-plaintext transform (a "CIPHERTEXT:" prefix over
// base64) so tests can assert the stored bytes actually changed, without
// depending on the real KMS backend (that round trip is covered separately by
// the root-package wiring test against a real kms.InMemoryBackend).
type fakeKMSEncryptor struct {
	keyIDs       []string
	mu           sync.Mutex
	encryptCalls int
	decryptCalls int
}

const fakeCiphertextPrefix = "CIPHERTEXT:"

var errUnrecognisedCiphertext = errors.New("fakeKMSEncryptor: not a recognised ciphertext")

func (f *fakeKMSEncryptor) Encrypt(_ context.Context, keyID string, plaintext []byte) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.encryptCalls++
	f.keyIDs = append(f.keyIDs, keyID)

	return []byte(fakeCiphertextPrefix + base64.StdEncoding.EncodeToString(plaintext)), nil
}

func (f *fakeKMSEncryptor) Decrypt(_ context.Context, ciphertext []byte) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.decryptCalls++

	s := string(ciphertext)
	if !strings.HasPrefix(s, fakeCiphertextPrefix) {
		return nil, errUnrecognisedCiphertext
	}

	return base64.StdEncoding.DecodeString(strings.TrimPrefix(s, fakeCiphertextPrefix))
}

// ensure fakeKMSEncryptor satisfies sm.KMSEncryptor at compile time.
var _ sm.KMSEncryptor = (*fakeKMSEncryptor)(nil)

// TestKMSEncryptor_NoEncryptor_StoresPlaintextUnchanged proves the additive,
// backward-compatible contract: a backend that never calls SetKMSEncryptor
// behaves exactly as before KMS integration -- values are stored and
// returned as plaintext, with no Ciphertext populated.
func TestKMSEncryptor_NoEncryptor_StoresPlaintextUnchanged(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	ctx := context.Background()

	_, err := b.CreateSecret(ctx, &sm.CreateSecretInput{
		Name:         "plain-secret",
		SecretString: "hello-world",
	})
	require.NoError(t, err)

	raw, ok := sm.SecretVersionRaw(b, sm.MockRegion, "plain-secret", "")
	require.True(t, ok)
	assert.Nil(t, raw.Ciphertext, "no encryptor wired: Ciphertext must stay nil")
	assert.Equal(t, "hello-world", raw.SecretString, "no encryptor wired: SecretString must be stored as plaintext")

	out, err := b.GetSecretValue(ctx, &sm.GetSecretValueInput{SecretID: "plain-secret"})
	require.NoError(t, err)
	assert.Equal(t, "hello-world", out.SecretString)
}

// TestKMSEncryptor_CreateGetRoundTrip_String proves CreateSecret encrypts a
// string value via the wired KMSEncryptor, the persisted form is ciphertext
// (not the original plaintext), and GetSecretValue transparently decrypts it
// back to the original value.
func TestKMSEncryptor_CreateGetRoundTrip_String(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	fake := &fakeKMSEncryptor{}
	b.SetKMSEncryptor(fake)
	ctx := context.Background()

	const plaintext = "super-secret-database-password"

	_, err := b.CreateSecret(ctx, &sm.CreateSecretInput{
		Name:         "encrypted-secret",
		SecretString: plaintext,
	})
	require.NoError(t, err)

	assert.Equal(t, 1, fake.encryptCalls, "CreateSecret with a value must encrypt exactly once")
	require.Len(t, fake.keyIDs, 1)
	assert.Equal(t, sm.DefaultKMSKeyAlias, fake.keyIDs[0],
		"a secret with no KmsKeyId must encrypt under the default managed-key alias")

	raw, ok := sm.SecretVersionRaw(b, sm.MockRegion, "encrypted-secret", "")
	require.True(t, ok)
	require.NotNil(t, raw.Ciphertext, "stored version must hold ciphertext when an encryptor is wired")
	assert.Empty(t, raw.SecretString, "SecretString must be cleared once the value is sealed")
	assert.NotEqual(t, plaintext, string(raw.Ciphertext), "stored bytes must differ from the plaintext")
	assert.True(t, raw.WasString)

	out, err := b.GetSecretValue(ctx, &sm.GetSecretValueInput{SecretID: "encrypted-secret"})
	require.NoError(t, err)
	assert.Equal(t, plaintext, out.SecretString, "GetSecretValue must transparently decrypt back to the original value")
	assert.Empty(t, out.SecretBinary)
	assert.Equal(t, 1, fake.decryptCalls)
}

// TestKMSEncryptor_CreateGetRoundTrip_Binary covers the SecretBinary path and
// an explicit customer-managed KmsKeyId.
func TestKMSEncryptor_CreateGetRoundTrip_Binary(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	fake := &fakeKMSEncryptor{}
	b.SetKMSEncryptor(fake)
	ctx := context.Background()

	plaintext := []byte{0x00, 0x01, 0xFF, 0xAB, 0xCD}
	const customKey = "arn:aws:kms:us-east-1:123456789012:key/custom-key"

	_, err := b.CreateSecret(ctx, &sm.CreateSecretInput{
		Name:         "encrypted-binary-secret",
		SecretBinary: plaintext,
		KmsKeyID:     customKey,
	})
	require.NoError(t, err)

	require.Len(t, fake.keyIDs, 1)
	assert.Equal(t, customKey, fake.keyIDs[0], "an explicit KmsKeyId must be used instead of the default alias")

	raw, ok := sm.SecretVersionRaw(b, sm.MockRegion, "encrypted-binary-secret", "")
	require.True(t, ok)
	require.NotNil(t, raw.Ciphertext)
	assert.Empty(t, raw.SecretBinary)
	assert.False(t, raw.WasString)
	assert.NotEqual(t, string(plaintext), string(raw.Ciphertext))

	out, err := b.GetSecretValue(ctx, &sm.GetSecretValueInput{SecretID: "encrypted-binary-secret"})
	require.NoError(t, err)
	assert.Equal(t, plaintext, out.SecretBinary)
	assert.Empty(t, out.SecretString)
}

// TestKMSEncryptor_PutSecretValue_NewVersionEncrypted proves PutSecretValue
// seals the new version, and that the previous (also sealed) version remains
// independently decryptable.
func TestKMSEncryptor_PutSecretValue_NewVersionEncrypted(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	fake := &fakeKMSEncryptor{}
	b.SetKMSEncryptor(fake)
	ctx := context.Background()

	_, err := b.CreateSecret(ctx, &sm.CreateSecretInput{Name: "rotating-secret", SecretString: "v1"})
	require.NoError(t, err)

	putOut, err := b.PutSecretValue(ctx, &sm.PutSecretValueInput{SecretID: "rotating-secret", SecretString: "v2"})
	require.NoError(t, err)

	raw, ok := sm.SecretVersionRaw(b, sm.MockRegion, "rotating-secret", putOut.VersionID)
	require.True(t, ok)
	require.NotNil(t, raw.Ciphertext)

	current, err := b.GetSecretValue(ctx, &sm.GetSecretValueInput{SecretID: "rotating-secret"})
	require.NoError(t, err)
	assert.Equal(t, "v2", current.SecretString)

	previous, err := b.GetSecretValue(ctx, &sm.GetSecretValueInput{
		SecretID: "rotating-secret", VersionStage: sm.StagingLabelPrevious,
	})
	require.NoError(t, err)
	assert.Equal(t, "v1", previous.SecretString, "the superseded AWSPREVIOUS version must still decrypt correctly")
}

// TestKMSEncryptor_RotateSecret_NoLambda_CarriesValueForward proves that
// RotateSecret without a rotation Lambda configured carries the sealed value
// forward into the new AWSPENDING/AWSCURRENT version without needing to
// re-encrypt it, and that it still decrypts correctly afterwards.
func TestKMSEncryptor_RotateSecret_NoLambda_CarriesValueForward(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	fake := &fakeKMSEncryptor{}
	b.SetKMSEncryptor(fake)
	ctx := context.Background()

	_, err := b.CreateSecret(ctx, &sm.CreateSecretInput{Name: "rotate-me", SecretString: "stable-value"})
	require.NoError(t, err)

	callsBeforeRotate := fake.encryptCalls

	_, err = b.RotateSecret(ctx, &sm.RotateSecretInput{SecretID: "rotate-me"})
	require.NoError(t, err)

	assert.Equal(t, callsBeforeRotate, fake.encryptCalls,
		"rotation without a Lambda ARN must carry the ciphertext forward, not re-encrypt")

	out, err := b.GetSecretValue(ctx, &sm.GetSecretValueInput{SecretID: "rotate-me"})
	require.NoError(t, err)
	assert.Equal(t, "stable-value", out.SecretString)
}

// TestKMSEncryptor_BatchGetSecretValue_Decrypts proves BatchGetSecretValue
// also routes through decryption.
func TestKMSEncryptor_BatchGetSecretValue_Decrypts(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	fake := &fakeKMSEncryptor{}
	b.SetKMSEncryptor(fake)
	ctx := context.Background()

	_, err := b.CreateSecret(ctx, &sm.CreateSecretInput{Name: "batch-a", SecretString: "value-a"})
	require.NoError(t, err)
	_, err = b.CreateSecret(ctx, &sm.CreateSecretInput{Name: "batch-b", SecretString: "value-b"})
	require.NoError(t, err)

	out, err := b.BatchGetSecretValue(ctx, &sm.BatchGetSecretValueInput{
		SecretIDList: []string{"batch-a", "batch-b"},
	})
	require.NoError(t, err)
	require.Empty(t, out.Errors)
	require.Len(t, out.SecretValues, 2)

	got := map[string]string{}
	for _, v := range out.SecretValues {
		got[v.Name] = v.SecretString
	}

	assert.Equal(t, "value-a", got["batch-a"])
	assert.Equal(t, "value-b", got["batch-b"])
}
