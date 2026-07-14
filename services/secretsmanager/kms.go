package secretsmanager

import (
	"context"
	"errors"
	"fmt"
)

// KMSEncryptor provides symmetric encrypt/decrypt for secret values via the
// real KMS backend. It is implemented by an adapter wrapping kms.InMemoryBackend
// (see cli.go's secretsManagerKMSAdapter and wireSecretsManagerKMS), mirroring
// the ssm.KMSEncryptor precedent used to wire SSM SecureString parameters to
// KMS.
type KMSEncryptor interface {
	// Encrypt encrypts plaintext under the given KMS key ID/ARN/alias and
	// returns the ciphertext blob to persist.
	Encrypt(ctx context.Context, keyID string, plaintext []byte) ([]byte, error)
	// Decrypt decrypts a ciphertext blob previously produced by Encrypt and
	// returns the original plaintext.
	Decrypt(ctx context.Context, ciphertext []byte) ([]byte, error)
}

// DefaultKMSKeyAlias is the key alias Secrets Manager encrypts a secret's
// values under when the secret does not specify KmsKeyId, mirroring real
// AWS's implicit account-level "aws/secretsmanager" managed key. Exported so
// cli.go's KMS adapter can recognise it and substitute a real backing key,
// since kms.InMemoryBackend.CreateAlias rejects the "alias/aws/" prefix
// (reserved for genuine AWS managed keys).
const DefaultKMSKeyAlias = "alias/aws/secretsmanager"

var (
	// ErrKMSEncryptionFailed wraps an error returned by the wired KMSEncryptor
	// while sealing a secret value.
	ErrKMSEncryptionFailed = errors.New("InvalidRequestException")
	// ErrKMSDecryptionFailed wraps an error returned by the wired KMSEncryptor
	// while opening a sealed secret value.
	ErrKMSDecryptionFailed = errors.New("InvalidRequestException")
	// ErrKMSNotConfigured is returned when a stored secret version was sealed
	// via KMS (Ciphertext is set) but no KMSEncryptor is currently wired to
	// decrypt it -- e.g. a snapshot was restored into a backend that never
	// called SetKMSEncryptor.
	ErrKMSNotConfigured = errors.New("InvalidRequestException: secret value was encrypted with KMS" +
		" but no KMS backend is wired")
)

// SetKMSEncryptor attaches e so that CreateSecret/PutSecretValue/UpdateSecret
// (and rotation) encrypt secret values via the real KMS backend, and
// GetSecretValue/BatchGetSecretValue decrypt them on read. Passing nil (the
// zero value) restores the default, backward-compatible behaviour: values are
// stored and returned exactly as given, with no encryption performed. A
// backend that never calls SetKMSEncryptor behaves identically to the
// pre-KMS-integration implementation.
func (b *InMemoryBackend) SetKMSEncryptor(e KMSEncryptor) {
	b.kms = e
}

// resolveEncryptKeyID returns the KMS key identifier a secret's values should
// be encrypted under: the secret's own KmsKeyId when set, otherwise the
// default managed-key alias (matching real AWS's implicit aws/secretsmanager
// key used when CreateSecret/PutSecretValue omit KmsKeyId).
func resolveEncryptKeyID(secret *Secret) string {
	if secret.KmsKeyID != "" {
		return secret.KmsKeyID
	}

	return DefaultKMSKeyAlias
}

// sealVersion builds a new SecretVersion from plaintext input, encrypting the
// value via the wired KMSEncryptor (into the Ciphertext field) when one is
// set. When b.kms is nil the version is populated exactly as before KMS
// integration -- SecretString/SecretBinary hold the plaintext directly and
// Ciphertext stays nil -- so backends that never call SetKMSEncryptor are
// unaffected. Must be called with b.mu held (write lock).
func (b *InMemoryBackend) sealVersion(
	ctx context.Context,
	secret *Secret,
	versionID string,
	secretString string,
	secretBinary []byte,
	stagingLabels []string,
	createdDate float64,
) (*SecretVersion, error) {
	v := &SecretVersion{
		VersionID:     versionID,
		StagingLabels: stagingLabels,
		CreatedDate:   createdDate,
	}

	if b.kms == nil {
		v.SecretString = secretString
		v.SecretBinary = secretBinary

		return v, nil
	}

	wasString := secretString != ""

	plaintext := []byte(secretString)
	if !wasString {
		plaintext = secretBinary
	}

	ct, err := b.kms.Encrypt(ctx, resolveEncryptKeyID(secret), plaintext)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrKMSEncryptionFailed, err)
	}

	v.Ciphertext = ct
	v.WasString = wasString

	return v, nil
}

// matchesExistingVersion reports whether existing already holds exactly the
// plaintext content in secretString/secretBinary -- decrypting existing first
// when it was sealed via KMS -- so CreateSecret/PutSecretValue/UpdateSecret
// can treat a retried request carrying the same version/ClientRequestToken
// as an idempotent no-op. Must be called with b.mu held.
func (b *InMemoryBackend) matchesExistingVersion(
	ctx context.Context, existing *SecretVersion, secretString string, secretBinary []byte,
) (bool, error) {
	existingString, existingBinary, err := b.openVersion(ctx, existing)
	if err != nil {
		return false, err
	}

	return existingString == secretString && string(existingBinary) == string(secretBinary), nil
}

// openVersion returns the plaintext SecretString/SecretBinary for a stored
// version. When the version was sealed via KMS (Ciphertext set), it is
// decrypted through the wired KMSEncryptor; otherwise the stored
// SecretString/SecretBinary fields are returned directly, unchanged from
// pre-KMS-integration behaviour. Must be called with b.mu held (at least a
// read lock).
func (b *InMemoryBackend) openVersion(ctx context.Context, v *SecretVersion) (string, []byte, error) {
	if v.Ciphertext == nil {
		return v.SecretString, v.SecretBinary, nil
	}

	if b.kms == nil {
		return "", nil, ErrKMSNotConfigured
	}

	pt, err := b.kms.Decrypt(ctx, v.Ciphertext)
	if err != nil {
		return "", nil, fmt.Errorf("%w: %w", ErrKMSDecryptionFailed, err)
	}

	if v.WasString {
		return string(pt), nil, nil
	}

	return "", pt, nil
}
