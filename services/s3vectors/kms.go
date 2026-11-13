package s3vectors

import (
	"context"
	"errors"
	"net/http"

	"github.com/blackbirdworks/gopherstack/pkgs/restroute"
)

// The ErrKms* errors are returned by a KeyChecker and surface as the matching Kms*Exception.
var (
	ErrKmsNotFound        = errors.New("kms key not found")
	ErrKmsDisabled        = errors.New("kms key disabled")
	ErrKmsInvalidState    = errors.New("kms key state invalid")
	ErrKmsInvalidKeyUsage = errors.New("kms key usage invalid")
)

// KeyChecker validates a KMS key ARN; it returns one of the ErrKms* errors.
type KeyChecker interface {
	CheckKey(ctx context.Context, keyARN string) error
}

// SetKeyChecker wires the KMS lookup behind data-plane operations on KMS-encrypted indexes.
func (b *InMemoryBackend) SetKeyChecker(k KeyChecker) {
	b.mu.Lock("SetKeyChecker")
	defer b.mu.Unlock()

	b.keys = k
}

// checkKeyLocked returns the Kms*Exception for an index whose customer managed key is unusable.
func (b *InMemoryBackend) checkKeyLocked(ix *Index) error {
	enc := ix.Encryption
	if b.keys == nil || enc.SseType != "aws:kms" || enc.KmsKeyArn == "" {
		return nil
	}

	err := b.keys.CheckKey(context.Background(), enc.KmsKeyArn)

	switch {
	case err == nil:
		return nil
	case errors.Is(err, ErrKmsNotFound):
		return kmsError("KmsNotFoundException", "The KMS key %s can't be found", enc.KmsKeyArn)
	case errors.Is(err, ErrKmsDisabled):
		return kmsError("KmsDisabledException", "The KMS key %s isn't enabled", enc.KmsKeyArn)
	case errors.Is(err, ErrKmsInvalidKeyUsage):
		return kmsError("KmsInvalidKeyUsageException", "The KMS key %s cannot be used for encryption", enc.KmsKeyArn)
	case errors.Is(err, ErrKmsInvalidState):
		return kmsError("KmsInvalidStateException", "The key state of KMS key %s isn't compatible", enc.KmsKeyArn)
	}

	return err
}

func kmsError(code, format string, args ...any) error {
	return restroute.NewError(http.StatusBadRequest, code, format, args...)
}
