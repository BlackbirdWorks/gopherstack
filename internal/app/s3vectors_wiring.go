package app

import (
	"context"
	"errors"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	kmsbackend "github.com/blackbirdworks/gopherstack/services/kms"
	s3vectorsbackend "github.com/blackbirdworks/gopherstack/services/s3vectors"
)

type s3VectorsKeys struct{ kms *kmsbackend.Handler }

func (k s3VectorsKeys) CheckKey(ctx context.Context, keyARN string) error {
	out, err := k.kms.Backend.DescribeKey(ctx, &kmsbackend.DescribeKeyInput{KeyID: keyARN})
	if err != nil {
		if errors.Is(err, kmsbackend.ErrKeyNotFound) {
			return s3vectorsbackend.ErrKmsNotFound
		}

		return err
	}

	md := out.KeyMetadata

	switch {
	case md.KeyState == kmsbackend.KeyStateDisabled:
		return s3vectorsbackend.ErrKmsDisabled
	case md.KeyState != kmsbackend.KeyStateEnabled:
		return s3vectorsbackend.ErrKmsInvalidState
	case md.KeyUsage != kmsbackend.KeyUsageEncryptDecrypt:
		return s3vectorsbackend.ErrKmsInvalidKeyUsage
	}

	return nil
}

// wireS3VectorsKMS makes KMS-encrypted S3 Vectors indexes fail data operations when their key is unusable.
func wireS3VectorsKMS(byName map[string]service.Registerable) {
	h, ok := byName["S3Vectors"].(*s3vectorsbackend.Handler)
	if !ok {
		return
	}

	if k, kok := byName["KMS"].(*kmsbackend.Handler); kok && k.Backend != nil {
		h.Backend.SetKeyChecker(s3VectorsKeys{kms: k})
	}
}
