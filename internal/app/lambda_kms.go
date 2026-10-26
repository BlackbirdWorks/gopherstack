package app

import (
	"context"
	"errors"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	kmsbackend "github.com/blackbirdworks/gopherstack/services/kms"
	lambdabackend "github.com/blackbirdworks/gopherstack/services/lambda"
)

var errLambdaKMSUnavailable = errors.New("kms backend unavailable")

// lambdaKMSResolver adapts KMS to lambda.KMSResolver. The KMS backend is
// resolved at call time, never captured in Provider.Init.
type lambdaKMSResolver struct {
	byName map[string]service.Registerable
}

func (a *lambdaKMSResolver) backend() (kmsbackend.StorageBackend, error) {
	h, ok := a.byName["KMS"].(*kmsbackend.Handler)
	if !ok || h == nil || h.Backend == nil {
		return nil, errLambdaKMSUnavailable
	}

	return h.Backend, nil
}

func (a *lambdaKMSResolver) CheckKey(keyARN string) error {
	bk, err := a.backend()
	if err != nil {
		return err
	}

	out, err := bk.DescribeKey(context.Background(), &kmsbackend.DescribeKeyInput{KeyID: keyARN})
	if err != nil {
		return mapLambdaKMSError(err)
	}

	md := out.KeyMetadata

	switch {
	case md.KeyState == kmsbackend.KeyStateDisabled:
		return fmt.Errorf("%w: key %s is disabled", lambdabackend.ErrKMSDisabled, keyARN)
	case md.KeyState != kmsbackend.KeyStateEnabled:
		return fmt.Errorf("%w: key %s is in state %s", lambdabackend.ErrKMSInvalidState, keyARN, md.KeyState)
	case md.KeyUsage != kmsbackend.KeyUsageEncryptDecrypt:
		return fmt.Errorf("%w: key %s is not an encrypt/decrypt key", lambdabackend.ErrKMSAccessDenied, keyARN)
	}

	return nil
}

func (a *lambdaKMSResolver) Encrypt(keyARN string, plaintext []byte, encContext map[string]string) ([]byte, error) {
	bk, err := a.backend()
	if err != nil {
		return nil, err
	}

	out, err := bk.Encrypt(context.Background(), &kmsbackend.EncryptInput{
		KeyID: keyARN, Plaintext: plaintext, EncryptionContext: encContext,
	})
	if err != nil {
		return nil, mapLambdaKMSError(err)
	}

	return out.CiphertextBlob, nil
}

func (a *lambdaKMSResolver) Decrypt(keyARN string, ciphertext []byte, encContext map[string]string) ([]byte, error) {
	bk, err := a.backend()
	if err != nil {
		return nil, err
	}

	out, err := bk.Decrypt(context.Background(), &kmsbackend.DecryptInput{
		KeyID: keyARN, CiphertextBlob: ciphertext, EncryptionContext: encContext,
	})
	if err != nil {
		return nil, mapLambdaKMSError(err)
	}

	return out.Plaintext, nil
}

// mapLambdaKMSError translates KMS failures to the KMS* errors Lambda surfaces.
func mapLambdaKMSError(err error) error {
	switch {
	case errors.Is(err, kmsbackend.ErrKeyNotFound):
		return fmt.Errorf("%w: %w", lambdabackend.ErrKMSNotFound, err)
	case errors.Is(err, kmsbackend.ErrKeyDisabled):
		return fmt.Errorf("%w: %w", lambdabackend.ErrKMSDisabled, err)
	case errors.Is(err, kmsbackend.ErrKeyInvalidState):
		return fmt.Errorf("%w: %w", lambdabackend.ErrKMSInvalidState, err)
	case errors.Is(err, kmsbackend.ErrAccessDenied),
		errors.Is(err, kmsbackend.ErrInvalidCiphertext),
		errors.Is(err, kmsbackend.ErrIncorrectKey):
		return fmt.Errorf("%w: %w", lambdabackend.ErrKMSAccessDenied, err)
	default:
		return err
	}
}

// wireLambdaKMS lets Lambda encrypt function environment variables under customer managed keys.
func wireLambdaKMS(byName map[string]service.Registerable) {
	lambdaH, ok := byName["Lambda"].(*lambdabackend.Handler)
	if !ok {
		return
	}

	lambdaBk, ok := lambdaH.Backend.(*lambdabackend.InMemoryBackend)
	if !ok {
		return
	}

	lambdaBk.SetKMSResolver(&lambdaKMSResolver{byName: byName})
}
