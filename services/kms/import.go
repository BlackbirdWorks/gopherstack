package kms

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"fmt"
	"io"
	"time"
)

// GetParametersForImport returns wrapping parameters for EXTERNAL-origin key material import.
// Returns a real RSA public key (DER-encoded SubjectPublicKeyInfo) that callers can use to
// RSA-OAEP-wrap their key material before calling ImportKeyMaterial.
func (b *InMemoryBackend) GetParametersForImport(
	ctx context.Context, input *GetParametersForImportInput,
) (*GetParametersForImportOutput, error) {
	validWrappingAlgorithms := map[string]struct{}{
		"RSAES_PKCS1_V1_5":         {},
		"RSAES_OAEP_SHA_1":         {},
		encryptionAlgorithmRSAOAEP: {},
		"RSA_AES_KEY_WRAP_SHA_1":   {},
		"RSA_AES_KEY_WRAP_SHA_256": {},
	}

	if input.WrappingAlgorithm != "" {
		if _, ok := validWrappingAlgorithms[input.WrappingAlgorithm]; !ok {
			return nil, fmt.Errorf(
				"%w: WrappingAlgorithm %q is not valid; must be one of RSAES_PKCS1_V1_5, "+
					"RSAES_OAEP_SHA_1, RSAES_OAEP_SHA_256, RSA_AES_KEY_WRAP_SHA_1, or RSA_AES_KEY_WRAP_SHA_256",
				ErrUnsupportedParameter,
				input.WrappingAlgorithm,
			)
		}
	}

	validWrappingKeySpecs := map[string]struct{}{
		"RSA_2048": {},
		"RSA_3072": {},
		"RSA_4096": {},
	}

	if input.WrappingKeySpec != "" {
		if _, ok := validWrappingKeySpecs[input.WrappingKeySpec]; !ok {
			return nil, fmt.Errorf(
				"%w: WrappingKeySpec %q is not valid; must be RSA_2048, RSA_3072, or RSA_4096",
				ErrUnsupportedParameter, input.WrappingKeySpec,
			)
		}
	}

	// Generate import token and RSA key pair BEFORE acquiring the lock.
	importToken := make([]byte, aes256Bytes)
	if _, readErr := io.ReadFull(rand.Reader, importToken); readErr != nil {
		return nil, fmt.Errorf("generating import token: %w", readErr)
	}

	rsaBits := rsaBits2048
	switch input.WrappingKeySpec {
	case "RSA_3072":
		rsaBits = rsaBits3072
	case "RSA_4096":
		rsaBits = rsaBits4096
	}

	privKey, genErr := rsa.GenerateKey(rand.Reader, rsaBits)
	if genErr != nil {
		return nil, fmt.Errorf("generating wrapping RSA key: %w", genErr)
	}

	pubKeyDER, marshalErr := x509.MarshalPKIXPublicKey(&privKey.PublicKey)
	if marshalErr != nil {
		return nil, fmt.Errorf("marshaling wrapping public key: %w", marshalErr)
	}

	b.mu.RLock("GetParametersForImport")
	defer b.mu.RUnlock()

	key, err := b.lookupKey(ctx, input.KeyID, ErrInvalidArn)
	if err != nil {
		return nil, err
	}

	if key.Origin != KeyOriginExternal {
		return nil, fmt.Errorf(
			"%w: GetParametersForImport is only valid for keys with Origin=%s",
			ErrUnsupportedOrigin,
			KeyOriginExternal,
		)
	}

	// Store private key (via sync.Map, no write lock needed) so ImportKeyMaterial
	// can unwrap RSA-OAEP-encrypted material from this caller.
	b.importWrappingKeys.Store(key.KeyID, privKey)

	return &GetParametersForImportOutput{
		KeyID:             key.KeyID,
		ImportToken:       importToken,
		PublicKey:         pubKeyDER,
		ParametersValidTo: UnixTimeFloat(time.Now().Add(getParametersValidityWindow)),
	}, nil
}

// resolveExpirationModel normalises the (expirationModel, validTo) pair from an
// ImportKeyMaterial request and returns the validated expiration model and ValidTo.
func resolveExpirationModel(expModel string, validTo float64) (string, float64, error) {
	if expModel == "" {
		if validTo > 0 {
			expModel = expirationModelExpires
		} else {
			expModel = expirationModelNoExpiry
		}
	}

	if expModel == expirationModelExpires && validTo == 0 {
		return "", 0, fmt.Errorf(
			"%w: ExpirationModel=%s requires ValidTo to be set",
			ErrValidation, expirationModelExpires,
		)
	}

	if expModel == expirationModelNoExpiry && validTo > 0 {
		return "", 0, fmt.Errorf(
			"%w: ExpirationModel=%s must not include ValidTo",
			ErrValidation, expirationModelNoExpiry,
		)
	}

	return expModel, validTo, nil
}

// resolveKeyMaterial detects whether material is RSA-OAEP-wrapped (≥ minRSAWrappedMaterialBytes)
// and decrypts it using the stored wrapping key, or returns it unchanged (raw AES-256 path).
func (b *InMemoryBackend) resolveKeyMaterial(keyID string, material []byte) ([]byte, error) {
	if len(material) < minRSAWrappedMaterialBytes {
		return material, nil
	}

	privKeyAny, loaded := b.importWrappingKeys.Load(keyID)
	if !loaded {
		return nil, fmt.Errorf(
			"%w: no wrapping key found for %s; call GetParametersForImport first",
			ErrInvalidImportToken, keyID,
		)
	}

	privKey, ok := privKeyAny.(*rsa.PrivateKey)
	if !ok {
		// Defensive only -- importWrappingKeys only ever stores *rsa.PrivateKey
		// (see GetParametersForImport), so no request can hit this.
		return nil, fmt.Errorf("%w: internal: wrapping key type assertion failed", ErrValidation)
	}

	raw, err := rsa.DecryptOAEP(sha256.New(), rand.Reader, privKey, material, nil)
	if err != nil {
		// InvalidCiphertextException's doc (kms@v1.55.4 types/errors.go) covers this
		// exactly for ImportKeyMaterial: "KMS could not decrypt the encrypted (wrapped)
		// key material."
		return nil, fmt.Errorf("%w: RSA-OAEP decrypt of key material failed: %w", ErrInvalidCiphertext, err)
	}

	b.importWrappingKeys.Delete(keyID)

	return raw, nil
}

// ImportKeyMaterial imports externally supplied key material; see ImportKeyMaterialWithResult.
func (b *InMemoryBackend) ImportKeyMaterial(ctx context.Context, input *ImportKeyMaterialInput) error {
	_, err := b.ImportKeyMaterialWithResult(ctx, input)

	return err
}

// DeleteImportedKeyMaterial removes imported key material; see DeleteImportedKeyMaterialWithResult.
func (b *InMemoryBackend) DeleteImportedKeyMaterial(ctx context.Context, input *DeleteImportedKeyMaterialInput) error {
	_, err := b.DeleteImportedKeyMaterialWithResult(ctx, input)

	return err
}
