package ocivault

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"time"
)

const (
	wrapAlgAES    = "RSA_OAEP_AES_SHA256"
	wrapAlgSHA256 = "RSA_OAEP_SHA256"
	wrapKeyBits   = 2048
)

// WrappedImportKey is the wrapped key material of an import request.
type WrappedImportKey struct {
	KeyMaterial       string `json:"keyMaterial"`
	WrappingAlgorithm string `json:"wrappingAlgorithm"`
}

// ImportKeyIn is the ImportKey request body.
type ImportKeyIn struct {
	WrappedImportKey WrappedImportKey `json:"wrappedImportKey"`
	CreateKeyIn
}

// ImportKeyVersionIn is the ImportKeyVersion request body.
type ImportKeyVersionIn struct {
	WrappedImportKey WrappedImportKey `json:"wrappedImportKey"`
}

type wrappingKeyWire struct {
	TimeCreated    time.Time `json:"timeCreated"`
	CompartmentID  string    `json:"compartmentId"`
	ID             string    `json:"id"`
	LifecycleState string    `json:"lifecycleState"`
	PublicKey      string    `json:"publicKey"`
	VaultID        string    `json:"vaultId"`
}

// ensureWrapping lazily generates the vault's RSA wrapping key pair.
func (b *InMemoryBackend) ensureWrapping(v *Vault) (*rsa.PrivateKey, error) {
	if len(v.WrappingKeyDER) == 0 {
		k, err := rsa.GenerateKey(rand.Reader, wrapKeyBits)
		if err != nil {
			return nil, errParam("unable to generate the wrapping key")
		}

		der, err := x509.MarshalPKCS8PrivateKey(k)
		if err != nil {
			return nil, errParam("unable to encode the wrapping key")
		}

		now := b.nowUTC()
		v.WrappingKeyDER, v.WrappingKeyCreated = der, &now
	}

	priv, aerr := parseRSAPrivate(v.WrappingKeyDER)
	if aerr != nil {
		return nil, aerr
	}

	return priv, nil
}

// GetWrappingKey returns the vault's public RSA wrapping key.
func (b *InMemoryBackend) GetWrappingKey(vaultID string) (wrappingKeyWire, error) {
	b.mu.Lock("GetWrappingKey")
	defer b.mu.Unlock()

	v, aerr := b.vaultByID(vaultID)
	if aerr != nil {
		return wrappingKeyWire{}, aerr
	}

	if _, err := b.ensureWrapping(v); err != nil {
		return wrappingKeyWire{}, err
	}

	return wrappingKeyWire{
		TimeCreated: *v.WrappingKeyCreated, CompartmentID: v.CompartmentID, ID: v.WrappingKeyID,
		LifecycleState: stateEnabled, PublicKey: publicKeyPEM(v.WrappingKeyDER), VaultID: v.ID,
	}, nil
}

func unwrapImport(v *Vault, w WrappedImportKey) ([]byte, *APIError) {
	if len(v.WrappingKeyDER) == 0 {
		return nil, errParam("The vault wrapping key has not been issued; call GetWrappingKey first")
	}

	priv, aerr := parseRSAPrivate(v.WrappingKeyDER)
	if aerr != nil {
		return nil, aerr
	}

	blob, err := base64.StdEncoding.DecodeString(w.KeyMaterial)
	if err != nil || len(blob) == 0 {
		return nil, errParam("wrappedImportKey.keyMaterial must be base64")
	}

	var mat []byte

	switch w.WrappingAlgorithm {
	case wrapAlgAES:
		mat, err = unwrapRSAAES(priv, blob)
	case wrapAlgSHA256:
		mat, err = unwrapRSA(priv, blob)
	default:
		return nil, errParam("wrappedImportKey.wrappingAlgorithm must be RSA_OAEP_AES_SHA256 or RSA_OAEP_SHA256")
	}

	if err != nil {
		return nil, errParam("The key material could not be unwrapped with the vault wrapping key")
	}

	return mat, nil
}

// ImportKey creates a key from wrapped external key material.
func (b *InMemoryBackend) ImportKey(vaultID string, in ImportKeyIn) (keyWire, string, error) {
	if aerr := validateCreateKey(&in.CreateKeyIn); aerr != nil {
		return keyWire{}, "", aerr
	}

	b.mu.Lock("ImportKey")
	defer b.mu.Unlock()

	v, aerr := b.activeVault(vaultID)
	if aerr != nil {
		return keyWire{}, "", aerr
	}

	mat, aerr := unwrapImport(v, in.WrappedImportKey)
	if aerr != nil {
		return keyWire{}, "", aerr
	}

	if aerr = checkMaterial(in.KeyShape, mat); aerr != nil {
		return keyWire{}, "", aerr
	}

	k, aerr := b.storeKey(v, in.CreateKeyIn, mat, originExternal)
	if aerr != nil {
		return keyWire{}, "", aerr
	}

	return keyView(k, stateEnabling), k.Etag, nil
}

// ImportKeyVersion adds wrapped external key material as a new current version.
func (b *InMemoryBackend) ImportKeyVersion(
	vaultID, keyID string,
	in ImportKeyVersionIn,
) (keyVersionWire, string, error) {
	b.mu.Lock("ImportKeyVersion")
	defer b.mu.Unlock()

	k, v, aerr := b.writableKey(vaultID, keyID, "")
	if aerr != nil {
		return keyVersionWire{}, "", aerr
	}

	if k.State != stateEnabled {
		return keyVersionWire{}, "", errState("Key " + keyID + " must be ENABLED to import a version")
	}

	mat, aerr := unwrapImport(v, in.WrappedImportKey)
	if aerr != nil {
		return keyVersionWire{}, "", aerr
	}

	if aerr = checkMaterial(k.Shape, mat); aerr != nil {
		return keyVersionWire{}, "", aerr
	}

	kv := b.addVersion(k, v.Prefix, originExternal, mat)
	k.Etag = newEtag()

	return keyVersionFull(k, kv, stateEnabling), k.Etag, nil
}
