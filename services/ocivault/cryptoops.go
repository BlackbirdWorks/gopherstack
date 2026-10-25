package ocivault

import (
	"crypto/rsa"
	"encoding/base64"
	"slices"
	"time"
)

// EncryptIn is the Encrypt request body.
type EncryptIn struct {
	AssociatedData map[string]string `json:"associatedData"`
	KeyID          string            `json:"keyId"`
	Plaintext      string            `json:"plaintext"`
	KeyVersionID   string            `json:"keyVersionId"`
	Algorithm      string            `json:"encryptionAlgorithm"`
}

// DecryptIn is the Decrypt request body.
type DecryptIn struct {
	AssociatedData map[string]string `json:"associatedData"`
	KeyID          string            `json:"keyId"`
	Ciphertext     string            `json:"ciphertext"`
	KeyVersionID   string            `json:"keyVersionId"`
	Algorithm      string            `json:"encryptionAlgorithm"`
}

// GenerateDEKIn is the GenerateDataEncryptionKey request body.
type GenerateDEKIn struct {
	AssociatedData      map[string]string `json:"associatedData"`
	KeyID               string            `json:"keyId"`
	KeyShape            KeyShape          `json:"keyShape"`
	IncludePlaintextKey bool              `json:"includePlaintextKey"`
}

// SignIn is the Sign request body.
type SignIn struct {
	KeyID        string `json:"keyId"`
	Message      string `json:"message"`
	Algorithm    string `json:"signingAlgorithm"`
	KeyVersionID string `json:"keyVersionId"`
	MessageType  string `json:"messageType"`
}

// VerifyIn is the Verify request body.
type VerifyIn struct {
	KeyID        string `json:"keyId"`
	KeyVersionID string `json:"keyVersionId"`
	Signature    string `json:"signature"`
	Message      string `json:"message"`
	Algorithm    string `json:"signingAlgorithm"`
	MessageType  string `json:"messageType"`
}

// ExportIn is the ExportKey request body.
type ExportIn struct {
	KeyID        string `json:"keyId"`
	Algorithm    string `json:"algorithm"`
	PublicKey    string `json:"publicKey"`
	KeyVersionID string `json:"keyVersionId"`
}

type encryptedWire struct {
	Ciphertext   string `json:"ciphertext"`
	KeyID        string `json:"keyId"`
	KeyVersionID string `json:"keyVersionId"`
	Algorithm    string `json:"encryptionAlgorithm"`
}

type decryptedWire struct {
	Plaintext         string `json:"plaintext"`
	PlaintextChecksum string `json:"plaintextChecksum"`
	KeyID             string `json:"keyId"`
	KeyVersionID      string `json:"keyVersionId"`
	Algorithm         string `json:"encryptionAlgorithm"`
}

type generatedWire struct {
	Ciphertext        string `json:"ciphertext"`
	Plaintext         string `json:"plaintext,omitempty"`
	PlaintextChecksum string `json:"plaintextChecksum,omitempty"`
}

type signedWire struct {
	KeyID        string `json:"keyId"`
	KeyVersionID string `json:"keyVersionId"`
	Signature    string `json:"signature"`
	Algorithm    string `json:"signingAlgorithm"`
}

type verifiedWire struct {
	IsSignatureValid bool `json:"isSignatureValid"`
}

type exportedWire struct {
	TimeCreated  time.Time `json:"timeCreated"`
	KeyVersionID string    `json:"keyVersionId"`
	KeyID        string    `json:"keyId"`
	VaultID      string    `json:"vaultId"`
	EncryptedKey string    `json:"encryptedKey"`
	Algorithm    string    `json:"algorithm"`
}

// usable returns the key and the requested (or current) version, requiring everything to be ENABLED.
func (b *InMemoryBackend) usable(vaultID, keyID, versionID string) (*Key, *KeyVersion, *APIError) {
	if keyID == "" {
		return nil, nil, errMissing("keyId is required")
	}

	k, v, aerr := b.keyFor(vaultID, keyID)
	if aerr != nil {
		return nil, nil, aerr
	}

	if v.State != stateActive {
		return nil, nil, errState("Vault " + v.ID + " is in state " + v.State + "; the operation requires ACTIVE")
	}

	if k.State != stateEnabled {
		return nil, nil, errState("Key " + keyID + " is in state " + k.State + "; the operation requires ENABLED")
	}

	if versionID == "" {
		versionID = k.CurrentVersion
	}

	kv, aerr := k.version(versionID)
	if aerr != nil {
		return nil, nil, aerr
	}

	if kv.State != stateEnabled {
		return nil, nil, errState("Key version " + versionID + " is in state " + kv.State)
	}

	return k, kv, nil
}

func decodePlaintext(s string) ([]byte, *APIError) {
	if s == "" {
		return nil, errMissing("plaintext is required")
	}

	b, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return nil, errParam("plaintext must be base64-encoded")
	}

	if len(b) > maxPlaintext {
		return nil, errParam("plaintext exceeds 4096 bytes; use GenerateDataEncryptionKey for larger payloads")
	}

	return b, nil
}

// sealWith encrypts under one key version; AES output embeds the version id, RSA output is raw OAEP.
func sealWith(k *Key, kv *KeyVersion, plaintext []byte, aad map[string]string, alg string) (string, string, *APIError) {
	switch k.Shape.Algorithm {
	case algAES:
		if alg != "" && alg != algAESGCM {
			return "", "", errParam("encryptionAlgorithm must be AES_256_GCM for AES keys")
		}

		ct, aerr := aesSeal(kv.ID, kv.Material, plaintext, aad)

		return ct, algAESGCM, aerr
	case algRSA:
		ct, aerr := rsaSeal(kv.Material, plaintext, alg)
		if alg == "" {
			alg = algOAEPSHA256
		}

		return ct, alg, aerr
	}

	return "", "", errParam("ECDSA keys cannot encrypt")
}

// Encrypt encrypts base64 plaintext with an AES or RSA key.
func (b *InMemoryBackend) Encrypt(vaultID string, in EncryptIn) (encryptedWire, error) {
	pt, aerr := decodePlaintext(in.Plaintext)
	if aerr != nil {
		return encryptedWire{}, aerr
	}

	b.mu.RLock("Encrypt")
	defer b.mu.RUnlock()

	k, kv, aerr := b.usable(vaultID, in.KeyID, in.KeyVersionID)
	if aerr != nil {
		return encryptedWire{}, aerr
	}

	ct, alg, aerr := sealWith(k, kv, pt, in.AssociatedData, in.Algorithm)
	if aerr != nil {
		return encryptedWire{}, aerr
	}

	return encryptedWire{Ciphertext: ct, KeyID: k.ID, KeyVersionID: kv.ID, Algorithm: alg}, nil
}

// openWith decrypts and reports the key version that succeeded.
func (b *InMemoryBackend) openWith(k *Key, in DecryptIn) ([]byte, *KeyVersion, *APIError) {
	if k.Shape.Algorithm == algAES {
		return b.openAES(k, in)
	}

	if k.Shape.Algorithm != algRSA {
		return nil, nil, errParam("ECDSA keys cannot decrypt")
	}

	if in.KeyVersionID != "" {
		_, kv, aerr := b.usable(k.VaultID, k.ID, in.KeyVersionID)
		if aerr != nil {
			return nil, nil, aerr
		}

		pt, aerr := rsaOpen(kv.Material, in.Ciphertext, in.Algorithm)

		return pt, kv, aerr
	}

	for _, kv := range slices.Backward(k.Versions) {
		if kv.State != stateEnabled {
			continue
		}

		if pt, aerr := rsaOpen(kv.Material, in.Ciphertext, in.Algorithm); aerr == nil {
			return pt, kv, nil
		}
	}

	return nil, nil, errParam("The ciphertext could not be decrypted with any enabled version of the key")
}

func (b *InMemoryBackend) openAES(k *Key, in DecryptIn) ([]byte, *KeyVersion, *APIError) {
	versionID, body, ok := sealVersion(in.Ciphertext)
	if !ok {
		return nil, nil, errParam("The ciphertext was not produced by this service")
	}

	if in.KeyVersionID != "" && in.KeyVersionID != versionID {
		return nil, nil, errParam("keyVersionId does not match the key version that produced the ciphertext")
	}

	if _, aerr := k.version(versionID); aerr != nil {
		return nil, nil, errParam("The ciphertext was not encrypted with this key")
	}

	_, kv, aerr := b.usable(k.VaultID, k.ID, versionID)
	if aerr != nil {
		return nil, nil, aerr
	}

	pt, aerr := aesOpen(kv.Material, body, in.AssociatedData)

	return pt, kv, aerr
}

// Decrypt reverses Encrypt; rotated key versions keep decrypting their own ciphertext.
func (b *InMemoryBackend) Decrypt(vaultID string, in DecryptIn) (decryptedWire, error) {
	if in.Ciphertext == "" {
		return decryptedWire{}, errMissing("ciphertext is required")
	}

	b.mu.RLock("Decrypt")
	defer b.mu.RUnlock()

	k, _, aerr := b.usable(vaultID, in.KeyID, "")
	if aerr != nil {
		return decryptedWire{}, aerr
	}

	pt, kv, aerr := b.openWith(k, in)
	if aerr != nil {
		return decryptedWire{}, aerr
	}

	alg := algAESGCM
	if k.Shape.Algorithm == algRSA {
		alg = in.Algorithm
		if alg == "" {
			alg = algOAEPSHA256
		}
	}

	return decryptedWire{
		Plaintext: base64.StdEncoding.EncodeToString(pt), PlaintextChecksum: checksum(pt),
		KeyID: k.ID, KeyVersionID: kv.ID, Algorithm: alg,
	}, nil
}

// GenerateDataEncryptionKey returns a fresh data key wrapped by the master key.
func (b *InMemoryBackend) GenerateDataEncryptionKey(vaultID string, in GenerateDEKIn) (generatedWire, error) {
	if aerr := validateShape(in.KeyShape); aerr != nil || in.KeyShape.Algorithm != algAES {
		return generatedWire{}, errParam("keyShape must describe an AES data encryption key (length 16, 24 or 32)")
	}

	dek, err := generateMaterial(in.KeyShape)
	if err != nil {
		return generatedWire{}, err
	}

	b.mu.RLock("GenerateDataEncryptionKey")
	defer b.mu.RUnlock()

	k, kv, aerr := b.usable(vaultID, in.KeyID, "")
	if aerr != nil {
		return generatedWire{}, aerr
	}

	ct, _, aerr := sealWith(k, kv, dek, in.AssociatedData, "")
	if aerr != nil {
		return generatedWire{}, aerr
	}

	out := generatedWire{Ciphertext: ct}
	if in.IncludePlaintextKey {
		out.Plaintext, out.PlaintextChecksum = base64.StdEncoding.EncodeToString(dek), checksum(dek)
	}

	return out, nil
}

func decodeSignInput(msg, msgType string) ([]byte, bool, *APIError) {
	if msg == "" {
		return nil, false, errMissing("message is required")
	}

	raw, err := base64.StdEncoding.DecodeString(msg)
	if err != nil {
		return nil, false, errParam("message must be base64-encoded")
	}

	switch msgType {
	case "", "RAW":
		return raw, false, nil
	case "DIGEST":
		return raw, true, nil
	}

	return nil, false, errParam("messageType must be RAW or DIGEST")
}

// Sign signs a message or digest with an RSA or ECDSA key version.
func (b *InMemoryBackend) Sign(vaultID string, in SignIn) (signedWire, error) {
	msg, isDigest, aerr := decodeSignInput(in.Message, in.MessageType)
	if aerr != nil {
		return signedWire{}, aerr
	}

	b.mu.RLock("Sign")
	defer b.mu.RUnlock()

	k, kv, aerr := b.usable(vaultID, in.KeyID, in.KeyVersionID)
	if aerr != nil {
		return signedWire{}, aerr
	}

	sig, aerr := signDigest(kv.Material, k.Shape, in.Algorithm, msg, isDigest)
	if aerr != nil {
		return signedWire{}, aerr
	}

	return signedWire{
		KeyID: k.ID, KeyVersionID: kv.ID, Signature: base64.StdEncoding.EncodeToString(sig), Algorithm: in.Algorithm,
	}, nil
}

// Verify checks a signature made by Sign.
func (b *InMemoryBackend) Verify(vaultID string, in VerifyIn) (verifiedWire, error) {
	msg, isDigest, aerr := decodeSignInput(in.Message, in.MessageType)
	if aerr != nil {
		return verifiedWire{}, aerr
	}

	sig, err := base64.StdEncoding.DecodeString(in.Signature)
	if err != nil || in.KeyVersionID == "" {
		return verifiedWire{}, errParam("signature must be base64 and keyVersionId is required")
	}

	b.mu.RLock("Verify")
	defer b.mu.RUnlock()

	k, kv, aerr := b.usable(vaultID, in.KeyID, in.KeyVersionID)
	if aerr != nil {
		return verifiedWire{}, aerr
	}

	ok, aerr := verifyDigest(kv.Material, k.Shape, in.Algorithm, msg, sig, isDigest)
	if aerr != nil {
		return verifiedWire{}, aerr
	}

	return verifiedWire{IsSignatureValid: ok}, nil
}

// ExportKey wraps a SOFTWARE key version's material with the caller's RSA public key.
func (b *InMemoryBackend) ExportKey(vaultID string, in ExportIn) (exportedWire, error) {
	pub, err := parseRSAPublicPEM(in.PublicKey)
	if err != nil {
		return exportedWire{}, errParam("publicKey must be a PEM RSA public key of at least 2048 bits")
	}

	b.mu.RLock("ExportKey")
	defer b.mu.RUnlock()

	k, kv, aerr := b.usable(vaultID, in.KeyID, in.KeyVersionID)
	if aerr != nil {
		return exportedWire{}, aerr
	}

	if k.ProtectionMode != modeSoftware {
		return exportedWire{}, errParam("Only keys with protectionMode SOFTWARE can be exported")
	}

	blob, aerr := wrapForExport(pub, in.Algorithm, kv.Material)
	if aerr != nil {
		return exportedWire{}, aerr
	}

	return exportedWire{
		TimeCreated: kv.TimeCreated, KeyVersionID: kv.ID, KeyID: k.ID, VaultID: k.VaultID,
		EncryptedKey: base64.StdEncoding.EncodeToString(blob), Algorithm: in.Algorithm,
	}, nil
}

func wrapForExport(pub *rsa.PublicKey, alg string, material []byte) ([]byte, *APIError) {
	switch alg {
	case wrapAlgAES:
		blob, err := wrapRSAAES(pub, material)
		if err != nil {
			return nil, errParam("unable to wrap the key material")
		}

		return blob, nil
	case wrapAlgSHA256:
		blob, err := rsaWrapDirect(pub, material)
		if err != nil {
			return nil, errParam("key material is too large for RSA_OAEP_SHA256; use RSA_OAEP_AES_SHA256")
		}

		return blob, nil
	}

	return nil, errParam("algorithm must be RSA_OAEP_AES_SHA256 or RSA_OAEP_SHA256")
}
