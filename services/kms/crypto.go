package kms

import (
	"crypto"
	"crypto/aes"
	"crypto/cipher"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/sha512"
	"crypto/x509"
	"encoding/asn1"
	"errors"
	"fmt"
	"io"
	"math/big"
	"slices"
	"strings"
)

const (
	// rsaBits2048 is the bit size for RSA-2048 key generation.
	rsaBits2048 = 2048
	// rsaBits3072 is the bit size for RSA-3072 key generation.
	rsaBits3072 = 3072
	// rsaBits4096 is the bit size for RSA-4096 key generation.
	rsaBits4096 = 4096
	// keySpecSymmetric is the key spec for symmetric AES-256-GCM keys.
	keySpecSymmetric = "SYMMETRIC_DEFAULT"
	// keySpecRSA2048 is the key spec for RSA-2048 asymmetric keys.
	keySpecRSA2048 = "RSA_2048"
)

var (
	// errUnsupportedKeySpec is returned when an unknown key spec is requested.
	errUnsupportedKeySpec = errors.New("unsupported key spec")
	// errMissingSymmetricKey is returned when symmetric key material is missing.
	errMissingSymmetricKey = errors.New("key material has no symmetric key")
	// errUnsupportedAlgorithm is returned when an unknown signing algorithm is given.
	errUnsupportedAlgorithm = errors.New("unsupported signing algorithm")
	// errUnsupportedHash is returned when an unknown hash algorithm is referenced.
	errUnsupportedHash = errors.New("unsupported hash")
	// errSignatureVerificationFailed is returned when a raw ECDSA verify check fails.
	errSignatureVerificationFailed = errors.New("signature verification failed")
	// errNoAsymmetricKey is returned when no asymmetric key material exists.
	errNoAsymmetricKey = errors.New("no asymmetric key material available")
	// errEmptyKeyMaterial is returned when marshaling empty key material.
	errEmptyKeyMaterial = errors.New("empty key material")
	// errNoKeyMaterialData is returned when deserializing empty serialized material.
	errNoKeyMaterialData = errors.New("no key material to deserialize")
	// errUnsupportedKeyType is returned for unknown private key types.
	errUnsupportedKeyType = errors.New("unsupported private key type")
	// errInvalidMessageType is returned when an unsupported message type is specified.
	errInvalidMessageType = errors.New("invalid message type: must be RAW or DIGEST")
)

// keyMaterial holds the actual cryptographic key bytes or keypairs for a KMS key.
type keyMaterial struct {
	rsaKey       *rsa.PrivateKey
	ecKey        *ecdsa.PrivateKey
	symmetricKey []byte
}

// serializedKeyMaterial is the JSON-serializable form of keyMaterial for persistence.
type serializedKeyMaterial struct {
	// SymmetricKey holds raw AES key bytes for symmetric keys.
	SymmetricKey []byte `json:"symmetric_key,omitempty"`
	// PrivKeyDER holds PKCS#8 DER-encoded private key for asymmetric keys.
	PrivKeyDER []byte `json:"priv_key_der,omitempty"`
}

// generateKeyMaterial creates real cryptographic key material for the given key spec.
func generateKeyMaterial(keySpec string) (*keyMaterial, error) {
	switch keySpec {
	case keySpecSymmetric:
		key := make([]byte, aes256Bytes)
		if _, err := io.ReadFull(rand.Reader, key); err != nil {
			return nil, fmt.Errorf("generating AES key: %w", err)
		}

		return &keyMaterial{symmetricKey: key}, nil
	case keySpecRSA2048:
		return generateRSAKeyMaterial(rsaBits2048)
	case keySpecRSA3072:
		return generateRSAKeyMaterial(rsaBits3072)
	case keySpecRSA4096:
		return generateRSAKeyMaterial(rsaBits4096)
	case keySpecECCP256:
		return generateECKeyMaterial(elliptic.P256())
	case keySpecECCP384:
		return generateECKeyMaterial(elliptic.P384())
	case keySpecECCP521:
		return generateECKeyMaterial(elliptic.P521())
	default:
		return nil, fmt.Errorf("%w: %s", errUnsupportedKeySpec, keySpec)
	}
}

func generateRSAKeyMaterial(bits int) (*keyMaterial, error) {
	k, err := rsa.GenerateKey(rand.Reader, bits)
	if err != nil {
		return nil, fmt.Errorf("generating RSA-%d key: %w", bits, err)
	}

	return &keyMaterial{rsaKey: k}, nil
}

func generateECKeyMaterial(curve elliptic.Curve) (*keyMaterial, error) {
	k, err := ecdsa.GenerateKey(curve, rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("generating ECDSA key: %w", err)
	}

	return &keyMaterial{ecKey: k}, nil
}

// padKeyID pads or truncates a key ID to exactly keyIDPrefixLen bytes.
func padKeyID(keyID string) []byte {
	b := make([]byte, keyIDPrefixLen)
	copy(b, keyID)

	return b
}

// encryptSymmetric encrypts plaintext with the key's AES-256-GCM material, embedding the key ID.
// The output format is: [keyIDPrefixLen bytes: padded keyID][nonce][AES-GCM ciphertext+tag].
func encryptSymmetric(plaintext []byte, keyID string, km *keyMaterial) ([]byte, error) {
	if km.symmetricKey == nil {
		return nil, errMissingSymmetricKey
	}

	block, err := aes.NewCipher(km.symmetricKey)
	if err != nil {
		return nil, fmt.Errorf("creating AES cipher: %w", err)
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("creating GCM: %w", err)
	}

	nonce := make([]byte, gcm.NonceSize())
	if _, readErr := io.ReadFull(rand.Reader, nonce); readErr != nil {
		return nil, fmt.Errorf("generating nonce: %w", readErr)
	}

	aad := []byte(keyID)
	encrypted := gcm.Seal(nonce, nonce, plaintext, aad)

	result := make([]byte, keyIDPrefixLen+len(encrypted))
	copy(result[:keyIDPrefixLen], padKeyID(keyID))
	copy(result[keyIDPrefixLen:], encrypted)

	return result, nil
}

// decryptSymmetric decrypts a ciphertext blob produced by encryptSymmetric.
// Returns (plaintext, keyID, error).
func decryptSymmetric(blob []byte, km *keyMaterial) ([]byte, string, error) {
	if len(blob) < keyIDPrefixLen {
		return nil, "", ErrCiphertextTooShort
	}

	if km.symmetricKey == nil {
		return nil, "", errMissingSymmetricKey
	}

	keyID := strings.TrimRight(string(blob[:keyIDPrefixLen]), "\x00")
	encrypted := blob[keyIDPrefixLen:]

	block, err := aes.NewCipher(km.symmetricKey)
	if err != nil {
		return nil, "", fmt.Errorf("creating AES cipher: %w", err)
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, "", fmt.Errorf("creating GCM: %w", err)
	}

	nonceSize := gcm.NonceSize()
	if len(encrypted) < nonceSize {
		return nil, "", ErrCiphertextTooShort
	}

	nonce, cipherOnly := encrypted[:nonceSize], encrypted[nonceSize:]
	aad := []byte(keyID)

	plaintext, openErr := gcm.Open(nil, nonce, cipherOnly, aad)
	if openErr != nil {
		return nil, "", fmt.Errorf("%w: %w", ErrInvalidCiphertext, openErr)
	}

	return plaintext, keyID, nil
}

// hashAndAlgorithm returns the hash value for the message and the [crypto.Hash] to use
// based on the signing algorithm. messageType must be "RAW" or "DIGEST".
func hashAndAlgorithm(
	message []byte,
	messageType, signingAlgorithm string,
) ([]byte, crypto.Hash, error) {
	hashAlg, err := signingAlgorithmHash(signingAlgorithm)
	if err != nil {
		return nil, 0, err
	}

	switch messageType {
	case "DIGEST":
		return message, hashAlg, nil
	case "", "RAW":
		digest, hashErr := computeHash(message, hashAlg)
		if hashErr != nil {
			return nil, 0, hashErr
		}

		return digest, hashAlg, nil
	default:
		return nil, 0, fmt.Errorf("%w: %q", errInvalidMessageType, messageType)
	}
}

// signingAlgorithmHash returns the [crypto.Hash] for a signing algorithm string.
func signingAlgorithmHash(signingAlgorithm string) (crypto.Hash, error) {
	switch {
	case strings.HasSuffix(signingAlgorithm, "_SHA_256"):
		return crypto.SHA256, nil
	case strings.HasSuffix(signingAlgorithm, "_SHA_384"):
		return crypto.SHA384, nil
	case strings.HasSuffix(signingAlgorithm, "_SHA_512"):
		return crypto.SHA512, nil
	default:
		return 0, fmt.Errorf("%w: %s", errUnsupportedAlgorithm, signingAlgorithm)
	}
}

// computeHash returns the hash digest of message using the given hash algorithm.
func computeHash(message []byte, h crypto.Hash) ([]byte, error) {
	switch h {
	case crypto.SHA256:
		d := sha256.Sum256(message)

		return d[:], nil
	case crypto.SHA384:
		d := sha512.Sum384(message)

		return d[:], nil
	case crypto.SHA512:
		d := sha512.Sum512(message)

		return d[:], nil
	default:
		return nil, fmt.Errorf("%w: %v", errUnsupportedHash, h)
	}
}

// signWithKeyMaterial signs a message using the key material and specified algorithm.
func signWithKeyMaterial(
	message []byte,
	messageType, signingAlgorithm string,
	km *keyMaterial,
) ([]byte, error) {
	digest, hashAlg, err := hashAndAlgorithm(message, messageType, signingAlgorithm)
	if err != nil {
		return nil, err
	}

	switch {
	case strings.HasPrefix(signingAlgorithm, "RSASSA_PSS_"):
		if km.rsaKey == nil {
			return nil, fmt.Errorf("%w: not an RSA key", ErrInvalidKeyUsage)
		}

		pssOpts := &rsa.PSSOptions{
			SaltLength: rsa.PSSSaltLengthEqualsHash,
			Hash:       hashAlg,
		}

		return rsa.SignPSS(rand.Reader, km.rsaKey, hashAlg, digest, pssOpts)

	case strings.HasPrefix(signingAlgorithm, "RSASSA_PKCS1_V1_5_"):
		if km.rsaKey == nil {
			return nil, fmt.Errorf("%w: not an RSA key", ErrInvalidKeyUsage)
		}

		return rsa.SignPKCS1v15(rand.Reader, km.rsaKey, hashAlg, digest)

	case strings.HasPrefix(signingAlgorithm, "ECDSA_"):
		if km.ecKey == nil {
			return nil, fmt.Errorf("%w: not an EC key", ErrInvalidKeyUsage)
		}

		return signECDSA(digest, km.ecKey)

	default:
		return nil, fmt.Errorf("%w: %s", errUnsupportedAlgorithm, signingAlgorithm)
	}
}

// ecdsaSignature is the ASN.1 structure for DER-encoded ECDSA signatures.
type ecdsaSignature struct {
	R, S *big.Int
}

// signECDSA signs a digest with an ECDSA key and returns a DER-encoded signature.
func signECDSA(digest []byte, key *ecdsa.PrivateKey) ([]byte, error) {
	r, s, err := ecdsa.Sign(rand.Reader, key, digest)
	if err != nil {
		return nil, fmt.Errorf("ECDSA signing: %w", err)
	}

	return asn1.Marshal(ecdsaSignature{R: r, S: s})
}

// verifyWithKeyMaterial verifies a signature using the key material and specified algorithm.
func verifyWithKeyMaterial(
	message, signature []byte,
	messageType, signingAlgorithm string,
	km *keyMaterial,
) (bool, error) {
	digest, hashAlg, err := hashAndAlgorithm(message, messageType, signingAlgorithm)
	if err != nil {
		return false, err
	}

	switch {
	case strings.HasPrefix(signingAlgorithm, "RSASSA_PSS_"):
		if km.rsaKey == nil {
			return false, fmt.Errorf("%w: not an RSA key", ErrInvalidKeyUsage)
		}

		pssOpts := &rsa.PSSOptions{
			SaltLength: rsa.PSSSaltLengthEqualsHash,
			Hash:       hashAlg,
		}

		err = rsa.VerifyPSS(&km.rsaKey.PublicKey, hashAlg, digest, signature, pssOpts)

	case strings.HasPrefix(signingAlgorithm, "RSASSA_PKCS1_V1_5_"):
		if km.rsaKey == nil {
			return false, fmt.Errorf("%w: not an RSA key", ErrInvalidKeyUsage)
		}

		err = rsa.VerifyPKCS1v15(&km.rsaKey.PublicKey, hashAlg, digest, signature)

	case strings.HasPrefix(signingAlgorithm, "ECDSA_"):
		if km.ecKey == nil {
			return false, fmt.Errorf("%w: not an EC key", ErrInvalidKeyUsage)
		}

		err = verifyECDSA(digest, signature, &km.ecKey.PublicKey)

	default:
		return false, fmt.Errorf("%w: %s", errUnsupportedAlgorithm, signingAlgorithm)
	}

	if err != nil {
		return false, fmt.Errorf("%w: %w", ErrInvalidSignature, err)
	}

	return true, nil
}

// verifyECDSA verifies a DER-encoded ECDSA signature against a digest.
func verifyECDSA(digest, signature []byte, pub *ecdsa.PublicKey) error {
	var sig ecdsaSignature
	if _, err := asn1.Unmarshal(signature, &sig); err != nil {
		return fmt.Errorf("parsing ECDSA signature: %w", err)
	}

	if !ecdsa.Verify(pub, digest, sig.R, sig.S) {
		return errSignatureVerificationFailed
	}

	return nil
}

// publicKeyDER returns the DER-encoded SubjectPublicKeyInfo for the key material's public key.
func publicKeyDER(km *keyMaterial) ([]byte, error) {
	var pub crypto.PublicKey

	switch {
	case km.rsaKey != nil:
		pub = &km.rsaKey.PublicKey
	case km.ecKey != nil:
		pub = &km.ecKey.PublicKey
	default:
		return nil, errNoAsymmetricKey
	}

	der, err := x509.MarshalPKIXPublicKey(pub)
	if err != nil {
		return nil, fmt.Errorf("marshaling public key: %w", err)
	}

	return der, nil
}

// marshalKeyMaterial serializes key material for JSON persistence.
func marshalKeyMaterial(km *keyMaterial) (serializedKeyMaterial, error) {
	if km.symmetricKey != nil {
		return serializedKeyMaterial{SymmetricKey: km.symmetricKey}, nil
	}

	var privKey crypto.PrivateKey

	switch {
	case km.rsaKey != nil:
		privKey = km.rsaKey
	case km.ecKey != nil:
		privKey = km.ecKey
	default:
		return serializedKeyMaterial{}, errEmptyKeyMaterial
	}

	der, err := x509.MarshalPKCS8PrivateKey(privKey)
	if err != nil {
		return serializedKeyMaterial{}, fmt.Errorf("marshaling private key: %w", err)
	}

	return serializedKeyMaterial{PrivKeyDER: der}, nil
}

// unmarshalKeyMaterial deserializes key material from a serializedKeyMaterial.
func unmarshalKeyMaterial(s serializedKeyMaterial) (*keyMaterial, error) {
	if len(s.SymmetricKey) > 0 {
		return &keyMaterial{symmetricKey: s.SymmetricKey}, nil
	}

	if len(s.PrivKeyDER) == 0 {
		return nil, errNoKeyMaterialData
	}

	priv, err := x509.ParsePKCS8PrivateKey(s.PrivKeyDER)
	if err != nil {
		return nil, fmt.Errorf("parsing private key: %w", err)
	}

	switch k := priv.(type) {
	case *rsa.PrivateKey:
		return &keyMaterial{rsaKey: k}, nil
	case *ecdsa.PrivateKey:
		return &keyMaterial{ecKey: k}, nil
	default:
		return nil, fmt.Errorf("%w: %T", errUnsupportedKeyType, priv)
	}
}

// defaultSigningAlgorithms returns the supported signing algorithms for a key spec.
func defaultSigningAlgorithms(keySpec string) []string {
	switch keySpec {
	case keySpecRSA2048, keySpecRSA3072, keySpecRSA4096:
		return []string{
			"RSASSA_PSS_SHA_256",
			"RSASSA_PSS_SHA_384",
			"RSASSA_PSS_SHA_512",
			"RSASSA_PKCS1_V1_5_SHA_256",
			"RSASSA_PKCS1_V1_5_SHA_384",
			"RSASSA_PKCS1_V1_5_SHA_512",
		}
	case keySpecECCP256:
		return []string{"ECDSA_SHA_256"}
	case keySpecECCP384:
		return []string{"ECDSA_SHA_384"}
	case keySpecECCP521:
		return []string{"ECDSA_SHA_512"}
	default:
		return nil
	}
}

// validateSigningAlgorithm returns an error if signingAlgorithm is not in the set of
// algorithms supported by keySpec, preventing key-spec/algorithm mismatches.
func validateSigningAlgorithm(signingAlgorithm, keySpec string) error {
	supported := defaultSigningAlgorithms(keySpec)
	if slices.Contains(supported, signingAlgorithm) {
		return nil
	}

	return fmt.Errorf(
		"%w: signing algorithm %q is not supported for key spec %q; supported: %v",
		errUnsupportedAlgorithm, signingAlgorithm, keySpec, supported,
	)
}
