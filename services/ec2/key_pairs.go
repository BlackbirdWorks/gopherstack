package ec2

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha1" //nolint:gosec // SHA-1 is the real AWS-documented RSA key-fingerprint algorithm, not used for security
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"fmt"
	"strings"
	"time"

	"golang.org/x/crypto/ssh"
)

// Errors for key pair operations.
var (
	ErrKeyPairNotFound      = errors.New("InvalidKeyPair.NotFound")
	ErrDuplicateKeyPairName = errors.New("InvalidKeyPair.Duplicate")
)

const (
	rsaKeyBits = 2048
	// stubFingerprintUUIDLen is the number of UUID hex characters used to build
	// a stub fingerprint for ImportKeyPair (no actual public key is parsed).
	stubFingerprintUUIDLen = 11
	keyTypeRSA             = "rsa"
	keyTypeED25519         = "ed25519"
)

// KeyPair represents an EC2 key pair.
type KeyPair struct {
	Name        string    `json:"name,omitempty"`
	KeyPairID   string    `json:"keyPairID,omitempty"`
	Fingerprint string    `json:"fingerprint,omitempty"`
	Material    string    `json:"material,omitempty"` // private key PEM, only on create
	KeyType     string    `json:"keyType,omitempty"`
	CreateTime  time.Time `json:"createTime,omitzero"`
	// PublicKey is the OpenSSH "ssh-rsa AAAA..." authorized_keys-format public
	// key, populated by CreateKeyPair (derived from the generated private key)
	// and by ImportKeyPair (decoded from PublicKeyMaterial). Used by the
	// optional Compute provider to seed authorized_keys on launch.
	PublicKey string `json:"publicKey,omitempty"`
}

// rsaFingerprint is real AWS's RSA algorithm: the SHA-1 digest of the DER
// encoded private key.
func rsaFingerprint(privDER []byte) string {
	sum := sha1.Sum(privDER) //nolint:gosec // real AWS-documented algorithm, not used for security
	parts := make([]string, len(sum))

	for i, by := range sum {
		parts[i] = fmt.Sprintf("%02x", by)
	}

	return strings.Join(parts, ":")
}

// ed25519Fingerprint is real AWS's ED25519 algorithm: the base64 SHA-256
// digest of the OpenSSH wire-format public key blob.
func ed25519Fingerprint(pub ssh.PublicKey) string {
	sum := sha256.Sum256(pub.Marshal())

	return base64.StdEncoding.EncodeToString(sum[:])
}

// generateRSAKeyMaterial creates a new 2048-bit RSA key pair.
func generateRSAKeyMaterial() (string, ssh.PublicKey, string, error) {
	privKey, err := rsa.GenerateKey(rand.Reader, rsaKeyBits)
	if err != nil {
		return "", nil, "", fmt.Errorf("failed to generate key: %w", err)
	}

	privDER := x509.MarshalPKCS1PrivateKey(privKey)
	privPEM := string(pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: privDER}))

	pub, err := ssh.NewPublicKey(&privKey.PublicKey)
	if err != nil {
		return "", nil, "", fmt.Errorf("failed to derive ssh public key: %w", err)
	}

	return privPEM, pub, rsaFingerprint(privDER), nil
}

// generateED25519KeyMaterial creates a new ED25519 key pair, PEM-encoded in
// the OpenSSH private-key format real AWS also uses for this KeyType.
func generateED25519KeyMaterial() (string, ssh.PublicKey, string, error) {
	pubKey, privKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return "", nil, "", fmt.Errorf("failed to generate key: %w", err)
	}

	block, err := ssh.MarshalPrivateKey(privKey, "")
	if err != nil {
		return "", nil, "", fmt.Errorf("failed to marshal key: %w", err)
	}

	pub, err := ssh.NewPublicKey(pubKey)
	if err != nil {
		return "", nil, "", fmt.Errorf("failed to derive ssh public key: %w", err)
	}

	return string(pem.EncodeToMemory(block)), pub, ed25519Fingerprint(pub), nil
}

// CreateKeyPair generates a new RSA key pair (the default KeyType). Use
// CreateKeyPairWithType to request ED25519.
func (b *InMemoryBackend) CreateKeyPair(name string, tags map[string]string) (*KeyPair, error) {
	return b.CreateKeyPairWithType(name, keyTypeRSA, tags)
}

// CreateKeyPairWithType generates a key pair of keyType (rsa or ed25519).
// The PPK KeyFormat is not modeled.
func (b *InMemoryBackend) CreateKeyPairWithType(name, keyType string, tags map[string]string) (*KeyPair, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: KeyName is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateKeyPair")
	defer b.mu.Unlock()

	if _, exists := b.keyPairs.Get(name); exists {
		return nil, fmt.Errorf("%w: %s", ErrDuplicateKeyPairName, name)
	}

	generate := generateRSAKeyMaterial

	if keyType == keyTypeED25519 {
		generate = generateED25519KeyMaterial
	} else {
		keyType = keyTypeRSA
	}

	privPEM, pub, fp, err := generate()
	if err != nil {
		return nil, err
	}

	authorized := strings.TrimSpace(string(ssh.MarshalAuthorizedKey(pub))) +
		" gopherstack-" + name

	kp := &KeyPair{
		Name:        name,
		KeyPairID:   newKeyPairID(),
		Fingerprint: fp,
		Material:    privPEM,
		KeyType:     keyType,
		CreateTime:  time.Now().UTC(),
		PublicKey:   authorized,
	}
	b.keyPairs.Put(kp)
	b.setTagsLocked(kp.Name, tags)

	return kp, nil
}

// importedKeyType infers a KeyPairInfo.KeyType value from OpenSSH-format
// public key material. Real AWS validates and infers the type from the
// material it's given; this mock does not validate publicKeyMaterial at all
// (pre-existing, unrelated to this), so unparseable material (including the
// empty string some callers pass) honestly falls back to "rsa" rather than
// erroring — there is no way to derive a real type from no material.
func importedKeyType(publicKeyMaterial string) string {
	pub, _, _, _, err := ssh.ParseAuthorizedKey([]byte(publicKeyMaterial))
	if err != nil {
		return keyTypeRSA
	}

	if pub.Type() == ssh.KeyAlgoED25519 {
		return "ed25519"
	}

	return keyTypeRSA
}

// ImportKeyPair stores a pre-existing key pair by name. publicKeyMaterial is
// the OpenSSH-format ("ssh-rsa AAAA...") public key the caller passed in
// PublicKeyMaterial; when non-empty it is persisted on the KeyPair so the
// optional Compute provider can write it to authorized_keys on launch.
func (b *InMemoryBackend) ImportKeyPair(name, publicKeyMaterial string, tags map[string]string) (*KeyPair, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: KeyName is required", ErrInvalidParameter)
	}

	b.mu.Lock("ImportKeyPair")
	defer b.mu.Unlock()

	if _, exists := b.keyPairs.Get(name); exists {
		return nil, fmt.Errorf("%w: %s", ErrDuplicateKeyPairName, name)
	}

	kp := &KeyPair{
		Name:        name,
		KeyPairID:   newKeyPairID(),
		Fingerprint: newKeyPairFingerprint(),
		KeyType:     importedKeyType(publicKeyMaterial),
		CreateTime:  time.Now().UTC(),
		PublicKey:   strings.TrimSpace(publicKeyMaterial),
	}
	b.keyPairs.Put(kp)
	b.setTagsLocked(kp.Name, tags)

	return kp, nil
}

// DescribeKeyPairs returns key pairs, optionally filtered by name.
// When names are provided, lookups are O(len(names)) via the key-pair map
// rather than scanning every key pair in the backend.
func (b *InMemoryBackend) DescribeKeyPairs(names []string) []*KeyPair {
	b.mu.RLock("DescribeKeyPairs")
	defer b.mu.RUnlock()

	if len(names) > 0 {
		out := make([]*KeyPair, 0, len(names))

		for _, n := range names {
			kp, ok := b.keyPairs.Get(n)
			if !ok {
				continue
			}

			cp := *kp
			cp.Material = "" // don't return private key material on describe
			out = append(out, &cp)
		}

		return out
	}

	out := make([]*KeyPair, 0, b.keyPairs.Len())

	for _, kp := range b.keyPairs.All() {
		cp := *kp
		cp.Material = "" // don't return private key material on describe
		out = append(out, &cp)
	}

	return out
}

// DeleteKeyPair removes a key pair by name.
func (b *InMemoryBackend) DeleteKeyPair(name string) error {
	b.mu.Lock("DeleteKeyPair")
	defer b.mu.Unlock()

	if _, ok := b.keyPairs.Get(name); !ok {
		return fmt.Errorf("%w: %s", ErrKeyPairNotFound, name)
	}
	b.keyPairs.Delete(name)
	delete(b.tags, name)

	return nil
}

// ---- Instance type offerings ----

// InstanceTypeOffering pairs an instance type with an AZ offering.
type InstanceTypeOffering struct {
	InstanceType     string `json:"instanceType,omitempty"`
	AvailabilityZone string `json:"availabilityZone,omitempty"`
	Location         string `json:"location,omitempty"`
	LocationType     string `json:"locationType,omitempty"`
}
