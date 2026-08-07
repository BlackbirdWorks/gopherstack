package ec2

import (
	"crypto/md5" //nolint:gosec // MD5 used for fingerprint display only, not security
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
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

// keyFingerprint computes the MD5 fingerprint of an RSA public key in DER form.
func keyFingerprint(pubKey *rsa.PublicKey) (string, error) {
	der, err := x509.MarshalPKIXPublicKey(pubKey)
	if err != nil {
		return "", err
	}

	sum := md5.Sum(der) //nolint:gosec // MD5 used for fingerprint display only, not security
	parts := make([]string, len(sum))

	for i, by := range sum {
		parts[i] = fmt.Sprintf("%02x", by)
	}

	return strings.Join(parts, ":"), nil
}

// CreateKeyPair generates a new RSA key pair. Real AWS also supports
// ED25519 (CreateKeyPairInput.KeyType); not modeled — see PARITY.md gaps.
func (b *InMemoryBackend) CreateKeyPair(name string, tags map[string]string) (*KeyPair, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: KeyName is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateKeyPair")
	defer b.mu.Unlock()

	if _, exists := b.keyPairs.Get(name); exists {
		return nil, fmt.Errorf("%w: %s", ErrDuplicateKeyPairName, name)
	}

	privKey, err := rsa.GenerateKey(rand.Reader, rsaKeyBits)
	if err != nil {
		return nil, fmt.Errorf("failed to generate key: %w", err)
	}

	fp, err := keyFingerprint(&privKey.PublicKey)
	if err != nil {
		return nil, fmt.Errorf("failed to compute fingerprint: %w", err)
	}

	privDER := x509.MarshalPKCS1PrivateKey(privKey)
	privPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: privDER})

	pub, sshErr := ssh.NewPublicKey(&privKey.PublicKey)
	if sshErr != nil {
		return nil, fmt.Errorf("failed to derive ssh public key: %w", sshErr)
	}

	authorized := strings.TrimSpace(string(ssh.MarshalAuthorizedKey(pub))) +
		" gopherstack-" + name

	kp := &KeyPair{
		Name:        name,
		KeyPairID:   newKeyPairID(),
		Fingerprint: fp,
		Material:    string(privPEM),
		KeyType:     keyTypeRSA, // the only type this backend ever generates
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
