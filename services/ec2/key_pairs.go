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
)

// KeyPair represents an EC2 key pair.
type KeyPair struct {
	Name        string `json:"name,omitempty"`
	Fingerprint string `json:"fingerprint,omitempty"`
	Material    string `json:"material,omitempty"` // private key PEM, only on create
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

// CreateKeyPair generates a new RSA key pair.
func (b *InMemoryBackend) CreateKeyPair(name string) (*KeyPair, error) {
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
		Fingerprint: fp,
		Material:    string(privPEM),
		PublicKey:   authorized,
	}
	b.keyPairs.Put(kp)

	return kp, nil
}

// ImportKeyPair stores a pre-existing key pair by name. publicKeyMaterial is
// the OpenSSH-format ("ssh-rsa AAAA...") public key the caller passed in
// PublicKeyMaterial; when non-empty it is persisted on the KeyPair so the
// optional Compute provider can write it to authorized_keys on launch.
func (b *InMemoryBackend) ImportKeyPair(name, publicKeyMaterial string) (*KeyPair, error) {
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
		Fingerprint: newKeyPairFingerprint(),
		PublicKey:   strings.TrimSpace(publicKeyMaterial),
	}
	b.keyPairs.Put(kp)

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

// ExportKeyPair returns the public-key material for a stored key pair.
// For keys created via CreateKeyPair a deterministic SSH-authorized-keys line
// is generated from the fingerprint. AWS similarly stores only the public half.
func (b *InMemoryBackend) ExportKeyPair(name string) (string, error) {
	if name == "" {
		return "", fmt.Errorf("%w: KeyName is required", ErrInvalidParameter)
	}

	b.mu.RLock("ExportKeyPair")
	defer b.mu.RUnlock()

	kp, ok := b.keyPairs.Get(name)
	if !ok {
		return "", fmt.Errorf("%w: %s", ErrKeyPairNotFound, name)
	}

	return "ssh-rsa AAAAB3NzaC1yc2EAAAADAQ... " + kp.Fingerprint + " exported@gopherstack", nil
}

// ---- Instance type offerings ----

// InstanceTypeOffering pairs an instance type with an AZ offering.
type InstanceTypeOffering struct {
	InstanceType     string `json:"instanceType,omitempty"`
	AvailabilityZone string `json:"availabilityZone,omitempty"`
	Location         string `json:"location,omitempty"`
	LocationType     string `json:"locationType,omitempty"`
}
