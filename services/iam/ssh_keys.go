package iam

import (
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"sort"
	"strings"
	"time"

	"golang.org/x/crypto/ssh"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// sshEncodingSSH and sshEncodingPEM are GetSSHPublicKey's Encoding values
// (aws-sdk-go-v2/service/iam/types.EncodingType).
const (
	sshEncodingSSH = "SSH"
	sshEncodingPEM = "PEM"
)

// sshFingerprintBytes is the number of bytes used to derive a fingerprint.
const sshFingerprintBytes = 8

// minSSHBodyLen is the minimum SSH public key body length for fingerprint derivation.
const minSSHBodyLen = 10

// UploadSSHPublicKey stores an SSH public key for a user.
func (b *InMemoryBackend) UploadSSHPublicKey(userName, body string) (*SSHPublicKey, error) {
	b.mu.RLock("UploadSSHPublicKey-check")
	_, exists := b.users.Get(userName)
	b.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("%w: user %q not found", ErrUserNotFound, userName)
	}

	keyID := strings.ToUpper(newID("apka"))
	fingerprint := computeSSHFingerprint(body)

	key := SSHPublicKey{
		SSHPublicKeyID:   keyID,
		UserName:         userName,
		SSHPublicKeyBody: body,
		Fingerprint:      fingerprint,
		Status:           accessKeyStatusActive,
		UploadDate:       time.Now().UTC(),
	}

	b.mu.Lock("UploadSSHPublicKey")
	b.comp().sshPublicKeys[keyID] = key
	b.mu.Unlock()

	return &key, nil
}

// GetSSHPublicKey retrieves an SSH public key by user name and key ID.
func (b *InMemoryBackend) GetSSHPublicKey(userName, keyID string) (*SSHPublicKey, error) {
	b.mu.RLock("GetSSHPublicKey")
	key, exists := b.comp().sshPublicKeys[keyID]
	b.mu.RUnlock()

	if !exists || key.UserName != userName {
		return nil, fmt.Errorf("%w: SSH public key %q not found for user %q", ErrAccessKeyNotFound, keyID, userName)
	}

	return &key, nil
}

// ListSSHPublicKeys returns all SSH public keys for a user.
func (b *InMemoryBackend) ListSSHPublicKeys(
	userName, marker string, maxItems int,
) (page.Page[SSHPublicKey], error) {
	b.mu.RLock("ListSSHPublicKeys-check")
	_, exists := b.users.Get(userName)
	b.mu.RUnlock()

	if !exists {
		return page.Page[SSHPublicKey]{}, fmt.Errorf("%w: user %q not found", ErrUserNotFound, userName)
	}

	b.mu.RLock("ListSSHPublicKeys")

	var keys []SSHPublicKey

	for _, k := range b.comp().sshPublicKeys {
		if k.UserName == userName {
			keys = append(keys, k)
		}
	}

	b.mu.RUnlock()

	sort.Slice(keys, func(i, j int) bool { return keys[i].SSHPublicKeyID < keys[j].SSHPublicKeyID })

	return page.New(keys, marker, maxItems, iamDefaultMaxItems), nil
}

// UpdateSSHPublicKey updates the status of an SSH public key.
func (b *InMemoryBackend) UpdateSSHPublicKey(userName, keyID, status string) error {
	b.mu.Lock("UpdateSSHPublicKey")
	defer b.mu.Unlock()

	c := b.comp()

	key, exists := c.sshPublicKeys[keyID]
	if !exists || key.UserName != userName {
		return fmt.Errorf("%w: SSH public key %q not found for user %q", ErrAccessKeyNotFound, keyID, userName)
	}

	key.Status = status
	c.sshPublicKeys[keyID] = key

	return nil
}

// DeleteSSHPublicKey removes an SSH public key.
func (b *InMemoryBackend) DeleteSSHPublicKey(userName, keyID string) error {
	b.mu.Lock("DeleteSSHPublicKey")
	defer b.mu.Unlock()

	c := b.comp()

	key, exists := c.sshPublicKeys[keyID]
	if !exists || key.UserName != userName {
		return fmt.Errorf("%w: SSH public key %q not found for user %q", ErrAccessKeyNotFound, keyID, userName)
	}

	delete(c.sshPublicKeys, keyID)

	return nil
}

// computeSSHFingerprint returns a placeholder fingerprint for an SSH public key body.
func computeSSHFingerprint(body string) string {
	if len(body) < minSSHBodyLen {
		return "aa:bb:cc:dd:ee:ff:00:11:22:33:44:55:66:77:88:99"
	}

	buf := make([]byte, sshFingerprintBytes)

	for i := range buf {
		buf[i] = body[i%len(body)]
	}

	parts := make([]string, sshFingerprintBytes)

	for i, b := range buf {
		parts[i] = fmt.Sprintf("%02x", b)
	}

	return strings.Join(parts, ":")
}

// convertSSHPublicKeyEncoding re-encodes body to match encoding, converting
// between authorized_keys ("ssh-rsa AAAA...") and PEM SubjectPublicKeyInfo
// format when the stored body is not already in that format.
// UploadSSHPublicKey accepts either format on upload ("must be encoded in
// ssh-rsa format or PEM format"), so GetSSHPublicKey must convert on read to
// honor the requested Encoding rather than always returning the stored body
// verbatim. encoding must already be sshEncodingSSH or sshEncodingPEM.
func convertSSHPublicKeyEncoding(body, encoding string) (string, error) {
	isAuthorizedKeys := false
	if _, _, _, _, err := ssh.ParseAuthorizedKey([]byte(body)); err == nil {
		isAuthorizedKeys = true
	}

	switch encoding {
	case sshEncodingSSH:
		if isAuthorizedKeys {
			return body, nil
		}

		return pemToAuthorizedKeys(body)
	case sshEncodingPEM:
		if !isAuthorizedKeys {
			if block, _ := pem.Decode([]byte(body)); block != nil {
				return body, nil
			}

			return "", fmt.Errorf(
				"%w: stored SSH public key is not in a recognized format",
				ErrUnrecognizedPublicKeyEncoding,
			)
		}

		return authorizedKeysToPEM(body)
	default:
		return "", fmt.Errorf("%w: Encoding must be SSH or PEM", ErrUnrecognizedPublicKeyEncoding)
	}
}

// authorizedKeysToPEM converts an authorized_keys-format SSH public key to
// PEM-encoded SubjectPublicKeyInfo.
func authorizedKeysToPEM(body string) (string, error) {
	pub, _, _, _, err := ssh.ParseAuthorizedKey([]byte(body))
	if err != nil {
		return "", fmt.Errorf("%w: %w", ErrUnrecognizedPublicKeyEncoding, err)
	}

	cryptoPub, ok := pub.(ssh.CryptoPublicKey)
	if !ok {
		return "", fmt.Errorf(
			"%w: key type %s cannot be converted to PEM",
			ErrUnrecognizedPublicKeyEncoding,
			pub.Type(),
		)
	}

	der, err := x509.MarshalPKIXPublicKey(cryptoPub.CryptoPublicKey())
	if err != nil {
		return "", fmt.Errorf("%w: %w", ErrUnrecognizedPublicKeyEncoding, err)
	}

	return string(pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: der})), nil
}

// pemToAuthorizedKeys converts a PEM-encoded SubjectPublicKeyInfo to
// authorized_keys format.
func pemToAuthorizedKeys(body string) (string, error) {
	block, _ := pem.Decode([]byte(body))
	if block == nil {
		return "", fmt.Errorf("%w: stored SSH public key is not valid PEM", ErrUnrecognizedPublicKeyEncoding)
	}

	pubAny, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		return "", fmt.Errorf("%w: %w", ErrUnrecognizedPublicKeyEncoding, err)
	}

	sshPub, err := ssh.NewPublicKey(pubAny)
	if err != nil {
		return "", fmt.Errorf("%w: %w", ErrUnrecognizedPublicKeyEncoding, err)
	}

	return strings.TrimSpace(string(ssh.MarshalAuthorizedKey(sshPub))), nil
}
