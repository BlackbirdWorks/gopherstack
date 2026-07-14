package iam

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
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

	c := b.comp()
	c.mu.Lock()
	c.sshPublicKeys[keyID] = key
	c.mu.Unlock()

	return &key, nil
}

// GetSSHPublicKey retrieves an SSH public key by user name and key ID.
func (b *InMemoryBackend) GetSSHPublicKey(userName, keyID string) (*SSHPublicKey, error) {
	c := b.comp()
	c.mu.Lock()
	key, exists := c.sshPublicKeys[keyID]
	c.mu.Unlock()

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

	c := b.comp()
	c.mu.Lock()

	var keys []SSHPublicKey

	for _, k := range c.sshPublicKeys {
		if k.UserName == userName {
			keys = append(keys, k)
		}
	}

	c.mu.Unlock()

	sort.Slice(keys, func(i, j int) bool { return keys[i].SSHPublicKeyID < keys[j].SSHPublicKeyID })

	return page.New(keys, marker, maxItems, iamDefaultMaxItems), nil
}

// UpdateSSHPublicKey updates the status of an SSH public key.
func (b *InMemoryBackend) UpdateSSHPublicKey(userName, keyID, status string) error {
	c := b.comp()
	c.mu.Lock()
	defer c.mu.Unlock()

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
	c := b.comp()
	c.mu.Lock()
	defer c.mu.Unlock()

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
