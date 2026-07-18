package transfer

import (
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"sort"
	"strings"
	"time"

	"golang.org/x/crypto/ssh"

	"github.com/google/uuid"
)

// ListSSHPublicKeys returns all SSH public keys for a user on a server.
func (b *InMemoryBackend) ListSSHPublicKeys(serverID, userName string) []*SSHPublicKey {
	b.mu.RLock("ListSSHPublicKeys")
	defer b.mu.RUnlock()

	keyMap := b.sshKeysByServerUser.Get(serverUserKey(serverID, userName))

	keys := make([]*SSHPublicKey, 0, len(keyMap))
	for _, k := range keyMap {
		cp := *k
		keys = append(keys, &cp)
	}

	sort.Slice(keys, func(i, j int) bool {
		return keys[i].SSHPublicKeyID < keys[j].SSHPublicKeyID
	})

	return keys
}

// ImportSSHPublicKey imports an SSH public key for a user on a server.
func (b *InMemoryBackend) ImportSSHPublicKey(
	serverID, userName, sshPublicKeyBody string,
) (*SSHPublicKey, error) {
	b.mu.Lock("ImportSshPublicKey")
	defer b.mu.Unlock()

	if !b.servers.Has(serverID) {
		return nil, fmt.Errorf("%w: server %s not found", ErrServerNotFound, serverID)
	}

	// Lazily initialize the body index for this server/user.
	if _, ok := b.sshKeyBodies[serverID]; !ok {
		b.sshKeyBodies[serverID] = make(map[string]map[string]struct{})
	}

	if _, ok := b.sshKeyBodies[serverID][userName]; !ok {
		b.sshKeyBodies[serverID][userName] = make(map[string]struct{})
	}

	// AWS limits each user to 50 SSH public keys.
	const maxSSHPublicKeysPerUser = 50
	if len(b.sshKeysByServerUser.Get(serverUserKey(serverID, userName))) >= maxSSHPublicKeysPerUser {
		return nil, fmt.Errorf(
			"%w: user %s on server %s has reached the maximum of %d SSH public keys",
			ErrValidation,
			userName,
			serverID,
			maxSSHPublicKeysPerUser,
		)
	}

	// Check for duplicate key body using O(1) index.
	normalizedBody := strings.TrimSpace(sshPublicKeyBody)
	if _, dup := b.sshKeyBodies[serverID][userName][normalizedBody]; dup {
		return nil, fmt.Errorf(
			"%w: SSH public key body already exists for user %s on server %s",
			ErrSSHPublicKeyDuplicate,
			userName,
			serverID,
		)
	}

	keyID := "key-" + uuid.NewString()[:8]
	fingerprint, keyType := computeSSHKeyFingerprintAndType(sshPublicKeyBody)

	k := &SSHPublicKey{
		SSHPublicKeyID:   keyID,
		SSHPublicKeyBody: sshPublicKeyBody,
		Fingerprint:      fingerprint,
		KeyType:          keyType,
		UserName:         userName,
		ServerID:         serverID,
		DateImported:     time.Now(),
	}
	b.sshPublicKeys.Put(k)
	b.sshKeyBodies[serverID][userName][normalizedBody] = struct{}{}

	return &SSHPublicKey{
		SSHPublicKeyID:   k.SSHPublicKeyID,
		SSHPublicKeyBody: k.SSHPublicKeyBody,
		Fingerprint:      k.Fingerprint,
		KeyType:          k.KeyType,
		UserName:         k.UserName,
		ServerID:         k.ServerID,
		DateImported:     k.DateImported,
	}, nil
}

// DeleteSSHPublicKey removes an SSH public key from a user on a server.
func (b *InMemoryBackend) DeleteSSHPublicKey(serverID, userName, sshPublicKeyID string) error {
	b.mu.Lock("DeleteSshPublicKey")
	defer b.mu.Unlock()

	key := sshPublicKeyKey(serverID, userName, sshPublicKeyID)

	k, ok := b.sshPublicKeys.Get(key)
	if !ok {
		return fmt.Errorf("%w: SSH key %s not found", ErrSSHPublicKeyNotFound, sshPublicKeyID)
	}

	if bodyIndex := b.sshKeyBodies[serverID][userName]; bodyIndex != nil {
		delete(bodyIndex, strings.TrimSpace(k.SSHPublicKeyBody))
	}

	b.sshPublicKeys.Delete(key)

	return nil
}

// computeSSHKeyFingerprintAndType computes the SHA256 fingerprint and detects the key type.
// Uses golang.org/x/crypto/ssh when possible for accurate fingerprint; falls back to manual SHA256.
func computeSSHKeyFingerprintAndType(keyBody string) (string, string) {
	// Try golang.org/x/crypto/ssh first for accurate fingerprint.
	pk, _, _, _, err := ssh.ParseAuthorizedKey([]byte(keyBody))
	if err == nil {
		return ssh.FingerprintSHA256(pk), pk.Type()
	}

	// Fallback: manual computation.
	parts := strings.Fields(keyBody)
	const minSSHKeyParts = 2
	if len(parts) < minSSHKeyParts {
		return "", ""
	}

	decoded, err := base64.StdEncoding.DecodeString(parts[1])
	if err != nil {
		return "", ""
	}

	sum := sha256.Sum256(decoded)
	fp := "SHA256:" + base64.StdEncoding.EncodeToString(sum[:])

	// Detect type from prefix.
	switch parts[0] {
	case defaultHostKeyType, sshKeyTypeECDSAP256, sshKeyTypeECDSAP384, sshKeyTypeECDSAP521, sshKeyTypeEd25519:
		return fp, parts[0]
	default:
		return fp, ""
	}
}

// CountUserSSHPublicKeys returns the number of SSH public keys for the given user on a server.
func (b *InMemoryBackend) CountUserSSHPublicKeys(serverID, userName string) int {
	b.mu.RLock("CountUserSSHPublicKeys")
	defer b.mu.RUnlock()

	return len(b.sshKeysByServerUser.Get(serverUserKey(serverID, userName)))
}
