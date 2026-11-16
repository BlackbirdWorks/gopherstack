package azurecontainerregistry

import (
	"net/http"

	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

// KeyChecker reports whether the Key Vault key at keyIdentifier exists, is enabled and is an RSA key.
type KeyChecker func(keyIdentifier string) error

// SetKeyChecker lets registries with a customer-managed key follow the state of that key.
func (b *InMemoryBackend) SetKeyChecker(c KeyChecker) {
	b.mu.Lock("SetKeyChecker")
	defer b.mu.Unlock()

	b.keyCheck = c
}

// CheckKey runs the key checker; without one every key is accepted.
func (b *InMemoryBackend) CheckKey(keyIdentifier string) error {
	b.mu.RLock("CheckKey")
	check := b.keyCheck
	b.mu.RUnlock()

	if check == nil {
		return nil
	}

	return check(keyIdentifier)
}

// SetEncryptionKey records the registry's customer-managed key; empty clears it.
func (b *InMemoryBackend) SetEncryptionKey(name, keyIdentifier string) error {
	b.mu.Lock("SetEncryptionKey")
	defer b.mu.Unlock()

	r, ok := b.regs[strs.Fold(name)]
	if !ok {
		return ErrRegistryNotFound
	}

	r.KeyID = keyIdentifier

	return nil
}

// keyAvailable is false while the registry's customer-managed key is revoked, disabled or deleted.
func (b *InMemoryBackend) keyAvailable(name string) bool {
	b.mu.RLock("keyAvailable")
	r, ok := b.regs[strs.Fold(name)]
	keyID := ""

	if ok {
		keyID = r.KeyID
	}

	b.mu.RUnlock()

	return keyID == "" || b.CheckKey(keyID) == nil
}

func writeKeyRevoked(w http.ResponseWriter) {
	writeRegistryError(w, http.StatusForbidden, "DENIED",
		"the customer-managed encryption key is not accessible; registry data is blocked until access is restored.")
}
