package ocifilestorage

const keyEnabled = "ENABLED"

// SetKeyChecker makes kmsKeyId validated against OCI Vault: fn returns the key's lifecycle state.
func (b *InMemoryBackend) SetKeyChecker(fn func(keyID string) (string, bool)) {
	b.mu.Lock("SetKeyChecker")
	b.keyState = fn
	b.mu.Unlock()
}

// checkKmsKey must be called with the lock held; an empty id means Oracle-managed keys.
func (b *InMemoryBackend) checkKmsKey(id string) *APIError {
	if id == "" || b.keyState == nil {
		return nil
	}

	state, ok := b.keyState(id)
	if !ok {
		return errNotFound(id)
	}

	if state != keyEnabled {
		return errState("The Vault key " + id + " is " + state + " and cannot encrypt the file system")
	}

	return nil
}
