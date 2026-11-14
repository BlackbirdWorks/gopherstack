package ocifunctions

const keyEnabled = "ENABLED"

// SetKeyChecker makes image policy kmsKeyId values validated against OCI Vault: fn returns the key's state.
func (b *InMemoryBackend) SetKeyChecker(fn func(keyID string) (string, bool)) {
	b.mu.Lock("SetKeyChecker")
	b.keyState = fn
	b.mu.Unlock()
}

// checkImagePolicyKeys must be called with the lock held.
func (b *InMemoryBackend) checkImagePolicyKeys(p *ImagePolicy) *APIError {
	if p == nil || b.keyState == nil {
		return nil
	}

	for _, k := range p.KeyDetails {
		state, ok := b.keyState(k.KmsKeyID)
		if !ok {
			return errNotFound(k.KmsKeyID)
		}

		if state != keyEnabled {
			return errState("The Vault key " + k.KmsKeyID + " is " + state + " and cannot verify image signatures")
		}
	}

	return nil
}
