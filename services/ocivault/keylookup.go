package ocivault

// KeyState reports the lifecycle state of a key by OCID, for services that reference Vault keys.
func (b *InMemoryBackend) KeyState(keyID string) (string, bool) {
	b.mu.RLock("KeyState")
	defer b.mu.RUnlock()

	k, ok := b.keys[keyID]
	if !ok {
		return "", false
	}

	return k.State, true
}
