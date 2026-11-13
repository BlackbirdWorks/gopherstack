package ocivault

// retrySeen reports whether the token was already used for scope; the caller holds the write lock.
func (b *InMemoryBackend) retrySeen(scope, token string) (string, bool) {
	return b.retry.Lookup(scope, token, b.nowUTC())
}

// retryNote records the first result of a token; the caller holds the write lock.
func (b *InMemoryBackend) retryNote(scope, token, value string) {
	b.retry.Remember(scope, token, value, b.nowUTC())
}

func (b *InMemoryBackend) replayKey(vaultID, keyID string) (keyWire, string, error) {
	k, _, aerr := b.keyFor(vaultID, keyID)
	if aerr != nil {
		return keyWire{}, "", aerr
	}

	return keyView(k), k.Etag, nil
}

func (b *InMemoryBackend) replayKeyVersion(vaultID, keyID, id string) (keyVersionWire, string, error) {
	k, _, aerr := b.keyFor(vaultID, keyID)
	if aerr != nil {
		return keyVersionWire{}, "", errInvalidatedToken()
	}

	kv, aerr := k.version(id)
	if aerr != nil {
		return keyVersionWire{}, "", errInvalidatedToken()
	}

	return keyVersionFull(k, kv, ""), k.Etag, nil
}

func (b *InMemoryBackend) replayVault(id, hostPort string) (vaultWire, string, error) {
	v, aerr := b.vaultByID(id)
	if aerr != nil {
		return vaultWire{}, "", aerr
	}

	return vaultView(v, hostPort), v.Etag, nil
}
