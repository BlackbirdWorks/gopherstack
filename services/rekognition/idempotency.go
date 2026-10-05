package rekognition

type idemEntry struct {
	resp        any
	fingerprint string
}

// IdempotencyLookup returns the response recorded for (op, token); a different fingerprint is a mismatch.
func (b *InMemoryBackend) IdempotencyLookup(op, token, fingerprint string) (any, bool, error) {
	if token == "" {
		return nil, false, nil
	}

	b.mu.RLock("IdempotencyLookup")
	defer b.mu.RUnlock()

	e, ok := b.idemResponses[op+"|"+token]
	if !ok {
		return nil, false, nil
	}

	if e.fingerprint != fingerprint {
		return nil, false, ErrIdempotentParameterMismatch
	}

	return e.resp, true, nil
}

// IdempotencyStore records resp as the answer to every later (op, token) request.
func (b *InMemoryBackend) IdempotencyStore(op, token, fingerprint string, resp any) {
	if token == "" {
		return
	}

	b.mu.Lock("IdempotencyStore")
	defer b.mu.Unlock()

	b.idemResponses[op+"|"+token] = idemEntry{fingerprint: fingerprint, resp: resp}
}
