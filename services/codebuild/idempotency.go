package codebuild

import (
	"encoding/json"
	"fmt"
	"time"
)

const (
	idempotencyTTL        = 5 * time.Minute
	maxIdempotencyEntries = 1024
)

type idempotencyEntry struct {
	expires     time.Time
	fingerprint string
	id          string
}

// idemFingerprint hashes a request's parameters; callers blank the token before passing cfg.
func idemFingerprint(parts ...any) string {
	raw, err := json.Marshal(parts)
	if err != nil {
		return fmt.Sprintf("%v", parts)
	}

	return string(raw)
}

// idemReplay returns the resource ID a live (op, token) already created. Caller holds b.mu.
func (b *InMemoryBackend) idemReplay(op, token, fingerprint string) (string, bool, error) {
	if token == "" {
		return "", false, nil
	}

	e, ok := b.idempotency[op+"|"+token]
	if !ok || time.Now().After(e.expires) {
		return "", false, nil
	}

	if e.fingerprint != fingerprint {
		return "", false, fmt.Errorf(
			"%w: idempotencyToken was already used with different parameters", ErrValidation,
		)
	}

	return e.id, true, nil
}

// idemRecord remembers the resource a token created, evicting expired then oldest entries at the cap.
func (b *InMemoryBackend) idemRecord(op, token, fingerprint, id string) {
	if token == "" {
		return
	}

	now := time.Now()

	if len(b.idempotency) >= maxIdempotencyEntries {
		var oldestKey string

		var oldest time.Time

		for k, e := range b.idempotency {
			if now.After(e.expires) {
				delete(b.idempotency, k)

				continue
			}

			if oldestKey == "" || e.expires.Before(oldest) {
				oldestKey, oldest = k, e.expires
			}
		}

		if len(b.idempotency) >= maxIdempotencyEntries {
			delete(b.idempotency, oldestKey)
		}
	}

	b.idempotency[op+"|"+token] = idempotencyEntry{
		expires: now.Add(idempotencyTTL), fingerprint: fingerprint, id: id,
	}
}
