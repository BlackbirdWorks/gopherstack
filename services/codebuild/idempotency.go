package codebuild

import (
	"errors"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/idempotency"
)

// idemFingerprint renders a request's parameters; callers blank the token before passing cfg.
func idemFingerprint(parts ...any) string { return idempotency.Fingerprint(parts) }

func (b *InMemoryBackend) idemReplay(op, token, fingerprint string) (string, bool, error) {
	id, hit, err := b.idem.Lookup(op, token, fingerprint)
	if errors.Is(err, idempotency.ErrParamsMismatch) {
		return "", false, fmt.Errorf(
			"%w: idempotencyToken was already used with different parameters", ErrValidation,
		)
	}

	return id, hit, err
}

func (b *InMemoryBackend) idemRecord(op, token, fingerprint, id string) {
	b.idem.Record(op, token, fingerprint, id)
}
