package workspaces

import (
	"errors"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/idempotency"
)

func (b *InMemoryBackend) idemReplay(op, token, fingerprint string) (string, bool, error) {
	id, hit, err := b.idem.Lookup(op, token, fingerprint)
	if errors.Is(err, idempotency.ErrParamsMismatch) {
		return "", false, awserr.New("ClientToken was already used with different parameters",
			awserr.ErrInvalidParameter)
	}

	return id, hit, err
}

func (b *InMemoryBackend) idemRecord(op, token, fingerprint, id string) {
	b.idem.Record(op, token, fingerprint, id)
}

func idemFingerprint(parts ...any) string { return fmt.Sprint(parts...) }
