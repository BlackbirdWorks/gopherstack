package bedrock

import (
	"errors"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/idempotency"
)

func idemFingerprint(v any) string { return idempotency.Fingerprint(v) }

// idemCreate replays a create by clientRequestToken; a token reused with other parameters
// fails with mismatch (ErrAlreadyExists, or ErrValidation where the op declares no ConflictException).
func idemCreate[T any](
	m *idempotency.Memo, op, token, fingerprint string, mismatch error,
	idOf func(*T) string, get func(string) (*T, error), create func() (*T, error),
) (*T, error) {
	res, err := idempotency.Create(m, op, token, fingerprint, idOf, get, create)
	if errors.Is(err, idempotency.ErrParamsMismatch) {
		return nil, fmt.Errorf("%w: client request token already used with different parameters", mismatch)
	}

	return res, err
}
