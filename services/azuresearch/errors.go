package azuresearch

import (
	"errors"
	"fmt"
)

// Backend errors.
var (
	ErrServiceNotFound   = errors.New("azuresearch: search service not found")
	ErrIndexNotFound     = errors.New("azuresearch: index not found")
	ErrObjectNotFound    = errors.New("azuresearch: object not found")
	ErrDocumentNotFound  = errors.New("azuresearch: document not found")
	ErrQueryKeyNotFound  = errors.New("azuresearch: query key not found")
	ErrQueryKeyNameTaken = errors.New("azuresearch: query key name already in use")
	ErrBadRequest        = errors.New("azuresearch: bad request")
	ErrPrecondition      = errors.New("azuresearch: precondition failed")
	ErrIndexInUse        = errors.New("azuresearch: index is referenced by an indexer")
	ErrUnauthorized      = errors.New("azuresearch: unauthorized")
	ErrForbidden         = errors.New("azuresearch: forbidden")
	ErrTooLarge          = errors.New("azuresearch: request entity too large")
	ErrQuota             = errors.New("azuresearch: service quota exceeded")
	ErrNilAppContext     = errors.New("azuresearch: nil app context")
	ErrKeyKind           = errors.New("azuresearch: key kind must be primary or secondary")
	errNullEntry         = errors.New("azuresearch: null snapshot entry")
)

// wrapf wraps sentinel with a service-style message that may start with a capital letter.
func wrapf(sentinel error, format string, args ...any) error {
	return fmt.Errorf("%w: %s", sentinel, fmt.Sprintf(format, args...))
}
