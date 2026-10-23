package azureappconfig

import (
	"errors"
	"net/http"
)

type problem struct {
	typ, title string
	status     int
}

func problemFor(err error) problem {
	switch {
	case errors.Is(err, ErrKeyNotFound), errors.Is(err, ErrSnapshotNotFound), errors.Is(err, ErrStoreNotFound):
		return problem{"not-found", "Resource not found", http.StatusNotFound}
	case errors.Is(err, ErrPrecondition):
		return problem{"precondition-failed", "Precondition failed", http.StatusPreconditionFailed}
	case errors.Is(err, ErrLocked):
		return problem{"key-locked", "Modifying the key-value is not allowed", http.StatusConflict}
	case errors.Is(err, ErrSnapshotExists):
		return problem{"already-exists", "The snapshot already exists", http.StatusConflict}
	case errors.Is(err, ErrSnapshotStatus):
		return problem{
			"invalid-state",
			"The snapshot is in a state that does not allow this change",
			http.StatusConflict,
		}
	case errors.Is(err, ErrUnauthorized):
		return problem{"unauthorized", "Unauthorized", http.StatusUnauthorized}
	case errors.Is(err, ErrForbidden):
		return problem{"forbidden", "Forbidden", http.StatusForbidden}
	case errors.Is(err, ErrTooLarge):
		return problem{"too-large", "Request body too large", http.StatusRequestEntityTooLarge}
	}

	return problem{"invalid-argument", "Invalid request", http.StatusBadRequest}
}

func problemBody(err error) (int, map[string]any) {
	p := problemFor(err)

	return p.status, map[string]any{
		"type": "https://azconfig.io/errors/" + p.typ, "title": p.title, fieldName: "", "detail": err.Error(),
		fieldStatus: p.status,
	}
}
