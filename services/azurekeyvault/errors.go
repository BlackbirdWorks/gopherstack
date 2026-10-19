package azurekeyvault

import (
	"errors"
	"fmt"
	"net/http"
)

// ErrNilAppContext is returned when Init is called with a nil AppContext.
var ErrNilAppContext = errors.New("azurekeyvault: nil app context")

// ErrSnapshotNull is returned when a snapshot contains a null entity.
var ErrSnapshotNull = errors.New("azurekeyvault: snapshot contains null entry")

// kvError is a Key Vault data-plane error with its wire code and status.
type kvError struct {
	Code    string
	Message string
	Status  int
}

func (e *kvError) Error() string { return e.Code + ": " + e.Message }

func newErr(status int, code, format string, args ...any) *kvError {
	return &kvError{Status: status, Code: code, Message: fmt.Sprintf(format, args...)}
}

func errBadParameter(format string, args ...any) *kvError {
	return newErr(http.StatusBadRequest, "BadParameter", format, args...)
}

func errNotFound(kind, name string) *kvError {
	return newErr(
		http.StatusNotFound,
		kind+"NotFound",
		"A %s with (name/id) %s was not found in this key vault. "+
			"If you recently deleted this %s you may be able to recover it using the correct recovery command.",
		lowerKind(kind),
		name,
		lowerKind(kind),
	)
}

func errDeletedConflict(kind, name string) *kvError {
	return newErr(
		http.StatusConflict,
		"Conflict",
		"%s %s is currently in a deleted but recoverable state, and its name cannot be reused; "+
			"in this state, the %s can only be recovered or purged.",
		kind,
		name,
		lowerKind(kind),
	)
}

func errNotDeleted(kind, name string) *kvError {
	return newErr(http.StatusNotFound, kind+"NotFound",
		"Deleted %s not found: %s", lowerKind(kind), name)
}

func errDisabled(kind string) *kvError {
	return newErr(http.StatusForbidden, "Forbidden",
		"Operation get is not allowed on a disabled %s.", lowerKind(kind))
}

func errVaultNotFound(name string) *kvError {
	return newErr(http.StatusNotFound, "VaultNotFound", "Vault %s was not found.", name)
}

func lowerKind(kind string) string {
	if kind == "Certificate" {
		return "certificate"
	}

	if kind == "Key" {
		return "key"
	}

	return "secret"
}
