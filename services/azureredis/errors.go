package azureredis

import "errors"

var (
	// ErrNotFound is returned for an unknown cache.
	ErrNotFound = errors.New("azureredis: cache not found")
	// ErrInvalid is returned for a malformed request.
	ErrInvalid = errors.New("azureredis: invalid request")
	// ErrNoBlobStore is returned when import or export has no Blob Storage to talk to.
	ErrNoBlobStore = errors.New("azureredis: blob storage is not available")
	// ErrNilAppContext is returned when Init is called with a nil AppContext.
	ErrNilAppContext = errors.New("azureredis: nil app context")

	errTLSKeyMissing  = errors.New("azureredis: --azure-redis-tls-cert set without --azure-redis-tls-key")
	errTLSCertMissing = errors.New("azureredis: --azure-redis-tls-key set without --azure-redis-tls-cert")
)
