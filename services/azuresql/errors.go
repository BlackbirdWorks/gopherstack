package azuresql

import "errors"

var (
	// ErrNotFound is returned for an unknown server.
	ErrNotFound = errors.New("azuresql: server not found")
	// ErrInvalid is returned for a malformed request.
	ErrInvalid = errors.New("azuresql: invalid request")
	// ErrRuntimeUnavailable is returned by a Launcher when no container runtime is reachable; the server then
	// runs state-only.
	ErrRuntimeUnavailable = errors.New("azuresql: container runtime unavailable")
	// ErrNilAppContext is returned when Init is called with a nil AppContext.
	ErrNilAppContext = errors.New("azuresql: nil app context")
)
