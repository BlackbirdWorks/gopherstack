package azurefunctions

import "errors"

var (
	// ErrNilAppContext is returned when Init is called with a nil AppContext.
	ErrNilAppContext = errors.New("azurefunctions: nil app context")
	// ErrAppNotFound is returned for an unknown function app.
	ErrAppNotFound = errors.New("azurefunctions: function app not found")
	// ErrFunctionNotFound is returned for an unknown function.
	ErrFunctionNotFound = errors.New("azurefunctions: function not found")
	// ErrKeyNotFound is returned for an unknown host or function key.
	ErrKeyNotFound = errors.New("azurefunctions: key not found")
	// ErrInvalidKeyType is returned for a host key type other than functionKeys or systemKeys.
	ErrInvalidKeyType = errors.New("azurefunctions: key type must be functionKeys or systemKeys")
	// ErrInvalidPackage is returned when a deployment is not a readable zip archive.
	ErrInvalidPackage = errors.New("azurefunctions: deployment package is not a valid zip archive")

	errNoRuntime     = errors.New("container runtime is unavailable")
	errStartFailed   = errors.New("function host failed to start")
	errStale         = errors.New("function app was redeployed while starting; retry")
	errUnsafePath    = errors.New("archive entry escapes the site directory")
	errTooLarge      = errors.New("archive is too large when unpacked")
	errNoHost        = errors.New("function app is not running")
	errUnsupportedRT = errors.New("unsupported function runtime")
)
