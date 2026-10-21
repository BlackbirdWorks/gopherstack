package azureappconfig

import "errors"

// Backend errors.
var (
	ErrStoreNotFound    = errors.New("azureappconfig: configuration store not found")
	ErrKeyNotFound      = errors.New("azureappconfig: key-value not found")
	ErrKeyIDNotFound    = errors.New("azureappconfig: access key not found")
	ErrSnapshotNotFound = errors.New("azureappconfig: snapshot not found")
	ErrSnapshotExists   = errors.New("azureappconfig: snapshot already exists")
	ErrSnapshotStatus   = errors.New("azureappconfig: snapshot is not in a state that allows this change")
	ErrLocked           = errors.New("azureappconfig: key-value is read-only")
	ErrPrecondition     = errors.New("azureappconfig: precondition failed")
	ErrInvalidName      = errors.New("azureappconfig: invalid store name")
	ErrInvalidArgument  = errors.New("azureappconfig: invalid argument")
	ErrUnauthorized     = errors.New("azureappconfig: unauthorized")
	ErrForbidden        = errors.New("azureappconfig: the access key is read-only")
	ErrNilAppContext    = errors.New("azureappconfig: nil app context")
	ErrTooLarge         = errors.New("azureappconfig: request body too large")
	errNullEntry        = errors.New("azureappconfig: null snapshot entry")
)
