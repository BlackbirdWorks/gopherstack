package azureblob

import (
	"errors"
	"net/http"
)

// Sentinel errors for Azure Blob Storage operations.
var (
	ErrContainerNotFound      = errors.New("azureblob: container not found")
	ErrContainerAlreadyExists = errors.New("azureblob: container already exists")
	ErrBlobNotFound           = errors.New("azureblob: blob not found")
	ErrSnapshotsPresent       = errors.New("azureblob: blob has snapshots")
	ErrInvalidMarker          = errors.New("azureblob: invalid list marker")

	// ErrSnapshotContainerNull and ErrSnapshotBlobNull are returned by
	// Restore when a snapshot's "containers" map (or a container's "Blobs"
	// map) holds a JSON null entry, which decodes to a nil pointer that
	// would panic on first dereference if stored as-is. See persistence.go.
	ErrSnapshotContainerNull = errors.New("azureblob: restore snapshot: container is null")
	ErrSnapshotBlobNull      = errors.New("azureblob: restore snapshot: blob is null")
)

// StorageError is a backend failure that maps one-to-one onto an Azure
// Storage REST error response. Header is written on 304 responses.
type StorageError struct {
	Header  http.Header
	Code    string
	Message string
	Status  int
}

func (e *StorageError) Error() string { return "azureblob: " + e.Code + ": " + e.Message }

func storageErr(status int, code, message string) *StorageError {
	return &StorageError{Status: status, Code: code, Message: message}
}
