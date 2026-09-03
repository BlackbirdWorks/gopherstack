package azureblob

import "errors"

// Sentinel errors for Azure Blob Storage operations.
var (
	ErrContainerNotFound      = errors.New("azureblob: container not found")
	ErrContainerAlreadyExists = errors.New("azureblob: container already exists")
	ErrBlobNotFound           = errors.New("azureblob: blob not found")
	ErrInvalidBlobType        = errors.New("azureblob: unsupported x-ms-blob-type")
	ErrInvalidRange           = errors.New("azureblob: invalid range")
)
