package appstream

import (
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

const (
	errResourceNotFound = "ResourceNotFoundException"
	errInvalidParameter = "InvalidParameterCombinationException"
	errResourceExists   = "ResourceAlreadyExistsException"
	errFleetNotStopped  = "InvalidAccountStatusException"
	errResourceInUse    = "ResourceInUseException"
	// errSerialization is used for requests that don't satisfy an
	// operation's input shape (e.g. a required member is absent). Some ops
	// (e.g. CreateApplication) declare no validation-style business
	// exception in their own SDK deserializer switch, because the SDK
	// client blocks such calls before they're ever sent; "Serialization
	// Exception" never appears in any per-op switch across this SDK
	// (grepped appstream@v1.64.5/deserializers.go), which is consistent
	// with it being a protocol-level error rather than an operation-
	// specific one -- the same class this package already uses for
	// malformed CBOR/JSON bodies (rpcv2cbor.go).
	errSerialization = "SerializationException"
)

var (
	// ErrNotFound is returned when a resource does not exist.
	ErrNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrAlreadyExists is returned when a resource already exists.
	ErrAlreadyExists = awserr.New(errResourceExists, awserr.ErrAlreadyExists)
	// ErrFleetNotStopped is returned when a fleet state transition is invalid
	// (e.g. starting a running fleet, stopping a stopped fleet, deleting a running fleet).
	ErrFleetNotStopped = awserr.New(errFleetNotStopped, awserr.ErrConflict)
	// ErrResourceInUse is returned when a resource cannot be deleted because it is in use.
	ErrResourceInUse = awserr.New(errResourceInUse, awserr.ErrConflict)
	// ErrSerialization is returned when a request doesn't satisfy an
	// operation's required input shape. Must precede awserr.ErrInvalidParameter
	// in errorCodeStatus's switch (same pattern as ErrFleetNotStopped/
	// ErrAlreadyExists there), since it also wraps that sentinel.
	ErrSerialization = awserr.New(errSerialization, awserr.ErrInvalidParameter)
)
