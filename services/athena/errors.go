package athena

import "errors"

// AWS Athena models a small, fixed set of API exceptions. The gopherstack
// sentinels below each map to one of these codes; handleError translates the
// sentinel into the __type field the SDK deserializes. See
// aws-sdk-go/service/athena/errors.go for the authoritative list.
const (
	errTypeInternalServer      = "InternalServerException"
	errTypeInvalidRequestExc   = "InvalidRequestException"
	errTypeMetadataExc         = "MetadataException"
	errTypeResourceNotFoundExc = "ResourceNotFoundException"
	errTypeSessionExistsExc    = "SessionAlreadyExistsException"
	errTypeTooManyRequestsExc  = "TooManyRequestsException"
)

var (
	// ErrNotFound is returned when a requested resource that AWS reports via
	// InvalidRequestException does not exist (workgroups, named queries, data
	// catalogs, query executions, notebooks, capacity reservations). AWS Athena
	// returns InvalidRequestException — not a dedicated NotFound code — for these.
	ErrNotFound = errors.New(errTypeInvalidRequestExc)
	// ErrAlreadyExists is returned when a resource already exists. AWS Athena
	// reports duplicate workgroups/catalogs/notebooks as InvalidRequestException.
	ErrAlreadyExists = errors.New(errTypeInvalidRequestExc)
	// ErrProtected is returned when an operation is not allowed on a protected
	// resource (e.g. deleting the primary workgroup). AWS returns
	// InvalidRequestException for these.
	ErrProtected = errors.New(errTypeInvalidRequestExc)
	// ErrValidation is returned when input fails validation
	// (InvalidRequestException).
	ErrValidation = errors.New(errTypeInvalidRequestExc)
	// ErrResourceNotFound is returned for resources AWS reports with the
	// dedicated ResourceNotFoundException code: sessions, calculation
	// executions, and prepared statements.
	ErrResourceNotFound = errors.New(errTypeResourceNotFoundExc)
	// ErrMetadata is returned when a metadata lookup against the (emulated Glue)
	// catalog fails — missing databases and tables — which AWS surfaces as
	// MetadataException.
	ErrMetadata = errors.New(errTypeMetadataExc)
	// ErrSessionExists is returned when a session with the same identity already
	// exists (SessionAlreadyExistsException).
	ErrSessionExists = errors.New(errTypeSessionExistsExc)
)
