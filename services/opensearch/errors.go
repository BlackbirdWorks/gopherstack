package opensearch

import (
	"errors"
)

// Errors returned by the OpenSearch backend.
var (
	ErrDomainNotFound           = errors.New("ResourceNotFoundException")
	ErrDomainAlreadyExists      = errors.New("ResourceAlreadyExistsException")
	ErrInvalidParameter         = errors.New("ValidationException")
	ErrValidation               = errors.New("ValidationException")
	ErrConnectionNotFound       = errors.New("ResourceNotFoundException")
	ErrDataSourceNotFound       = errors.New("ResourceNotFoundException")
	ErrDataSourceAlreadyExists  = errors.New("ResourceAlreadyExistsException")
	ErrPackageNotFound          = errors.New("ResourceNotFoundException")
	ErrApplicationNotFound      = errors.New("ResourceNotFoundException")
	ErrApplicationAlreadyExists = errors.New("ResourceAlreadyExistsException")
	ErrScheduledActionNotFound  = errors.New("ResourceNotFoundException")
	ErrAttachmentNotFound       = errors.New("ResourceNotFoundException")
	ErrAttachmentConflict       = errors.New("ConflictException")
	ErrCapabilityNotFound       = errors.New("ResourceNotFoundException")
	ErrMigrationNotFound        = errors.New("ResourceNotFoundException")
	ErrInsightNotFound          = errors.New("ResourceNotFoundException")
	ErrWorkspaceNotFound        = errors.New("ResourceNotFoundException")
	ErrAccessDenied             = errors.New("AccessDeniedException")
	// ErrPackageAssociated is returned by DeletePackage when the package is
	// still associated with a domain. Real AWS: DeletePackage's own doc
	// comment, "The package can't be associated with any OpenSearch Service
	// domain".
	ErrPackageAssociated = errors.New("ConflictException")
	// ErrServerlessTagLimitExceeded is returned by TagResource when applying
	// new tags would push a collection's tag count past the documented
	// 50-tag-per-resource cap. TagResource is the only serverless op
	// declaring ServiceQuotaExceededException (opensearchserverless
	// v1.34.4 deserializers.go awsAwsjson10_deserializeOpErrorTagResource).
	ErrServerlessTagLimitExceeded = errors.New("ServiceQuotaExceededException")
)
