package servicediscovery

import "github.com/blackbirdworks/gopherstack/pkgs/awserr"

var (
	// ErrNamespaceNotFound is returned when a namespace does not exist.
	ErrNamespaceNotFound = awserr.New("NamespaceNotFound", awserr.ErrNotFound)
	// ErrServiceNotFound is returned when a service does not exist.
	ErrServiceNotFound = awserr.New("ServiceNotFound", awserr.ErrNotFound)
	// ErrInstanceNotFound is returned when an instance does not exist.
	ErrInstanceNotFound = awserr.New("InstanceNotFound", awserr.ErrNotFound)
	// ErrOperationNotFound is returned when an operation does not exist.
	ErrOperationNotFound = awserr.New("OperationNotFound", awserr.ErrNotFound)
	// ErrNamespaceAlreadyExists is returned when a namespace with the same name already exists.
	ErrNamespaceAlreadyExists = awserr.New("NamespaceAlreadyExists", awserr.ErrAlreadyExists)
	// ErrServiceAttributesNotFound is returned when no attributes exist for a service.
	ErrServiceAttributesNotFound = awserr.New("ServiceAttributesNotFound", awserr.ErrNotFound)
	// ErrInvalidInput is returned when an input value is invalid.
	ErrInvalidInput = awserr.New("InvalidInput", awserr.ErrInvalidParameter)
	// ErrResourceNotFound is returned when a tagged resource ARN is not found.
	ErrResourceNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrResourceInUse is returned when a delete is attempted on a non-empty namespace or service.
	ErrResourceInUse = awserr.New("ResourceInUse", awserr.ErrConflict)
	// ErrCustomHealthNotFound is returned when UpdateInstanceCustomHealthStatus is called on a
	// service that has no HealthCheckCustomConfig.
	ErrCustomHealthNotFound = awserr.New("CustomHealthNotFound", awserr.ErrNotFound)
	// ErrTooManyTags is returned when a request would leave a resource with more than
	// maxTagCount tags.
	ErrTooManyTags = awserr.New("TooManyTagsException", awserr.ErrInvalidParameter)
)
