package kafkaconnect

import "github.com/blackbirdworks/gopherstack/pkgs/awserr"

var (
	// ErrConnectorNotFound is returned when a connector ARN does not resolve.
	ErrConnectorNotFound = awserr.New("connector not found", awserr.ErrNotFound)
	// ErrConnectorNameInUse is returned when a connector name is already in use.
	ErrConnectorNameInUse = awserr.New("a connector with this name already exists", awserr.ErrAlreadyExists)
	// ErrCustomPluginNotFound is returned when a custom plugin ARN does not resolve.
	ErrCustomPluginNotFound = awserr.New("custom plugin not found", awserr.ErrNotFound)
	// ErrCustomPluginNameInUse is returned when a custom plugin name is already in use.
	ErrCustomPluginNameInUse = awserr.New("a custom plugin with this name already exists", awserr.ErrAlreadyExists)
	// ErrWorkerConfigNotFound is returned when a worker configuration ARN does not resolve.
	ErrWorkerConfigNotFound = awserr.New("worker configuration not found", awserr.ErrNotFound)
	// ErrWorkerConfigNameInUse is returned when a worker configuration name is already in use.
	ErrWorkerConfigNameInUse = awserr.New(
		"a worker configuration with this name already exists",
		awserr.ErrAlreadyExists,
	)
	// ErrConnectorOperationNotFound is returned when a connector operation ARN does not resolve.
	ErrConnectorOperationNotFound = awserr.New("connector operation not found", awserr.ErrNotFound)
	// ErrResourceNotFound is returned by the generic tag operations when resourceArn resolves to nothing.
	ErrResourceNotFound = awserr.New("resource not found", awserr.ErrNotFound)
	// ErrVersionMismatch is returned when currentVersion does not match a connector's actual version.
	ErrVersionMismatch = awserr.New(
		"the current version specified does not match the connector's actual current version",
		awserr.ErrConflict,
	)
	// ErrValidation is returned when request input fails validation.
	ErrValidation = awserr.New("invalid request", awserr.ErrInvalidParameter)
)
