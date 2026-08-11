package kinesisanalytics

import (
	"errors"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

var (
	// ErrNotFound is returned when an application does not exist.
	ErrNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrAlreadyExists is returned when an application already exists.
	ErrAlreadyExists = awserr.New("ResourceInUseException", awserr.ErrAlreadyExists)
	// ErrConcurrentUpdate is returned when the application version does not match.
	ErrConcurrentUpdate = errors.New("ConcurrentModificationException: application version mismatch")
	// ErrValidation is returned for invalid input parameters.
	ErrValidation = awserr.New("InvalidArgumentException", awserr.ErrInvalidParameter)
	// ErrLimitExceeded is returned when a resource limit is reached.
	ErrLimitExceeded = awserr.New("LimitExceededException", awserr.ErrConflict)
	// ErrTooManyTags is returned when tagging an application would exceed the maximum tag
	// count. AWS models this as a dedicated TooManyTagsException on CreateApplication and
	// TagResource, distinct from the generic LimitExceededException (verified against
	// aws-sdk-go-v2/service/kinesisanalytics deserializers.go per-operation error lists).
	ErrTooManyTags = errors.New("TooManyTagsException: application tag limit exceeded")
	// ErrResourceInUse is returned when the app is in an incompatible state for the requested operation.
	ErrResourceInUse = awserr.New("ResourceInUseException", awserr.ErrAlreadyExists)
	// ErrUnableToDetectSchema is returned by DiscoverInputSchema when no sample data can be
	// obtained from the requested source. Real AWS has no ResourceNotFoundException on this op
	// (verified: aws-sdk-go-v2/service/kinesisanalytics@v1.33.4 deserializers.go
	// awsAwsjson11_deserializeOpErrorDiscoverInputSchema only switches on InvalidArgumentException,
	// ResourceProvisionedThroughputExceededException, ServiceUnavailableException, and
	// UnableToDetectSchemaException) -- a source that doesn't exist or has no data surfaces here.
	ErrUnableToDetectSchema = errors.New(
		"UnableToDetectSchemaException: unable to sample data from the requested source",
	)
)
