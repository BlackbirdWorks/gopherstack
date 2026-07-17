package shield

import "github.com/blackbirdworks/gopherstack/pkgs/awserr"

var (
	// ErrProtectionNotFound is returned when a protection does not exist.
	ErrProtectionNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrProtectionAlreadyExists is returned when a protection for the resource already exists.
	ErrProtectionAlreadyExists = awserr.New("ResourceAlreadyExistsException", awserr.ErrConflict)
	// ErrSubscriptionAlreadyExists is returned when a Shield Advanced subscription already exists.
	ErrSubscriptionAlreadyExists = awserr.New("ResourceAlreadyExistsException", awserr.ErrConflict)
	// ErrSubscriptionNotFound is returned when no subscription exists.
	ErrSubscriptionNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrSubscriptionRequired is returned when an operation requires an active Shield Advanced subscription.
	ErrSubscriptionRequired = awserr.New("InvalidOperationException: subscription required", awserr.ErrConflict)
	// ErrProtectionGroupNotFound is returned when a protection group does not exist.
	ErrProtectionGroupNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrProtectionGroupAlreadyExists is returned when a protection group with the same ID already exists.
	ErrProtectionGroupAlreadyExists = awserr.New("ResourceAlreadyExistsException", awserr.ErrConflict)
	// ErrAttackNotFound is returned when an attack does not exist.
	ErrAttackNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrValidation is returned when input validation fails.
	ErrValidation = awserr.New("InvalidParameterException", awserr.ErrInvalidParameter)
)
