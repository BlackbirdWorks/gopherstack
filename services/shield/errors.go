package shield

import (
	"errors"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

var (
	// ErrProtectionNotFound is returned when a protection does not exist.
	ErrProtectionNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrProtectionAlreadyExists is returned when a protection for the resource already exists.
	ErrProtectionAlreadyExists = awserr.New("ResourceAlreadyExistsException", awserr.ErrConflict)
	// ErrSubscriptionAlreadyExists is returned when a Shield Advanced subscription already exists.
	ErrSubscriptionAlreadyExists = awserr.New("ResourceAlreadyExistsException", awserr.ErrConflict)
	// ErrSubscriptionNotFound is returned when no subscription exists.
	ErrSubscriptionNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrSubscriptionRequired is returned when an operation requires an active Shield Advanced
	// subscription. It wraps awserr.ErrConflict for backward-compatible errors.Is matching against
	// generic conflict handling, but handler.go's handleError special-cases it (via a direct
	// errors.Is(err, ErrSubscriptionRequired) check ahead of the generic ErrConflict case) to the
	// wire-correct "InvalidOperationException" __type -- the real Shield API code for "operation
	// would not cause any change to occur" / requires-prerequisite errors, not
	// "ResourceAlreadyExistsException".
	ErrSubscriptionRequired = awserr.New("InvalidOperationException: subscription required", awserr.ErrConflict)
	// ErrProtectionGroupNotFound is returned when a protection group does not exist.
	ErrProtectionGroupNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrProtectionGroupAlreadyExists is returned when a protection group with the same ID already exists.
	ErrProtectionGroupAlreadyExists = awserr.New("ResourceAlreadyExistsException", awserr.ErrConflict)
	// ErrAttackNotFound is returned when an attack does not exist.
	ErrAttackNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrValidation is returned when input validation fails.
	ErrValidation = awserr.New("InvalidParameterException", awserr.ErrInvalidParameter)
	// ErrLimitExceeded is returned when an operation would exceed a Shield Advanced subscription
	// quota (e.g. subscriptionMaxProtections, subscriptionMaxProtectionGroups,
	// subscriptionMaxMembersPerGroup, or the 10-bucket DRT log bucket cap). Maps to the real
	// LimitsExceededException wire type -- distinct from ErrValidation/InvalidParameterException
	// because AWS Shield uses a dedicated error family for quota violations.
	ErrLimitExceeded = errors.New("shield: limit exceeded")
	// ErrNoAssociatedRole is returned when a DRT operation (AssociateDRTLogBucket) requires an IAM
	// role to already be associated via AssociateDRTRole first. Maps to the real
	// NoAssociatedRoleException wire type.
	ErrNoAssociatedRole = errors.New("shield: no DRT role associated")
)
