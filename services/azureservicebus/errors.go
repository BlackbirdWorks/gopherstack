package azureservicebus

import "errors"

// Sentinel errors for Azure Service Bus operations.
var (
	ErrQueueNotFound        = errors.New("azureservicebus: queue not found")
	ErrQueueAlreadyExists   = errors.New("azureservicebus: queue already exists")
	ErrTopicNotFound        = errors.New("azureservicebus: topic not found")
	ErrTopicAlreadyExists   = errors.New("azureservicebus: topic already exists")
	ErrSubscriptionNotFound = errors.New("azureservicebus: subscription not found")
	ErrSubscriptionExists   = errors.New("azureservicebus: subscription already exists")
	ErrMessageNotFound      = errors.New("azureservicebus: message not found")
	ErrLockTokenMismatch    = errors.New("azureservicebus: lock token mismatch")
	ErrMessageNotLocked     = errors.New("azureservicebus: message is not locked")
	ErrInvalidEntityRef     = errors.New("azureservicebus: invalid entity reference")
	ErrInvalidEntityConfig  = errors.New("azureservicebus: invalid entity configuration")
	ErrDeadLetterSource     = errors.New("azureservicebus: cannot dead-letter a dead-lettered message")

	ErrSessionIDRequired = errors.New("the SessionId was not set on a message, and it cannot be sent to the entity. " +
		"Entities that have session support enabled can only receive messages that have the SessionId set to a valid value")
	ErrSessionRequired = errors.New("it is not possible for an entity that requires sessions " +
		"to create a non-sessionful message receiver")
	ErrNotSessionEntity = errors.New("it is not possible to create a session receiver " +
		"on an entity that does not require sessions")
	ErrSessionCannotBeLocked = errors.New("the requested session cannot be accepted: it is locked by another receiver")
	ErrSessionLockLost       = errors.New("the session lock has expired or was lost")
	ErrNoSessionAvailable    = errors.New("no session is available")

	// ErrSnapshotNull* are returned by Restore when a snapshot map/slice holds
	// a JSON null entry, which decodes to a nil pointer that would panic on
	// first dereference if stored as-is. Mirrors services/azurequeue's
	// identical family of snapshot-validation errors.
	ErrSnapshotQueueNull        = errors.New("azureservicebus: restore snapshot: queue is null")
	ErrSnapshotTopicNull        = errors.New("azureservicebus: restore snapshot: topic is null")
	ErrSnapshotSubscriptionNull = errors.New("azureservicebus: restore snapshot: subscription is null")
	ErrSnapshotMessageNull      = errors.New("azureservicebus: restore snapshot: message is null")
)
