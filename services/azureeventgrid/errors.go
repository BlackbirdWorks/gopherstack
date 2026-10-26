package azureeventgrid

import "errors"

// Backend errors.
var (
	ErrTopicNotFound          = errors.New("azureeventgrid: topic not found")
	ErrSubscriptionNotFound   = errors.New("azureeventgrid: event subscription not found")
	ErrDomainNotFound         = errors.New("azureeventgrid: domain not found")
	ErrDomainTopicNotFound    = errors.New("azureeventgrid: domain topic not found")
	ErrNamespaceNotFound      = errors.New("azureeventgrid: namespace not found")
	ErrNamespaceTopicNotFound = errors.New("azureeventgrid: namespace topic not found")
	ErrPullSubNotFound        = errors.New("azureeventgrid: namespace topic subscription not found")
	ErrInvalidName            = errors.New("azureeventgrid: invalid entity name")
	ErrInvalidSpec            = errors.New("azureeventgrid: invalid event subscription")
	ErrWebhookValidation      = errors.New("azureeventgrid: webhook validation handshake failed")
	ErrTargetUnavailable      = errors.New("azureeventgrid: delivery target is not wired")
	ErrInvalidEvent           = errors.New("azureeventgrid: invalid event")
	ErrTooManyEvents          = errors.New("azureeventgrid: too many events in one request")
	ErrNilAppContext          = errors.New("azureeventgrid: nil app context")
	ErrAmbiguousTopic         = errors.New("azureeventgrid: cannot determine the topic of the request")
	ErrBadLockToken           = errors.New("azureeventgrid: lock token is invalid or expired")
)
