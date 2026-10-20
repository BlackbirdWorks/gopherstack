package azureeventhubs

import "errors"

// Backend errors.
var (
	ErrNamespaceNotFound     = errors.New("azureeventhubs: namespace not found")
	ErrHubNotFound           = errors.New("azureeventhubs: event hub not found")
	ErrConsumerGroupNotFound = errors.New("azureeventhubs: consumer group not found")
	ErrRuleNotFound          = errors.New("azureeventhubs: authorization rule not found")
	ErrPartitionNotFound     = errors.New("azureeventhubs: partition not found")
	ErrInvalidPartitionCount = errors.New("azureeventhubs: partition count out of range")
	ErrPartitionCountShrink  = errors.New("azureeventhubs: partition count cannot be decreased")
	ErrInvalidRetention      = errors.New("azureeventhubs: invalid retention")
	ErrProtectedEntity       = errors.New("azureeventhubs: entity cannot be deleted")
	ErrInvalidName           = errors.New("azureeventhubs: invalid entity name")
	ErrEventTooLarge         = errors.New("azureeventhubs: event exceeds the maximum size")
	ErrReceiverStolen        = errors.New("azureeventhubs: receiver superseded by a higher epoch")
	ErrTooManyReceivers      = errors.New("azureeventhubs: too many concurrent receivers")
	ErrInvalidSelector       = errors.New("azureeventhubs: invalid selector filter")
	ErrNilAppContext         = errors.New("azureeventhubs: nil app context")
	errNoSuchRights          = errors.New("azureeventhubs: invalid rights")
)
