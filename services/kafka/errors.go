package kafka

import "github.com/blackbirdworks/gopherstack/pkgs/awserr"

var (
	// ErrNotFound is returned when a requested resource does not exist.
	ErrNotFound = awserr.New("NotFoundException", awserr.ErrNotFound)
	// ErrAlreadyExists is returned when a resource already exists.
	ErrAlreadyExists = awserr.New("ConflictException", awserr.ErrAlreadyExists)
	// ErrValidation is returned when input validation fails.
	ErrValidation = awserr.New("BadRequestException", awserr.ErrInvalidParameter)
	// ErrTopicExists is returned when a topic with that name already exists on
	// the cluster. CreateTopic's own error switch models the specific
	// TopicExistsException in addition to the generic ConflictException.
	ErrTopicExists = awserr.New("TopicExistsException", awserr.ErrAlreadyExists)
	// ErrTopicNotFound is returned when a topic does not exist on a cluster
	// that does exist. DeleteTopic/UpdateTopic's own error switches model the
	// specific UnknownTopicOrPartitionException (the real Kafka protocol's own
	// name for a missing topic) in addition to the generic NotFoundException.
	ErrTopicNotFound = awserr.New("UnknownTopicOrPartitionException", awserr.ErrNotFound)
)
