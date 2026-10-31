package ociqueue

import "time"

// Lifecycle states.
const (
	stateActive   = "ACTIVE"
	stateCreating = "CREATING"
	stateUpdating = "UPDATING"
	stateDeleting = "DELETING"
	stateDeleted  = "DELETED"
)

// Tags are the shared freeform and defined tag sets of a resource.
type Tags struct {
	FreeformTags map[string]string         `json:"freeformTags,omitempty"`
	DefinedTags  map[string]map[string]any `json:"definedTags,omitempty"`
}

func (t *Tags) merge(in Tags) {
	if in.FreeformTags != nil {
		t.FreeformTags = in.FreeformTags
	}

	if in.DefinedTags != nil {
		t.DefinedTags = in.DefinedTags
	}
}

// Message is one stored queue message.
type Message struct {
	Created      time.Time         `json:"created"`
	Expire       time.Time         `json:"expire"`
	VisibleAfter time.Time         `json:"visibleAfter"`
	Props        map[string]string `json:"props,omitempty"`
	Content      string            `json:"content"`
	Channel      string            `json:"channel,omitempty"`
	Receipt      string            `json:"receipt,omitempty"`
	ID           int64             `json:"id"`
	Deliveries   int               `json:"deliveries"`
}

// Queue is a message queue with its dead letter queue.
type Queue struct {
	TimeCreated   time.Time     `json:"timeCreated"`
	TimeUpdated   time.Time     `json:"timeUpdated"`
	TransitionAt  *time.Time    `json:"transitionAt,omitempty"`
	Tags          Tags          `json:"tags"`
	wake          chan struct{} // closed and replaced whenever a poller should re-check
	ID            string        `json:"id"`
	CompartmentID string        `json:"compartmentId"`
	Name          string        `json:"name"`
	State         string        `json:"state"`
	KMSKeyID      string        `json:"kmsKeyId,omitempty"`
	Etag          string        `json:"etag"`
	Messages      []*Message    `json:"messages,omitempty"`
	DLQ           []*Message    `json:"dlq,omitempty"`
	Retention     int           `json:"retentionInSeconds"`
	Visibility    int           `json:"visibilityInSeconds"`
	Timeout       int           `json:"timeoutInSeconds"`
	DLQCount      int           `json:"deadLetterQueueDeliveryCount"`
	ChannelLimit  int           `json:"channelConsumptionLimit"`
	NextMsgID     int64         `json:"nextMsgId"`
	Seq           int64         `json:"seq"`
}

// WorkRequest records an asynchronous admin operation.
type WorkRequest struct {
	Accepted      time.Time `json:"accepted"`
	FinishAt      time.Time `json:"finishAt"`
	ID            string    `json:"id"`
	Operation     string    `json:"operation"`
	CompartmentID string    `json:"compartmentId"`
	EntityType    string    `json:"entityType"`
	EntityID      string    `json:"entityId"`
	EntityURI     string    `json:"entityUri"`
	Action        string    `json:"action"`
	Seq           int64     `json:"seq"`
}
