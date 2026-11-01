package ocinotifications

import "time"

// Topic lifecycle states.
const (
	stateActive   = "ACTIVE"
	stateCreating = "CREATING"
	stateDeleting = "DELETING"
)

// Subscription lifecycle states.
const (
	subPending = "PENDING"
	subActive  = "ACTIVE"
)

// Delivery states.
const (
	deliveryPending   = "PENDING"
	deliveryDelivered = "DELIVERED"
	deliveryDiscarded = "DISCARDED"
	deliveryRecorded  = "RECORDED"
)

// Delivery kinds.
const (
	kindMessage      = "MESSAGE"
	kindConfirmation = "CONFIRMATION"
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

// Topic is a notification topic.
type Topic struct {
	TimeCreated   time.Time  `json:"timeCreated"`
	TransitionAt  *time.Time `json:"transitionAt,omitempty"`
	Tags          Tags       `json:"tags"`
	ID            string     `json:"id"`
	ShortID       string     `json:"shortId"`
	CompartmentID string     `json:"compartmentId"`
	Name          string     `json:"name"`
	Description   string     `json:"description"`
	State         string     `json:"state"`
	Etag          string     `json:"etag"`
	Seq           int64      `json:"seq"`
}

// Subscription is one endpoint subscribed to a topic.
type Subscription struct {
	TimeCreated   time.Time `json:"timeCreated"`
	Tags          Tags      `json:"tags"`
	ID            string    `json:"id"`
	TopicID       string    `json:"topicId"`
	CompartmentID string    `json:"compartmentId"`
	Protocol      string    `json:"protocol"`
	Endpoint      string    `json:"endpoint"`
	State         string    `json:"state"`
	Metadata      string    `json:"metadata,omitempty"`
	BaseURL       string    `json:"baseUrl"`
	ConfirmToken  string    `json:"confirmToken"`
	UnsubToken    string    `json:"unsubToken"`
	Etag          string    `json:"etag"`
	MaxRetryMs    int       `json:"maxRetryMs"`
	Seq           int64     `json:"seq"`
}

// Delivery records one message or confirmation handed to a subscription endpoint.
type Delivery struct {
	Published       time.Time `json:"published"`
	NextAttempt     time.Time `json:"nextAttempt"`
	Deadline        time.Time `json:"deadline"`
	Completed       time.Time `json:"completed"`
	SubscriptionID  string    `json:"subscriptionId"`
	Title           string    `json:"title,omitempty"`
	Kind            string    `json:"kind"`
	MessageID       string    `json:"messageId"`
	TopicID         string    `json:"topicId"`
	TopicName       string    `json:"topicName"`
	LastError       string    `json:"lastError,omitempty"`
	Protocol        string    `json:"protocol"`
	Endpoint        string    `json:"endpoint"`
	ID              string    `json:"id"`
	Body            string    `json:"body"`
	MessageType     string    `json:"messageType"`
	ConfirmationURL string    `json:"confirmationUrl,omitempty"`
	UnsubscribeURL  string    `json:"unsubscribeUrl,omitempty"`
	Status          string    `json:"status"`
	Attempts        int       `json:"attempts"`
	LastStatus      int       `json:"lastStatus,omitempty"`
	Seq             int64     `json:"seq"`
	inflight        bool
}
