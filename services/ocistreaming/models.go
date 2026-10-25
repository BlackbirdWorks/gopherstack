package ocistreaming

import "time"

// Lifecycle states.
const (
	stateActive   = "ACTIVE"
	stateCreating = "CREATING"
	stateDeleting = "DELETING"
	stateDeleted  = "DELETED"
)

// Tags are the shared freeform and defined tag sets of a resource.
type Tags struct {
	FreeformTags       map[string]string         `json:"freeformTags,omitempty"`
	DefinedTags        map[string]map[string]any `json:"definedTags,omitempty"`
	SecurityAttributes map[string]map[string]any `json:"securityAttributes,omitempty"`
}

// KafkaSettings are the Kafka-compatible settings of a stream pool.
type KafkaSettings struct {
	BootstrapServers       *string `json:"bootstrapServers,omitempty"`
	AutoCreateTopicsEnable *bool   `json:"autoCreateTopicsEnable,omitempty"`
	LogRetentionHours      *int    `json:"logRetentionHours,omitempty"`
	NumPartitions          *int    `json:"numPartitions,omitempty"`
}

// PrivateEndpoint holds the private endpoint settings of a stream pool.
type PrivateEndpoint struct {
	SubnetID          *string  `json:"subnetId,omitempty"`
	PrivateEndpointIP *string  `json:"privateEndpointIp,omitempty"`
	NsgIDs            []string `json:"nsgIds,omitempty"`
}

// Pool is a stream pool.
type Pool struct {
	TimeCreated   time.Time        `json:"timeCreated"`
	TransitionAt  *time.Time       `json:"transitionAt,omitempty"`
	Endpoint      *PrivateEndpoint `json:"privateEndpoint,omitempty"`
	Tags          Tags             `json:"tags"`
	Kafka         KafkaSettings    `json:"kafka"`
	ID            string           `json:"id"`
	CompartmentID string           `json:"compartmentId"`
	Name          string           `json:"name"`
	State         string           `json:"state"`
	KMSKeyID      string           `json:"kmsKeyId,omitempty"`
	Etag          string           `json:"etag"`
	Seq           int64            `json:"seq"`
	Private       bool             `json:"private,omitempty"`
	IsDefault     bool             `json:"isDefault,omitempty"`
}

// Message is one stored record; its offset is the partition base plus its index.
type Message struct {
	Time  time.Time `json:"time"`
	Key   []byte    `json:"key,omitempty"`
	Value []byte    `json:"value"`
}

// Partition is one ordered log of a stream.
type Partition struct {
	Messages    []Message `json:"messages,omitempty"`
	Base        int64     `json:"base"`
	WindowSec   int64     `json:"windowSec"`
	WindowBytes int       `json:"windowBytes"`
}

// Stream is a partitioned message log.
type Stream struct {
	TimeCreated   time.Time    `json:"timeCreated"`
	TransitionAt  *time.Time   `json:"transitionAt,omitempty"`
	Tags          Tags         `json:"tags"`
	ID            string       `json:"id"`
	PoolID        string       `json:"poolId"`
	CompartmentID string       `json:"compartmentId"`
	Name          string       `json:"name"`
	State         string       `json:"state"`
	Etag          string       `json:"etag"`
	Parts         []*Partition `json:"partitions"`
	Retention     int          `json:"retentionInHours"`
	Seq           int64        `json:"seq"`
	RoundRobin    int          `json:"roundRobin"`
}

// Reservation is one partition's consumer-group reservation.
type Reservation struct {
	Until    time.Time `json:"until"`
	Instance string    `json:"instance"`
}

// Group is a consumer group of a stream.
type Group struct {
	Instances map[string]time.Time `json:"instances"`
	Rotation  map[string]int       `json:"rotation"`
	StreamID  string               `json:"streamId"`
	Name      string               `json:"name"`
	Next      []int64              `json:"next"`
	Reserved  []Reservation        `json:"reserved"`
}

// Harness is a Kafka Connect harness.
type Harness struct {
	TimeCreated   time.Time  `json:"timeCreated"`
	TransitionAt  *time.Time `json:"transitionAt,omitempty"`
	Tags          Tags       `json:"tags"`
	ID            string     `json:"id"`
	CompartmentID string     `json:"compartmentId"`
	Name          string     `json:"name"`
	State         string     `json:"state"`
	Etag          string     `json:"etag"`
	Seq           int64      `json:"seq"`
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
