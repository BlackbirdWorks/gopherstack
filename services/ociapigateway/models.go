package ociapigateway

import (
	"encoding/json"
	"time"
)

// Lifecycle states.
const (
	stateActive   = "ACTIVE"
	stateCreating = "CREATING"
	stateUpdating = "UPDATING"
	stateDeleting = "DELETING"
	stateDeleted  = "DELETED"
)

// Endpoint types.
const (
	endpointPublic  = "PUBLIC"
	endpointPrivate = "PRIVATE"
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

// Gateway is an API gateway.
type Gateway struct {
	TimeCreated   time.Time       `json:"timeCreated"`
	TimeUpdated   time.Time       `json:"timeUpdated"`
	Tags          Tags            `json:"tags"`
	TransitionAt  *time.Time      `json:"transitionAt,omitempty"`
	Locks         json.RawMessage `json:"locks,omitempty"`
	CaBundles     json.RawMessage `json:"caBundles,omitempty"`
	ResponseCache json.RawMessage `json:"responseCache,omitempty"`
	ID            string          `json:"id"`
	CompartmentID string          `json:"compartmentId"`
	EndpointType  string          `json:"endpointType"`
	DisplayName   string          `json:"displayName"`
	SubnetID      string          `json:"subnetId"`
	Hostname      string          `json:"hostname"`
	CertificateID string          `json:"certificateId,omitempty"`
	IPMode        string          `json:"ipMode,omitempty"`
	State         string          `json:"state"`
	Etag          string          `json:"etag"`
	NSGs          []string        `json:"networkSecurityGroupIds,omitempty"`
}

// Deployment is an API deployment on a gateway.
type Deployment struct {
	TimeCreated   time.Time       `json:"timeCreated"`
	TimeUpdated   time.Time       `json:"timeUpdated"`
	Tags          Tags            `json:"tags"`
	TransitionAt  *time.Time      `json:"transitionAt,omitempty"`
	ID            string          `json:"id"`
	GatewayID     string          `json:"gatewayId"`
	CompartmentID string          `json:"compartmentId"`
	PathPrefix    string          `json:"pathPrefix"`
	DisplayName   string          `json:"displayName"`
	State         string          `json:"state"`
	Etag          string          `json:"etag"`
	Locks         json.RawMessage `json:"locks,omitempty"`
	Spec          json.RawMessage `json:"specification"`
}

// Certificate is a TLS certificate usable by a gateway.
type Certificate struct {
	TimeCreated       time.Time       `json:"timeCreated"`
	TimeUpdated       time.Time       `json:"timeUpdated"`
	TimeNotValidAfter time.Time       `json:"timeNotValidAfter"`
	Tags              Tags            `json:"tags"`
	TransitionAt      *time.Time      `json:"transitionAt,omitempty"`
	Locks             json.RawMessage `json:"locks,omitempty"`
	ID                string          `json:"id"`
	CompartmentID     string          `json:"compartmentId"`
	DisplayName       string          `json:"displayName"`
	CertPEM           string          `json:"certificate"`
	Intermediate      string          `json:"intermediateCertificates,omitempty"`
	State             string          `json:"state"`
	Etag              string          `json:"etag"`
	SubjectNames      []string        `json:"subjectNames"`
}

// WorkRequest records an asynchronous operation.
type WorkRequest struct {
	Accepted      time.Time `json:"accepted"`
	FinishAt      time.Time `json:"finishAt"`
	ID            string    `json:"id"`
	Operation     string    `json:"operation"`
	CompartmentID string    `json:"compartmentId"`
	EntityType    string    `json:"entityType"`
	EntityID      string    `json:"entityId"`
	Action        string    `json:"action"`
	Seq           int64     `json:"seq"`
}
