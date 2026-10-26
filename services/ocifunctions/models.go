package ocifunctions

import (
	"encoding/json"
	"maps"
	"time"
)

// Lifecycle states.
const (
	stateActive   = "ACTIVE"
	stateCreating = "CREATING"
	stateDeleting = "DELETING"
	stateDeleted  = "DELETED"
)

// Tags are the shared tag sets of a resource.
type Tags struct {
	FreeformTags       map[string]string         `json:"freeformTags,omitempty"`
	DefinedTags        map[string]map[string]any `json:"definedTags,omitempty"`
	SecurityAttributes map[string]map[string]any `json:"securityAttributes,omitempty"`
}

// replace overwrites each tag set the update supplied (OCI tag updates replace, they do not merge).
func (t *Tags) replace(in Tags) {
	if in.FreeformTags != nil {
		t.FreeformTags = maps.Clone(in.FreeformTags)
	}

	if in.DefinedTags != nil {
		t.DefinedTags = maps.Clone(in.DefinedTags)
	}

	if in.SecurityAttributes != nil {
		t.SecurityAttributes = maps.Clone(in.SecurityAttributes)
	}
}

// TraceConfig is the tracing configuration of an application or function.
type TraceConfig struct {
	IsEnabled *bool   `json:"isEnabled,omitempty"`
	DomainID  *string `json:"domainId,omitempty"`
}

// LoggingConfig is the application logging configuration.
type LoggingConfig struct {
	LineFormat string `json:"lineFormat,omitempty"`
}

// KeyDetail references a KMS key trusted by the image policy.
type KeyDetail struct {
	KmsKeyID string `json:"kmsKeyId"`
}

// ImagePolicy is the image signature verification policy of an application.
type ImagePolicy struct {
	IsPolicyEnabled *bool       `json:"isPolicyEnabled"`
	KeyDetails      []KeyDetail `json:"keyDetails,omitempty"`
}

// Concurrency is a function's provisioned concurrency configuration.
type Concurrency struct {
	Count    *int   `json:"count,omitempty"`
	Strategy string `json:"strategy"`
}

// Application is a group of functions sharing network and configuration.
type Application struct {
	TimeCreated   time.Time         `json:"timeCreated"`
	TimeUpdated   time.Time         `json:"timeUpdated"`
	TransitionAt  *time.Time        `json:"transitionAt,omitempty"`
	TraceConfig   *TraceConfig      `json:"traceConfig,omitempty"`
	Logging       *LoggingConfig    `json:"logging,omitempty"`
	ImagePolicy   *ImagePolicy      `json:"imagePolicy,omitempty"`
	Config        map[string]string `json:"config,omitempty"`
	Tags          Tags              `json:"tags"`
	ID            string            `json:"id"`
	CompartmentID string            `json:"compartmentId"`
	Name          string            `json:"name"`
	State         string            `json:"state"`
	Shape         string            `json:"shape"`
	SyslogURL     string            `json:"syslogUrl,omitempty"`
	Etag          string            `json:"etag"`
	SubnetIDs     []string          `json:"subnetIds"`
	NSGIDs        []string          `json:"networkSecurityGroupIds,omitempty"`
	Seq           int64             `json:"seq"`
}

// Function is a container-image function of an application.
type Function struct {
	Tags          Tags              `json:"tags"`
	TimeUpdated   time.Time         `json:"timeUpdated"`
	TimeCreated   time.Time         `json:"timeCreated"`
	TransitionAt  *time.Time        `json:"transitionAt,omitempty"`
	TraceConfig   *TraceConfig      `json:"traceConfig,omitempty"`
	Detached      *int              `json:"detachedModeTimeoutInSeconds,omitempty"`
	Config        map[string]string `json:"config,omitempty"`
	Provisioned   Concurrency       `json:"provisionedConcurrencyConfig"`
	ID            string            `json:"id"`
	AppID         string            `json:"applicationId"`
	CompartmentID string            `json:"compartmentId"`
	Name          string            `json:"name"`
	State         string            `json:"state"`
	Image         string            `json:"image"`
	ImageDigest   string            `json:"imageDigest,omitempty"`
	Etag          string            `json:"etag"`
	Success       json.RawMessage   `json:"successDestination,omitempty"`
	Failure       json.RawMessage   `json:"failureDestination,omitempty"`
	Memory        int64             `json:"memoryInMBs"`
	Seq           int64             `json:"seq"`
	Timeout       int               `json:"timeoutInSeconds"`
}

// WorkRequest records an asynchronous operation that completes at FinishAt.
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
