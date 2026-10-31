package ocilogging

import (
	"encoding/json"
	"time"
)

// Lifecycle states shared by log groups, logs and agent configurations.
const (
	stateActive   = "ACTIVE"
	stateCreating = "CREATING"
	stateUpdating = "UPDATING"
	stateDeleting = "DELETING"
	stateDeleted  = "DELETED"
	stateInactive = "INACTIVE"
)

// Log types.
const (
	logTypeCustom  = "CUSTOM"
	logTypeService = "SERVICE"
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

// LogGroup is a container of logs.
type LogGroup struct {
	TimeCreated   time.Time  `json:"timeCreated"`
	TimeModified  time.Time  `json:"timeModified"`
	TransitionAt  *time.Time `json:"transitionAt,omitempty"`
	Tags          Tags       `json:"tags"`
	ID            string     `json:"id"`
	CompartmentID string     `json:"compartmentId"`
	Name          string     `json:"displayName"`
	Description   string     `json:"description,omitempty"`
	State         string     `json:"state"`
	Etag          string     `json:"etag"`
	Seq           int64      `json:"seq"`
}

// ServiceSource is the OCISERVICE source of a SERVICE log.
type ServiceSource struct {
	Parameters map[string]string `json:"parameters,omitempty"`
	Service    string            `json:"service"`
	Resource   string            `json:"resource"`
	Category   string            `json:"category"`
}

// LogConfig is the stored log configuration.
type LogConfig struct {
	Source        *ServiceSource `json:"source,omitempty"`
	Archiving     *bool          `json:"archiving,omitempty"`
	CompartmentID string         `json:"compartmentId,omitempty"`
}

// Log is a custom or service log inside a log group.
type Log struct {
	TimeCreated  time.Time  `json:"timeCreated"`
	TimeModified time.Time  `json:"timeModified"`
	TransitionAt *time.Time `json:"transitionAt,omitempty"`
	Config       *LogConfig `json:"configuration,omitempty"`
	Tags         Tags       `json:"tags"`
	ID           string     `json:"id"`
	GroupID      string     `json:"logGroupId"`
	Name         string     `json:"displayName"`
	LogType      string     `json:"logType"`
	State        string     `json:"state"`
	Etag         string     `json:"etag"`
	Retention    int        `json:"retentionDuration"`
	Seq          int64      `json:"seq"`
	Enabled      bool       `json:"isEnabled"`
}

// AgentConfig is a stored unified agent configuration.
type AgentConfig struct {
	TimeCreated   time.Time       `json:"timeCreated"`
	TimeModified  time.Time       `json:"timeModified"`
	TransitionAt  *time.Time      `json:"transitionAt,omitempty"`
	ServiceConfig json.RawMessage `json:"serviceConfiguration"`
	Tags          Tags            `json:"tags"`
	ID            string          `json:"id"`
	CompartmentID string          `json:"compartmentId"`
	Name          string          `json:"displayName"`
	Description   string          `json:"description,omitempty"`
	State         string          `json:"state"`
	ConfigType    string          `json:"configurationType"`
	Etag          string          `json:"etag"`
	Groups        []string        `json:"groupList,omitempty"`
	Seq           int64           `json:"seq"`
	Enabled       bool            `json:"isEnabled"`
}

// Entry is one ingested log record.
type Entry struct {
	Time     time.Time `json:"time"`
	Ingested time.Time `json:"ingested"`
	ID       string    `json:"id"`
	Data     string    `json:"data"`
	Source   string    `json:"source"`
	Type     string    `json:"type"`
	Subject  string    `json:"subject,omitempty"`
	Seq      int64     `json:"seq"`
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
