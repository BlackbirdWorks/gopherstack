package ocinosql

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

// Capacity modes.
const (
	modeProvisioned = "PROVISIONED"
	modeOnDemand    = "ON_DEMAND"
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

// Limits are a table's throughput and storage limits.
type Limits struct {
	Mode       string `json:"mode"`
	ReadUnits  int    `json:"readUnits"`
	WriteUnits int    `json:"writeUnits"`
	StorageGB  int    `json:"storageGb"`
}

// Row is one stored table row.
type Row struct {
	Expire  *time.Time      `json:"expire,omitempty"`
	Version string          `json:"version"`
	Value   json.RawMessage `json:"value"`
}

// IndexKey is one key of a secondary index.
type IndexKey struct {
	Column   string `json:"column"`
	JSONPath string `json:"jsonPath,omitempty"`
	JSONType string `json:"jsonType,omitempty"`
}

// Index is a secondary index on a table.
type Index struct {
	Created      time.Time  `json:"created"`
	TransitionAt *time.Time `json:"transitionAt,omitempty"`
	Name         string     `json:"name"`
	State        string     `json:"state"`
	Keys         []IndexKey `json:"keys"`
}

// Usage is the consumption recorded for one minute.
type Usage struct {
	Reads  int `json:"reads"`
	Writes int `json:"writes"`
}

// Table is a NoSQL table with its rows and indexes.
type Table struct {
	TimeCreated   time.Time         `json:"timeCreated"`
	TimeUpdated   time.Time         `json:"timeUpdated"`
	Tags          Tags              `json:"tags"`
	TransitionAt  *time.Time        `json:"transitionAt,omitempty"`
	Rows          map[string]*Row   `json:"rows,omitempty"`
	Indexes       map[string]*Index `json:"indexes,omitempty"`
	Usage         map[int64]*Usage  `json:"usage,omitempty"`
	Identity      *Identity         `json:"identity,omitempty"`
	CompartmentID string            `json:"compartmentId"`
	ID            string            `json:"id"`
	Name          string            `json:"name"`
	State         string            `json:"state"`
	DDL           string            `json:"ddl"`
	Etag          string            `json:"etag"`
	Columns       []Column          `json:"columns"`
	PrimaryKey    []string          `json:"primaryKey"`
	ShardKey      []string          `json:"shardKey"`
	Limits        Limits            `json:"limits"`
	TTLHours      int               `json:"ttlHours"`
	RowSeq        int64             `json:"rowSeq"`
	AutoReclaim   bool              `json:"autoReclaimable,omitempty"`
}

// WorkRequest records an asynchronous admin operation.
type WorkRequest struct {
	Accepted      time.Time `json:"accepted"`
	FinishAt      time.Time `json:"finishAt"`
	ID            string    `json:"id"`
	Operation     string    `json:"operation"`
	CompartmentID string    `json:"compartmentId"`
	EntityID      string    `json:"entityId"`
	Action        string    `json:"action"`
	Seq           int64     `json:"seq"`
}
