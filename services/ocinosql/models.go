package ocinosql

import (
	"encoding/json/jsontext"
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
	Expire  *time.Time     `json:"expire,omitempty"`
	Version string         `json:"version"`
	Value   jsontext.Value `json:"value"`
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
	Replicas      []*Replica        `json:"replicas,omitempty"`
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
	SchemaFrozen  bool              `json:"schemaFrozen,omitempty"`
}

// Replica is a remote-region copy of a table; the emulator records its metadata only.
type Replica struct {
	TransitionAt *time.Time `json:"transitionAt,omitempty"`
	Region       string     `json:"region"`
	TableID      string     `json:"tableId"`
	Mode         string     `json:"mode"`
	State        string     `json:"state"`
	WriteUnits   int        `json:"writeUnits"`
}

// Configuration is the service-level configuration of a hosted environment.
type Configuration struct {
	KmsCreated   *time.Time `json:"kmsCreated,omitempty"`
	KmsUpdated   *time.Time `json:"kmsUpdated,omitempty"`
	TransitionAt *time.Time `json:"transitionAt,omitempty"`
	KmsKeyID     string     `json:"kmsKeyId,omitempty"`
	KmsVaultID   string     `json:"kmsVaultId,omitempty"`
	KmsState     string     `json:"kmsState"`
	Etag         string     `json:"etag"`
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
	EntityType    string    `json:"entityType,omitempty"`
	Seq           int64     `json:"seq"`
	Canceled      bool      `json:"canceled,omitempty"`
}
