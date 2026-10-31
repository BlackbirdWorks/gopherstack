package ocimysql

import (
	"encoding/json"
	"time"
)

// Lifecycle states.
const (
	stateCreating = "CREATING"
	stateActive   = "ACTIVE"
	stateInactive = "INACTIVE"
	stateUpdating = "UPDATING"
	stateDeleting = "DELETING"
	stateDeleted  = "DELETED"
	stateFailed   = "FAILED"
)

// Configuration types.
const (
	configDefault = "DEFAULT"
	configCustom  = "CUSTOM"
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

// BackupPolicy is the automatic backup policy of a DB system.
type BackupPolicy struct {
	Tags            Tags            `json:"tags"`
	WindowStartTime string          `json:"windowStartTime"`
	SoftDelete      string          `json:"softDelete"`
	CopyPolicies    json.RawMessage `json:"copyPolicies,omitempty"`
	RetentionInDays int             `json:"retentionInDays"`
	IsEnabled       bool            `json:"isEnabled"`
	PitrEnabled     bool            `json:"pitrEnabled"`
}

// DeletionPolicy controls what DeleteDBSystem does.
type DeletionPolicy struct {
	AutomaticBackupRetention string `json:"automaticBackupRetention"`
	FinalBackup              string `json:"finalBackup"`
	IsDeleteProtected        bool   `json:"isDeleteProtected"`
}

// Source says where a DB system's data came from.
type Source struct {
	SourceType string `json:"sourceType"`
	BackupID   string `json:"backupId,omitempty"`
}

// DBSystem is a MySQL DB system. AdminPassword and RootSecret are never returned by the API.
type DBSystem struct {
	TimeCreated    time.Time                  `json:"timeCreated"`
	TimeUpdated    time.Time                  `json:"timeUpdated"`
	LastAutoBackup time.Time                  `json:"lastAutoBackup"`
	Tags           Tags                       `json:"tags"`
	TransitionAt   *time.Time                 `json:"transitionAt,omitempty"`
	Source         *Source                    `json:"source,omitempty"`
	Extra          map[string]json.RawMessage `json:"extra,omitempty"`
	Maintenance    map[string]any             `json:"maintenance,omitempty"`
	FD             string                     `json:"faultDomain"`
	AdminUser      string                     `json:"adminUsername"`
	ID             string                     `json:"id"`
	CompartmentID  string                     `json:"compartmentId"`
	Name           string                     `json:"displayName"`
	Description    string                     `json:"description,omitempty"`
	SubnetID       string                     `json:"subnetId"`
	Shape          string                     `json:"shapeName"`
	Version        string                     `json:"mysqlVersion"`
	ConfigID       string                     `json:"configurationId"`
	Hostname       string                     `json:"hostnameLabel,omitempty"`
	AD             string                     `json:"availabilityDomain"`
	EngineHost     string                     `json:"engineHost,omitempty"`
	State          string                     `json:"state"`
	After          string                     `json:"after,omitempty"`
	Details        string                     `json:"details,omitempty"`
	Etag           string                     `json:"etag"`
	IP             string                     `json:"ipAddress"`
	AdminPass      string                     `json:"adminPassword,omitempty"`
	RootSecret     string                     `json:"rootSecret,omitempty"`
	DatabaseMode   string                     `json:"databaseMode"`
	AccessMode     string                     `json:"accessMode"`
	CrashRecovery  string                     `json:"crashRecovery"`
	DBManagement   string                     `json:"databaseManagement"`
	Deletion       DeletionPolicy             `json:"deletionPolicy"`
	BackupPolicy   BackupPolicy               `json:"backupPolicy"`
	DataGB         int                        `json:"dataStorageSizeInGBs"`
	MaxGB          int                        `json:"maxStorageSizeInGBs,omitempty"`
	Port           int                        `json:"port"`
	PortX          int                        `json:"portX"`
	EnginePort     int                        `json:"enginePort,omitempty"`
	Seq            int64                      `json:"seq"`
	AutoExpand     bool                       `json:"isAutoExpandStorageEnabled"`
	HA             bool                       `json:"isHighlyAvailable"`
}

// Backup is a DB system backup. Dump holds the logical dump when the docker engine took it.
type Backup struct {
	TimeCreated   time.Time  `json:"timeCreated"`
	TimeUpdated   time.Time  `json:"timeUpdated"`
	TransitionAt  *time.Time `json:"transitionAt,omitempty"`
	Snapshot      *DBSystem  `json:"snapshot,omitempty"`
	Tags          Tags       `json:"tags"`
	ID            string     `json:"id"`
	CompartmentID string     `json:"compartmentId"`
	DBSystemID    string     `json:"dbSystemId"`
	Name          string     `json:"displayName"`
	Description   string     `json:"description,omitempty"`
	State         string     `json:"state"`
	Details       string     `json:"details,omitempty"`
	BackupType    string     `json:"backupType"`
	CreationType  string     `json:"creationType"`
	SoftDelete    string     `json:"softDelete"`
	Etag          string     `json:"etag"`
	Dump          []byte     `json:"dump,omitempty"`
	Retention     int        `json:"retentionInDays"`
	SizeGB        int        `json:"backupSizeInGBs"`
	Seq           int64      `json:"seq"`
}

// ChannelSource is the replication source of a channel; the password is never stored.
type ChannelSource struct {
	Extra       map[string]json.RawMessage `json:"extra,omitempty"`
	SourceType  string                     `json:"sourceType"`
	Hostname    string                     `json:"hostname"`
	Username    string                     `json:"username"`
	SslMode     string                     `json:"sslMode"`
	Port        int                        `json:"port"`
	HasPassword bool                       `json:"hasPassword"`
}

// ChannelTarget is the DB system a channel replicates into.
type ChannelTarget struct {
	TargetType                      string          `json:"targetType"`
	DBSystemID                      string          `json:"dbSystemId"`
	ChannelName                     string          `json:"channelName"`
	ApplierUsername                 string          `json:"applierUsername"`
	TablesWithoutPrimaryKeyHandling string          `json:"tablesWithoutPrimaryKeyHandling"`
	Filters                         json.RawMessage `json:"filters,omitempty"`
	DelayInSeconds                  int             `json:"delayInSeconds"`
}

// Channel is a replication channel. It records configuration and lifecycle only; no data is replicated.
type Channel struct {
	TimeCreated   time.Time     `json:"timeCreated"`
	TimeUpdated   time.Time     `json:"timeUpdated"`
	Tags          Tags          `json:"tags"`
	TransitionAt  *time.Time    `json:"transitionAt,omitempty"`
	Details       string        `json:"details,omitempty"`
	ID            string        `json:"id"`
	CompartmentID string        `json:"compartmentId"`
	Name          string        `json:"displayName"`
	Description   string        `json:"description,omitempty"`
	State         string        `json:"state"`
	After         string        `json:"after,omitempty"`
	Etag          string        `json:"etag"`
	Target        ChannelTarget `json:"target"`
	Source        ChannelSource `json:"source"`
	Seq           int64         `json:"seq"`
	Enabled       bool          `json:"isEnabled"`
}

// Configuration is a DEFAULT or CUSTOM set of MySQL variables for a shape.
type Configuration struct {
	TimeCreated   time.Time      `json:"timeCreated"`
	TimeUpdated   time.Time      `json:"timeUpdated"`
	Variables     map[string]any `json:"variables"`
	Tags          Tags           `json:"tags"`
	ID            string         `json:"id"`
	CompartmentID string         `json:"compartmentId"`
	Shape         string         `json:"shapeName"`
	Type          string         `json:"type"`
	State         string         `json:"state"`
	Name          string         `json:"displayName"`
	Description   string         `json:"description,omitempty"`
	ParentID      string         `json:"parentConfigurationId,omitempty"`
	LowerCase     string         `json:"lowerCaseTableNames,omitempty"`
	Etag          string         `json:"etag"`
	Seq           int64          `json:"seq"`
}

// WorkRequest records an asynchronous operation.
type WorkRequest struct {
	Accepted      time.Time  `json:"accepted"`
	FinishAt      *time.Time `json:"finishAt,omitempty"`
	ID            string     `json:"id"`
	Operation     string     `json:"operation"`
	CompartmentID string     `json:"compartmentId"`
	EntityType    string     `json:"entityType"`
	EntityID      string     `json:"entityId"`
	EntityURI     string     `json:"entityUri"`
	Action        string     `json:"action"`
	Failure       string     `json:"failure,omitempty"`
	Seq           int64      `json:"seq"`
}
