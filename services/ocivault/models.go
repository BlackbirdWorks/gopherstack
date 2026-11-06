package ocivault

import (
	"encoding/json"
	"time"
)

// Lifecycle states.
const (
	stateActive           = "ACTIVE"
	stateCreating         = "CREATING"
	stateEnabled          = "ENABLED"
	stateEnabling         = "ENABLING"
	stateDisabled         = "DISABLED"
	stateDisabling        = "DISABLING"
	statePendingDeletion  = "PENDING_DELETION"
	stateSchedulingDelete = "SCHEDULING_DELETION"
	stateCancellingDelete = "CANCELLING_DELETION"
	stateDeleted          = "DELETED"
	stateBackupInProgress = "BACKUP_IN_PROGRESS"
	stateRestoring        = "RESTORING"
)

const vaultTypePrivate = "VIRTUAL_PRIVATE"

// Vault is a KMS vault.
type Vault struct {
	transition
	TimeCreated        time.Time                 `json:"timeCreated"`
	TimeOfDeletion     *time.Time                `json:"timeOfDeletion,omitempty"`
	WrappingKeyCreated *time.Time                `json:"wrappingKeyCreated,omitempty"`
	FreeformTags       map[string]string         `json:"freeformTags,omitempty"`
	DefinedTags        map[string]map[string]any `json:"definedTags,omitempty"`
	ID                 string                    `json:"id"`
	Prefix             string                    `json:"prefix"`
	CompartmentID      string                    `json:"compartmentId"`
	DisplayName        string                    `json:"displayName"`
	VaultType          string                    `json:"vaultType"`
	State              string                    `json:"state"`
	WrappingKeyID      string                    `json:"wrappingKeyId"`
	RestoredFrom       string                    `json:"restoredFrom,omitempty"`
	ReplicationID      string                    `json:"replicationId,omitempty"`
	Replicas           []*VaultReplica           `json:"replicas,omitempty"`
	Etag               string                    `json:"etag"`
	WrappingKeyDER     []byte                    `json:"wrappingKeyDer,omitempty"`
	Seq                int64                     `json:"seq"`
}

// RotationDetails is a key's auto-rotation schedule.
type RotationDetails struct {
	ScheduleStart *time.Time `json:"timeOfScheduleStart,omitempty"`
	NextRotation  *time.Time `json:"timeOfNextRotation,omitempty"`
	LastRotation  *time.Time `json:"timeOfLastRotation,omitempty"`
	LastStatus    string     `json:"lastRotationStatus,omitempty"`
	LastMessage   string     `json:"lastRotationMessage,omitempty"`
	IntervalDays  int        `json:"rotationIntervalInDays,omitempty"`
}

// KeyVersion is one version of a key's material.
type KeyVersion struct {
	TimeCreated    time.Time  `json:"timeCreated"`
	TimeOfDeletion *time.Time `json:"timeOfDeletion,omitempty"`
	ID             string     `json:"id"`
	State          string     `json:"state"`
	Origin         string     `json:"origin"`
	Material       []byte     `json:"material"`
	AutoRotated    bool       `json:"autoRotated,omitempty"`
}

// Key is a master encryption key.
type Key struct {
	transition
	TimeCreated    time.Time                 `json:"timeCreated"`
	TimeOfDeletion *time.Time                `json:"timeOfDeletion,omitempty"`
	Rotation       *RotationDetails          `json:"rotation,omitempty"`
	FreeformTags   map[string]string         `json:"freeformTags,omitempty"`
	DefinedTags    map[string]map[string]any `json:"definedTags,omitempty"`
	ID             string                    `json:"id"`
	VaultID        string                    `json:"vaultId"`
	CompartmentID  string                    `json:"compartmentId"`
	DisplayName    string                    `json:"displayName"`
	State          string                    `json:"state"`
	ProtectionMode string                    `json:"protectionMode"`
	CurrentVersion string                    `json:"currentVersion"`
	PrevState      string                    `json:"prevState,omitempty"`
	RestoredFrom   string                    `json:"restoredFrom,omitempty"`
	ReplicationID  string                    `json:"replicationId,omitempty"`
	Etag           string                    `json:"etag"`
	Shape          KeyShape                  `json:"shape"`
	Versions       []*KeyVersion             `json:"versions"`
	Seq            int64                     `json:"seq"`
	AutoRotation   bool                      `json:"autoRotation"`
}

// SecretVersion is one stored (KMS-encrypted) version of a secret.
type SecretVersion struct {
	TimeCreated    time.Time  `json:"timeCreated"`
	TimeOfDeletion *time.Time `json:"timeOfDeletion,omitempty"`
	TimeOfExpiry   *time.Time `json:"timeOfExpiry,omitempty"`
	Name           string     `json:"name,omitempty"`
	Ciphertext     string     `json:"ciphertext"`
	Number         int64      `json:"number"`
}

// Secret is a vault secret.
type Secret struct {
	TimeCreated    time.Time                 `json:"timeCreated"`
	TimeOfDeletion *time.Time                `json:"timeOfDeletion,omitempty"`
	FreeformTags   map[string]string         `json:"freeformTags,omitempty"`
	DefinedTags    map[string]map[string]any `json:"definedTags,omitempty"`
	Metadata       map[string]any            `json:"metadata,omitempty"`
	GenContext     *GenerationContext        `json:"genContext,omitempty"`
	Rotation       *SecretRotation           `json:"rotation,omitempty"`
	transition
	KeyID         string            `json:"keyId"`
	VaultID       string            `json:"vaultId"`
	CompartmentID string            `json:"compartmentId"`
	Description   string            `json:"description,omitempty"`
	State         string            `json:"state"`
	Etag          string            `json:"etag"`
	ID            string            `json:"id"`
	Name          string            `json:"name"`
	Versions      []*SecretVersion  `json:"versions"`
	Rules         []json.RawMessage `json:"rules,omitempty"`
	Current       int64             `json:"current"`
	Previous      int64             `json:"previous"`
	Pending       int64             `json:"pending"`
	Seq           int64             `json:"seq"`
	AutoGen       bool              `json:"autoGen,omitempty"`
}
