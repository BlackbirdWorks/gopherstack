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
)

// Vault is a KMS vault.
type Vault struct {
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
	ID             string                    `json:"id"`
	VaultID        string                    `json:"vaultId"`
	CompartmentID  string                    `json:"compartmentId"`
	Name           string                    `json:"name"`
	KeyID          string                    `json:"keyId"`
	Description    string                    `json:"description,omitempty"`
	State          string                    `json:"state"`
	Etag           string                    `json:"etag"`
	Rules          []json.RawMessage         `json:"rules,omitempty"`
	Versions       []*SecretVersion          `json:"versions"`
	Seq            int64                     `json:"seq"`
	Current        int64                     `json:"current"`
	Previous       int64                     `json:"previous"`
	Pending        int64                     `json:"pending"`
}
