package ociidentity

import "time"

// Lifecycle states shared by IAM resources.
const (
	stateCreating = "CREATING"
	stateActive   = "ACTIVE"
	stateDeleting = "DELETING"
	stateDeleted  = "DELETED"
)

// Tags carries the freeform and defined tags every taggable resource has.
type Tags struct {
	FreeformTags map[string]string         `json:"freeformTags"`
	DefinedTags  map[string]map[string]any `json:"definedTags"`
}

// Meta is the field set common to compartments, users, groups, dynamic groups and policies.
type Meta struct {
	TimeCreated time.Time `json:"timeCreated"`
	Tags
	ID             string `json:"id"`
	CompartmentID  string `json:"compartmentId"`
	Name           string `json:"name"`
	Description    string `json:"description"`
	LifecycleState string `json:"lifecycleState"`
}

// Internal is bookkeeping that never reaches the wire.
type Internal struct {
	TransitionAt *time.Time `json:"transitionAt,omitempty"`
	TimeDeleted  *time.Time `json:"timeDeleted,omitempty"`
	Etag         string     `json:"etag"`
	Seq          int64      `json:"seq"`
}

// Compartment is a stored compartment.
type Compartment struct {
	Meta
	Internal Internal `json:"internal"`
}

type compartmentWire struct {
	Meta
	IsAccessible bool `json:"isAccessible"`
}

// Capabilities are the per-user credential switches.
type Capabilities struct {
	CanUseConsolePassword         bool `json:"canUseConsolePassword"`
	CanUseAPIKeys                 bool `json:"canUseApiKeys"`
	CanUseAuthTokens              bool `json:"canUseAuthTokens"`
	CanUseSMTPCredentials         bool `json:"canUseSmtpCredentials"`
	CanUseCustomerSecretKeys      bool `json:"canUseCustomerSecretKeys"`
	CanUseOAuth2ClientCredentials bool `json:"canUseOAuth2ClientCredentials"`
	CanUseDBCredentials           bool `json:"canUseDbCredentials"`
}

// APIKey is an uploaded RSA public key.
type APIKey struct {
	TimeCreated time.Time `json:"timeCreated"`
	Fingerprint string    `json:"fingerprint"`
	PEM         string    `json:"pem"`
}

// Credential is an auth token, customer secret key or SMTP credential; the secret itself is never stored.
type Credential struct {
	TimeCreated time.Time `json:"timeCreated"`
	ID          string    `json:"id"`
	Text        string    `json:"text"`
	Username    string    `json:"username,omitempty"`
}

// User is a stored IAM user with its credentials.
type User struct {
	Meta
	UIPasswordCreated *time.Time   `json:"uiPasswordCreated,omitempty"`
	Email             string       `json:"email,omitempty"`
	DBUserName        string       `json:"dbUserName,omitempty"`
	Capabilities      Capabilities `json:"capabilities"`
	APIKeys           []APIKey     `json:"apiKeys"`
	AuthTokens        []Credential `json:"authTokens"`
	SecretKeys        []Credential `json:"secretKeys"`
	SMTP              []Credential `json:"smtp"`
	Internal          Internal     `json:"internal"`
}

type userWire struct {
	Meta
	Email          string       `json:"email,omitempty"`
	DBUserName     string       `json:"dbUserName,omitempty"`
	Capabilities   Capabilities `json:"capabilities"`
	EmailVerified  bool         `json:"emailVerified"`
	IsMfaActivated bool         `json:"isMfaActivated"`
}

// Group is a stored IAM group.
type Group struct {
	Meta
	Internal Internal `json:"internal"`
}

// Membership links a user to a group.
type Membership struct {
	TimeCreated   time.Time `json:"timeCreated"`
	ID            string    `json:"id"`
	CompartmentID string    `json:"compartmentId"`
	GroupID       string    `json:"groupId"`
	UserID        string    `json:"userId"`
	Seq           int64     `json:"seq"`
}

type membershipWire struct {
	TimeCreated    time.Time `json:"timeCreated"`
	ID             string    `json:"id"`
	CompartmentID  string    `json:"compartmentId"`
	GroupID        string    `json:"groupId"`
	UserID         string    `json:"userId"`
	LifecycleState string    `json:"lifecycleState"`
}

// DynamicGroup is a stored dynamic group.
type DynamicGroup struct {
	Meta
	MatchingRule string   `json:"matchingRule"`
	Internal     Internal `json:"internal"`
}

type dynamicGroupWire struct {
	Meta
	MatchingRule string `json:"matchingRule"`
}

// Policy is a stored IAM policy.
type Policy struct {
	Meta
	VersionDate string   `json:"versionDate,omitempty"`
	Statements  []string `json:"statements"`
	Internal    Internal `json:"internal"`
}

type policyWire struct {
	Meta
	VersionDate *string  `json:"versionDate"`
	Statements  []string `json:"statements"`
}

// TagNamespace is a stored tag namespace.
type TagNamespace struct {
	Meta
	Internal  Internal `json:"internal"`
	IsRetired bool     `json:"isRetired"`
}

type tagNamespaceWire struct {
	Meta
	IsRetired bool `json:"isRetired"`
}

// TagDef is a stored tag definition.
type TagDef struct {
	Meta
	NamespaceID   string         `json:"namespaceId"`
	NamespaceName string         `json:"namespaceName"`
	Validator     map[string]any `json:"validator,omitempty"`
	Internal      Internal       `json:"internal"`
	IsRetired     bool           `json:"isRetired"`
	IsCostTrack   bool           `json:"isCostTracking"`
}

type tagWire struct {
	Meta
	Validator        map[string]any `json:"validator"`
	TagNamespaceID   string         `json:"tagNamespaceId"`
	TagNamespaceName string         `json:"tagNamespaceName"`
	IsRetired        bool           `json:"isRetired"`
	IsCostTracking   bool           `json:"isCostTracking"`
}

// TagDefault is a stored tag default.
type TagDefault struct {
	TimeCreated   time.Time `json:"timeCreated"`
	ID            string    `json:"id"`
	CompartmentID string    `json:"compartmentId"`
	NamespaceID   string    `json:"namespaceId"`
	TagID         string    `json:"tagId"`
	TagName       string    `json:"tagName"`
	Value         string    `json:"value"`
	Seq           int64     `json:"seq"`
	IsRequired    bool      `json:"isRequired"`
}

type tagDefaultWire struct {
	TimeCreated       time.Time `json:"timeCreated"`
	ID                string    `json:"id"`
	CompartmentID     string    `json:"compartmentId"`
	TagNamespaceID    string    `json:"tagNamespaceId"`
	TagDefinitionID   string    `json:"tagDefinitionId"`
	TagDefinitionName string    `json:"tagDefinitionName"`
	Value             string    `json:"value"`
	LifecycleState    string    `json:"lifecycleState"`
	IsRequired        bool      `json:"isRequired"`
}

// WorkRequest tracks an asynchronous compartment or tag operation.
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

type wrResourceWire struct {
	EntityType string `json:"entityType"`
	ActionType string `json:"actionType"`
	Identifier string `json:"identifier"`
	EntityURI  string `json:"entityUri,omitempty"`
}

type workRequestWire struct {
	TimeAccepted    time.Time        `json:"timeAccepted"`
	TimeStarted     *time.Time       `json:"timeStarted,omitempty"`
	TimeFinished    *time.Time       `json:"timeFinished,omitempty"`
	OperationType   string           `json:"operationType"`
	Status          string           `json:"status"`
	ID              string           `json:"id"`
	CompartmentID   string           `json:"compartmentId"`
	Resources       []wrResourceWire `json:"resources"`
	Errors          []string         `json:"errors"`
	PercentComplete float32          `json:"percentComplete"`
}
