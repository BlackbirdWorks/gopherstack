package redshift

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"time"
)

// ---------------------------------------------------------------------------
// Sentinel errors — Redshift Serverless
// ---------------------------------------------------------------------------

var (
	// ErrNamespaceNotFound is returned when a serverless namespace does not exist.
	ErrNamespaceNotFound = errors.New("ResourceNotFoundException")
	// ErrNamespaceAlreadyExists is returned when a serverless namespace already exists.
	ErrNamespaceAlreadyExists = errors.New("ConflictException")
	// ErrWorkgroupNotFound is returned when a serverless workgroup does not exist.
	ErrWorkgroupNotFound = errors.New("ResourceNotFoundException")
	// ErrWorkgroupAlreadyExists is returned when a serverless workgroup already exists.
	ErrWorkgroupAlreadyExists = errors.New("ConflictException")
	// ErrServerlessSnapshotNotFound is returned when a serverless snapshot does not exist.
	ErrServerlessSnapshotNotFound = errors.New("ResourceNotFoundException")
	// ErrServerlessConflict is returned when a serverless resource already exists.
	ErrServerlessConflict = errors.New("ConflictException")
	// ErrUsageLimitSLNotFound is returned when a serverless usage limit does not exist.
	ErrUsageLimitSLNotFound = errors.New("ResourceNotFoundException")
	// ErrScheduledActionSLNotFound is returned when a serverless scheduled action does not exist.
	ErrScheduledActionSLNotFound = errors.New("ResourceNotFoundException")
	// ErrServerlessValidation is returned when a request is missing a required field.
	ErrServerlessValidation = errors.New("ValidationException")
)

// ---------------------------------------------------------------------------
// Serverless models
//
// Field names/JSON keys verified against aws-sdk-go-v2/service/redshiftserverless
// @v1.38.5 types.Namespace/Workgroup/Snapshot/UsageLimit/ScheduledActionResponse
// (awsAwsjson11_deserializeDocument* in deserializers.go).
// ---------------------------------------------------------------------------

// Namespace represents a Redshift Serverless namespace.
type Namespace struct {
	CreationDate                time.Time `json:"creationDate"`
	NamespaceArn                string    `json:"namespaceArn"`
	NamespaceID                 string    `json:"namespaceId"`
	NamespaceName               string    `json:"namespaceName"`
	AdminUsername               string    `json:"adminUsername,omitempty"`
	DBName                      string    `json:"dbName,omitempty"`
	DefaultIamRoleArn           string    `json:"defaultIamRoleArn,omitempty"`
	KmsKeyID                    string    `json:"kmsKeyId,omitempty"`
	AdminPasswordSecretArn      string    `json:"adminPasswordSecretArn,omitempty"`
	AdminPasswordSecretKmsKeyID string    `json:"adminPasswordSecretKmsKeyId,omitempty"`
	Status                      string    `json:"status"`
	IamRoles                    []string  `json:"iamRoles,omitempty"`
	LogExports                  []string  `json:"logExports,omitempty"`
}

// CreateNamespaceParams holds the mutable fields accepted by CreateNamespace.
// Grouped into a struct because the real CreateNamespaceInput carries more
// fields than fit a readable positional parameter list.
type CreateNamespaceParams struct {
	NamespaceName               string
	AdminUsername               string
	DBName                      string
	KmsKeyID                    string
	DefaultIamRoleArn           string
	AdminPasswordSecretKmsKeyID string
	IamRoles                    []string
	LogExports                  []string
	ManageAdminPassword         bool
}

// UpdateNamespaceParams holds the mutable fields accepted by UpdateNamespace.
type UpdateNamespaceParams struct {
	AdminUsername               string
	DBName                      string
	KmsKeyID                    string
	DefaultIamRoleArn           string
	AdminPasswordSecretKmsKeyID string
	ManageAdminPassword         *bool
	IamRoles                    []string
	LogExports                  []string
}

// Workgroup represents a Redshift Serverless workgroup.
type Workgroup struct {
	CreationDate                         time.Time          `json:"creationDate"`
	PricePerformanceTarget               *PerformanceTarget `json:"pricePerformanceTarget,omitempty"`
	IPAddressType                        string             `json:"ipAddressType,omitempty"`
	WorkgroupArn                         string             `json:"workgroupArn"`
	WorkgroupID                          string             `json:"workgroupId"`
	WorkgroupName                        string             `json:"workgroupName"`
	NamespaceName                        string             `json:"namespaceName"`
	Status                               string             `json:"status"`
	TrackName                            string             `json:"trackName,omitempty"`
	SecurityGroupIDs                     []string           `json:"securityGroupIds,omitempty"`
	ConfigParameters                     []ConfigParameter  `json:"configParameters,omitempty"`
	SubnetIDs                            []string           `json:"subnetIds,omitempty"`
	Endpoint                             WorkgroupEndpoint  `json:"endpoint"`
	MaxCapacity                          int                `json:"maxCapacity,omitempty"`
	Port                                 int                `json:"port,omitempty"`
	BaseCapacity                         int                `json:"baseCapacity"`
	EnhancedVpcRouting                   bool               `json:"enhancedVpcRouting,omitempty"`
	ExtraComputeForAutomaticOptimization bool               `json:"extraComputeForAutomaticOptimization,omitempty"`
	PubliclyAccessible                   bool               `json:"publiclyAccessible,omitempty"`
}

// WorkgroupEndpoint holds the endpoint address and port.
type WorkgroupEndpoint struct {
	Address string `json:"address"`
	Port    int    `json:"port"`
}

// ConfigParameter is a workgroup advanced-control key/value pair.
type ConfigParameter struct {
	ParameterKey   string `json:"parameterKey,omitempty"`
	ParameterValue string `json:"parameterValue,omitempty"`
}

// PerformanceTarget is a workgroup's price-performance target setting.
type PerformanceTarget struct {
	Status string `json:"status,omitempty"`
	Level  int    `json:"level,omitempty"`
}

// WorkgroupParams holds the mutable fields shared by CreateWorkgroup and
// UpdateWorkgroup (NamespaceName is create-only and passed separately).
type WorkgroupParams struct {
	PricePerformanceTarget               *PerformanceTarget
	IPAddressType                        string
	TrackName                            string
	ConfigParameters                     []ConfigParameter
	SubnetIDs                            []string
	SecurityGroupIDs                     []string
	BaseCapacity                         int
	MaxCapacity                          int
	Port                                 int
	EnhancedVpcRouting                   bool
	ExtraComputeForAutomaticOptimization bool
	PubliclyAccessible                   bool
}

// ServerlessSnapshot represents a Redshift Serverless namespace snapshot.
type ServerlessSnapshot struct {
	SnapshotCreateTime        time.Time `json:"snapshotCreateTime"`
	SnapshotArn               string    `json:"snapshotArn"`
	SnapshotName              string    `json:"snapshotName"`
	NamespaceName             string    `json:"namespaceName"`
	NamespaceArn              string    `json:"namespaceArn"`
	Status                    string    `json:"status"`
	AdminUsername             string    `json:"adminUsername,omitempty"`
	AccountsWithRestoreAccess []string  `json:"accountsWithRestoreAccess,omitempty"`
	SnapshotRetentionPeriod   int       `json:"snapshotRetentionPeriod,omitempty"`
}

// ServerlessUsageLimit represents a serverless usage limit.
type ServerlessUsageLimit struct {
	UsageLimitArn string `json:"usageLimitArn"`
	UsageLimitID  string `json:"usageLimitId"`
	ResourceArn   string `json:"resourceArn"`
	UsageType     string `json:"usageType"`
	Period        string `json:"period"`
	BreachAction  string `json:"breachAction"`
	Amount        int64  `json:"amount"`
}

// ServerlessScheduledAction represents a serverless scheduled action.
//
// Schedule and TargetAction are stored as raw JSON (not decoded into a typed
// union) and echoed back verbatim: the real Schedule shape is a tagged union
// keyed "at" (epoch seconds) or "cron" (bare AWS cron string, no wrapping
// function call unlike classic Redshift's "cron(...)"), and TargetAction is
// keyed "createSnapshot" with a nested CreateSnapshotScheduleActionParameters
// object (confirmed via awsAwsjson11_serializeDocumentSchedule/TargetAction in
// serializers.go) -- this backend does not execute scheduled actions, so
// passthrough preserves wire fidelity without fabricating execution semantics.
type ServerlessScheduledAction struct {
	StartTime                  time.Time       `json:"-"`
	EndTime                    time.Time       `json:"-"`
	NamespaceName              string          `json:"namespaceName,omitempty"`
	RoleArn                    string          `json:"roleArn,omitempty"`
	ScheduledActionDescription string          `json:"scheduledActionDescription,omitempty"`
	ScheduledActionName        string          `json:"scheduledActionName"`
	ScheduledActionUUID        string          `json:"scheduledActionUuid,omitempty"`
	State                      string          `json:"state"`
	Schedule                   json.RawMessage `json:"schedule,omitempty"`
	TargetAction               json.RawMessage `json:"targetAction,omitempty"`
}

// ---------------------------------------------------------------------------
// Status constants for serverless resources
// ---------------------------------------------------------------------------

const (
	slStatusAvailable = "AVAILABLE"
	// slStateActive/slStateDisabled are ScheduledActionResponse.State values
	// (types.StateActive/StateDisabled), distinct from the Namespace/Workgroup/
	// Snapshot "status" field family above.
	slStateActive   = "ACTIVE"
	slStateDisabled = "DISABLED"

	// Magic number constants for serverless operations.
	slIDHexBytes          = 8    // bytes for random resource IDs (produces 16-char hex)
	slEndpointHexBytes    = 6    // bytes for random endpoint suffix (produces 12-char hex)
	slUUIDHexBytes        = 16   // bytes for a UUID-shaped ScheduledActionUUID
	slSecretHexBytes      = 8    // bytes for a fabricated Secrets Manager ARN suffix
	slDefaultBaseCapacity = 32   // default RPU if not specified
	slServerlessPort      = 5439 // default Redshift serverless port
	slCredTokenHexBytes   = 4    // bytes for credential token suffix
	slCredSecretHexBytes  = 20   // bytes for credential secret
	slCredExpiryMinutes   = 15   // default credential TTL in minutes
	slCredMinDuration     = 900  // GetCredentialsInput.DurationSeconds minimum (real API)
	slCredMaxDuration     = 3600 // GetCredentialsInput.DurationSeconds maximum (real API)
	slDefaultPageSize     = 100  // default max results per page
)

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func randomHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)

	return hex.EncodeToString(b)
}

func serverlessDefaultPageSize() int { return slDefaultPageSize }
