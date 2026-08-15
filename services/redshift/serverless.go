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
	// ErrServerlessResourceNotFound is returned by TagResource/UntagResource/
	// ListTagsForResource when resourceArn matches no known taggable resource.
	ErrServerlessResourceNotFound = errors.New("ResourceNotFoundException")
	// ErrCustomDomainSLNotFound is returned when a serverless custom domain
	// association does not exist (or does not belong to the given workgroup).
	ErrCustomDomainSLNotFound = errors.New("ResourceNotFoundException")
	// ErrCustomDomainSLConflict is returned when a custom domain name is
	// already associated with a workgroup.
	ErrCustomDomainSLConflict = errors.New("ConflictException")
	// ErrResourcePolicySLNotFound is returned when no resource policy exists
	// for the given resourceArn.
	ErrResourcePolicySLNotFound = errors.New("ResourceNotFoundException")
	// ErrSnapshotCopyConfigSLNotFound is returned when a serverless snapshot
	// copy configuration does not exist.
	ErrSnapshotCopyConfigSLNotFound = errors.New("ResourceNotFoundException")
	// ErrRecoveryPointNotFound is returned when a recovery point does not exist.
	ErrRecoveryPointNotFound = errors.New("ResourceNotFoundException")
	// ErrTableRestoreSLNotFound is returned when a serverless table restore
	// request does not exist.
	ErrTableRestoreSLNotFound = errors.New("ResourceNotFoundException")
	// ErrEndpointAccessSLNotFound is returned when a serverless VPC endpoint
	// does not exist.
	ErrEndpointAccessSLNotFound = errors.New("ResourceNotFoundException")
	// ErrEndpointAccessSLAlreadyExists is returned when a serverless VPC
	// endpoint name is already in use.
	ErrEndpointAccessSLAlreadyExists = errors.New("ConflictException")
	// ErrServerlessTrackNotFound is returned when a named maintenance track
	// does not exist.
	ErrServerlessTrackNotFound = errors.New("ResourceNotFoundException")
	// ErrServerlessDryRun is returned by UpdateLakehouseConfiguration when
	// DryRun is true and the request would otherwise have succeeded (real
	// DryRunException: "the request was successful, but dry run was enabled
	// so no action was taken", confirmed in the pinned service-2.json).
	ErrServerlessDryRun = errors.New("DryRunException")
)

// ---------------------------------------------------------------------------
// Serverless models
//
// Field names/JSON keys verified against aws-sdk-go-v2/service/redshiftserverless
// @v1.38.5 types.Namespace/Workgroup/Snapshot/UsageLimit/ScheduledActionResponse
// (awsAwsjson11_deserializeDocument* in deserializers.go).
// ---------------------------------------------------------------------------

// Namespace represents a Redshift Serverless namespace.
//
// CatalogArn/LakehouseRegistrationStatus are real Namespace members
// (confirmed against types.Namespace in types/types.go) written by
// UpdateLakehouseConfiguration -- see serverless_lakehouse.go.
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
	CatalogArn                  string    `json:"catalogArn,omitempty"`
	LakehouseRegistrationStatus string    `json:"lakehouseRegistrationStatus,omitempty"`
	Status                      string    `json:"status"`
	IamRoles                    []string  `json:"iamRoles,omitempty"`
	LogExports                  []string  `json:"logExports,omitempty"`
}

// CreateNamespaceParams holds the mutable fields accepted by CreateNamespace.
// Grouped into a struct because the real CreateNamespaceInput carries more
// fields than fit a readable positional parameter list.
//
// AdminUserPassword and RedshiftIdcApplicationArn are accepted for wire
// compatibility but intentionally never persisted: real CreateNamespaceInput
// has both (confirmed against api_op_CreateNamespace.go), but neither is
// ever echoed back on the Namespace shape (types.go has no such members), so
// no client can observe whether this backend stores them. AdminUserPassword
// is additionally a credential -- see CreateNamespace's doc comment.
type CreateNamespaceParams struct {
	Tags                        map[string]string
	NamespaceName               string
	AdminUsername               string
	AdminUserPassword           string
	DBName                      string
	KmsKeyID                    string
	DefaultIamRoleArn           string
	AdminPasswordSecretKmsKeyID string
	RedshiftIdcApplicationArn   string
	IamRoles                    []string
	LogExports                  []string
	ManageAdminPassword         bool
}

// UpdateNamespaceParams holds the mutable fields accepted by UpdateNamespace.
//
// AdminUserPassword is accepted for wire compatibility but intentionally
// never persisted -- see CreateNamespaceParams' doc comment; the same
// credential-handling rationale applies here. There is deliberately no DBName
// field: UpdateNamespaceInput has no dbName member (a namespace's database
// name can't be changed after creation, confirmed against
// api_op_UpdateNamespace.go), unlike CreateNamespaceInput which does.
type UpdateNamespaceParams struct {
	AdminUsername               string
	AdminUserPassword           string
	KmsKeyID                    string
	DefaultIamRoleArn           string
	AdminPasswordSecretKmsKeyID string
	ManageAdminPassword         *bool
	IamRoles                    []string
	LogExports                  []string
}

// Workgroup represents a Redshift Serverless workgroup.
type Workgroup struct {
	CreationDate time.Time `json:"creationDate"`
	// CustomDomainCertificateExpiryTime/CustomDomainCertificateArn/CustomDomainName
	// mirror the association written by CreateCustomDomainAssociation (see
	// serverless_custom_domains.go) -- real Workgroup carries these three
	// fields directly (confirmed against the "Workgroup" shape in
	// service-2.json), not just via GetCustomDomainAssociation.
	CustomDomainCertificateExpiryTime    *time.Time         `json:"customDomainCertificateExpiryTime,omitempty"`
	PricePerformanceTarget               *PerformanceTarget `json:"pricePerformanceTarget,omitempty"`
	IPAddressType                        string             `json:"ipAddressType,omitempty"`
	CustomDomainCertificateArn           string             `json:"customDomainCertificateArn,omitempty"`
	CustomDomainName                     string             `json:"customDomainName,omitempty"`
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

// ServerlessCustomDomainAssociation represents a Redshift Serverless custom
// domain association (the "Association" shape in service-2.json), distinct
// from the classic-Redshift CustomDomainAssociation in custom_domains.go
// (cluster-keyed, query/XML protocol) -- this one is workgroup-keyed JSON.
// Keyed by CustomDomainName: a real custom domain name can be associated with
// only one workgroup at a time (Create/Get/Delete/Update all require the pair,
// but CustomDomainName alone is the natural unique key since it identifies a
// real DNS name).
type ServerlessCustomDomainAssociation struct {
	CustomDomainCertificateExpiryTime *time.Time `json:"customDomainCertificateExpiryTime,omitempty"`
	CustomDomainCertificateArn        string     `json:"customDomainCertificateArn,omitempty"`
	CustomDomainName                  string     `json:"customDomainName"`
	WorkgroupName                     string     `json:"workgroupName"`
}

// ServerlessResourcePolicy is the resource policy attached to a Redshift
// Serverless resource ARN, used to share snapshots across accounts
// (confirmed against the "ResourcePolicy" shape in service-2.json).
type ServerlessResourcePolicy struct {
	ResourceArn string `json:"resourceArn"`
	Policy      string `json:"policy"`
}

// ServerlessSnapshotCopyConfiguration configures cross-region snapshot
// copying for a Redshift Serverless namespace (the "SnapshotCopyConfiguration"
// shape in service-2.json). This backend does not perform real cross-region
// replication -- like the rest of this service's snapshot handling, it only
// tracks the configuration object itself.
type ServerlessSnapshotCopyConfiguration struct {
	SnapshotCopyConfigurationArn string `json:"snapshotCopyConfigurationArn"`
	SnapshotCopyConfigurationID  string `json:"snapshotCopyConfigurationId"`
	NamespaceName                string `json:"namespaceName"`
	DestinationRegion            string `json:"destinationRegion"`
	DestinationKmsKeyID          string `json:"destinationKmsKeyId,omitempty"`
	SnapshotRetentionPeriod      int    `json:"snapshotRetentionPeriod,omitempty"`
}

// RecoveryPoint is a namespace's automatically created recovery point (the
// "RecoveryPoint" shape in service-2.json: "Recovery points are created every
// 30 minutes and kept for 24 hours" -- there is no CreateRecoveryPoint
// operation anywhere in this API, confirmed absent from the operations list).
// This backend generates one recovery point per workgroup at CreateWorkgroup
// time instead of running a real 30-minute background scheduler, matching
// this service's existing instant-apply convention elsewhere (e.g. snapshots
// created instantaneously) -- see generateRecoveryPointLocked.
type RecoveryPoint struct {
	RecoveryPointCreateTime time.Time `json:"recoveryPointCreateTime"`
	NamespaceArn            string    `json:"namespaceArn,omitempty"`
	NamespaceName           string    `json:"namespaceName,omitempty"`
	RecoveryPointID         string    `json:"recoveryPointId"`
	WorkgroupName           string    `json:"workgroupName,omitempty"`
	TotalSizeInMegaBytes    float64   `json:"totalSizeInMegaBytes,omitempty"`
}

// ServerlessTableRestoreStatus represents a Redshift Serverless table-level
// restore request (the "TableRestoreStatus" shape in service-2.json),
// distinct from classic Redshift's cluster-keyed TableRestoreStatus in
// models.go (table_restore.go). RequestTime is tagged json:"-" because it is
// an epoch-seconds number on the wire (confirmed via
// awsAwsjson11_deserializeDocumentTableRestoreStatus's smithytime.ParseEpochSeconds
// in aws-sdk-go-v2/service/redshiftserverless@v1.38.5/deserializers.go),
// unlike RecoveryPointCreateTime's ISO8601 string above -- the response
// wrapper (see handler_serverless_table_restore.go) does the epoch
// conversion at the boundary, same pattern as slScheduledActionWire.
type ServerlessTableRestoreStatus struct {
	RequestTime           time.Time `json:"-"`
	Message               string    `json:"message,omitempty"`
	NamespaceName         string    `json:"namespaceName,omitempty"`
	NewTableName          string    `json:"newTableName,omitempty"`
	RecoveryPointID       string    `json:"recoveryPointId,omitempty"`
	SnapshotName          string    `json:"snapshotName,omitempty"`
	SourceDatabaseName    string    `json:"sourceDatabaseName,omitempty"`
	SourceSchemaName      string    `json:"sourceSchemaName,omitempty"`
	SourceTableName       string    `json:"sourceTableName,omitempty"`
	Status                string    `json:"status,omitempty"`
	TableRestoreRequestID string    `json:"tableRestoreRequestId"`
	TargetDatabaseName    string    `json:"targetDatabaseName,omitempty"`
	TargetSchemaName      string    `json:"targetSchemaName,omitempty"`
	WorkgroupName         string    `json:"workgroupName,omitempty"`
	ProgressInMegaBytes   int64     `json:"progressInMegaBytes,omitempty"`
	TotalDataInMegaBytes  int64     `json:"totalDataInMegaBytes,omitempty"`
}

// ServerlessEndpointAccess is a Redshift Serverless managed VPC endpoint
// (the "EndpointAccess" shape in service-2.json), distinct from classic
// Redshift's cluster-keyed EndpointAccess in models.go (endpoint_access.go)
// -- this one is workgroup-keyed JSON with SubnetIDs (individual subnet
// IDs) rather than a SubnetGroupName. VpcEndpoint (the nested
// vpcEndpointId/vpcId/networkInterfaces object) is deliberately unmodeled,
// the same judgment call families.EndpointAccess already made for classic
// Redshift: real types.VpcEndpoint.NetworkInterfaces needs
// AvailabilityZone/PrivateIpAddress/NetworkInterfaceId/SubnetId per ENI,
// none of which this backend tracks anywhere, and vpcId/vpcEndpointId would
// have to be fabricated with no real ENI allocation behind them -- left
// absent rather than invented. VpcSecurityGroupIDs is tagged json:"-" and
// expanded into the real vpcSecurityGroups>{status,vpcSecurityGroupId} list
// at the response boundary (see slEndpointAccessWire); OwnerAccount is a
// real request/List-filter field the EndpointAccess response shape itself
// never echoes (confirmed absent from its members).
type ServerlessEndpointAccess struct {
	EndpointCreateTime  time.Time `json:"endpointCreateTime"`
	Address             string    `json:"address,omitempty"`
	EndpointArn         string    `json:"endpointArn"`
	EndpointName        string    `json:"endpointName"`
	EndpointStatus      string    `json:"endpointStatus"`
	OwnerAccount        string    `json:"-"`
	WorkgroupName       string    `json:"workgroupName"`
	SubnetIDs           []string  `json:"subnetIds,omitempty"`
	VpcSecurityGroupIDs []string  `json:"-"`
	Port                int       `json:"port,omitempty"`
}

// ManagedWorkgroupListItem represents a Glue Data Catalog-managed Redshift
// Serverless workgroup (the "ManagedWorkgroupListItem" shape in
// service-2.json) -- auto-provisioned when a query runs against data shared
// through Lake Formation / Glue federation, confirmed by ListManagedWorkgroupsRequest's
// sourceArn filter being a Glue database/catalog ARN
// ("^arn:aws[a-z-]*:glue:..." in the SourceArn shape's pattern). This
// backend has no Glue Data Catalog or Lake Formation integration anywhere,
// so there is never a real managed workgroup to list -- see
// ListManagedWorkgroupsSL.
type ManagedWorkgroupListItem struct {
	CreationDate         time.Time `json:"creationDate"`
	ManagedWorkgroupID   string    `json:"managedWorkgroupId,omitempty"`
	ManagedWorkgroupName string    `json:"managedWorkgroupName,omitempty"`
	SourceArn            string    `json:"sourceArn,omitempty"`
	Status               string    `json:"status,omitempty"`
}

// ServerlessTrack represents a Redshift Serverless maintenance track (the
// "ServerlessTrack" shape in service-2.json). Real AWS documents exactly two
// track names, "current" and "trailing" (TrackName itself is an untyped
// *string in the SDK, not a Go enum, but every AWS doc/console reference
// names only these two -- the same pair classic Redshift's own
// DescribeClusterTracks already models, see handler_cluster_info.go's
// modelVersion10-keyed catalog). UpdateTargets (newer versions this track
// could update to) is honestly left empty: this backend has a single static
// release (modelVersion10), so there is no second version to invent an
// upgrade path to.
type ServerlessTrack struct {
	TrackName        string         `json:"trackName,omitempty"`
	WorkgroupVersion string         `json:"workgroupVersion,omitempty"`
	UpdateTargets    []UpdateTarget `json:"updateTargets,omitempty"`
}

// UpdateTarget is a single available upgrade target within a ServerlessTrack.
type UpdateTarget struct {
	TrackName        string `json:"trackName,omitempty"`
	WorkgroupVersion string `json:"workgroupVersion,omitempty"`
}

// ServerlessLakehouseConfig tracks a namespace's lakehouse/Glue Data Catalog
// federation association written by UpdateLakehouseConfiguration. Only
// CatalogName is echoed indirectly (via the derived CatalogArn stored on
// Namespace itself, a real Namespace member -- see serverless.go's Namespace
// doc comment); LakehouseIdcApplicationArn has NO Namespace member at all
// (confirmed absent from types.Namespace) so it lives here instead, kept
// out of every other namespace response the same way Namespace's own
// AdminUserPassword is kept out -- its only real observable surface is
// UpdateLakehouseConfiguration's own response.
type ServerlessLakehouseConfig struct {
	NamespaceName              string `json:"namespaceName"`
	CatalogName                string `json:"catalogName,omitempty"`
	LakehouseIdcApplicationArn string `json:"lakehouseIdcApplicationArn,omitempty"`
}

// LakehouseConfigResult is UpdateLakehouseConfigurationOutput's real shape --
// flat, not enveloped, and distinct from (a strict subset of) Namespace's own
// fields (confirmed against the Output struct in api_op_UpdateLakehouseConfiguration.go).
type LakehouseConfigResult struct {
	NamespaceName               string `json:"namespaceName,omitempty"`
	CatalogArn                  string `json:"catalogArn,omitempty"`
	LakehouseIdcApplicationArn  string `json:"lakehouseIdcApplicationArn,omitempty"`
	LakehouseRegistrationStatus string `json:"lakehouseRegistrationStatus,omitempty"`
}

// UpdateLakehouseConfigParams holds UpdateLakehouseConfigurationInput's
// mutable fields.
type UpdateLakehouseConfigParams struct {
	NamespaceName              string
	CatalogName                string
	LakehouseIdcApplicationArn string
	LakehouseIdcRegistration   string
	LakehouseRegistration      string
	DryRun                     bool
}

// slResourceTagSet holds the tags attached to a taggable Redshift Serverless
// resource, keyed by the resource's own ARN -- confirmed against
// TagResourceRequest/UntagResourceRequest/ListTagsForResourceRequest's
// "resourceArn" member in service-2.json. Namespace/Workgroup/Snapshot have no
// "tags" field of their own on the wire (confirmed absent from their shapes),
// so tags live here and are reachable only via ListTagsForResource.
type slResourceTagSet struct {
	Tags        map[string]string `json:"tags"`
	ResourceArn string            `json:"resourceArn"`
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
	// slCertExpiryDays is a fabricated-but-consistent validity window for a
	// custom domain association's certificate, mirroring the same
	// fabricated-but-consistent convention used for AdminPasswordSecretArn
	// above -- this backend does not do real ACM certificate issuance.
	slCertExpiryDays = 365
	// slIdcTokenHexBytes is the byte length of GetIdentityCenterAuthToken's
	// synthetic opaque token (32 hex chars), mirroring classic Redshift's own
	// GetIdentityCenterAuthToken (handler_idc_applications.go).
	slIdcTokenHexBytes = 16

	// slTrackCurrent/slTrackTrailing are the two real Redshift Serverless
	// maintenance track names -- see ServerlessTrack's doc comment.
	slTrackCurrent  = "current"
	slTrackTrailing = "trailing"

	// lakehouseIdcAssociate/lakehouseIdcDisassociate are
	// UpdateLakehouseConfigurationInput.LakehouseIdcRegistration's two real
	// enum values (types.LakehouseIdcRegistration).
	lakehouseIdcAssociate    = "Associate"
	lakehouseIdcDisassociate = "Disassociate"
	// lakehouseRegister/lakehouseDeregister are
	// UpdateLakehouseConfigurationInput.LakehouseRegistration's two real enum
	// values (types.LakehouseRegistration).
	lakehouseRegister   = "Register"
	lakehouseDeregister = "Deregister"
	// slLakehouseRegistered/slLakehouseDeregistered are this backend's
	// LakehouseRegistrationStatus values. Real AWS does not publish an enum
	// for this field (LakehouseRegistrationStatus is a plain *string in the
	// SDK, confirmed in types/types.go, with no documented value list) --
	// these are a direct, honest derivation from the client's own
	// LakehouseRegistration request value, not an invented status vocabulary.
	slLakehouseRegistered   = "Registered"
	slLakehouseDeregistered = "Deregistered"
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
