package mq

const (
	// BrokerStateRunning indicates an active broker.
	BrokerStateRunning = "RUNNING"
	// BrokerStateCreating indicates a broker being provisioned.
	BrokerStateCreating = "CREATION_IN_PROGRESS"
	// BrokerStateDeleting indicates a broker being removed.
	BrokerStateDeleting = "DELETION_IN_PROGRESS"
	// BrokerStateRebooting indicates a broker reboot in progress.
	BrokerStateRebooting = "REBOOT_IN_PROGRESS"

	// EngineTypeActiveMQ is the ActiveMQ engine type.
	EngineTypeActiveMQ = "ACTIVEMQ"
	// EngineTypeRabbitMQ is the RabbitMQ engine type.
	EngineTypeRabbitMQ = "RABBITMQ"

	// DeploymentModeSingleInstance is the single-instance deployment mode.
	DeploymentModeSingleInstance = "SINGLE_INSTANCE"
	// DeploymentModeActiveStandby is the active/standby multi-AZ deployment mode.
	DeploymentModeActiveStandby = "ACTIVE_STANDBY_MULTI_AZ"
	// DeploymentModeCluster is the cluster multi-AZ deployment mode (RabbitMQ).
	DeploymentModeCluster = "CLUSTER_MULTI_AZ"

	// StorageTypeEFS is the EFS storage type (ActiveMQ). AWS MQ's
	// BrokerStorageType enum uses the uppercase form on the wire (see
	// aws-sdk-go-v2/service/mq/types.BrokerStorageTypeEfs); a lowercase value
	// here would round-trip through JSON fine but silently fail any
	// client-side comparison against the SDK's typed enum constants.
	StorageTypeEFS = "EFS"
	// StorageTypeEBS is the EBS storage type (RabbitMQ). See StorageTypeEFS
	// for why this must match the SDK's uppercase enum value.
	StorageTypeEBS = "EBS"

	// PromoteModeFailover is the failover promote mode.
	PromoteModeFailover = "FAILOVER"
	// PromoteModeSwitchover is the switchover promote mode.
	PromoteModeSwitchover = "SWITCHOVER"

	// ChangeTypeCreate marks a broker user create that is pending a broker
	// reboot. Mirrors aws-sdk-go-v2/service/mq/types.ChangeTypeCreate.
	ChangeTypeCreate = "CREATE"
	// ChangeTypeUpdate marks a broker user update that is pending a broker
	// reboot. Mirrors aws-sdk-go-v2/service/mq/types.ChangeTypeUpdate.
	ChangeTypeUpdate = "UPDATE"
	// ChangeTypeDelete marks a broker user delete that is pending a broker
	// reboot. Mirrors aws-sdk-go-v2/service/mq/types.ChangeTypeDelete.
	ChangeTypeDelete = "DELETE"
)

// BrokerInstance holds endpoint information for a broker instance.
type BrokerInstance struct {
	ConsoleURL string   `json:"consoleURL"`
	Endpoints  []string `json:"endpoints"`
}

// SharedResource describes a single resource shared to a broker via AWS
// Resource Access Manager (cross-account VPC subnets/configurations shared
// through a RAM resource share). This backend does not model RAM resource
// sharing, so DescribeSharedResources never populates one -- the type exists
// only to document the real DescribeSharedResourcesOutput wire shape.
type SharedResource struct {
	Error             *SharedResourceError `json:"error,omitempty"`
	ResourceArn       string               `json:"resourceArn"`
	Status            string               `json:"status"`
	Type              string               `json:"type"`
	DNSNames          []string             `json:"dnsNames,omitempty"`
	ResourceShareArns []string             `json:"resourceShareArns,omitempty"`
}

// SharedResourceError describes an error encountered provisioning a shared resource.
type SharedResourceError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// User represents an Amazon MQ broker user.
//
// Pending holds a not-yet-applied create/update/delete: real Amazon MQ only
// applies ActiveMQ broker-user changes on the next broker reboot (see
// aws-sdk-go-v2/service/mq/types.UserPendingChanges). Password intentionally
// keeps the json:"-" tag even though the rest of User now persists across
// Snapshot/Restore (see store_setup.go) -- matching the same secrets-out-of-
// the-persisted-blob precedent already used for
// LdapServerMetadata.ServiceAccountPassword, a restored user's password is
// always blank and must be reset via UpdateUser.
type User struct {
	Pending  *UserPendingChanges `json:"pending,omitempty"`
	Username string              `json:"username"`
	Password string              `json:"-"`
	Groups   []string            `json:"groups,omitempty"`
	Console  bool                `json:"consoleAccess"`
}

// UserPendingChanges mirrors aws-sdk-go-v2/service/mq/types.UserPendingChanges:
// the not-yet-applied create/update/delete staged for a broker user, applied
// atomically when the broker next reboots.
type UserPendingChanges struct {
	Console       *bool    `json:"consoleAccess,omitempty"`
	PendingChange string   `json:"pendingChange"`
	Groups        []string `json:"groups,omitempty"`
}

// UserSummary is a summary of a broker user (returned in lists).
type UserSummary struct {
	Username      string `json:"username"`
	PendingChange string `json:"pendingChange,omitempty"`
	Console       bool   `json:"consoleAccess"`
}

// ConfigurationID holds a reference to a broker configuration.
type ConfigurationID struct {
	ID       string `json:"id"`
	Revision int32  `json:"revision"`
}

// Configurations holds pending and current configuration references.
type Configurations struct {
	Current *ConfigurationID  `json:"current,omitempty"`
	Pending *ConfigurationID  `json:"pending,omitempty"`
	History []ConfigurationID `json:"history,omitempty"`
}

// Broker represents an Amazon MQ broker.
type Broker struct {
	EncryptionOptions          *EncryptionOptions       `json:"encryptionOptions,omitempty"`
	Users                      map[string]*User         `json:"users,omitempty"`
	Configurations             *Configurations          `json:"configurations,omitempty"`
	PendingDataReplicationMeta *DataReplicationMetadata `json:"pendingDataReplicationMetadata,omitempty"`
	PendingLdapServerMetadata  *LdapServerMetadata      `json:"pendingLdapServerMetadata,omitempty"`
	DataReplicationMetadata    *DataReplicationMetadata `json:"dataReplicationMetadata,omitempty"`
	Tags                       map[string]string        `json:"-"`
	LogsSummary                *LogsSummary             `json:"logsSummary,omitempty"`
	Logs                       *Logs                    `json:"logs,omitempty"`
	LdapServerMetadata         *LdapServerMetadata      `json:"ldapServerMetadata,omitempty"`
	MaintenanceWindowStartTime *WeeklyStartTime         `json:"maintenanceWindowStartTime,omitempty"`
	DataReplicationMode        string                   `json:"dataReplicationMode,omitempty"`
	PendingEngineVersion       string                   `json:"pendingEngineVersion,omitempty"`
	AuthenticationStrategy     string                   `json:"authenticationStrategy,omitempty"`
	CreatorRequestID           string                   `json:"creatorRequestId,omitempty"`
	EngineVersion              string                   `json:"engineVersion"`
	PendingDataReplicationMode string                   `json:"pendingDataReplicationMode,omitempty"`
	BrokerArn                  string                   `json:"brokerArn"`
	PendingHostInstanceType    string                   `json:"pendingHostInstanceType,omitempty"`
	Created                    string                   `json:"created"`
	BrokerName                 string                   `json:"brokerName"`
	HostInstanceType           string                   `json:"hostInstanceType"`
	BrokerID                   string                   `json:"brokerId"`
	EngineType                 string                   `json:"engineType"`
	DeploymentMode             string                   `json:"deploymentMode"`
	BrokerState                string                   `json:"brokerState"`
	PendingAuthStrategy        string                   `json:"pendingAuthenticationStrategy,omitempty"`
	StorageType                string                   `json:"storageType,omitempty"`
	ActionsRequired            []ActionRequired         `json:"actionsRequired,omitempty"`
	PendingSecurityGroups      []string                 `json:"pendingSecurityGroups,omitempty"`
	BrokerInstances            []BrokerInstance         `json:"brokerInstances,omitempty"`
	SecurityGroups             []string                 `json:"securityGroups,omitempty"`
	SubnetIDs                  []string                 `json:"subnetIds,omitempty"`
	PubliclyAccessible         bool                     `json:"publiclyAccessible"`
	AutoMinorVersionUpgrade    bool                     `json:"autoMinorVersionUpgrade"`
}

// EncryptionOptions configures KMS encryption for an Amazon MQ broker.
type EncryptionOptions struct {
	KMSKeyID       string `json:"kmsKeyId,omitempty"`
	UseAWSOwnedKey bool   `json:"useAwsOwnedKey"`
}

// WeeklyStartTime defines the broker maintenance window start time.
type WeeklyStartTime struct {
	DayOfWeek string `json:"dayOfWeek,omitempty"`
	TimeOfDay string `json:"timeOfDay,omitempty"`
	TimeZone  string `json:"timeZone,omitempty"`
}

// LdapServerMetadata configures LDAP authentication for a broker.
type LdapServerMetadata struct {
	RoleBase               string   `json:"roleBase,omitempty"`
	RoleName               string   `json:"roleName,omitempty"`
	RoleSearchMatching     string   `json:"roleSearchMatching,omitempty"`
	UserBase               string   `json:"userBase,omitempty"`
	UserRoleName           string   `json:"userRoleName,omitempty"`
	UserSearchMatching     string   `json:"userSearchMatching,omitempty"`
	ServiceAccountUsername string   `json:"serviceAccountUsername,omitempty"`
	ServiceAccountPassword string   `json:"-"`
	Hosts                  []string `json:"hosts,omitempty"`
	RoleSearchSubtree      bool     `json:"roleSearchSubtree"`
	UserSearchSubtree      bool     `json:"userSearchSubtree"`
}

// Logs configures CloudWatch Logs export for an Amazon MQ broker.
type Logs struct {
	Audit   bool `json:"audit"`
	General bool `json:"general"`
}

// LogsSummary holds the configured logs plus their resolved log group ARNs.
type LogsSummary struct {
	Pending         *Logs  `json:"pending,omitempty"`
	GeneralLogGroup string `json:"generalLogGroup,omitempty"`
	AuditLogGroup   string `json:"auditLogGroup,omitempty"`
	General         bool   `json:"general"`
	Audit           bool   `json:"audit"`
}

// ActionRequired describes a service-side action required on the broker.
type ActionRequired struct {
	ActionRequiredCode string `json:"actionRequiredCode,omitempty"`
	ActionRequiredInfo string `json:"actionRequiredInfo,omitempty"`
}

// DataReplicationMetadata describes an active CRDR (cross-region disaster recovery) link.
type DataReplicationMetadata struct {
	DataReplicationCounterpart string `json:"dataReplicationCounterpart,omitempty"`
	DataReplicationRole        string `json:"dataReplicationRole,omitempty"`
}

// ConfigurationRevision holds revision metadata for a configuration.
type ConfigurationRevision struct {
	Created     string `json:"created"`
	Description string `json:"description,omitempty"`
	Revision    int32  `json:"revision"`
}

// Configuration represents an Amazon MQ configuration. Data and Revisions
// carry real json tags (rather than "-") so they round-trip through
// Snapshot/Restore -- see store_setup.go for why store.Table marshals this
// type directly. Tags keeps "-" deliberately: it is persisted separately via
// backendSnapshot.Tags and re-linked by reestablishTagPointers so the
// b.tags[arn]/cfg.Tags shared-pointer invariant survives a restore.
type Configuration struct {
	Tags           map[string]string       `json:"-"`
	Data           map[int32]string        `json:"data,omitempty"`
	LatestRevision *ConfigurationRevision  `json:"latestRevision"`
	Arn            string                  `json:"arn"`
	ID             string                  `json:"id"`
	Name           string                  `json:"name"`
	Description    string                  `json:"description"`
	EngineType     string                  `json:"engineType"`
	EngineVersion  string                  `json:"engineVersion"`
	Created        string                  `json:"created"`
	Revisions      []ConfigurationRevision `json:"revisions,omitempty"`
}

// CreateBrokerOptions carries optional configuration for CreateBrokerWithOptions.
// Zero values are ignored and treated as "not specified".
type CreateBrokerOptions struct {
	Configuration              *ConfigurationID
	EncryptionOptions          *EncryptionOptions
	MaintenanceWindowStartTime *WeeklyStartTime
	LdapServerMetadata         *LdapServerMetadata
	Logs                       *Logs
	StorageType                string
	AuthenticationStrategy     string
	CreatorRequestID           string
	// DataReplicationMode and DataReplicationPrimaryBrokerArn accept and echo
	// CreateBrokerInput's CRDR fields (CreateBrokerInput.DataReplicationMode /
	// DataReplicationPrimaryBrokerArn in aws-sdk-go-v2/service/mq). Full CRDR
	// simulation (actually pairing brokers, propagating data) is out of
	// scope -- see PARITY.md's deferred CRDR note -- but the fields must not
	// be silently dropped: DataReplicationMode is stored verbatim and
	// DataReplicationPrimaryBrokerArn seeds DataReplicationMetadata so a
	// client reading DescribeBroker back sees its own request echoed.
	DataReplicationMode             string
	DataReplicationPrimaryBrokerArn string
}

// UpdateBrokerOptions carries optional fields for UpdateBrokerWithOptions.
// Zero values are ignored and treated as "not specified".
type UpdateBrokerOptions struct {
	Logs                       *Logs
	LdapServerMetadata         *LdapServerMetadata
	MaintenanceWindowStartTime *WeeklyStartTime
	Configuration              *ConfigurationID
	AuthenticationStrategy     string
	DataReplicationMode        string
}

// EngineVersion holds a single engine version entry.
type EngineVersion struct {
	Name string `json:"name"`
}

// BrokerEngineType describes an engine type and its available versions.
type BrokerEngineType struct {
	EngineType     string          `json:"engineType"`
	EngineVersions []EngineVersion `json:"engineVersions"`
}

// AvailabilityZone describes a single availability zone.
type AvailabilityZone struct {
	Name string `json:"name"`
}

// BrokerInstanceOption describes a broker host instance type and its options.
type BrokerInstanceOption struct {
	EngineType               string             `json:"engineType"`
	HostInstanceType         string             `json:"hostInstanceType"`
	StorageType              string             `json:"storageType"`
	AvailabilityZones        []AvailabilityZone `json:"availabilityZones"`
	SupportedEngineVersions  []string           `json:"supportedEngineVersions"`
	SupportedDeploymentModes []string           `json:"supportedDeploymentModes"`
}
