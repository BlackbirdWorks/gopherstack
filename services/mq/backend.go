// Package mq provides an in-memory stub of Amazon MQ.
package mq

import (
	"fmt"
	"maps"
	"net"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const (
	engineVersion5183  = "5.18.3"
	engineVersion5176  = "5.17.6"
	engineVersion5167  = "5.16.7"
	engineVersion51516 = "5.15.16"
)

var (
	// ErrNotFound is returned when a requested resource does not exist.
	ErrNotFound = awserr.New("NotFoundException", awserr.ErrNotFound)
	// ErrAlreadyExists is returned when a resource already exists.
	ErrAlreadyExists = awserr.New("ConflictException", awserr.ErrAlreadyExists)
	// ErrValidation is returned when a request contains an invalid parameter.
	ErrValidation = awserr.New("BadRequestException", awserr.ErrInvalidParameter)
)

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

	// StorageTypeEFS is the EFS storage type (ActiveMQ).
	StorageTypeEFS = "efs"
	// StorageTypeEBS is the EBS storage type (RabbitMQ).
	StorageTypeEBS = "ebs"

	// PromoteModeFailover is the failover promote mode.
	PromoteModeFailover = "FAILOVER"
	// PromoteModeSwitchover is the switchover promote mode.
	PromoteModeSwitchover = "SWITCHOVER"

	// defaultHostInstanceType is the default broker instance type.
	defaultHostInstanceType = "mq.m5.large"

	// maxConfigurationRevisions is the maximum number of revisions retained per configuration.
	// AWS MQ supports up to 50 revisions; older ones are pruned when this limit is exceeded.
	maxConfigurationRevisions = 50

	// maxConfigurationDataBytes caps the size of a single revision's data payload.
	// AWS MQ rejects configuration data larger than 250 KiB; we enforce the same limit
	// to avoid unbounded memory growth from a malicious or buggy client.
	maxConfigurationDataBytes = 256 * 1024
)

// validateCreateBrokerInput validates the three most commonly invalid fields in
// a CreateBroker request before acquiring the backend lock.
func validateCreateBrokerInput(name, deploymentMode, engineType string) error {
	if engineType != EngineTypeActiveMQ && engineType != EngineTypeRabbitMQ {
		return fmt.Errorf("%w: engineType must be ACTIVEMQ or RABBITMQ, got %q", ErrValidation, engineType)
	}

	if err := validateBrokerName(name); err != nil {
		return err
	}

	return validateDeploymentModeForEngine(deploymentMode, engineType)
}

// validateBrokerName checks that a broker name is 1-50 characters and matches
// the AWS MQ allowed pattern: starts with alphanumeric, contains only
// alphanumeric characters, hyphens, and underscores.
func validateBrokerName(name string) error {
	if len(name) == 0 || len(name) > 50 {
		return fmt.Errorf("%w: brokerName must be 1-50 characters (got %d)", ErrValidation, len(name))
	}

	if !isAlphanumeric(rune(name[0])) {
		return fmt.Errorf("%w: brokerName must start with an alphanumeric character", ErrValidation)
	}

	for _, c := range name[1:] {
		if !isAlphanumeric(c) && c != '-' && c != '_' {
			return fmt.Errorf(
				"%w: brokerName must contain only alphanumeric characters, hyphens, and underscores, got %q",
				ErrValidation, c,
			)
		}
	}

	return nil
}

// minPasswordUniqueChars is the minimum number of distinct characters required
// in an ActiveMQ broker user password.
const minPasswordUniqueChars = 4

func isAlphanumeric(c rune) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
}

// validateDeploymentModeForEngine checks that the deployment mode is compatible
// with the engine type. AWS MQ enforces: ACTIVE_STANDBY_MULTI_AZ is ActiveMQ only;
// CLUSTER_MULTI_AZ is RabbitMQ only.
func validateDeploymentModeForEngine(mode, engineType string) error {
	switch mode {
	case "", DeploymentModeSingleInstance:
		return nil
	case DeploymentModeActiveStandby:
		if engineType == EngineTypeRabbitMQ {
			return fmt.Errorf(
				"%w: ACTIVE_STANDBY_MULTI_AZ deployment mode is not supported for RabbitMQ brokers",
				ErrValidation,
			)
		}

		return nil
	case DeploymentModeCluster:
		if engineType == EngineTypeActiveMQ {
			return fmt.Errorf(
				"%w: CLUSTER_MULTI_AZ deployment mode is not supported for ActiveMQ brokers",
				ErrValidation,
			)
		}

		return nil
	default:
		return fmt.Errorf(
			"%w: deploymentMode must be SINGLE_INSTANCE, ACTIVE_STANDBY_MULTI_AZ, or CLUSTER_MULTI_AZ, got %q",
			ErrValidation, mode,
		)
	}
}

// validateActiveMQPassword enforces AWS MQ ActiveMQ password constraints:
// 12-250 characters, no commas, at least 4 unique characters.
func validateActiveMQPassword(password string) error {
	if len(password) < 12 || len(password) > 250 {
		return fmt.Errorf(
			"%w: ActiveMQ password must be 12-250 characters (got %d)",
			ErrValidation, len(password),
		)
	}

	if strings.ContainsRune(password, ',') {
		return fmt.Errorf("%w: ActiveMQ password must not contain commas", ErrValidation)
	}

	unique := make(map[rune]struct{})
	for _, c := range password {
		unique[c] = struct{}{}
	}

	if len(unique) < minPasswordUniqueChars {
		return fmt.Errorf(
			"%w: ActiveMQ password must contain at least %d unique characters (got %d)",
			ErrValidation, minPasswordUniqueChars, len(unique),
		)
	}

	return nil
}

// BrokerInstance holds endpoint information for a broker instance.
type BrokerInstance struct {
	ConsoleURL string   `json:"consoleURL"`
	Endpoints  []string `json:"endpoints"`
}

// User represents an Amazon MQ broker user.
type User struct {
	Username string   `json:"username"`
	Password string   `json:"-"`
	Groups   []string `json:"groups,omitempty"`
	Console  bool     `json:"consoleAccess"`
}

// UserSummary is a summary of a broker user (returned in lists).
type UserSummary struct {
	Username string `json:"username"`
	Console  bool   `json:"consoleAccess"`
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
	Users                      map[string]*User         `json:"-"`
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

// Configuration represents an Amazon MQ configuration.
type Configuration struct {
	Tags           map[string]string       `json:"-"`
	Data           map[int32]string        `json:"-"`
	LatestRevision *ConfigurationRevision  `json:"latestRevision"`
	Arn            string                  `json:"arn"`
	ID             string                  `json:"id"`
	Name           string                  `json:"name"`
	Description    string                  `json:"description"`
	EngineType     string                  `json:"engineType"`
	EngineVersion  string                  `json:"engineVersion"`
	Created        string                  `json:"created"`
	Revisions      []ConfigurationRevision `json:"-"`
}

// InMemoryBackend stores Amazon MQ state in memory.
type InMemoryBackend struct {
	brokers        map[string]*Broker
	configurations map[string]*Configuration
	tags           map[string]map[string]string
	mu             *lockmetrics.RWMutex
	accountID      string
	region         string
}

// NewInMemoryBackend creates a new in-memory Amazon MQ backend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		brokers:        make(map[string]*Broker),
		configurations: make(map[string]*Configuration),
		tags:           make(map[string]map[string]string),
		accountID:      accountID,
		region:         region,
		mu:             lockmetrics.New("mq"),
	}
}

// Region returns the region configured for this backend.
func (b *InMemoryBackend) Region() string { return b.region }

// AccountID returns the account ID configured for this backend.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// Reset clears all backend state, preserving only the account ID and region.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.brokers = make(map[string]*Broker)
	b.configurations = make(map[string]*Configuration)
	b.tags = make(map[string]map[string]string)
}

// --- Broker operations ---

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
}

// CreateBroker creates a new Amazon MQ broker (compatibility wrapper).
func (b *InMemoryBackend) CreateBroker(
	name, deploymentMode, engineType, engineVersion, hostInstanceType string,
	publiclyAccessible, autoMinorVersionUpgrade bool,
	securityGroups, subnetIDs []string,
	users []*User,
	tags map[string]string,
) (*Broker, error) {
	return b.CreateBrokerWithOptions(
		name, deploymentMode, engineType, engineVersion, hostInstanceType,
		publiclyAccessible, autoMinorVersionUpgrade,
		securityGroups, subnetIDs, users, tags, nil,
	)
}

// CreateBrokerWithOptions creates a new Amazon MQ broker, accepting the optional
// AWS-MQ fields (encryption, LDAP, logs, maintenance window, storage type,
// authentication strategy, initial configuration, and CreatorRequestId for
// idempotency).
//

func (b *InMemoryBackend) CreateBrokerWithOptions(
	name, deploymentMode, engineType, engineVersion, hostInstanceType string,
	publiclyAccessible, autoMinorVersionUpgrade bool,
	securityGroups, subnetIDs []string,
	users []*User,
	tags map[string]string,
	opts *CreateBrokerOptions,
) (*Broker, error) {
	b.mu.Lock("CreateBroker")
	defer b.mu.Unlock()

	if err := validateCreateBrokerInput(name, deploymentMode, engineType); err != nil {
		return nil, err
	}

	// Idempotency: a retry with the same CreatorRequestId returns the existing broker.
	if opts != nil && opts.CreatorRequestID != "" {
		if existing := b.findBrokerByCreatorRequestID(opts.CreatorRequestID); existing != nil {
			return b.copyBroker(existing), nil
		}
	}

	// Check for duplicate by name.
	for _, br := range b.brokers {
		if br.BrokerName == name {
			return nil, fmt.Errorf("%w: broker %s already exists", ErrAlreadyExists, name)
		}
	}

	if deploymentMode == "" {
		deploymentMode = DeploymentModeSingleInstance
	}

	if engineVersion == "" {
		if engineType == EngineTypeRabbitMQ {
			engineVersion = "3.11.20"
		} else {
			engineVersion = "5.15.14"
		}
	}

	if hostInstanceType == "" {
		hostInstanceType = defaultHostInstanceType
	}

	storageType, err := resolveStorageType(engineType, optsStorageType(opts))
	if err != nil {
		return nil, err
	}

	id := uuid.NewString()
	brokerArn := arn.Build("mq", b.region, b.accountID, "broker:"+name)
	created := time.Now().UTC().Format(time.RFC3339)

	endpoint := buildEndpoint(engineType, b.region, id)
	instances := []BrokerInstance{
		{
			ConsoleURL: fmt.Sprintf("http://%s.mq.%s.amazonaws.com:8162", id, b.region),
			Endpoints:  []string{endpoint},
		},
	}

	userMap := make(map[string]*User)
	for _, u := range users {
		cp := *u
		userMap[u.Username] = &cp
	}

	tagsCopy := make(map[string]string)
	maps.Copy(tagsCopy, tags)

	br := &Broker{
		BrokerArn:               brokerArn,
		BrokerID:                id,
		BrokerName:              name,
		BrokerState:             BrokerStateRunning,
		DeploymentMode:          deploymentMode,
		EngineType:              engineType,
		EngineVersion:           engineVersion,
		HostInstanceType:        hostInstanceType,
		StorageType:             storageType,
		PubliclyAccessible:      publiclyAccessible,
		AutoMinorVersionUpgrade: autoMinorVersionUpgrade,
		SecurityGroups:          securityGroups,
		SubnetIDs:               subnetIDs,
		BrokerInstances:         instances,
		Users:                   userMap,
		Tags:                    tagsCopy,
		Created:                 created,
	}

	applyCreateBrokerOptions(br, opts)

	b.brokers[id] = br
	b.tags[brokerArn] = tagsCopy

	return b.copyBroker(br), nil
}

// applyCreateBrokerOptions copies optional broker fields from opts into br.
func applyCreateBrokerOptions(br *Broker, opts *CreateBrokerOptions) {
	if opts == nil {
		return
	}

	br.AuthenticationStrategy = opts.AuthenticationStrategy
	br.CreatorRequestID = opts.CreatorRequestID
	br.EncryptionOptions = opts.EncryptionOptions
	br.MaintenanceWindowStartTime = opts.MaintenanceWindowStartTime
	br.LdapServerMetadata = opts.LdapServerMetadata
	br.Logs = opts.Logs

	if opts.Logs != nil {
		br.LogsSummary = &LogsSummary{
			General:         opts.Logs.General,
			Audit:           opts.Logs.Audit,
			GeneralLogGroup: logGroupName(br.BrokerID, "general"),
			AuditLogGroup:   logGroupName(br.BrokerID, "audit"),
		}
	}

	if opts.Configuration != nil {
		br.Configurations = &Configurations{Current: opts.Configuration}
	}
}

// optsStorageType safely extracts the requested storage type from opts.
func optsStorageType(opts *CreateBrokerOptions) string {
	if opts == nil {
		return ""
	}

	return opts.StorageType
}

// resolveStorageType picks the default storage type for the engine when none is
// supplied and validates that any explicit choice is allowed for that engine.
func resolveStorageType(engineType, requested string) (string, error) {
	switch engineType {
	case EngineTypeRabbitMQ:
		if requested == "" {
			return StorageTypeEBS, nil
		}

		if requested != StorageTypeEBS {
			return "", fmt.Errorf("%w: RabbitMQ requires storageType=%q", ErrValidation, StorageTypeEBS)
		}

		return requested, nil
	case EngineTypeActiveMQ:
		if requested == "" {
			return StorageTypeEFS, nil
		}

		if requested != StorageTypeEFS && requested != StorageTypeEBS {
			return "", fmt.Errorf(
				"%w: ActiveMQ storageType must be %q or %q, got %q",
				ErrValidation, StorageTypeEFS, StorageTypeEBS, requested,
			)
		}

		return requested, nil
	default:
		return "", fmt.Errorf("%w: unsupported engineType %q", ErrValidation, engineType)
	}
}

// findBrokerByCreatorRequestID returns a broker matching the given idempotency token.
// Caller must hold a lock.
func (b *InMemoryBackend) findBrokerByCreatorRequestID(reqID string) *Broker {
	for _, br := range b.brokers {
		if br.CreatorRequestID == reqID {
			return br
		}
	}

	return nil
}

// logGroupName builds a deterministic CloudWatch log group name for a broker channel.
func logGroupName(brokerID, channel string) string {
	return fmt.Sprintf("/aws/amazonmq/broker/%s/%s", brokerID, channel)
}

// buildEndpoint returns the endpoint URL for the broker using the AWS-format
// hostname: b-{brokerID}-1.mq.{region}.amazonaws.com.
func buildEndpoint(engineType, region, brokerID string) string {
	host := fmt.Sprintf("b-%s-1.mq.%s.amazonaws.com", brokerID, region)

	switch engineType {
	case EngineTypeRabbitMQ:
		return "amqps://" + net.JoinHostPort(host, "5671")
	default:
		return "ssl://" + net.JoinHostPort(host, "61617")
	}
}

// DescribeBroker returns a broker by ID or name.
func (b *InMemoryBackend) DescribeBroker(brokerID string) (*Broker, error) {
	b.mu.Lock("DescribeBroker")
	defer b.mu.Unlock()

	br := b.lookupBroker(brokerID)
	if br == nil {
		return nil, fmt.Errorf("%w: broker %s not found", ErrNotFound, brokerID)
	}

	cp := b.copyBroker(br)
	promoteRebootingToRunning(br)

	return cp, nil
}

// ListBrokers returns all brokers sorted by name.
func (b *InMemoryBackend) ListBrokers() []*Broker {
	b.mu.Lock("ListBrokers")
	defer b.mu.Unlock()

	list := make([]*Broker, 0, len(b.brokers))
	for _, br := range b.brokers {
		list = append(list, b.copyBroker(br))
		promoteRebootingToRunning(br)
	}

	sort.Slice(list, func(i, j int) bool { return list[i].BrokerName < list[j].BrokerName })

	return list
}

// DeleteBroker removes a broker by ID or name.
func (b *InMemoryBackend) DeleteBroker(brokerID string) (*Broker, error) {
	b.mu.Lock("DeleteBroker")
	defer b.mu.Unlock()

	br := b.lookupBroker(brokerID)
	if br == nil {
		return nil, fmt.Errorf("%w: broker %s not found", ErrNotFound, brokerID)
	}

	cp := b.copyBroker(br)
	delete(b.brokers, br.BrokerID)
	delete(b.tags, br.BrokerArn)

	return cp, nil
}

// RebootBroker simulates a broker reboot. The broker transitions to
// REBOOT_IN_PROGRESS once, then is restored to RUNNING on the next
// DescribeBroker / ListBrokers call so callers can observe the transition.
func (b *InMemoryBackend) RebootBroker(brokerID string) error {
	b.mu.Lock("RebootBroker")
	defer b.mu.Unlock()

	br := b.lookupBroker(brokerID)
	if br == nil {
		return fmt.Errorf("%w: broker %s not found", ErrNotFound, brokerID)
	}

	br.BrokerState = BrokerStateRebooting

	return nil
}

// promoteRebootingToRunning advances any broker stuck in REBOOT_IN_PROGRESS
// back to RUNNING. Caller must hold a write lock or call from a context where
// promoting in-place is safe (the result is only observed via a returned copy).
func promoteRebootingToRunning(br *Broker) {
	if br != nil && br.BrokerState == BrokerStateRebooting {
		br.BrokerState = BrokerStateRunning
	}
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

// UpdateBroker updates mutable broker fields (compatibility wrapper).
func (b *InMemoryBackend) UpdateBroker(
	brokerID, engineVersion, hostInstanceType string,
	autoMinorVersionUpgrade *bool,
	securityGroups []string,
) (*Broker, error) {
	return b.UpdateBrokerWithOptions(
		brokerID, engineVersion, hostInstanceType,
		autoMinorVersionUpgrade, securityGroups, nil,
	)
}

// UpdateBrokerWithOptions updates mutable broker fields including optional extended fields.
func (b *InMemoryBackend) UpdateBrokerWithOptions(
	brokerID, engineVersion, hostInstanceType string,
	autoMinorVersionUpgrade *bool,
	securityGroups []string,
	opts *UpdateBrokerOptions,
) (*Broker, error) {
	b.mu.Lock("UpdateBroker")
	defer b.mu.Unlock()

	br := b.lookupBroker(brokerID)
	if br == nil {
		return nil, fmt.Errorf("%w: broker %s not found", ErrNotFound, brokerID)
	}

	applyBrokerCoreFields(br, engineVersion, hostInstanceType, autoMinorVersionUpgrade, securityGroups)
	applyUpdateBrokerOptions(br, opts)

	return b.copyBroker(br), nil
}

// applyBrokerCoreFields applies the non-optional update fields to a broker.
func applyBrokerCoreFields(
	br *Broker,
	engineVersion, hostInstanceType string,
	autoMinorVersionUpgrade *bool,
	securityGroups []string,
) {
	if engineVersion != "" {
		br.EngineVersion = engineVersion
	}

	if hostInstanceType != "" {
		br.HostInstanceType = hostInstanceType
	}

	if autoMinorVersionUpgrade != nil {
		br.AutoMinorVersionUpgrade = *autoMinorVersionUpgrade
	}

	if securityGroups != nil {
		br.SecurityGroups = securityGroups
	}
}

// applyUpdateBrokerOptions applies optional update fields to a broker.
func applyUpdateBrokerOptions(br *Broker, opts *UpdateBrokerOptions) {
	if opts == nil {
		return
	}

	if opts.AuthenticationStrategy != "" {
		br.AuthenticationStrategy = opts.AuthenticationStrategy
	}

	if opts.Logs != nil {
		br.Logs = opts.Logs
		br.LogsSummary = &LogsSummary{
			General:         opts.Logs.General,
			Audit:           opts.Logs.Audit,
			GeneralLogGroup: logGroupName(br.BrokerID, "general"),
			AuditLogGroup:   logGroupName(br.BrokerID, "audit"),
		}
	}

	if opts.LdapServerMetadata != nil {
		br.LdapServerMetadata = opts.LdapServerMetadata
	}

	if opts.MaintenanceWindowStartTime != nil {
		br.MaintenanceWindowStartTime = opts.MaintenanceWindowStartTime
	}

	if opts.Configuration != nil {
		if br.Configurations == nil {
			br.Configurations = &Configurations{}
		}

		br.Configurations.Current = opts.Configuration
	}

	if opts.DataReplicationMode != "" {
		br.DataReplicationMode = opts.DataReplicationMode
	}
}

// lookupBroker finds a broker by ID or by name; caller must hold a lock.
func (b *InMemoryBackend) lookupBroker(brokerID string) *Broker {
	if br, ok := b.brokers[brokerID]; ok {
		return br
	}

	for _, br := range b.brokers {
		if br.BrokerName == brokerID {
			return br
		}
	}

	return nil
}

// copyBroker returns a shallow copy of a broker with deep-copied slices/maps.
func (b *InMemoryBackend) copyBroker(br *Broker) *Broker {
	cp := *br

	cp.Tags = make(map[string]string, len(br.Tags))
	maps.Copy(cp.Tags, br.Tags)

	cp.Users = make(map[string]*User, len(br.Users))
	for k, u := range br.Users {
		uc := *u
		cp.Users[k] = &uc
	}

	if len(br.SubnetIDs) > 0 {
		cp.SubnetIDs = append([]string{}, br.SubnetIDs...)
	}

	if len(br.SecurityGroups) > 0 {
		cp.SecurityGroups = append([]string{}, br.SecurityGroups...)
	}

	cp.BrokerInstances = append([]BrokerInstance{}, br.BrokerInstances...)

	return &cp
}

// --- User operations ---

// CreateUser creates a user on a broker.
func (b *InMemoryBackend) CreateUser(brokerID, username, password string, groups []string, console bool) error {
	b.mu.Lock("CreateUser")
	defer b.mu.Unlock()

	br := b.lookupBroker(brokerID)
	if br == nil {
		return fmt.Errorf("%w: broker %s not found", ErrNotFound, brokerID)
	}

	if _, ok := br.Users[username]; ok {
		return fmt.Errorf("%w: user %s already exists on broker %s", ErrAlreadyExists, username, brokerID)
	}

	if br.EngineType == EngineTypeActiveMQ && password != "" {
		if err := validateActiveMQPassword(password); err != nil {
			return err
		}
	}

	br.Users[username] = &User{
		Username: username,
		Password: password,
		Groups:   groups,
		Console:  console,
	}

	return nil
}

// DescribeUser returns a user from a broker.
func (b *InMemoryBackend) DescribeUser(brokerID, username string) (*User, error) {
	b.mu.RLock("DescribeUser")
	defer b.mu.RUnlock()

	br := b.lookupBroker(brokerID)
	if br == nil {
		return nil, fmt.Errorf("%w: broker %s not found", ErrNotFound, brokerID)
	}

	u, ok := br.Users[username]
	if !ok {
		return nil, fmt.Errorf("%w: user %s not found on broker %s", ErrNotFound, username, brokerID)
	}

	cp := *u

	return &cp, nil
}

// UpdateUser updates a broker user.
func (b *InMemoryBackend) UpdateUser(brokerID, username, password string, groups []string, console *bool) error {
	b.mu.Lock("UpdateUser")
	defer b.mu.Unlock()

	br := b.lookupBroker(brokerID)
	if br == nil {
		return fmt.Errorf("%w: broker %s not found", ErrNotFound, brokerID)
	}

	u, ok := br.Users[username]
	if !ok {
		return fmt.Errorf("%w: user %s not found on broker %s", ErrNotFound, username, brokerID)
	}

	if password != "" {
		if br.EngineType == EngineTypeActiveMQ {
			if err := validateActiveMQPassword(password); err != nil {
				return err
			}
		}

		u.Password = password
	}

	if groups != nil {
		u.Groups = groups
	}

	if console != nil {
		u.Console = *console
	}

	return nil
}

// DeleteUser removes a user from a broker.
func (b *InMemoryBackend) DeleteUser(brokerID, username string) error {
	b.mu.Lock("DeleteUser")
	defer b.mu.Unlock()

	br := b.lookupBroker(brokerID)
	if br == nil {
		return fmt.Errorf("%w: broker %s not found", ErrNotFound, brokerID)
	}

	if _, ok := br.Users[username]; !ok {
		return fmt.Errorf("%w: user %s not found on broker %s", ErrNotFound, username, brokerID)
	}

	delete(br.Users, username)

	return nil
}

// ListUsers returns all users for a broker.
func (b *InMemoryBackend) ListUsers(brokerID string) ([]UserSummary, error) {
	b.mu.RLock("ListUsers")
	defer b.mu.RUnlock()

	br := b.lookupBroker(brokerID)
	if br == nil {
		return nil, fmt.Errorf("%w: broker %s not found", ErrNotFound, brokerID)
	}

	list := make([]UserSummary, 0, len(br.Users))
	for _, u := range br.Users {
		list = append(list, UserSummary{Username: u.Username, Console: u.Console})
	}

	sort.Slice(list, func(i, j int) bool { return list[i].Username < list[j].Username })

	return list, nil
}

// --- Configuration operations ---

// CreateConfiguration creates a new Amazon MQ configuration.
func (b *InMemoryBackend) CreateConfiguration(
	name, description, engineType, engineVersion string,
	tags map[string]string,
) (*Configuration, error) {
	b.mu.Lock("CreateConfiguration")
	defer b.mu.Unlock()

	if engineType != EngineTypeActiveMQ && engineType != EngineTypeRabbitMQ {
		return nil, fmt.Errorf("%w: engineType must be ACTIVEMQ or RABBITMQ, got %q", ErrValidation, engineType)
	}

	for _, c := range b.configurations {
		if c.Name == name {
			return nil, fmt.Errorf("%w: configuration %s already exists", ErrAlreadyExists, name)
		}
	}

	if engineVersion == "" {
		if engineType == EngineTypeRabbitMQ {
			engineVersion = "3.11.20"
		} else {
			engineVersion = "5.15.14"
		}
	}

	id := "c-" + uuid.NewString()[:8]
	configArn := arn.Build("mq", b.region, b.accountID, "configuration:"+id)
	now := time.Now().UTC().Format(time.RFC3339)

	rev := ConfigurationRevision{
		Created:     now,
		Description: description,
		Revision:    1,
	}

	tagsCopy := make(map[string]string)
	maps.Copy(tagsCopy, tags)

	cfg := &Configuration{
		Arn:            configArn,
		ID:             id,
		Name:           name,
		Description:    description,
		EngineType:     engineType,
		EngineVersion:  engineVersion,
		LatestRevision: &rev,
		Created:        now,
		Tags:           tagsCopy,
		Revisions:      []ConfigurationRevision{rev},
		Data:           map[int32]string{1: ""},
	}

	b.configurations[id] = cfg
	b.tags[configArn] = tagsCopy

	return b.copyConfiguration(cfg), nil
}

// DescribeConfiguration returns a configuration by ID.
func (b *InMemoryBackend) DescribeConfiguration(configID string) (*Configuration, error) {
	b.mu.RLock("DescribeConfiguration")
	defer b.mu.RUnlock()

	cfg, ok := b.configurations[configID]
	if !ok {
		return nil, fmt.Errorf("%w: configuration %s not found", ErrNotFound, configID)
	}

	return b.copyConfiguration(cfg), nil
}

// ListConfigurations returns all configurations sorted by name.
func (b *InMemoryBackend) ListConfigurations() []*Configuration {
	b.mu.RLock("ListConfigurations")
	defer b.mu.RUnlock()

	list := make([]*Configuration, 0, len(b.configurations))
	for _, c := range b.configurations {
		list = append(list, b.copyConfiguration(c))
	}

	sort.Slice(list, func(i, j int) bool { return list[i].Name < list[j].Name })

	return list
}

// UpdateConfiguration updates a configuration (creates a new revision).
func (b *InMemoryBackend) UpdateConfiguration(configID, description, data string) (*Configuration, error) {
	if len(data) > maxConfigurationDataBytes {
		return nil, fmt.Errorf(
			"%w: configuration data exceeds %d bytes (got %d)",
			ErrValidation, maxConfigurationDataBytes, len(data),
		)
	}

	b.mu.Lock("UpdateConfiguration")
	defer b.mu.Unlock()

	cfg, ok := b.configurations[configID]
	if !ok {
		return nil, fmt.Errorf("%w: configuration %s not found", ErrNotFound, configID)
	}

	nextRev := cfg.LatestRevision.Revision + 1
	now := time.Now().UTC().Format(time.RFC3339)

	rev := ConfigurationRevision{
		Created:     now,
		Description: description,
		Revision:    nextRev,
	}

	if description != "" {
		cfg.Description = description
	}

	cfg.LatestRevision = &rev
	cfg.Revisions = append(cfg.Revisions, rev)
	cfg.Data[nextRev] = data

	// Prune oldest revision when the cap is exceeded.
	if len(cfg.Revisions) > maxConfigurationRevisions {
		oldest := cfg.Revisions[0]
		cfg.Revisions = cfg.Revisions[1:]
		delete(cfg.Data, oldest.Revision)
	}

	return b.copyConfiguration(cfg), nil
}

// copyConfiguration returns a copy of a configuration.
func (b *InMemoryBackend) copyConfiguration(c *Configuration) *Configuration {
	cp := *c

	cp.Tags = make(map[string]string, len(c.Tags))
	maps.Copy(cp.Tags, c.Tags)

	if c.LatestRevision != nil {
		rev := *c.LatestRevision
		cp.LatestRevision = &rev
	}

	cp.Revisions = append([]ConfigurationRevision{}, c.Revisions...)

	cp.Data = make(map[int32]string, len(c.Data))
	maps.Copy(cp.Data, c.Data)

	return &cp
}

// DeleteConfiguration removes a configuration by ID.
func (b *InMemoryBackend) DeleteConfiguration(configID string) error {
	b.mu.Lock("DeleteConfiguration")
	defer b.mu.Unlock()

	if _, ok := b.configurations[configID]; !ok {
		return fmt.Errorf("%w: configuration %s not found", ErrNotFound, configID)
	}

	cfg := b.configurations[configID]
	delete(b.configurations, configID)
	delete(b.tags, cfg.Arn)

	return nil
}

// DescribeConfigurationRevision returns a specific revision of a configuration.
func (b *InMemoryBackend) DescribeConfigurationRevision(
	configID string,
	revision int32,
) (*ConfigurationRevision, string, error) {
	b.mu.RLock("DescribeConfigurationRevision")
	defer b.mu.RUnlock()

	cfg, ok := b.configurations[configID]
	if !ok {
		return nil, "", fmt.Errorf("%w: configuration %s not found", ErrNotFound, configID)
	}

	for _, rev := range cfg.Revisions {
		if rev.Revision == revision {
			cp := rev
			data := cfg.Data[revision]

			return &cp, data, nil
		}
	}

	return nil, "", fmt.Errorf("%w: revision %d not found for configuration %s", ErrNotFound, revision, configID)
}

// ListConfigurationRevisions returns all revisions for a configuration.
func (b *InMemoryBackend) ListConfigurationRevisions(configID string) ([]ConfigurationRevision, error) {
	b.mu.RLock("ListConfigurationRevisions")
	defer b.mu.RUnlock()

	cfg, ok := b.configurations[configID]
	if !ok {
		return nil, fmt.Errorf("%w: configuration %s not found", ErrNotFound, configID)
	}

	revisions := make([]ConfigurationRevision, len(cfg.Revisions))
	copy(revisions, cfg.Revisions)

	return revisions, nil
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

// DescribeBrokerEngineTypes returns supported broker engine types and versions.
// If engineType is non-empty, the result is filtered to that engine type.
func (b *InMemoryBackend) DescribeBrokerEngineTypes(engineType string) []BrokerEngineType {
	all := []BrokerEngineType{
		{
			EngineType: EngineTypeActiveMQ,
			EngineVersions: []EngineVersion{
				{Name: engineVersion5183},
				{Name: engineVersion5176},
				{Name: engineVersion5167},
				{Name: engineVersion51516},
			},
		},
		{
			EngineType: EngineTypeRabbitMQ,
			EngineVersions: []EngineVersion{
				{Name: "3.13.2"},
				{Name: "3.12.13"},
				{Name: "3.11.28"},
				{Name: "3.10.25"},
			},
		},
	}

	if engineType == "" {
		return all
	}

	for _, et := range all {
		if et.EngineType == engineType {
			return []BrokerEngineType{et}
		}
	}

	return []BrokerEngineType{}
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

// DescribeBrokerInstanceOptions returns broker instance options.
// Filters are optional; empty string means no filter applied.
func (b *InMemoryBackend) DescribeBrokerInstanceOptions(
	engineType, hostInstanceType, storageType string,
) []BrokerInstanceOption {
	zones := []AvailabilityZone{
		{Name: b.region + "a"},
		{Name: b.region + "b"},
		{Name: b.region + "c"},
	}

	all := []BrokerInstanceOption{
		{
			EngineType:               EngineTypeActiveMQ,
			HostInstanceType:         "mq.m5.large",
			StorageType:              "efs",
			AvailabilityZones:        zones,
			SupportedDeploymentModes: []string{DeploymentModeSingleInstance, "ACTIVE_STANDBY_MULTI_AZ"},
			SupportedEngineVersions: []string{
				engineVersion5183,
				engineVersion5176,
				engineVersion5167,
				engineVersion51516,
			},
		},
		{
			EngineType:               EngineTypeActiveMQ,
			HostInstanceType:         "mq.m5.xlarge",
			StorageType:              "efs",
			AvailabilityZones:        zones,
			SupportedDeploymentModes: []string{DeploymentModeSingleInstance, "ACTIVE_STANDBY_MULTI_AZ"},
			SupportedEngineVersions: []string{
				engineVersion5183,
				engineVersion5176,
				engineVersion5167,
				engineVersion51516,
			},
		},
		{
			EngineType:               EngineTypeRabbitMQ,
			HostInstanceType:         "mq.m5.large",
			StorageType:              "ebs",
			AvailabilityZones:        zones,
			SupportedDeploymentModes: []string{DeploymentModeSingleInstance, "CLUSTER_MULTI_AZ"},
			SupportedEngineVersions:  []string{"3.13.2", "3.12.13", "3.11.28", "3.10.25"},
		},
	}

	result := make([]BrokerInstanceOption, 0, len(all))

	for _, opt := range all {
		if engineType != "" && opt.EngineType != engineType {
			continue
		}

		if hostInstanceType != "" && opt.HostInstanceType != hostInstanceType {
			continue
		}

		if storageType != "" && opt.StorageType != storageType {
			continue
		}

		result = append(result, opt)
	}

	return result
}

// Promote promotes a standby broker to the primary role.
// In the in-memory stub this is a no-op that validates the broker exists.
func (b *InMemoryBackend) Promote(brokerID, mode string) (*Broker, error) {
	b.mu.RLock("Promote")
	defer b.mu.RUnlock()

	if mode != PromoteModeFailover && mode != PromoteModeSwitchover {
		return nil, fmt.Errorf(
			"%w: mode must be FAILOVER or SWITCHOVER, got %q",
			ErrValidation, mode,
		)
	}

	br := b.lookupBroker(brokerID)
	if br == nil {
		return nil, fmt.Errorf("%w: broker %s not found", ErrNotFound, brokerID)
	}

	return b.copyBroker(br), nil
}

// --- Tag operations ---

// ListTags returns tags for a resource ARN.
func (b *InMemoryBackend) ListTags(resourceARN string) map[string]string {
	b.mu.RLock("ListTags")
	defer b.mu.RUnlock()

	t := b.tags[resourceARN]
	cp := make(map[string]string, len(t))
	maps.Copy(cp, t)

	return cp
}

// CreateTags adds or updates tags for a resource ARN.
// Note: b.tags[arn] and the corresponding broker/config Tags field share
// the same map pointer, so a single write here updates both automatically.
func (b *InMemoryBackend) CreateTags(resourceARN string, tags map[string]string) {
	b.mu.Lock("CreateTags")
	defer b.mu.Unlock()

	if b.tags[resourceARN] == nil {
		b.tags[resourceARN] = make(map[string]string)
	}

	maps.Copy(b.tags[resourceARN], tags)
}

// DeleteTags removes the specified tag keys from a resource ARN.
// Note: b.tags[arn] and the corresponding broker/config Tags field share
// the same map pointer, so a single delete here updates both automatically.
func (b *InMemoryBackend) DeleteTags(resourceARN string, tagKeys []string) {
	b.mu.Lock("DeleteTags")
	defer b.mu.Unlock()

	for _, k := range tagKeys {
		delete(b.tags[resourceARN], k)
	}
}
