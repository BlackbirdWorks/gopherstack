package mq

import (
	"fmt"
	"maps"
	"net"
	"sort"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

const (
	// defaultHostInstanceType is the default broker instance type.
	defaultHostInstanceType = "mq.m5.large"

	engineVersion5183  = "5.18.3"
	engineVersion5176  = "5.17.6"
	engineVersion5167  = "5.16.7"
	engineVersion51516 = "5.15.16"
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
	if err := validateCreateBrokerInput(name, deploymentMode, engineType); err != nil {
		return nil, err
	}

	if err := validateTagsMap(tags); err != nil {
		return nil, err
	}

	b.mu.Lock("CreateBroker")
	defer b.mu.Unlock()

	// Idempotency: a retry with the same CreatorRequestId returns the existing broker.
	if opts != nil && opts.CreatorRequestID != "" {
		if existing := b.findBrokerByCreatorRequestID(opts.CreatorRequestID); existing != nil {
			return b.copyBroker(existing), nil
		}
	}

	// Check for duplicate by name.
	if b.lookupBrokerByName(name) != nil {
		return nil, fmt.Errorf("%w: broker %s already exists", ErrAlreadyExists, name)
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

	instances := buildBrokerInstances(engineType, deploymentMode, b.region, id)

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

	b.brokers.Put(br)
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
	br.DataReplicationMode = opts.DataReplicationMode

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

	if opts.DataReplicationPrimaryBrokerArn != "" {
		br.DataReplicationMetadata = &DataReplicationMetadata{
			DataReplicationCounterpart: opts.DataReplicationPrimaryBrokerArn,
		}
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
	var found *Broker

	b.brokers.Range(func(br *Broker) bool {
		if br.CreatorRequestID == reqID {
			found = br

			return false
		}

		return true
	})

	return found
}

// lookupBrokerByName returns a broker matching the given name exactly, or nil.
// Caller must hold a lock.
func (b *InMemoryBackend) lookupBrokerByName(name string) *Broker {
	var found *Broker

	b.brokers.Range(func(br *Broker) bool {
		if br.BrokerName == name {
			found = br

			return false
		}

		return true
	})

	return found
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

	if br.BrokerState == BrokerStateDeleting {
		b.brokers.Delete(br.BrokerID)
		delete(b.tags, br.BrokerArn)

		return nil, fmt.Errorf("%w: broker %s not found", ErrNotFound, brokerID)
	}

	cp := b.copyBroker(br)
	promoteBrokerReboot(br)

	return cp, nil
}

// ListBrokers returns all brokers sorted by name.
func (b *InMemoryBackend) ListBrokers() []*Broker {
	b.mu.Lock("ListBrokers")
	defer b.mu.Unlock()

	all := b.brokers.All()
	list := make([]*Broker, 0, len(all))

	for _, br := range all {
		if br.BrokerState == BrokerStateDeleting {
			b.brokers.Delete(br.BrokerID)
			delete(b.tags, br.BrokerArn)

			continue
		}

		list = append(list, b.copyBroker(br))
		promoteBrokerReboot(br)
	}

	sort.Slice(list, func(i, j int) bool { return list[i].BrokerName < list[j].BrokerName })

	return list
}

// DeleteBroker transitions a broker to DELETION_IN_PROGRESS and returns its
// identifiers. The broker is fully removed from the map on the next
// DescribeBroker / ListBrokers call via promoteDeletingToDeleted.
func (b *InMemoryBackend) DeleteBroker(brokerID string) (*Broker, error) {
	b.mu.Lock("DeleteBroker")
	defer b.mu.Unlock()

	br := b.lookupBroker(brokerID)
	if br == nil {
		return nil, fmt.Errorf("%w: broker %s not found", ErrNotFound, brokerID)
	}

	cp := b.copyBroker(br)
	br.BrokerState = BrokerStateDeleting

	return cp, nil
}

// promoteBrokerReboot advances any broker stuck in REBOOT_IN_PROGRESS back to
// RUNNING, atomically applying every field UpdateBroker staged into a
// Pending* slot (see applyBrokerCoreFields/applyUpdateBrokerOptions) plus any
// staged user create/update/delete (see users.go). Real Amazon MQ only takes
// these changes live on the next reboot -- see DescribeBrokerOutput's
// pendingEngineVersion/pendingHostInstanceType/pendingSecurityGroups/
// pendingAuthenticationStrategy/pendingLdapServerMetadata/
// Configurations.pending/Logs.pending/pendingDataReplicationMode wire fields.
// Caller must hold a write lock or call from a context where promoting
// in-place is safe (the result is only observed via a returned copy taken
// before this runs, e.g. DescribeBroker/ListBrokers).
func promoteBrokerReboot(br *Broker) {
	if br == nil || br.BrokerState != BrokerStateRebooting {
		return
	}

	promotePendingScalarFields(br)
	promotePendingLogs(br)
	promotePendingConfiguration(br)
	promotePendingDataReplication(br)
	promoteBrokerUsers(br)

	br.BrokerState = BrokerStateRunning
}

// promotePendingScalarFields swaps every scalar/simple Pending* field into
// its live counterpart. Caller must hold a write lock.
func promotePendingScalarFields(br *Broker) {
	if br.PendingEngineVersion != "" {
		br.EngineVersion = br.PendingEngineVersion
		br.PendingEngineVersion = ""
	}

	if br.PendingHostInstanceType != "" {
		br.HostInstanceType = br.PendingHostInstanceType
		br.PendingHostInstanceType = ""
	}

	if br.PendingSecurityGroups != nil {
		br.SecurityGroups = br.PendingSecurityGroups
		br.PendingSecurityGroups = nil
	}

	if br.PendingAuthStrategy != "" {
		br.AuthenticationStrategy = br.PendingAuthStrategy
		br.PendingAuthStrategy = ""
	}

	if br.PendingLdapServerMetadata != nil {
		br.LdapServerMetadata = br.PendingLdapServerMetadata
		br.PendingLdapServerMetadata = nil
	}
}

// promotePendingLogs applies a staged Logs change (LogsSummary.Pending) to
// the broker's active log settings. Caller must hold a write lock.
func promotePendingLogs(br *Broker) {
	if br.LogsSummary == nil || br.LogsSummary.Pending == nil {
		return
	}

	pending := br.LogsSummary.Pending
	br.Logs = pending
	br.LogsSummary.General = pending.General
	br.LogsSummary.Audit = pending.Audit
	br.LogsSummary.Pending = nil
}

// promotePendingConfiguration swaps a staged Configurations.Pending
// association into Current, pushing the prior Current onto History (matching
// how UpdateConfiguration's revision history grows). Caller must hold a
// write lock.
func promotePendingConfiguration(br *Broker) {
	if br.Configurations == nil || br.Configurations.Pending == nil {
		return
	}

	if br.Configurations.Current != nil {
		br.Configurations.History = append(br.Configurations.History, *br.Configurations.Current)
	}

	br.Configurations.Current = br.Configurations.Pending
	br.Configurations.Pending = nil
}

// promotePendingDataReplication applies a staged data-replication-mode
// change. Caller must hold a write lock.
func promotePendingDataReplication(br *Broker) {
	if br.PendingDataReplicationMode == "" {
		return
	}

	br.DataReplicationMode = br.PendingDataReplicationMode
	br.DataReplicationMetadata = br.PendingDataReplicationMeta
	br.PendingDataReplicationMode = ""
	br.PendingDataReplicationMeta = nil
}

// buildBrokerInstances returns the correct number of BrokerInstance entries
// for the given engine type and deployment mode.
func buildBrokerInstances(engineType, deploymentMode, region, id string) []BrokerInstance {
	consoleURL := fmt.Sprintf("http://%s.mq.%s.amazonaws.com:8162", id, region)
	endpoint := buildEndpoint(engineType, region, id)

	switch deploymentMode {
	case DeploymentModeActiveStandby:
		return []BrokerInstance{
			{
				ConsoleURL: fmt.Sprintf("http://%s-1.mq.%s.amazonaws.com:8162", id, region),
				Endpoints:  []string{buildEndpointSuffix(engineType, region, id, "-1")},
			},
			{
				ConsoleURL: fmt.Sprintf("http://%s-2.mq.%s.amazonaws.com:8162", id, region),
				Endpoints:  []string{buildEndpointSuffix(engineType, region, id, "-2")},
			},
		}
	case DeploymentModeCluster:
		return []BrokerInstance{
			{
				ConsoleURL: fmt.Sprintf("http://%s-1.mq.%s.amazonaws.com:15671", id, region),
				Endpoints:  []string{buildEndpointSuffix(engineType, region, id, "-1")},
			},
			{
				ConsoleURL: fmt.Sprintf("http://%s-2.mq.%s.amazonaws.com:15671", id, region),
				Endpoints:  []string{buildEndpointSuffix(engineType, region, id, "-2")},
			},
			{
				ConsoleURL: fmt.Sprintf("http://%s-3.mq.%s.amazonaws.com:15671", id, region),
				Endpoints:  []string{buildEndpointSuffix(engineType, region, id, "-3")},
			},
		}
	default:
		return []BrokerInstance{{ConsoleURL: consoleURL, Endpoints: []string{endpoint}}}
	}
}

// buildEndpointSuffix builds an endpoint URL with a host suffix (e.g. "-1", "-2").
func buildEndpointSuffix(engineType, region, id, suffix string) string {
	host := id + suffix + ".mq." + region + ".amazonaws.com"

	switch engineType {
	case EngineTypeRabbitMQ:
		return "amqps://" + net.JoinHostPort(host, "5671")
	default:
		return "ssl://" + net.JoinHostPort(host, "61617")
	}
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

// applyBrokerCoreFields stages the non-optional update fields on a broker.
// Real Amazon MQ only takes EngineVersion/HostInstanceType/SecurityGroups
// live on the next reboot (see DescribeBrokerOutput's pendingEngineVersion/
// pendingHostInstanceType/pendingSecurityGroups), so these are written to
// their Pending* counterparts here and swapped in by promoteBrokerReboot.
// AutoMinorVersionUpgrade has no Pending* counterpart in the SDK and applies
// immediately, matching UpdateBrokerInput's own doc ("Automatic upgrades
// occur during the scheduled maintenance window or after a manual broker
// reboot" -- the flag itself, unlike the version it gates, is not staged).
func applyBrokerCoreFields(
	br *Broker,
	engineVersion, hostInstanceType string,
	autoMinorVersionUpgrade *bool,
	securityGroups []string,
) {
	if engineVersion != "" {
		br.PendingEngineVersion = engineVersion
	}

	if hostInstanceType != "" {
		br.PendingHostInstanceType = hostInstanceType
	}

	if autoMinorVersionUpgrade != nil {
		br.AutoMinorVersionUpgrade = *autoMinorVersionUpgrade
	}

	if securityGroups != nil {
		br.PendingSecurityGroups = securityGroups
	}
}

// applyUpdateBrokerOptions stages the optional update fields on a broker.
// AuthenticationStrategy/Logs/LdapServerMetadata/Configuration/
// DataReplicationMode all have dedicated Pending* (or nested .Pending)
// counterparts on DescribeBrokerOutput and only take effect after the next
// reboot (see promoteBrokerReboot). MaintenanceWindowStartTime has no
// Pending* counterpart in the SDK and applies immediately -- changing when
// maintenance happens does not itself require a reboot.
func applyUpdateBrokerOptions(br *Broker, opts *UpdateBrokerOptions) {
	if opts == nil {
		return
	}

	if opts.AuthenticationStrategy != "" {
		br.PendingAuthStrategy = opts.AuthenticationStrategy
	}

	if opts.Logs != nil {
		if br.LogsSummary == nil {
			br.LogsSummary = &LogsSummary{
				GeneralLogGroup: logGroupName(br.BrokerID, "general"),
				AuditLogGroup:   logGroupName(br.BrokerID, "audit"),
			}
		}

		br.LogsSummary.Pending = opts.Logs
	}

	if opts.LdapServerMetadata != nil {
		br.PendingLdapServerMetadata = opts.LdapServerMetadata
	}

	if opts.MaintenanceWindowStartTime != nil {
		br.MaintenanceWindowStartTime = opts.MaintenanceWindowStartTime
	}

	if opts.Configuration != nil {
		if br.Configurations == nil {
			br.Configurations = &Configurations{}
		}

		br.Configurations.Pending = opts.Configuration
	}

	if opts.DataReplicationMode != "" {
		br.PendingDataReplicationMode = opts.DataReplicationMode
	}
}

// lookupBroker finds a broker by ID or by name; caller must hold a lock.
func (b *InMemoryBackend) lookupBroker(brokerID string) *Broker {
	if br, ok := b.brokers.Get(brokerID); ok {
		return br
	}

	return b.lookupBrokerByName(brokerID)
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
			StorageType:              StorageTypeEFS,
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
			StorageType:              StorageTypeEFS,
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
			StorageType:              StorageTypeEBS,
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
