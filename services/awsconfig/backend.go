package awsconfig

import (
	"fmt"
	"slices"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const (
	recorderStatusActive  = "ACTIVE"
	recorderStatusPending = "PENDING"
	recorderStatusSuccess = "SUCCESS"
)

var (
	// ErrNotFound is returned when a configuration recorder is not found.
	ErrNotFound = awserr.New("NoSuchConfigurationRecorder", awserr.ErrNotFound)
	// ErrNoSuchDeliveryChannel is returned when a delivery channel is not found.
	ErrNoSuchDeliveryChannel = awserr.New("NoSuchDeliveryChannelException", awserr.ErrNotFound)
	// ErrNoSuchConfigRule is returned when a config rule is not found.
	ErrNoSuchConfigRule = awserr.New("NoSuchConfigRuleException", awserr.ErrNotFound)
	// ErrNoSuchAggregator is returned when a configuration aggregator is not found.
	ErrNoSuchAggregator = awserr.New("NoSuchConfigurationAggregatorException", awserr.ErrNotFound)
	// ErrNoSuchConformancePack is returned when a conformance pack is not found.
	ErrNoSuchConformancePack = awserr.New("NoSuchConformancePackException", awserr.ErrNotFound)
	// ErrNoSuchOrganizationConfigRule is returned when an organization config rule is not found.
	ErrNoSuchOrganizationConfigRule = awserr.New("NoSuchOrganizationConfigRuleException", awserr.ErrNotFound)
	// ErrNoSuchOrganizationConformancePack is returned when an org conformance pack is not found.
	ErrNoSuchOrganizationConformancePack = awserr.New(
		"OrganizationConformancePackNotFoundException",
		awserr.ErrNotFound,
	)
	// ErrNoSuchAggregationAuthorization is returned when an aggregation authorization is not found.
	ErrNoSuchAggregationAuthorization = awserr.New("NoSuchAggregationAuthorizationException", awserr.ErrNotFound)
	// ErrAlreadyExists is returned when a resource already exists.
	ErrAlreadyExists = awserr.New("MaxNumberOfConfigurationRecordersExceededException", awserr.ErrAlreadyExists)
	// ErrNoDeliveryChannel is returned when starting a recorder with no delivery channel configured.
	ErrNoDeliveryChannel = awserr.New("NoAvailableDeliveryChannelException", awserr.ErrInvalidParameter)
	// ErrValidation is returned when a required field is missing or invalid.
	ErrValidation = awserr.New("ValidationException", awserr.ErrInvalidParameter)
)

// RecordingGroup holds the resource recording configuration for a recorder.
type RecordingGroup struct {
	ResourceTypes              []string `json:"resourceTypes,omitempty"`
	AllSupported               bool     `json:"allSupported,omitempty"`
	IncludeGlobalResourceTypes bool     `json:"includeGlobalResourceTypes,omitempty"`
}

// ConfigurationRecorder represents an AWS Config configuration recorder.
type ConfigurationRecorder struct {
	RecordingGroup *RecordingGroup `json:"recordingGroup,omitempty"`
	Name           string          `json:"name"`
	RoleARN        string          `json:"roleARN"`
	Status         string          `json:"status,omitempty"` // PENDING or ACTIVE
}

// DeliverySnapshotProperties holds snapshot delivery configuration for a channel.
type DeliverySnapshotProperties struct {
	DeliveryFrequency string `json:"deliveryFrequency,omitempty"`
}

// DeliveryChannel represents an AWS Config delivery channel.
type DeliveryChannel struct {
	ConfigSnapshotDeliveryProperties *DeliverySnapshotProperties `json:"configSnapshotDeliveryProperties,omitempty"`
	Name                             string                      `json:"name"`
	S3Bucket                         string                      `json:"s3BucketName,omitempty"`
	S3KeyPrefix                      string                      `json:"s3KeyPrefix,omitempty"`
	SNSArn                           string                      `json:"snsTopicARN,omitempty"`
}

// AggregationAuthorization represents an AWS Config aggregation authorization.
type AggregationAuthorization struct {
	AggregationAuthorizationArn string `json:"AggregationAuthorizationArn,omitempty"`
	AuthorizedAccountID         string `json:"authorizedAccountId"`
	AuthorizedAwsRegion         string `json:"authorizedAwsRegion"`
	CreationTime                string `json:"CreationTime,omitempty"`
}

// ConfigRuleSource represents the source definition of an AWS Config config rule.
type ConfigRuleSource struct {
	Owner            string `json:"Owner,omitempty"`
	SourceIdentifier string `json:"SourceIdentifier,omitempty"`
}

// ConfigRuleScope restricts which resources trigger an AWS Config rule.
type ConfigRuleScope struct {
	ComplianceResourceID    string   `json:"ComplianceResourceId,omitempty"`
	TagKey                  string   `json:"TagKey,omitempty"`
	TagValue                string   `json:"TagValue,omitempty"`
	ComplianceResourceTypes []string `json:"ComplianceResourceTypes,omitempty"`
}

// ConfigRule represents an AWS Config config rule.
type ConfigRule struct {
	Source                    *ConfigRuleSource `json:"Source,omitempty"`
	Scope                     *ConfigRuleScope  `json:"Scope,omitempty"`
	ConfigRuleName            string            `json:"ConfigRuleName"`
	ConfigRuleArn             string            `json:"ConfigRuleArn,omitempty"`
	ConfigRuleID              string            `json:"ConfigRuleId,omitempty"`
	Description               string            `json:"Description,omitempty"`
	InputParameters           string            `json:"InputParameters,omitempty"`
	MaximumExecutionFrequency string            `json:"MaximumExecutionFrequency,omitempty"`
	ConfigRuleState           string            `json:"ConfigRuleState,omitempty"`
}

// AccountAggregationSource identifies AWS accounts to aggregate from.
type AccountAggregationSource struct {
	AccountIDs    []string `json:"AccountIds"`
	AwsRegions    []string `json:"AwsRegions,omitempty"`
	AllAwsRegions bool     `json:"AllAwsRegions,omitempty"`
}

// OrganizationAggregationSource identifies an organization to aggregate from.
type OrganizationAggregationSource struct {
	RoleArn       string   `json:"RoleArn"`
	AwsRegions    []string `json:"AwsRegions,omitempty"`
	AllAwsRegions bool     `json:"AllAwsRegions,omitempty"`
}

// ConfigurationAggregator represents an AWS Config configuration aggregator.
type ConfigurationAggregator struct {
	OrganizationAggregationSource *OrganizationAggregationSource `json:"OrganizationAggregationSource,omitempty"`
	ConfigurationAggregatorArn    string                         `json:"ConfigurationAggregatorArn,omitempty"`
	ConfigurationAggregatorName   string                         `json:"ConfigurationAggregatorName"`
	CreationTime                  string                         `json:"CreationTime,omitempty"`
	AccountAggregationSources     []AccountAggregationSource     `json:"AccountAggregationSources,omitempty"`
}

// ConformancePack represents an AWS Config conformance pack.
type ConformancePack struct {
	ConformancePackArn      string `json:"ConformancePackArn,omitempty"`
	ConformancePackID       string `json:"ConformancePackId,omitempty"`
	ConformancePackName     string `json:"ConformancePackName"`
	DeliveryS3Bucket        string `json:"DeliveryS3Bucket,omitempty"`
	DeliveryS3KeyPrefix     string `json:"DeliveryS3KeyPrefix,omitempty"`
	LastUpdateRequestedTime string `json:"LastUpdateRequestedTime,omitempty"`
}

// OrganizationConfigRule represents an AWS Config organization config rule.
type OrganizationConfigRule struct {
	OrganizationConfigRuleName string `json:"organizationConfigRuleName"`
}

// OrganizationConformancePack represents an AWS Config organization conformance pack.
type OrganizationConformancePack struct {
	OrganizationConformancePackName string `json:"organizationConformancePackName"`
}

// StoredQuery represents an AWS Config stored query.
type StoredQuery struct {
	QueryName string `json:"QueryName"`
	QueryID   string `json:"QueryId,omitempty"`
}

// Tag represents an AWS resource tag.
type Tag struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
}

// ConfigurationRecorderStatus represents the recording status of a recorder.
type ConfigurationRecorderStatus struct {
	LastErrorCode string `json:"lastErrorCode,omitempty"`
	LastStartTime string `json:"lastStartTime,omitempty"`
	LastStatus    string `json:"lastStatus,omitempty"`
	LastStopTime  string `json:"lastStopTime,omitempty"`
	Name          string `json:"name"`
	Recording     bool   `json:"recording"`
}

// ConfigurationRecorderSummary is a lightweight summary returned by ListConfigurationRecorders.
type ConfigurationRecorderSummary struct {
	Arn            string `json:"arn"`
	Name           string `json:"name"`
	RecordingScope string `json:"recordingScope"`
}

// StoredQueryMetadata is summary metadata returned by ListStoredQueries.
type StoredQueryMetadata struct {
	QueryArn  string `json:"QueryArn"`
	QueryID   string `json:"QueryId"`
	QueryName string `json:"QueryName"`
}

// BaseConfigurationItem is a lightweight configuration snapshot for a single resource.
type BaseConfigurationItem struct {
	ResourceType string `json:"resourceType,omitempty"`
	ResourceID   string `json:"resourceId,omitempty"`
}

// AggregateResourceIdentifier identifies a resource in an aggregator.
type AggregateResourceIdentifier struct {
	SourceAccountID string `json:"SourceAccountId,omitempty"`
	SourceRegion    string `json:"SourceRegion,omitempty"`
	ResourceID      string `json:"ResourceId,omitempty"`
	ResourceType    string `json:"ResourceType,omitempty"`
}

// ResourceKey identifies a resource by type and ID.
type ResourceKey struct {
	ResourceType string `json:"resourceType,omitempty"`
	ResourceID   string `json:"resourceId,omitempty"`
}

// InMemoryBackend is the in-memory store for AWS Config resources.
type InMemoryBackend struct {
	recorders              map[string]*ConfigurationRecorder
	channels               map[string]*DeliveryChannel
	aggregationAuths       map[string]*AggregationAuthorization
	configRules            map[string]*ConfigRule
	ruleEvaluations        map[string]string // rule name → compliance type after evaluation
	aggregators            map[string]*ConfigurationAggregator
	conformancePacks       map[string]*ConformancePack
	orgConfigRules         map[string]*OrganizationConfigRule
	orgConformancePacks    map[string]*OrganizationConformancePack
	storedQueries          map[string]*StoredQuery
	resourceTags           map[string][]Tag                          // ARN → tags
	retentionConfigs       map[string]*RetentionConfiguration        // name → config
	remediationConfigs     map[string]*RemediationConfiguration      // rule name → config
	remediationExceptions  map[string][]RemediationException         // rule name → exceptions
	resourceConfigs        map[string]map[string]*ResourceConfigItem // type → id → item
	customRulePolicies     map[string]string                         // rule name → policy text
	orgCustomRulePolicies  map[string]string                         // rule name → policy text
	mu                     *lockmetrics.RWMutex
	accountID              string
	region                 string
	ruleCounter            int
	conformancePackCounter int
	aggregatorCounter      int
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend() *InMemoryBackend {
	return NewInMemoryBackendWithMeta("123456789012", "us-east-1")
}

// NewInMemoryBackendWithMeta creates a new InMemoryBackend with account and region context.
func NewInMemoryBackendWithMeta(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		recorders:             make(map[string]*ConfigurationRecorder),
		channels:              make(map[string]*DeliveryChannel),
		aggregationAuths:      make(map[string]*AggregationAuthorization),
		configRules:           make(map[string]*ConfigRule),
		ruleEvaluations:       make(map[string]string),
		aggregators:           make(map[string]*ConfigurationAggregator),
		conformancePacks:      make(map[string]*ConformancePack),
		orgConfigRules:        make(map[string]*OrganizationConfigRule),
		orgConformancePacks:   make(map[string]*OrganizationConformancePack),
		storedQueries:         make(map[string]*StoredQuery),
		resourceTags:          make(map[string][]Tag),
		retentionConfigs:      make(map[string]*RetentionConfiguration),
		remediationConfigs:    make(map[string]*RemediationConfiguration),
		remediationExceptions: make(map[string][]RemediationException),
		resourceConfigs:       make(map[string]map[string]*ResourceConfigItem),
		customRulePolicies:    make(map[string]string),
		orgCustomRulePolicies: make(map[string]string),
		mu:                    lockmetrics.New("awsconfig"),
		accountID:             accountID,
		region:                region,
	}
}

// PutConfigurationRecorder creates or updates a configuration recorder.
// When updating an existing recorder, the Status is preserved; RoleARN and RecordingGroup are updated.
// A new recorder starts in PENDING state.
func (b *InMemoryBackend) PutConfigurationRecorder(name, roleARN string, recordingGroup *RecordingGroup) error {
	if name == "" {
		return fmt.Errorf("%w: ConfigurationRecorder name is required", ErrValidation)
	}

	if roleARN == "" {
		return fmt.Errorf("%w: ConfigurationRecorder roleARN is required", ErrValidation)
	}

	b.mu.Lock("PutConfigurationRecorder")
	defer b.mu.Unlock()

	if existing, ok := b.recorders[name]; ok {
		existing.RoleARN = roleARN
		existing.RecordingGroup = recordingGroup

		return nil
	}

	b.recorders[name] = &ConfigurationRecorder{
		Name:           name,
		RoleARN:        roleARN,
		Status:         recorderStatusPending,
		RecordingGroup: recordingGroup,
	}

	return nil
}

// DescribeConfigurationRecorders returns configuration recorders filtered by the
// provided name list.  An empty/nil names list returns all recorders sorted by name.
func (b *InMemoryBackend) DescribeConfigurationRecorders(names []string) []ConfigurationRecorder {
	b.mu.RLock("DescribeConfigurationRecorders")
	defer b.mu.RUnlock()

	out := make([]ConfigurationRecorder, 0, len(b.recorders))

	if len(names) == 0 {
		for _, r := range b.recorders {
			out = append(out, *r)
		}
	} else {
		for _, n := range names {
			if r, ok := b.recorders[n]; ok {
				out = append(out, *r)
			}
		}
	}

	slices.SortFunc(out, func(a, b ConfigurationRecorder) int {
		if a.Name < b.Name {
			return -1
		}

		if a.Name > b.Name {
			return 1
		}

		return 0
	})

	return out
}

// StartConfigurationRecorder starts a configuration recorder.
func (b *InMemoryBackend) StartConfigurationRecorder(name string) error {
	if name == "" {
		return fmt.Errorf("%w: ConfigurationRecorderName is required", ErrValidation)
	}

	b.mu.Lock("StartConfigurationRecorder")
	defer b.mu.Unlock()

	r, ok := b.recorders[name]
	if !ok {
		return fmt.Errorf("%w: %s", ErrNotFound, name)
	}

	if len(b.channels) == 0 {
		return fmt.Errorf("%w: no delivery channel configured", ErrNoDeliveryChannel)
	}

	r.Status = recorderStatusActive

	return nil
}

// StopConfigurationRecorder stops an active configuration recorder.
func (b *InMemoryBackend) StopConfigurationRecorder(name string) error {
	if name == "" {
		return fmt.Errorf("%w: ConfigurationRecorderName is required", ErrValidation)
	}

	b.mu.Lock("StopConfigurationRecorder")
	defer b.mu.Unlock()

	r, ok := b.recorders[name]
	if !ok {
		return fmt.Errorf("%w: %s", ErrNotFound, name)
	}

	r.Status = recorderStatusPending

	return nil
}

// PutDeliveryChannel creates or updates a delivery channel.
func (b *InMemoryBackend) PutDeliveryChannel(
	name, s3Bucket, snsArn, s3KeyPrefix string,
	props *DeliverySnapshotProperties,
) error {
	if name == "" {
		return fmt.Errorf("%w: DeliveryChannel name is required", ErrValidation)
	}

	if s3Bucket == "" {
		return fmt.Errorf("%w: DeliveryChannel s3BucketName is required", ErrValidation)
	}

	b.mu.Lock("PutDeliveryChannel")
	defer b.mu.Unlock()

	b.channels[name] = &DeliveryChannel{
		Name:                             name,
		S3Bucket:                         s3Bucket,
		SNSArn:                           snsArn,
		S3KeyPrefix:                      s3KeyPrefix,
		ConfigSnapshotDeliveryProperties: props,
	}

	return nil
}

// DescribeDeliveryChannels returns delivery channels filtered by the provided name list.
// An empty/nil names list returns all channels sorted by name.
func (b *InMemoryBackend) DescribeDeliveryChannels(names []string) []DeliveryChannel {
	b.mu.RLock("DescribeDeliveryChannels")
	defer b.mu.RUnlock()

	out := make([]DeliveryChannel, 0, len(b.channels))

	if len(names) == 0 {
		for _, c := range b.channels {
			out = append(out, *c)
		}
	} else {
		for _, n := range names {
			if c, ok := b.channels[n]; ok {
				out = append(out, *c)
			}
		}
	}

	slices.SortFunc(out, func(a, b DeliveryChannel) int {
		if a.Name < b.Name {
			return -1
		}

		if a.Name > b.Name {
			return 1
		}

		return 0
	})

	return out
}

// DeleteDeliveryChannel removes a delivery channel by name.
func (b *InMemoryBackend) DeleteDeliveryChannel(name string) error {
	if name == "" {
		return fmt.Errorf("%w: DeliveryChannelName is required", ErrValidation)
	}

	b.mu.Lock("DeleteDeliveryChannel")
	defer b.mu.Unlock()

	if _, ok := b.channels[name]; !ok {
		return fmt.Errorf("%w: %s", ErrNoSuchDeliveryChannel, name)
	}

	delete(b.channels, name)

	return nil
}

// DeleteConfigurationRecorder removes a configuration recorder by name.
func (b *InMemoryBackend) DeleteConfigurationRecorder(name string) error {
	if name == "" {
		return fmt.Errorf("%w: ConfigurationRecorderName is required", ErrValidation)
	}

	b.mu.Lock("DeleteConfigurationRecorder")
	defer b.mu.Unlock()

	if _, ok := b.recorders[name]; !ok {
		return fmt.Errorf("%w: %s", ErrNotFound, name)
	}

	delete(b.recorders, name)

	return nil
}

// recorderStatus builds a ConfigurationRecorderStatus from a recorder.
func recorderStatus(r *ConfigurationRecorder) ConfigurationRecorderStatus {
	recording := r.Status == recorderStatusActive
	lastStatus := recorderStatusPending
	if recording {
		lastStatus = recorderStatusSuccess
	}

	return ConfigurationRecorderStatus{
		Name:       r.Name,
		Recording:  recording,
		LastStatus: lastStatus,
	}
}

// DescribeConfigurationRecorderStatus returns recording status for recorders filtered
// by the provided name list.  An empty/nil list returns status for all recorders,
// sorted by name.
func (b *InMemoryBackend) DescribeConfigurationRecorderStatus(names []string) []ConfigurationRecorderStatus {
	b.mu.RLock("DescribeConfigurationRecorderStatus")
	defer b.mu.RUnlock()

	out := make([]ConfigurationRecorderStatus, 0, len(b.recorders))

	if len(names) == 0 {
		for _, r := range b.recorders {
			out = append(out, recorderStatus(r))
		}
	} else {
		for _, n := range names {
			if r, ok := b.recorders[n]; ok {
				out = append(out, recorderStatus(r))
			}
		}
	}

	slices.SortFunc(out, func(a, b ConfigurationRecorderStatus) int {
		if a.Name < b.Name {
			return -1
		}

		if a.Name > b.Name {
			return 1
		}

		return 0
	})

	return out
}

// Reset clears all in-memory state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.recorders = make(map[string]*ConfigurationRecorder)
	b.channels = make(map[string]*DeliveryChannel)
	b.aggregationAuths = make(map[string]*AggregationAuthorization)
	b.configRules = make(map[string]*ConfigRule)
	b.ruleEvaluations = make(map[string]string)
	b.ruleCounter = 0
	b.aggregators = make(map[string]*ConfigurationAggregator)
	b.conformancePacks = make(map[string]*ConformancePack)
	b.orgConfigRules = make(map[string]*OrganizationConfigRule)
	b.orgConformancePacks = make(map[string]*OrganizationConformancePack)
	b.storedQueries = make(map[string]*StoredQuery)
	b.resourceTags = make(map[string][]Tag)
	b.retentionConfigs = make(map[string]*RetentionConfiguration)
	b.remediationConfigs = make(map[string]*RemediationConfiguration)
	b.remediationExceptions = make(map[string][]RemediationException)
	b.resourceConfigs = make(map[string]map[string]*ResourceConfigItem)
	b.customRulePolicies = make(map[string]string)
	b.orgCustomRulePolicies = make(map[string]string)
}

// aggregationAuthKey returns a composite key for an aggregation authorization.
func aggregationAuthKey(accountID, region string) string {
	return accountID + "#" + region
}

// PutAggregationAuthorization creates or updates an aggregation authorization.
func (b *InMemoryBackend) PutAggregationAuthorization(accountID, region string) error {
	b.mu.Lock("PutAggregationAuthorization")
	defer b.mu.Unlock()

	arn := fmt.Sprintf(
		"arn:aws:config:%s:%s:aggregation-authorization/%s/%s",
		b.region, b.accountID, accountID, region,
	)

	b.aggregationAuths[aggregationAuthKey(accountID, region)] = &AggregationAuthorization{
		AuthorizedAccountID:         accountID,
		AuthorizedAwsRegion:         region,
		AggregationAuthorizationArn: arn,
	}

	return nil
}

// DescribeAggregationAuthorizations returns all aggregation authorizations sorted by
// account ID then region.
func (b *InMemoryBackend) DescribeAggregationAuthorizations() []AggregationAuthorization {
	b.mu.RLock("DescribeAggregationAuthorizations")
	defer b.mu.RUnlock()

	out := make([]AggregationAuthorization, 0, len(b.aggregationAuths))
	for _, a := range b.aggregationAuths {
		out = append(out, *a)
	}

	slices.SortFunc(out, func(a, b AggregationAuthorization) int {
		if a.AuthorizedAccountID != b.AuthorizedAccountID {
			if a.AuthorizedAccountID < b.AuthorizedAccountID {
				return -1
			}

			return 1
		}

		if a.AuthorizedAwsRegion < b.AuthorizedAwsRegion {
			return -1
		}

		if a.AuthorizedAwsRegion > b.AuthorizedAwsRegion {
			return 1
		}

		return 0
	})

	return out
}

// DeleteAggregationAuthorization deletes an aggregation authorization by account ID and region.
func (b *InMemoryBackend) DeleteAggregationAuthorization(accountID, region string) error {
	if accountID == "" {
		return fmt.Errorf("%w: AuthorizedAccountId is required", ErrValidation)
	}

	if region == "" {
		return fmt.Errorf("%w: AuthorizedAwsRegion is required", ErrValidation)
	}

	b.mu.Lock("DeleteAggregationAuthorization")
	defer b.mu.Unlock()

	key := aggregationAuthKey(accountID, region)
	if _, ok := b.aggregationAuths[key]; !ok {
		return fmt.Errorf(
			"%w: aggregation authorization for account %s region %s not found",
			ErrNoSuchAggregationAuthorization,
			accountID,
			region,
		)
	}

	delete(b.aggregationAuths, key)

	return nil
}

// PutConfigRule creates or updates a config rule with full metadata.
func (b *InMemoryBackend) PutConfigRule(input *ConfigRule) error {
	if input == nil || input.ConfigRuleName == "" {
		return fmt.Errorf("%w: ConfigRuleName is required", ErrValidation)
	}

	b.mu.Lock("PutConfigRule")
	defer b.mu.Unlock()

	existing, ok := b.configRules[input.ConfigRuleName]
	if ok {
		// Preserve ARN and ID on update.
		input.ConfigRuleArn = existing.ConfigRuleArn
		input.ConfigRuleID = existing.ConfigRuleID
	} else {
		b.ruleCounter++
		input.ConfigRuleArn = fmt.Sprintf(
			"arn:aws:config:%s:%s:config-rule/config-rule-%08d",
			b.region, b.accountID, b.ruleCounter,
		)
		input.ConfigRuleID = fmt.Sprintf("config-rule-%08d", b.ruleCounter)
	}

	if input.ConfigRuleState == "" {
		input.ConfigRuleState = "ACTIVE"
	}

	cp := *input
	// Deep-copy Source to avoid shared pointer.
	if input.Source != nil {
		srcCopy := *input.Source
		cp.Source = &srcCopy
	}

	b.configRules[input.ConfigRuleName] = &cp

	return nil
}

// DescribeConfigRules returns config rules optionally filtered by name list, sorted by name.
func (b *InMemoryBackend) DescribeConfigRules(names []string) []ConfigRule {
	b.mu.RLock("DescribeConfigRules")
	defer b.mu.RUnlock()

	out := make([]ConfigRule, 0, len(b.configRules))

	if len(names) == 0 {
		for _, r := range b.configRules {
			out = append(out, *r)
		}
	} else {
		for _, n := range names {
			if r, ok := b.configRules[n]; ok {
				out = append(out, *r)
			}
		}
	}

	slices.SortFunc(out, func(a, b ConfigRule) int {
		if a.ConfigRuleName < b.ConfigRuleName {
			return -1
		}

		if a.ConfigRuleName > b.ConfigRuleName {
			return 1
		}

		return 0
	})

	return out
}

// DeleteConfigRule deletes a config rule by name.
func (b *InMemoryBackend) DeleteConfigRule(name string) error {
	if name == "" {
		return fmt.Errorf("%w: ConfigRuleName is required", ErrValidation)
	}

	b.mu.Lock("DeleteConfigRule")
	defer b.mu.Unlock()

	if _, ok := b.configRules[name]; !ok {
		return fmt.Errorf("%w: %s", ErrNoSuchConfigRule, name)
	}

	delete(b.configRules, name)
	delete(b.ruleEvaluations, name)

	return nil
}

// PutConfigurationAggregator creates or updates a configuration aggregator.
func (b *InMemoryBackend) PutConfigurationAggregator(
	name string,
	accountSources []AccountAggregationSource,
	orgSource *OrganizationAggregationSource,
) error {
	if name == "" {
		return fmt.Errorf("%w: ConfigurationAggregatorName is required", ErrValidation)
	}

	b.mu.Lock("PutConfigurationAggregator")
	defer b.mu.Unlock()

	b.aggregatorCounter++
	arn := fmt.Sprintf(
		"arn:aws:config:%s:%s:config-aggregator/config-aggregator-%08d",
		b.region, b.accountID, b.aggregatorCounter,
	)

	b.aggregators[name] = &ConfigurationAggregator{
		ConfigurationAggregatorName:   name,
		ConfigurationAggregatorArn:    arn,
		AccountAggregationSources:     accountSources,
		OrganizationAggregationSource: orgSource,
	}

	return nil
}

// DeleteConfigurationAggregator deletes a configuration aggregator by name.
func (b *InMemoryBackend) DeleteConfigurationAggregator(name string) error {
	if name == "" {
		return fmt.Errorf("%w: ConfigurationAggregatorName is required", ErrValidation)
	}

	b.mu.Lock("DeleteConfigurationAggregator")
	defer b.mu.Unlock()

	if _, ok := b.aggregators[name]; !ok {
		return fmt.Errorf("%w: %s", ErrNoSuchAggregator, name)
	}

	delete(b.aggregators, name)

	return nil
}

// PutConformancePack creates or updates a conformance pack.
func (b *InMemoryBackend) PutConformancePack(name, deliveryS3Bucket, deliveryS3KeyPrefix string) error {
	if name == "" {
		return fmt.Errorf("%w: ConformancePackName is required", ErrValidation)
	}

	b.mu.Lock("PutConformancePack")
	defer b.mu.Unlock()

	b.conformancePackCounter++
	packID := fmt.Sprintf("conformance-pack-%08d", b.conformancePackCounter)
	arn := fmt.Sprintf(
		"arn:aws:config:%s:%s:conformance-pack/%s/%s",
		b.region, b.accountID, name, packID,
	)

	b.conformancePacks[name] = &ConformancePack{
		ConformancePackName: name,
		ConformancePackArn:  arn,
		ConformancePackID:   packID,
		DeliveryS3Bucket:    deliveryS3Bucket,
		DeliveryS3KeyPrefix: deliveryS3KeyPrefix,
	}

	return nil
}

// DeleteConformancePack deletes a conformance pack by name.
func (b *InMemoryBackend) DeleteConformancePack(name string) error {
	if name == "" {
		return fmt.Errorf("%w: ConformancePackName is required", ErrValidation)
	}

	b.mu.Lock("DeleteConformancePack")
	defer b.mu.Unlock()

	if _, ok := b.conformancePacks[name]; !ok {
		return fmt.Errorf("%w: %s", ErrNoSuchConformancePack, name)
	}

	delete(b.conformancePacks, name)

	return nil
}

// DeleteEvaluationResults clears evaluation results for a config rule.
// In this stub implementation the operation always succeeds (idempotent).
func (b *InMemoryBackend) DeleteEvaluationResults(_ string) error {
	return nil
}

// PutOrganizationConfigRule creates or updates an organization config rule.
func (b *InMemoryBackend) PutOrganizationConfigRule(name string) error {
	if name == "" {
		return fmt.Errorf("%w: OrganizationConfigRuleName is required", ErrValidation)
	}

	b.mu.Lock("PutOrganizationConfigRule")
	defer b.mu.Unlock()

	b.orgConfigRules[name] = &OrganizationConfigRule{OrganizationConfigRuleName: name}

	return nil
}

// DeleteOrganizationConfigRule deletes an organization config rule by name.
func (b *InMemoryBackend) DeleteOrganizationConfigRule(name string) error {
	if name == "" {
		return fmt.Errorf("%w: OrganizationConfigRuleName is required", ErrValidation)
	}

	b.mu.Lock("DeleteOrganizationConfigRule")
	defer b.mu.Unlock()

	if _, ok := b.orgConfigRules[name]; !ok {
		return fmt.Errorf("%w: %s", ErrNoSuchOrganizationConfigRule, name)
	}

	delete(b.orgConfigRules, name)

	return nil
}

// PutOrganizationConformancePack creates or updates an organization conformance pack.
func (b *InMemoryBackend) PutOrganizationConformancePack(name string) error {
	if name == "" {
		return fmt.Errorf("%w: OrganizationConformancePackName is required", ErrValidation)
	}

	b.mu.Lock("PutOrganizationConformancePack")
	defer b.mu.Unlock()

	b.orgConformancePacks[name] = &OrganizationConformancePack{OrganizationConformancePackName: name}

	return nil
}

// DeleteOrganizationConformancePack deletes an organization conformance pack by name.
func (b *InMemoryBackend) DeleteOrganizationConformancePack(name string) error {
	if name == "" {
		return fmt.Errorf("%w: OrganizationConformancePackName is required", ErrValidation)
	}

	b.mu.Lock("DeleteOrganizationConformancePack")
	defer b.mu.Unlock()

	if _, ok := b.orgConformancePacks[name]; !ok {
		return fmt.Errorf("%w: %s", ErrNoSuchOrganizationConformancePack, name)
	}

	delete(b.orgConformancePacks, name)

	return nil
}

// AssociateResourceTypes associates resource types with a configuration recorder identified
// by its ARN. The ARN is matched first by exact name, then falls back to a synthetic stub.
// Returns the updated recorder.
func (b *InMemoryBackend) AssociateResourceTypes(
	recorderARN string,
	_ []string,
) (*ConfigurationRecorder, error) {
	if recorderARN == "" {
		return nil, fmt.Errorf("%w: ConfigurationRecorderArn is required", ErrValidation)
	}

	b.mu.RLock("AssociateResourceTypes")
	defer b.mu.RUnlock()

	// Match by exact recorder name first (most common for LocalStack compatibility).
	if r, ok := b.recorders[recorderARN]; ok {
		return &ConfigurationRecorder{Name: r.Name, RoleARN: r.RoleARN, Status: r.Status}, nil
	}

	// Fall back to a synthetic recorder so callers don't error on a missing recorder.
	return &ConfigurationRecorder{
		Name:   recorderARN,
		Status: recorderStatusPending,
	}, nil
}

// BatchGetAggregateResourceConfig returns configuration items for aggregate resources.
// In this stub, all requested identifiers are returned as unprocessed.
func (b *InMemoryBackend) BatchGetAggregateResourceConfig(
	_ string,
	identifiers []AggregateResourceIdentifier,
) ([]BaseConfigurationItem, []AggregateResourceIdentifier) {
	if len(identifiers) == 0 {
		return []BaseConfigurationItem{}, []AggregateResourceIdentifier{}
	}

	return []BaseConfigurationItem{}, identifiers
}

// BatchGetResourceConfig returns configuration items for a list of resources.
// In this stub, all requested keys are returned as unprocessed.
func (b *InMemoryBackend) BatchGetResourceConfig(
	keys []ResourceKey,
) ([]BaseConfigurationItem, []ResourceKey) {
	if len(keys) == 0 {
		return []BaseConfigurationItem{}, []ResourceKey{}
	}

	return []BaseConfigurationItem{}, keys
}
