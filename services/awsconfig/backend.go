package awsconfig

import (
	"fmt"
	"slices"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
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
	// The wire error type is NoSuchOrganizationConformancePackException (verified against
	// aws-sdk-go-v2/service/configservice's DeleteOrganizationConformancePack deserializer).
	ErrNoSuchOrganizationConformancePack = awserr.New(
		"NoSuchOrganizationConformancePackException",
		awserr.ErrNotFound,
	)
	// ErrAlreadyExists is returned when a resource already exists.
	ErrAlreadyExists = awserr.New("MaxNumberOfConfigurationRecordersExceededException", awserr.ErrAlreadyExists)
	// ErrNoDeliveryChannel is returned when starting a recorder with no delivery channel configured.
	ErrNoDeliveryChannel = awserr.New("NoAvailableDeliveryChannelException", awserr.ErrInvalidParameter)
	// ErrValidation is returned when a required field is missing or invalid.
	ErrValidation = awserr.New("ValidationException", awserr.ErrInvalidParameter)
	// ErrResourceNotFound is returned when a referenced resource evaluation does not exist.
	ErrResourceNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
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
	Arn            string          `json:"arn,omitempty"`
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
//
// Phase 3.3 datalayer conversion: every map[string]*T resource collection is
// now a *store.Table[T] registered on b.registry (see store_setup.go's
// registerAllTables), which collapses Reset/Snapshot/Restore to one
// b.registry call each instead of one hand-written block per map. Fields
// whose value is not a *T (a scalar, a slice, or a nested map) have no
// natural store.Table key and are left as plain maps -- see each field's own
// comment for why, and persistence.go's doc comment for the persistence
// audit of each.
type InMemoryBackend struct {
	registry         *store.Registry
	recorders        *store.Table[ConfigurationRecorder]
	channels         *store.Table[DeliveryChannel]
	aggregationAuths *store.Table[AggregationAuthorization]
	configRules      *store.Table[ConfigRule]
	// ruleEvaluations is a scalar-valued map (rule name → rolled-up compliance
	// type) -- no *T for store.Table to key on -- so it stays a plain map.
	ruleEvaluations map[string]string
	// ruleResourceEvals holds per-(rule, resource) evaluation results, flattened
	// to a single store.Table[StoredEvaluation] keyed by
	// "<ruleName>|<resourceType>\x1f<resourceID>" (storedEvaluationKeyFn),
	// mirroring codecommit's nested-per-parent conversion. Two secondary
	// indexes replace the two access patterns the nested map used to answer
	// directly: ruleResourceEvalsByRule ("all evaluations for rule X", used by
	// evaluateManagedRuleLocked/recomputeRollupLocked/GetComplianceDetailsByConfigRule)
	// and ruleResourceEvalsByResource ("all evaluations for resource Y across
	// every rule", used by GetComplianceDetailsByResource).
	ruleResourceEvals           *store.Table[StoredEvaluation]
	ruleResourceEvalsByRule     *store.Index[StoredEvaluation]
	ruleResourceEvalsByResource *store.Index[StoredEvaluation]
	// resourceHistory is a slice-valued map (resourceKey → ordered history) --
	// no *T for store.Table to key on -- so it stays a plain map.
	resourceHistory map[string][]ResourceConfigItem
	// resourceEvaluations records StartResourceEvaluation runs keyed by
	// evaluation id (ResourceEvaluation.ResourceEvaluationID, a real field).
	resourceEvaluations *store.Table[ResourceEvaluation]
	aggregators         *store.Table[ConfigurationAggregator]
	conformancePacks    *store.Table[ConformancePack]
	orgConfigRules      *store.Table[OrganizationConfigRule]
	orgConformancePacks *store.Table[OrganizationConformancePack]
	storedQueries       *store.Table[StoredQuery]
	// resourceTags is a slice-valued map (ARN → tags) -- left as a plain map.
	resourceTags       map[string][]Tag
	retentionConfigs   *store.Table[RetentionConfiguration]
	remediationConfigs *store.Table[RemediationConfiguration]
	// remediationExceptions is a slice-valued map (rule name → exceptions) --
	// left as a plain map.
	remediationExceptions map[string][]RemediationException
	// resourceConfigs holds discovered resource configuration items,
	// flattened to a single store.Table[ResourceConfigItem] keyed by
	// "<resourceType>|<resourceID>" (resourceConfigItemKeyFn) since
	// ResourceConfigItem already carries real ResourceType/ResourceID
	// fields -- no hidden field needed, unlike codecommit's dirty tables.
	// resourceConfigsByType replaces the old map[string]map[string]*T's
	// outer-map lookup for "all resources of type X" (ListDiscoveredResources).
	resourceConfigs       *store.Table[ResourceConfigItem]
	resourceConfigsByType *store.Index[ResourceConfigItem]
	// customRulePolicies/orgCustomRulePolicies are scalar-valued maps (rule
	// name → policy text) -- left as plain maps.
	customRulePolicies     map[string]string
	orgCustomRulePolicies  map[string]string
	mu                     *lockmetrics.RWMutex
	accountID              string
	region                 string
	ruleCounter            int
	conformancePackCounter int
	aggregatorCounter      int
	resourceEvalCounter    int
	captureCounter         int
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend() *InMemoryBackend {
	return NewInMemoryBackendWithMeta("123456789012", "us-east-1")
}

// NewInMemoryBackendWithMeta creates a new InMemoryBackend with account and region context.
func NewInMemoryBackendWithMeta(accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		registry:              store.NewRegistry(),
		ruleEvaluations:       make(map[string]string),
		resourceHistory:       make(map[string][]ResourceConfigItem),
		resourceTags:          make(map[string][]Tag),
		remediationExceptions: make(map[string][]RemediationException),
		customRulePolicies:    make(map[string]string),
		orgCustomRulePolicies: make(map[string]string),
		mu:                    lockmetrics.New("awsconfig"),
		accountID:             accountID,
		region:                region,
	}

	registerAllTables(b)

	return b
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

	if existing, ok := b.recorders.Get(name); ok {
		existing.RoleARN = roleARN
		existing.RecordingGroup = recordingGroup

		return nil
	}

	b.recorders.Put(&ConfigurationRecorder{
		Name:           name,
		RoleARN:        roleARN,
		Status:         recorderStatusPending,
		RecordingGroup: recordingGroup,
	})

	return nil
}

// recorderArn builds the ARN for a configuration recorder owned by this backend,
// matching the "arn" field the real service serializes on ConfigurationRecorder
// (aws-sdk-go-v2/service/configservice/types.ConfigurationRecorder.Arn).
func (b *InMemoryBackend) recorderArn(name string) string {
	return fmt.Sprintf("arn:aws:config:%s:%s:config-recorder/%s", b.region, b.accountID, name)
}

// DescribeConfigurationRecorders returns configuration recorders filtered by the
// provided name list.  An empty/nil names list returns all recorders sorted by name.
func (b *InMemoryBackend) DescribeConfigurationRecorders(names []string) []ConfigurationRecorder {
	b.mu.RLock("DescribeConfigurationRecorders")
	defer b.mu.RUnlock()

	out := make([]ConfigurationRecorder, 0, b.recorders.Len())

	if len(names) == 0 {
		for _, r := range b.recorders.All() {
			cp := *r
			cp.Arn = b.recorderArn(r.Name)
			out = append(out, cp)
		}
	} else {
		for _, n := range names {
			if r, ok := b.recorders.Get(n); ok {
				cp := *r
				cp.Arn = b.recorderArn(r.Name)
				out = append(out, cp)
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

	r, ok := b.recorders.Get(name)
	if !ok {
		return fmt.Errorf("%w: %s", ErrNotFound, name)
	}

	if b.channels.Len() == 0 {
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

	r, ok := b.recorders.Get(name)
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

	b.channels.Put(&DeliveryChannel{
		Name:                             name,
		S3Bucket:                         s3Bucket,
		SNSArn:                           snsArn,
		S3KeyPrefix:                      s3KeyPrefix,
		ConfigSnapshotDeliveryProperties: props,
	})

	return nil
}

// DescribeDeliveryChannels returns delivery channels filtered by the provided name list.
// An empty/nil names list returns all channels sorted by name.
func (b *InMemoryBackend) DescribeDeliveryChannels(names []string) []DeliveryChannel {
	b.mu.RLock("DescribeDeliveryChannels")
	defer b.mu.RUnlock()

	out := make([]DeliveryChannel, 0, b.channels.Len())

	if len(names) == 0 {
		for _, c := range b.channels.All() {
			out = append(out, *c)
		}
	} else {
		for _, n := range names {
			if c, ok := b.channels.Get(n); ok {
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

	if !b.channels.Has(name) {
		return fmt.Errorf("%w: %s", ErrNoSuchDeliveryChannel, name)
	}

	b.channels.Delete(name)

	return nil
}

// DeleteConfigurationRecorder removes a configuration recorder by name.
func (b *InMemoryBackend) DeleteConfigurationRecorder(name string) error {
	if name == "" {
		return fmt.Errorf("%w: ConfigurationRecorderName is required", ErrValidation)
	}

	b.mu.Lock("DeleteConfigurationRecorder")
	defer b.mu.Unlock()

	if !b.recorders.Has(name) {
		return fmt.Errorf("%w: %s", ErrNotFound, name)
	}

	b.recorders.Delete(name)

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

	out := make([]ConfigurationRecorderStatus, 0, b.recorders.Len())

	if len(names) == 0 {
		for _, r := range b.recorders.All() {
			out = append(out, recorderStatus(r))
		}
	} else {
		for _, n := range names {
			if r, ok := b.recorders.Get(n); ok {
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

// Reset clears all in-memory state. Note conformancePackCounter and
// aggregatorCounter are deliberately NOT reset here -- this is a pre-existing
// quirk of the map-based implementation (they were never zeroed in the old
// Reset either), preserved as-is per Phase 3.3's mechanical-conversion scope.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.registry.ResetAll()
	b.ruleEvaluations = make(map[string]string)
	b.resourceHistory = make(map[string][]ResourceConfigItem)
	b.resourceEvalCounter = 0
	b.captureCounter = 0
	b.ruleCounter = 0
	b.resourceTags = make(map[string][]Tag)
	b.remediationExceptions = make(map[string][]RemediationException)
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

	b.aggregationAuths.Put(&AggregationAuthorization{
		AuthorizedAccountID:         accountID,
		AuthorizedAwsRegion:         region,
		AggregationAuthorizationArn: arn,
	})

	return nil
}

// DescribeAggregationAuthorizations returns all aggregation authorizations sorted by
// account ID then region.
func (b *InMemoryBackend) DescribeAggregationAuthorizations() []AggregationAuthorization {
	b.mu.RLock("DescribeAggregationAuthorizations")
	defer b.mu.RUnlock()

	all := b.aggregationAuths.All()
	out := make([]AggregationAuthorization, 0, len(all))

	for _, a := range all {
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

// DeleteAggregationAuthorization deletes an aggregation authorization by account ID
// and region. Real AWS Config's DeleteAggregationAuthorization is idempotent -- its
// error model (verified against aws-sdk-go-v2/service/configservice's deserializer)
// only lists InvalidParameterValueException, never a not-found exception -- so
// deleting a nonexistent authorization succeeds silently, matching AWS.
func (b *InMemoryBackend) DeleteAggregationAuthorization(accountID, region string) error {
	if accountID == "" {
		return fmt.Errorf("%w: AuthorizedAccountId is required", ErrValidation)
	}

	if region == "" {
		return fmt.Errorf("%w: AuthorizedAwsRegion is required", ErrValidation)
	}

	b.mu.Lock("DeleteAggregationAuthorization")
	defer b.mu.Unlock()

	b.aggregationAuths.Delete(aggregationAuthKey(accountID, region))

	return nil
}

// PutConfigRule creates or updates a config rule with full metadata.
func (b *InMemoryBackend) PutConfigRule(input *ConfigRule) error {
	if input == nil || input.ConfigRuleName == "" {
		return fmt.Errorf("%w: ConfigRuleName is required", ErrValidation)
	}

	b.mu.Lock("PutConfigRule")
	defer b.mu.Unlock()

	existing, ok := b.configRules.Get(input.ConfigRuleName)
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

	b.configRules.Put(&cp)

	return nil
}

// DescribeConfigRules returns config rules optionally filtered by name list, sorted by name.
func (b *InMemoryBackend) DescribeConfigRules(names []string) []ConfigRule {
	b.mu.RLock("DescribeConfigRules")
	defer b.mu.RUnlock()

	out := make([]ConfigRule, 0, b.configRules.Len())

	if len(names) == 0 {
		for _, r := range b.configRules.All() {
			out = append(out, *r)
		}
	} else {
		for _, n := range names {
			if r, ok := b.configRules.Get(n); ok {
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

	if !b.configRules.Has(name) {
		return fmt.Errorf("%w: %s", ErrNoSuchConfigRule, name)
	}

	b.configRules.Delete(name)
	b.clearRuleEvaluationsLocked(name)

	return nil
}

// clearRuleEvaluationsLocked removes every stored evaluation (rollup and
// per-resource) for ruleName. The caller must hold the write lock.
//
// ruleResourceEvals has no bulk "delete everything under this rule" operation
// (unlike the old map[string]map[string]*T's single outer-map delete);
// snapshot the rule's entries via slices.Clone first since Table.Delete
// mutates the very index slice ruleResourceEvalsByRule.Get returns.
func (b *InMemoryBackend) clearRuleEvaluationsLocked(ruleName string) {
	delete(b.ruleEvaluations, ruleName)

	for _, e := range slices.Clone(b.ruleResourceEvalsByRule.Get(ruleName)) {
		b.ruleResourceEvals.Delete(storedEvaluationKeyFn(e))
	}
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

	b.aggregators.Put(&ConfigurationAggregator{
		ConfigurationAggregatorName:   name,
		ConfigurationAggregatorArn:    arn,
		AccountAggregationSources:     accountSources,
		OrganizationAggregationSource: orgSource,
	})

	return nil
}

// DeleteConfigurationAggregator deletes a configuration aggregator by name.
func (b *InMemoryBackend) DeleteConfigurationAggregator(name string) error {
	if name == "" {
		return fmt.Errorf("%w: ConfigurationAggregatorName is required", ErrValidation)
	}

	b.mu.Lock("DeleteConfigurationAggregator")
	defer b.mu.Unlock()

	if !b.aggregators.Has(name) {
		return fmt.Errorf("%w: %s", ErrNoSuchAggregator, name)
	}

	b.aggregators.Delete(name)

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

	b.conformancePacks.Put(&ConformancePack{
		ConformancePackName: name,
		ConformancePackArn:  arn,
		ConformancePackID:   packID,
		DeliveryS3Bucket:    deliveryS3Bucket,
		DeliveryS3KeyPrefix: deliveryS3KeyPrefix,
	})

	return nil
}

// DeleteConformancePack deletes a conformance pack by name.
func (b *InMemoryBackend) DeleteConformancePack(name string) error {
	if name == "" {
		return fmt.Errorf("%w: ConformancePackName is required", ErrValidation)
	}

	b.mu.Lock("DeleteConformancePack")
	defer b.mu.Unlock()

	if !b.conformancePacks.Has(name) {
		return fmt.Errorf("%w: %s", ErrNoSuchConformancePack, name)
	}

	b.conformancePacks.Delete(name)

	return nil
}

// DeleteEvaluationResults clears the rollup and per-resource evaluation results
// recorded for a config rule (so a subsequent StartConfigRulesEvaluation starts
// from a clean slate), matching real AWS Config which errors
// NoSuchConfigRuleException for an unknown rule (verified against
// aws-sdk-go-v2/service/configservice's DeleteEvaluationResults deserializer).
func (b *InMemoryBackend) DeleteEvaluationResults(ruleName string) error {
	if ruleName == "" {
		return fmt.Errorf("%w: ConfigRuleName is required", ErrValidation)
	}

	b.mu.Lock("DeleteEvaluationResults")
	defer b.mu.Unlock()

	if !b.configRules.Has(ruleName) {
		return fmt.Errorf("%w: %s", ErrNoSuchConfigRule, ruleName)
	}

	b.clearRuleEvaluationsLocked(ruleName)

	return nil
}

// PutOrganizationConfigRule creates or updates an organization config rule.
func (b *InMemoryBackend) PutOrganizationConfigRule(name string) error {
	if name == "" {
		return fmt.Errorf("%w: OrganizationConfigRuleName is required", ErrValidation)
	}

	b.mu.Lock("PutOrganizationConfigRule")
	defer b.mu.Unlock()

	b.orgConfigRules.Put(&OrganizationConfigRule{OrganizationConfigRuleName: name})

	return nil
}

// DeleteOrganizationConfigRule deletes an organization config rule by name.
func (b *InMemoryBackend) DeleteOrganizationConfigRule(name string) error {
	if name == "" {
		return fmt.Errorf("%w: OrganizationConfigRuleName is required", ErrValidation)
	}

	b.mu.Lock("DeleteOrganizationConfigRule")
	defer b.mu.Unlock()

	if !b.orgConfigRules.Has(name) {
		return fmt.Errorf("%w: %s", ErrNoSuchOrganizationConfigRule, name)
	}

	b.orgConfigRules.Delete(name)

	return nil
}

// PutOrganizationConformancePack creates or updates an organization conformance pack.
func (b *InMemoryBackend) PutOrganizationConformancePack(name string) error {
	if name == "" {
		return fmt.Errorf("%w: OrganizationConformancePackName is required", ErrValidation)
	}

	b.mu.Lock("PutOrganizationConformancePack")
	defer b.mu.Unlock()

	b.orgConformancePacks.Put(&OrganizationConformancePack{OrganizationConformancePackName: name})

	return nil
}

// DeleteOrganizationConformancePack deletes an organization conformance pack by name.
func (b *InMemoryBackend) DeleteOrganizationConformancePack(name string) error {
	if name == "" {
		return fmt.Errorf("%w: OrganizationConformancePackName is required", ErrValidation)
	}

	b.mu.Lock("DeleteOrganizationConformancePack")
	defer b.mu.Unlock()

	if !b.orgConformancePacks.Has(name) {
		return fmt.Errorf("%w: %s", ErrNoSuchOrganizationConformancePack, name)
	}

	b.orgConformancePacks.Delete(name)

	return nil
}

// recorderNameFromArn extracts a recorder's name from either a bare name or a
// full "arn:aws:config:<region>:<account>:config-recorder/<name>" ARN, so
// AssociateResourceTypes/DisassociateResourceTypes accept both forms (real
// SDK callers always send the full ARN; unit tests exercise the bare name).
func recorderNameFromArn(recorderARN string) string {
	if idx := strings.LastIndex(recorderARN, "/"); idx >= 0 {
		return recorderARN[idx+1:]
	}

	return recorderARN
}

// mergeResourceTypes returns existing with every type in added that is not
// already present appended, preserving existing order and de-duplicating.
func mergeResourceTypes(existing, added []string) []string {
	seen := make(map[string]struct{}, len(existing))
	for _, t := range existing {
		seen[t] = struct{}{}
	}

	out := existing

	for _, t := range added {
		if _, ok := seen[t]; ok {
			continue
		}

		seen[t] = struct{}{}
		out = append(out, t)
	}

	return out
}

// removeResourceTypes returns existing with every type in removed dropped.
func removeResourceTypes(existing, removed []string) []string {
	if len(existing) == 0 || len(removed) == 0 {
		return existing
	}

	drop := make(map[string]struct{}, len(removed))
	for _, t := range removed {
		drop[t] = struct{}{}
	}

	out := existing[:0]

	for _, t := range existing {
		if _, ok := drop[t]; !ok {
			out = append(out, t)
		}
	}

	return out
}

// AssociateResourceTypes adds resourceTypes to a configuration recorder's
// RecordingGroup, matching AssociateResourceTypesInput/Output
// (aws-sdk-go-v2/service/configservice). recorderARN may be the recorder's
// bare name or its full ARN. Errors with ErrNotFound (wire type
// NoSuchConfigurationRecorderException) when no matching recorder exists,
// matching the real API's declared error model instead of fabricating a
// synthetic recorder for unknown input.
func (b *InMemoryBackend) AssociateResourceTypes(
	recorderARN string,
	resourceTypes []string,
) (*ConfigurationRecorder, error) {
	if recorderARN == "" {
		return nil, fmt.Errorf("%w: ConfigurationRecorderArn is required", ErrValidation)
	}

	b.mu.Lock("AssociateResourceTypes")
	defer b.mu.Unlock()

	name := recorderNameFromArn(recorderARN)

	r, ok := b.recorders.Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrNotFound, recorderARN)
	}

	if r.RecordingGroup == nil {
		r.RecordingGroup = &RecordingGroup{}
	}

	r.RecordingGroup.ResourceTypes = mergeResourceTypes(r.RecordingGroup.ResourceTypes, resourceTypes)

	cp := *r
	cp.Arn = b.recorderArn(r.Name)
	rgCopy := *r.RecordingGroup
	cp.RecordingGroup = &rgCopy

	return &cp, nil
}

// BatchGetAggregateResourceConfig returns configuration items for aggregate
// resources. This emulator does not model multi-account aggregation
// separately from the account's own resource-config state (mirroring
// SelectAggregateResourceConfig), so each identifier is resolved against
// b.resourceConfigs (populated by PutResourceConfig) instead of being
// blanket-reported unprocessed; only identifiers with no matching discovered
// resource are unprocessed.
func (b *InMemoryBackend) BatchGetAggregateResourceConfig(
	_ string,
	identifiers []AggregateResourceIdentifier,
) ([]BaseConfigurationItem, []AggregateResourceIdentifier) {
	b.mu.RLock("BatchGetAggregateResourceConfig")
	defer b.mu.RUnlock()

	items := make([]BaseConfigurationItem, 0, len(identifiers))
	unprocessed := make([]AggregateResourceIdentifier, 0, len(identifiers))

	for _, id := range identifiers {
		item, ok := b.resourceConfigs.Get(resourceConfigItemKey(id.ResourceType, id.ResourceID))
		if !ok {
			unprocessed = append(unprocessed, id)

			continue
		}

		items = append(items, BaseConfigurationItem{ResourceType: item.ResourceType, ResourceID: item.ResourceID})
	}

	return items, unprocessed
}

// BatchGetResourceConfig returns configuration items for the requested resource
// keys, resolving each against b.resourceConfigs (populated by PutResourceConfig)
// instead of blanket-reporting every key unprocessed; only keys with no matching
// discovered resource are unprocessed.
func (b *InMemoryBackend) BatchGetResourceConfig(
	keys []ResourceKey,
) ([]BaseConfigurationItem, []ResourceKey) {
	b.mu.RLock("BatchGetResourceConfig")
	defer b.mu.RUnlock()

	items := make([]BaseConfigurationItem, 0, len(keys))
	unprocessed := make([]ResourceKey, 0, len(keys))

	for _, k := range keys {
		item, ok := b.resourceConfigs.Get(resourceConfigItemKey(k.ResourceType, k.ResourceID))
		if !ok {
			unprocessed = append(unprocessed, k)

			continue
		}

		items = append(items, BaseConfigurationItem{ResourceType: item.ResourceType, ResourceID: item.ResourceID})
	}

	return items, unprocessed
}
