package awsconfig

import (
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

var (
	// ErrNotFound is returned when a resource is not found.
	ErrNotFound = awserr.New("NoSuchConfigurationRecorder", awserr.ErrNotFound)
	// ErrAlreadyExists is returned when a resource already exists.
	ErrAlreadyExists = awserr.New("MaxNumberOfConfigurationRecordersExceededException", awserr.ErrAlreadyExists)
	// ErrNoDeliveryChannel is returned when starting a recorder with no delivery channel configured.
	ErrNoDeliveryChannel = awserr.New("NoAvailableDeliveryChannelException", awserr.ErrInvalidParameter)
)

// ConfigurationRecorder represents an AWS Config configuration recorder.
type ConfigurationRecorder struct {
	Name    string `json:"name"`
	RoleARN string `json:"roleARN"`
	Status  string `json:"status,omitempty"` // PENDING or ACTIVE
}

// DeliveryChannel represents an AWS Config delivery channel.
type DeliveryChannel struct {
	Name     string `json:"name"`
	S3Bucket string `json:"s3BucketName,omitempty"`
	SNSArn   string `json:"snsTopicARN,omitempty"`
}

// AggregationAuthorization represents an AWS Config aggregation authorization.
type AggregationAuthorization struct {
	AuthorizedAccountID string `json:"authorizedAccountId"`
	AuthorizedAwsRegion string `json:"authorizedAwsRegion"`
}

// ConfigRule represents an AWS Config config rule.
type ConfigRule struct {
	ConfigRuleName string `json:"configRuleName"`
}

// ConfigurationAggregator represents an AWS Config configuration aggregator.
type ConfigurationAggregator struct {
	ConfigurationAggregatorName string `json:"configurationAggregatorName"`
}

// ConformancePack represents an AWS Config conformance pack.
type ConformancePack struct {
	ConformancePackName string `json:"conformancePackName"`
}

// OrganizationConfigRule represents an AWS Config organization config rule.
type OrganizationConfigRule struct {
	OrganizationConfigRuleName string `json:"organizationConfigRuleName"`
}

// OrganizationConformancePack represents an AWS Config organization conformance pack.
type OrganizationConformancePack struct {
	OrganizationConformancePackName string `json:"organizationConformancePackName"`
}

// InMemoryBackend is the in-memory store for AWS Config resources.
type InMemoryBackend struct {
	recorders           map[string]*ConfigurationRecorder
	channels            map[string]*DeliveryChannel
	aggregationAuths    map[string]*AggregationAuthorization
	configRules         map[string]*ConfigRule
	aggregators         map[string]*ConfigurationAggregator
	conformancePacks    map[string]*ConformancePack
	orgConfigRules      map[string]*OrganizationConfigRule
	orgConformancePacks map[string]*OrganizationConformancePack
	mu                  *lockmetrics.RWMutex
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend() *InMemoryBackend {
	return &InMemoryBackend{
		recorders:           make(map[string]*ConfigurationRecorder),
		channels:            make(map[string]*DeliveryChannel),
		aggregationAuths:    make(map[string]*AggregationAuthorization),
		configRules:         make(map[string]*ConfigRule),
		aggregators:         make(map[string]*ConfigurationAggregator),
		conformancePacks:    make(map[string]*ConformancePack),
		orgConfigRules:      make(map[string]*OrganizationConfigRule),
		orgConformancePacks: make(map[string]*OrganizationConformancePack),
		mu:                  lockmetrics.New("awsconfig"),
	}
}

// PutConfigurationRecorder creates or updates a configuration recorder.
func (b *InMemoryBackend) PutConfigurationRecorder(name, roleARN string) error {
	b.mu.Lock("PutConfigurationRecorder")
	defer b.mu.Unlock()

	b.recorders[name] = &ConfigurationRecorder{Name: name, RoleARN: roleARN, Status: "PENDING"}

	return nil
}

// DescribeConfigurationRecorders returns all configuration recorders.
func (b *InMemoryBackend) DescribeConfigurationRecorders() []ConfigurationRecorder {
	b.mu.RLock("DescribeConfigurationRecorders")
	defer b.mu.RUnlock()

	out := make([]ConfigurationRecorder, 0, len(b.recorders))
	for _, r := range b.recorders {
		out = append(out, *r)
	}

	return out
}

// StartConfigurationRecorder starts a configuration recorder.
func (b *InMemoryBackend) StartConfigurationRecorder(name string) error {
	b.mu.Lock("StartConfigurationRecorder")
	defer b.mu.Unlock()

	r, ok := b.recorders[name]
	if !ok {
		return fmt.Errorf("%w: %s", ErrNotFound, name)
	}

	if len(b.channels) == 0 {
		return fmt.Errorf("%w: no delivery channel configured", ErrNoDeliveryChannel)
	}

	r.Status = "ACTIVE"

	return nil
}

// PutDeliveryChannel creates or updates a delivery channel.
func (b *InMemoryBackend) PutDeliveryChannel(name, s3Bucket, snsArn string) error {
	b.mu.Lock("PutDeliveryChannel")
	defer b.mu.Unlock()

	b.channels[name] = &DeliveryChannel{Name: name, S3Bucket: s3Bucket, SNSArn: snsArn}

	return nil
}

// DescribeDeliveryChannels returns all delivery channels.
func (b *InMemoryBackend) DescribeDeliveryChannels() []DeliveryChannel {
	b.mu.RLock("DescribeDeliveryChannels")
	defer b.mu.RUnlock()

	out := make([]DeliveryChannel, 0, len(b.channels))
	for _, c := range b.channels {
		out = append(out, *c)
	}

	return out
}

// DeleteDeliveryChannel removes a delivery channel by name.
func (b *InMemoryBackend) DeleteDeliveryChannel(name string) error {
	b.mu.Lock("DeleteDeliveryChannel")
	defer b.mu.Unlock()

	if _, ok := b.channels[name]; !ok {
		return fmt.Errorf("%w: delivery channel %s not found", ErrNotFound, name)
	}

	delete(b.channels, name)

	return nil
}

// DeleteConfigurationRecorder removes a configuration recorder by name.
func (b *InMemoryBackend) DeleteConfigurationRecorder(name string) error {
	b.mu.Lock("DeleteConfigurationRecorder")
	defer b.mu.Unlock()

	if _, ok := b.recorders[name]; !ok {
		return fmt.Errorf("%w: configuration recorder %s not found", ErrNotFound, name)
	}

	delete(b.recorders, name)

	return nil
}

// ConfigurationRecorderStatus represents the recording status of a recorder.
type ConfigurationRecorderStatus struct {
	Name      string `json:"name"`
	Recording bool   `json:"recording"`
}

// DescribeConfigurationRecorderStatus returns the recording status of all recorders.
func (b *InMemoryBackend) DescribeConfigurationRecorderStatus() []ConfigurationRecorderStatus {
	b.mu.RLock("DescribeConfigurationRecorderStatus")
	defer b.mu.RUnlock()

	out := make([]ConfigurationRecorderStatus, 0, len(b.recorders))
	for _, r := range b.recorders {
		out = append(out, ConfigurationRecorderStatus{
			Name:      r.Name,
			Recording: r.Status == "ACTIVE",
		})
	}

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
	b.aggregators = make(map[string]*ConfigurationAggregator)
	b.conformancePacks = make(map[string]*ConformancePack)
	b.orgConfigRules = make(map[string]*OrganizationConfigRule)
	b.orgConformancePacks = make(map[string]*OrganizationConformancePack)
}

// aggregationAuthKey returns a composite key for an aggregation authorization.
func aggregationAuthKey(accountID, region string) string {
	return accountID + "#" + region
}

// PutAggregationAuthorization creates or updates an aggregation authorization.
func (b *InMemoryBackend) PutAggregationAuthorization(accountID, region string) error {
	b.mu.Lock("PutAggregationAuthorization")
	defer b.mu.Unlock()

	b.aggregationAuths[aggregationAuthKey(accountID, region)] = &AggregationAuthorization{
		AuthorizedAccountID: accountID,
		AuthorizedAwsRegion: region,
	}

	return nil
}

// DeleteAggregationAuthorization deletes an aggregation authorization by account ID and region.
func (b *InMemoryBackend) DeleteAggregationAuthorization(accountID, region string) error {
	b.mu.Lock("DeleteAggregationAuthorization")
	defer b.mu.Unlock()

	key := aggregationAuthKey(accountID, region)
	if _, ok := b.aggregationAuths[key]; !ok {
		return fmt.Errorf(
			"%w: aggregation authorization for account %s region %s not found",
			ErrNotFound,
			accountID,
			region,
		)
	}

	delete(b.aggregationAuths, key)

	return nil
}

// PutConfigRule creates or updates a config rule.
func (b *InMemoryBackend) PutConfigRule(name string) error {
	b.mu.Lock("PutConfigRule")
	defer b.mu.Unlock()

	b.configRules[name] = &ConfigRule{ConfigRuleName: name}

	return nil
}

// DeleteConfigRule deletes a config rule by name.
func (b *InMemoryBackend) DeleteConfigRule(name string) error {
	b.mu.Lock("DeleteConfigRule")
	defer b.mu.Unlock()

	if _, ok := b.configRules[name]; !ok {
		return fmt.Errorf("%w: config rule %s not found", ErrNotFound, name)
	}

	delete(b.configRules, name)

	return nil
}

// PutConfigurationAggregator creates or updates a configuration aggregator.
func (b *InMemoryBackend) PutConfigurationAggregator(name string) error {
	b.mu.Lock("PutConfigurationAggregator")
	defer b.mu.Unlock()

	b.aggregators[name] = &ConfigurationAggregator{ConfigurationAggregatorName: name}

	return nil
}

// DeleteConfigurationAggregator deletes a configuration aggregator by name.
func (b *InMemoryBackend) DeleteConfigurationAggregator(name string) error {
	b.mu.Lock("DeleteConfigurationAggregator")
	defer b.mu.Unlock()

	if _, ok := b.aggregators[name]; !ok {
		return fmt.Errorf("%w: configuration aggregator %s not found", ErrNotFound, name)
	}

	delete(b.aggregators, name)

	return nil
}

// PutConformancePack creates or updates a conformance pack.
func (b *InMemoryBackend) PutConformancePack(name string) error {
	b.mu.Lock("PutConformancePack")
	defer b.mu.Unlock()

	b.conformancePacks[name] = &ConformancePack{ConformancePackName: name}

	return nil
}

// DeleteConformancePack deletes a conformance pack by name.
func (b *InMemoryBackend) DeleteConformancePack(name string) error {
	b.mu.Lock("DeleteConformancePack")
	defer b.mu.Unlock()

	if _, ok := b.conformancePacks[name]; !ok {
		return fmt.Errorf("%w: conformance pack %s not found", ErrNotFound, name)
	}

	delete(b.conformancePacks, name)

	return nil
}

// DeleteEvaluationResults clears evaluation results for a config rule.
// In this stub implementation the operation always succeeds.
func (b *InMemoryBackend) DeleteEvaluationResults(_ string) error {
	return nil
}

// PutOrganizationConfigRule creates or updates an organization config rule.
func (b *InMemoryBackend) PutOrganizationConfigRule(name string) error {
	b.mu.Lock("PutOrganizationConfigRule")
	defer b.mu.Unlock()

	b.orgConfigRules[name] = &OrganizationConfigRule{OrganizationConfigRuleName: name}

	return nil
}

// DeleteOrganizationConfigRule deletes an organization config rule by name.
func (b *InMemoryBackend) DeleteOrganizationConfigRule(name string) error {
	b.mu.Lock("DeleteOrganizationConfigRule")
	defer b.mu.Unlock()

	if _, ok := b.orgConfigRules[name]; !ok {
		return fmt.Errorf("%w: organization config rule %s not found", ErrNotFound, name)
	}

	delete(b.orgConfigRules, name)

	return nil
}

// PutOrganizationConformancePack creates or updates an organization conformance pack.
func (b *InMemoryBackend) PutOrganizationConformancePack(name string) error {
	b.mu.Lock("PutOrganizationConformancePack")
	defer b.mu.Unlock()

	b.orgConformancePacks[name] = &OrganizationConformancePack{OrganizationConformancePackName: name}

	return nil
}

// DeleteOrganizationConformancePack deletes an organization conformance pack by name.
func (b *InMemoryBackend) DeleteOrganizationConformancePack(name string) error {
	b.mu.Lock("DeleteOrganizationConformancePack")
	defer b.mu.Unlock()

	if _, ok := b.orgConformancePacks[name]; !ok {
		return fmt.Errorf("%w: organization conformance pack %s not found", ErrNotFound, name)
	}

	delete(b.orgConformancePacks, name)

	return nil
}

// AssociateResourceTypes associates resource types with a configuration recorder by ARN.
// In this stub, the operation always returns an empty ConfigurationRecorder response.
func (b *InMemoryBackend) AssociateResourceTypes(
	recorderARN string,
	_ []string,
) (*ConfigurationRecorder, error) {
	b.mu.RLock("AssociateResourceTypes")
	defer b.mu.RUnlock()

	// Look up the recorder by ARN among all stored recorders.
	for _, r := range b.recorders {
		if r.RoleARN == recorderARN || r.Name == recorderARN {
			return &ConfigurationRecorder{
				Name:    r.Name,
				RoleARN: r.RoleARN,
				Status:  r.Status,
			}, nil
		}
	}

	// Return a stub recorder referencing the requested ARN.
	return &ConfigurationRecorder{
		Name:    recorderARN,
		RoleARN: recorderARN,
	}, nil
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

// BatchGetAggregateResourceConfig returns configuration items for aggregate resources.
// In this stub, all requested identifiers are returned as unprocessed.
func (b *InMemoryBackend) BatchGetAggregateResourceConfig(
	_ string,
	identifiers []AggregateResourceIdentifier,
) ([]BaseConfigurationItem, []AggregateResourceIdentifier) {
	return []BaseConfigurationItem{}, identifiers
}

// BatchGetResourceConfig returns configuration items for a list of resources.
// In this stub, all requested keys are returned as unprocessed.
func (b *InMemoryBackend) BatchGetResourceConfig(
	keys []ResourceKey,
) ([]BaseConfigurationItem, []ResourceKey) {
	return []BaseConfigurationItem{}, keys
}
