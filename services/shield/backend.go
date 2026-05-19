package shield

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

// subscriptionCommitmentDays is the default Shield Advanced subscription commitment period.
const subscriptionCommitmentDays int64 = 365

// protectionIDBytes is the number of random bytes used for protection/group IDs.
const protectionIDBytes = 16

// Aggregation values for protection groups.
const (
	AggregationSum  = "SUM"
	AggregationMean = "MEAN"
	AggregationMax  = "MAX"
)

// Pattern values for protection groups.
const (
	PatternAll            = "ALL"
	PatternArbitrary      = "ARBITRARY"
	PatternByResourceType = "BY_RESOURCE_TYPE"
)

// AutoRenew values for subscriptions.
const (
	AutoRenewEnabled  = "ENABLED"
	AutoRenewDisabled = "DISABLED"
)

// ResourceType values for Shield Advanced protected resources.
const (
	ResourceTypeCloudFrontDistribution  = "CLOUDFRONT_DISTRIBUTION"
	ResourceTypeRoute53HostedZone       = "ROUTE_53_HOSTED_ZONE"
	ResourceTypeApplicationLoadBalancer = "APPLICATION_LOAD_BALANCER"
	ResourceTypeClassicLoadBalancer     = "CLASSIC_LOAD_BALANCER"
	ResourceTypeElasticIPAllocation     = "ELASTIC_IP_ALLOCATION"
	ResourceTypeGlobalAccelerator       = "GLOBAL_ACCELERATOR"
)

// ProactiveEngagementStatus values.
const (
	ProactiveEngagementEnabled  = "ENABLED"
	ProactiveEngagementDisabled = "DISABLED"
	ProactiveEngagementPending  = "PENDING"
)

var (
	// ErrProtectionNotFound is returned when a protection does not exist.
	ErrProtectionNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrProtectionAlreadyExists is returned when a protection for the resource already exists.
	ErrProtectionAlreadyExists = awserr.New("ResourceAlreadyExistsException", awserr.ErrConflict)
	// ErrSubscriptionAlreadyExists is returned when a Shield Advanced subscription already exists.
	ErrSubscriptionAlreadyExists = awserr.New("ResourceAlreadyExistsException", awserr.ErrConflict)
	// ErrSubscriptionNotFound is returned when no subscription exists.
	ErrSubscriptionNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrSubscriptionRequired is returned when an operation requires an active Shield Advanced subscription.
	ErrSubscriptionRequired = awserr.New("InvalidOperationException: subscription required", awserr.ErrConflict)
	// ErrProtectionGroupNotFound is returned when a protection group does not exist.
	ErrProtectionGroupNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrProtectionGroupAlreadyExists is returned when a protection group with the same ID already exists.
	ErrProtectionGroupAlreadyExists = awserr.New("ResourceAlreadyExistsException", awserr.ErrConflict)
	// ErrAttackNotFound is returned when an attack does not exist.
	ErrAttackNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrValidation is returned when input validation fails.
	ErrValidation = awserr.New("InvalidParameterException", awserr.ErrInvalidParameter)
)

// validAggregations returns the set of valid aggregation values for protection groups.
func validAggregations() map[string]struct{} {
	return map[string]struct{}{
		AggregationSum:  {},
		AggregationMean: {},
		AggregationMax:  {},
	}
}

// validPatterns returns the set of valid pattern values for protection groups.
func validPatterns() map[string]struct{} {
	return map[string]struct{}{
		PatternAll:            {},
		PatternArbitrary:      {},
		PatternByResourceType: {},
	}
}

// newShieldID generates a new random hex ID (16 bytes = 32 hex chars).
// Must be called without holding the lock.
func newShieldID() string {
	b := make([]byte, protectionIDBytes)

	if _, err := rand.Read(b); err != nil {
		// Fallback: deterministic counter - practically unreachable.
		return hex.EncodeToString([]byte("fallback-shield-id"))
	}

	return hex.EncodeToString(b)
}

// protectionARN builds a Shield protection ARN.
// Shield ARNs are global (no region component).
func protectionARN(accountID, protectionID string) string {
	return fmt.Sprintf("arn:aws:shield::%s:protection/%s", accountID, protectionID)
}

// protectionGroupARN builds a Shield protection group ARN.
func protectionGroupARN(accountID, groupID string) string {
	return fmt.Sprintf("arn:aws:shield::%s:protection-group/%s", accountID, groupID)
}

// Protection represents an AWS Shield Advanced protection.
type Protection struct {
	CreationTime   time.Time         `json:"creationTime"`
	Tags           map[string]string `json:"tags,omitempty"`
	ID             string            `json:"id"`
	ProtectionArn  string            `json:"protectionArn"`
	Name           string            `json:"name"`
	ResourceARN    string            `json:"resourceARN"`
	HealthCheckIDs []string          `json:"healthCheckIds,omitempty"`
}

// cloneProtection returns a deep copy of p, including its Tags map.
func cloneProtection(p *Protection) *Protection {
	cp := *p
	cp.Tags = maps.Clone(p.Tags)

	if p.HealthCheckIDs != nil {
		cp.HealthCheckIDs = append([]string(nil), p.HealthCheckIDs...)
	}

	return &cp
}

// Subscription represents an AWS Shield Advanced subscription.
type Subscription struct {
	StartTime            time.Time `json:"startTime"`
	EndTime              time.Time `json:"endTime"`
	AutoRenew            string    `json:"autoRenew"`
	TimeCommitmentInDays int64     `json:"timeCommitmentInDays"`
}

// EmergencyContact represents an emergency contact for proactive engagement.
type EmergencyContact struct {
	EmailAddress string `json:"emailAddress"`
	PhoneNumber  string `json:"phoneNumber,omitempty"`
	ContactNotes string `json:"contactNotes,omitempty"`
}

// DRTAccess holds DRT log bucket and role configuration.
type DRTAccess struct {
	RoleArn       string   `json:"roleArn"`
	LogBucketList []string `json:"logBucketList"`
}

// ProtectionGroup represents a Shield Advanced protection group.
type ProtectionGroup struct {
	CreationTime       time.Time `json:"creationTime"`
	ID                 string    `json:"id"`
	ProtectionGroupArn string    `json:"protectionGroupArn"`
	Aggregation        string    `json:"aggregation"`
	Pattern            string    `json:"pattern"`
	ResourceType       string    `json:"resourceType,omitempty"`
	Members            []string  `json:"members"`
}

// cloneProtectionGroup returns a deep copy of a ProtectionGroup.
func cloneProtectionGroup(pg *ProtectionGroup) *ProtectionGroup {
	cp := *pg

	if pg.Members != nil {
		cp.Members = append([]string(nil), pg.Members...)
	}

	return &cp
}

// AttackVector represents a type of attack traffic seen during an attack.
type AttackVector struct {
	VectorType string `json:"VectorType"`
}

// AttackCounter represents a named counter during an attack.
//

type AttackCounter struct {
	Name    string  `json:"Name"`
	Unit    string  `json:"Unit"`
	Max     float64 `json:"Max"`
	Average float64 `json:"Average"`
	Sum     float64 `json:"Sum"`
	N       int64   `json:"N"`
}

// Mitigation represents a mitigation applied during an attack.
type Mitigation struct {
	MitigationName string `json:"MitigationName"`
}

// Attack represents a Shield Advanced attack event.
type Attack struct {
	StartTime      time.Time       `json:"startTime"`
	EndTime        time.Time       `json:"endTime"`
	AttackID       string          `json:"attackId"`
	ResourceARN    string          `json:"resourceArn"`
	AttackVectors  []AttackVector  `json:"attackVectors,omitempty"`
	AttackCounters []AttackCounter `json:"attackCounters,omitempty"`
	Mitigations    []Mitigation    `json:"mitigations,omitempty"`
}

// AttackVolume represents volume metrics in attack statistics.
type AttackVolume struct {
	BitsPerSecond     *AttackVolumeStatistics `json:"BitsPerSecond,omitempty"`
	PacketsPerSecond  *AttackVolumeStatistics `json:"PacketsPerSecond,omitempty"`
	RequestsPerSecond *AttackVolumeStatistics `json:"RequestsPerSecond,omitempty"`
}

// AttackVolumeStatistics is a single volume metric.
type AttackVolumeStatistics struct {
	Max float64 `json:"Max"`
}

// AttackStatistics represents Shield Advanced attack statistics.
type AttackStatistics struct {
	DataItems []AttackStatisticsItem `json:"dataItems"`
	TimeRange AttackTimeRange        `json:"timeRange"`
}

// AttackTimeRange represents a time range for attack statistics.
type AttackTimeRange struct {
	FromInclusive int64 `json:"fromInclusive"`
	ToExclusive   int64 `json:"toExclusive"`
}

// AttackStatisticsItem is a single item in attack statistics.
type AttackStatisticsItem struct {
	AttackVolume *AttackVolume `json:"AttackVolume,omitempty"`
	AttackCount  int64         `json:"AttackCount"`
}

// ALARConfig holds Application Layer Automatic Response configuration for a protection.
type ALARConfig struct {
	// Action is either "BLOCK" or "COUNT".
	Action  string `json:"action"`
	Enabled bool   `json:"enabled"`
}

// InMemoryBackend is an in-memory store for Shield Advanced resources.
type InMemoryBackend struct {
	protections      map[string]*Protection
	protectionGroups map[string]*ProtectionGroup
	attacks          map[string]*Attack
	// alarConfigs maps ResourceArn -> ALARConfig
	alarConfigs               map[string]*ALARConfig
	subscription              *Subscription
	drtAccess                 *DRTAccess
	resourceARNIndex          map[string]string
	nameIndex                 map[string]string
	mu                        *lockmetrics.RWMutex
	accountID                 string
	region                    string
	proactiveEngagementStatus string
	emergencyContacts         []EmergencyContact
}

// NewInMemoryBackend creates a new in-memory Shield backend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		protections:      make(map[string]*Protection),
		protectionGroups: make(map[string]*ProtectionGroup),
		attacks:          make(map[string]*Attack),
		alarConfigs:      make(map[string]*ALARConfig),
		resourceARNIndex: make(map[string]string),
		nameIndex:        make(map[string]string),
		accountID:        accountID,
		region:           region,
		mu:               lockmetrics.New("shield"),
	}
}

// Region returns the AWS region this backend is configured for.
func (b *InMemoryBackend) Region() string { return b.region }

// AccountID returns the AWS account ID this backend is configured for.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// GetALARConfig returns the ALAR config for a resource ARN, or nil if none.
func (b *InMemoryBackend) GetALARConfig(resourceARN string) *ALARConfig {
	b.mu.RLock("GetALARConfig")
	defer b.mu.RUnlock()

	cfg, ok := b.alarConfigs[resourceARN]
	if !ok {
		return nil
	}

	cp := *cfg

	return &cp
}

// CreateSubscription enables Shield Advanced. Returns an error if already subscribed.
func (b *InMemoryBackend) CreateSubscription() error {
	b.mu.Lock("CreateSubscription")
	defer b.mu.Unlock()

	if b.subscription != nil {
		return fmt.Errorf("%w: subscription already exists", ErrSubscriptionAlreadyExists)
	}

	now := time.Now()
	b.subscription = &Subscription{
		StartTime:            now,
		EndTime:              now.AddDate(1, 0, 0),
		AutoRenew:            "ENABLED",
		TimeCommitmentInDays: subscriptionCommitmentDays,
	}

	if b.proactiveEngagementStatus == "" {
		b.proactiveEngagementStatus = ProactiveEngagementDisabled
	}

	return nil
}

// DescribeSubscription returns the current Shield Advanced subscription.
func (b *InMemoryBackend) DescribeSubscription() (*Subscription, error) {
	b.mu.RLock("DescribeSubscription")
	defer b.mu.RUnlock()

	if b.subscription == nil {
		return nil, fmt.Errorf("%w: no subscription found", ErrSubscriptionNotFound)
	}

	s := *b.subscription

	return &s, nil
}

// GetSubscriptionState returns ACTIVE or INACTIVE.
func (b *InMemoryBackend) GetSubscriptionState() string {
	b.mu.RLock("GetSubscriptionState")
	defer b.mu.RUnlock()

	if b.subscription != nil {
		return "ACTIVE"
	}

	return "INACTIVE"
}

// CreateProtection creates a new Shield protection for the given resource ARN.
func (b *InMemoryBackend) CreateProtection(name, resourceARN string, tags map[string]string) (*Protection, error) {
	id := newShieldID()

	b.mu.Lock("CreateProtection")
	defer b.mu.Unlock()

	if b.subscription == nil {
		return nil, fmt.Errorf(
			"%w: Shield Advanced subscription is required to create protections",
			ErrSubscriptionRequired,
		)
	}

	if _, exists := b.nameIndex[name]; exists {
		return nil, fmt.Errorf("%w: protection %q already exists", ErrProtectionAlreadyExists, name)
	}

	if _, exists := b.resourceARNIndex[resourceARN]; exists {
		return nil, fmt.Errorf("%w: protection for resource %s already exists", ErrProtectionAlreadyExists, resourceARN)
	}

	pArn := protectionARN(b.accountID, id)

	p := &Protection{
		ID:            id,
		ProtectionArn: pArn,
		Name:          name,
		ResourceARN:   resourceARN,
		CreationTime:  time.Now(),
		Tags:          cloneTags(tags),
	}
	b.protections[id] = p
	b.resourceARNIndex[resourceARN] = id
	b.nameIndex[name] = id

	return cloneProtection(p), nil
}

// DescribeProtection returns a protection by ID or resource ARN.
func (b *InMemoryBackend) DescribeProtection(protectionID, resourceARN string) (*Protection, error) {
	b.mu.RLock("DescribeProtection")
	defer b.mu.RUnlock()

	if protectionID != "" {
		p, ok := b.protections[protectionID]
		if !ok {
			return nil, fmt.Errorf("%w: protection %q not found", ErrProtectionNotFound, protectionID)
		}

		return cloneProtection(p), nil
	}

	if pid, ok := b.resourceARNIndex[resourceARN]; ok {
		return cloneProtection(b.protections[pid]), nil
	}

	return nil, fmt.Errorf("%w: no protection for resource %q", ErrProtectionNotFound, resourceARN)
}

// DeleteProtection deletes a protection by ID.
func (b *InMemoryBackend) DeleteProtection(protectionID string) error {
	b.mu.Lock("DeleteProtection")
	defer b.mu.Unlock()

	if p, ok := b.protections[protectionID]; ok {
		delete(b.resourceARNIndex, p.ResourceARN)
		delete(b.nameIndex, p.Name)
	} else {
		return fmt.Errorf("%w: protection %q not found", ErrProtectionNotFound, protectionID)
	}

	delete(b.protections, protectionID)

	return nil
}

// ListProtections returns all protections sorted by name.
func (b *InMemoryBackend) ListProtections() []*Protection {
	b.mu.RLock("ListProtections")
	defer b.mu.RUnlock()

	list := make([]*Protection, 0, len(b.protections))

	for _, p := range b.protections {
		list = append(list, cloneProtection(p))
	}

	slices.SortFunc(list, func(a, b *Protection) int {
		if a.Name < b.Name {
			return -1
		}

		if a.Name > b.Name {
			return 1
		}

		return 0
	})

	return list
}

const (
	maxTagsPerResource = 50
	maxTagKeyLen       = 128
	maxTagValueLen     = 256
)

// TagResource adds tags to a protection, keyed by Shield protection ARN or resource ARN.
// Requires an active subscription. Enforces 50-tag cap and key/value length limits.
func (b *InMemoryBackend) TagResource(resourceARN string, tags map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	if b.subscription == nil {
		return fmt.Errorf("%w: Shield Advanced subscription is required", ErrSubscriptionRequired)
	}

	for k, v := range tags {
		if len(k) > maxTagKeyLen {
			return fmt.Errorf("%w: tag key %q exceeds 128 characters", ErrValidation, k)
		}

		if len(v) > maxTagValueLen {
			return fmt.Errorf("%w: tag value for key %q exceeds 256 characters", ErrValidation, k)
		}
	}

	p, err := b.resolveTaggableProtection(resourceARN)
	if err != nil {
		return err
	}

	if p.Tags == nil {
		p.Tags = make(map[string]string)
	}

	if len(p.Tags)+len(tags) > maxTagsPerResource {
		return fmt.Errorf("%w: resource would exceed the 50-tag limit", ErrValidation)
	}

	maps.Copy(p.Tags, tags)

	return nil
}

// ListTagsForResource returns the tags for a protection.
func (b *InMemoryBackend) ListTagsForResource(resourceARN string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	p, err := b.resolveTaggableProtection(resourceARN)
	if err != nil {
		return nil, err
	}

	return maps.Clone(p.Tags), nil
}

// UntagResource removes tags from a protection.
func (b *InMemoryBackend) UntagResource(resourceARN string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	p, err := b.resolveTaggableProtection(resourceARN)
	if err != nil {
		return err
	}

	for _, k := range tagKeys {
		delete(p.Tags, k)
	}

	return nil
}

// resolveTaggableProtection resolves a Shield protection ARN or resource ARN to a Protection.
// AWS TagResource accepts:
//   - arn:aws:shield::<acct>:protection/<id>  (Shield protection ARN)
//   - any other ARN treated as a resource ARN lookup
//
// Must be called with b.mu held.
func (b *InMemoryBackend) resolveTaggableProtection(resourceARN string) (*Protection, error) {
	// Shield protection ARN: arn:aws:shield::<acct>:protection/<id>
	if p := b.resolveShieldProtectionARN(resourceARN); p != nil {
		return p, nil
	}

	// Fall back to resource ARN index.
	if pid, ok := b.resourceARNIndex[resourceARN]; ok {
		return b.protections[pid], nil
	}

	return nil, fmt.Errorf("%w: protection %q not found", ErrProtectionNotFound, resourceARN)
}

// resolveShieldProtectionARN resolves a Shield protection ARN (arn:aws:shield::*:protection/<id>)
// to a protection, or returns nil if the ARN is not a Shield protection ARN or not found.
func (b *InMemoryBackend) resolveShieldProtectionARN(resourceARN string) *Protection {
	if !strings.HasPrefix(resourceARN, "arn:aws:shield::") || !strings.Contains(resourceARN, ":protection/") {
		return nil
	}

	parts := strings.SplitN(resourceARN, ":protection/", 2) //nolint:mnd // split into prefix and ID
	if len(parts) < 2 {                                     //nolint:mnd // require 2 parts
		return nil
	}

	id := parts[1]

	if p, ok := b.protections[id]; ok {
		return p
	}

	// Scan by ProtectionArn in case format differs.
	for _, p := range b.protections {
		if p.ProtectionArn == resourceARN {
			return p
		}
	}

	return nil
}

// cloneTags returns a deep copy of the given tag map.
func cloneTags(tags map[string]string) map[string]string {
	if tags == nil {
		return make(map[string]string)
	}

	return maps.Clone(tags)
}

// Reset clears all Shield protections and subscription state atomically.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	b.protections = make(map[string]*Protection)
	b.protectionGroups = make(map[string]*ProtectionGroup)
	b.attacks = make(map[string]*Attack)
	b.alarConfigs = make(map[string]*ALARConfig)
	b.resourceARNIndex = make(map[string]string)
	b.nameIndex = make(map[string]string)
	b.subscription = nil
	b.drtAccess = nil
	b.emergencyContacts = nil
	b.proactiveEngagementStatus = ""
	b.mu.Unlock()
}

// EnableApplicationLayerAutomaticResponse enables ALAR for the given resource ARN.
// action must be "BLOCK" or "COUNT".
func (b *InMemoryBackend) EnableApplicationLayerAutomaticResponse(resourceARN, action string) error {
	b.mu.Lock("EnableApplicationLayerAutomaticResponse")
	defer b.mu.Unlock()

	if b.subscription == nil {
		return fmt.Errorf("%w: Shield Advanced subscription is required", ErrSubscriptionRequired)
	}

	if _, ok := b.resourceARNIndex[resourceARN]; !ok {
		return fmt.Errorf("%w: no protection found for resource %q", ErrProtectionNotFound, resourceARN)
	}

	b.alarConfigs[resourceARN] = &ALARConfig{Enabled: true, Action: action}

	return nil
}

// DisableApplicationLayerAutomaticResponse disables ALAR for the given resource ARN.
func (b *InMemoryBackend) DisableApplicationLayerAutomaticResponse(resourceARN string) error {
	b.mu.Lock("DisableApplicationLayerAutomaticResponse")
	defer b.mu.Unlock()

	if _, ok := b.resourceARNIndex[resourceARN]; !ok {
		return fmt.Errorf("%w: no protection found for resource %q", ErrProtectionNotFound, resourceARN)
	}

	delete(b.alarConfigs, resourceARN)

	return nil
}

// UpdateApplicationLayerAutomaticResponse updates the ALAR action for a resource ARN.
func (b *InMemoryBackend) UpdateApplicationLayerAutomaticResponse(resourceARN, action string) error {
	b.mu.Lock("UpdateApplicationLayerAutomaticResponse")
	defer b.mu.Unlock()

	if _, ok := b.resourceARNIndex[resourceARN]; !ok {
		return fmt.Errorf("%w: no protection found for resource %q", ErrProtectionNotFound, resourceARN)
	}

	cfg, ok := b.alarConfigs[resourceARN]
	if !ok || !cfg.Enabled {
		return fmt.Errorf(
			"%w: ALAR is not enabled for resource %q; enable it first",
			ErrValidation,
			resourceARN,
		)
	}

	cfg.Action = action

	return nil
}

// resourceARNMatchesType returns true if the resource ARN belongs to the given Shield resource type.
func resourceARNMatchesType(resourceARN, resourceType string) bool {
	arn := strings.ToLower(resourceARN)

	switch resourceType {
	case ResourceTypeCloudFrontDistribution:
		return strings.Contains(arn, ":cloudfront:") || strings.Contains(arn, "cloudfront")
	case ResourceTypeRoute53HostedZone:
		return strings.Contains(arn, ":route53:") || strings.Contains(arn, "hostedzone")
	case ResourceTypeApplicationLoadBalancer:
		return strings.Contains(arn, "elasticloadbalancing") && strings.Contains(arn, "loadbalancer/app/")
	case ResourceTypeClassicLoadBalancer:
		return strings.Contains(arn, "elasticloadbalancing") && strings.Contains(arn, "loadbalancer/") &&
			!strings.Contains(arn, "/app/") && !strings.Contains(arn, "/net/")
	case ResourceTypeElasticIPAllocation:
		return strings.Contains(arn, ":ec2:") &&
			(strings.Contains(arn, "eip-allocation") || strings.Contains(arn, "/eip"))
	case ResourceTypeGlobalAccelerator:
		return strings.Contains(arn, "globalaccelerator")
	}

	return false
}

// ListResourcesInProtectionGroup returns the member ARNs for a protection group.
// For Pattern=ALL returns all protections; for Pattern=BY_RESOURCE_TYPE derives members by resource type.
func (b *InMemoryBackend) ListResourcesInProtectionGroup(protectionGroupID string) ([]string, error) {
	b.mu.RLock("ListResourcesInProtectionGroup")
	defer b.mu.RUnlock()

	pg, ok := b.protectionGroups[protectionGroupID]
	if !ok {
		return nil, fmt.Errorf("%w: protection group %q not found", ErrProtectionGroupNotFound, protectionGroupID)
	}

	switch pg.Pattern {
	case PatternAll:
		arns := make([]string, 0, len(b.protections))
		for _, p := range b.protections {
			arns = append(arns, p.ResourceARN)
		}

		slices.Sort(arns)

		return arns, nil

	case PatternByResourceType:
		arns := make([]string, 0)
		for _, p := range b.protections {
			if resourceARNMatchesType(p.ResourceARN, pg.ResourceType) {
				arns = append(arns, p.ResourceARN)
			}
		}

		slices.Sort(arns)

		return arns, nil
	}

	members := append([]string(nil), pg.Members...)

	return members, nil
}

// AddProtectionInternal creates a protection directly (for tests).
func (b *InMemoryBackend) AddProtectionInternal(name, resourceARN string) *Protection {
	id := newShieldID()

	b.mu.Lock("AddProtectionInternal")
	defer b.mu.Unlock()

	pArn := protectionARN(b.accountID, id)

	p := &Protection{
		ID:            id,
		ProtectionArn: pArn,
		Name:          name,
		ResourceARN:   resourceARN,
		CreationTime:  time.Now(),
		Tags:          make(map[string]string),
	}
	b.protections[id] = p
	b.resourceARNIndex[resourceARN] = id
	b.nameIndex[name] = id

	return cloneProtection(p)
}

// AddSubscriptionInternal creates a subscription directly (for tests).
func (b *InMemoryBackend) AddSubscriptionInternal() {
	b.mu.Lock("AddSubscriptionInternal")
	defer b.mu.Unlock()

	now := time.Now()
	b.subscription = &Subscription{
		StartTime:            now,
		EndTime:              now.AddDate(1, 0, 0),
		AutoRenew:            AutoRenewEnabled,
		TimeCommitmentInDays: subscriptionCommitmentDays,
	}
}

// DeleteSubscription cancels the active Shield Advanced subscription.
func (b *InMemoryBackend) DeleteSubscription() error {
	b.mu.Lock("DeleteSubscription")
	defer b.mu.Unlock()

	if b.subscription == nil {
		return fmt.Errorf("%w: no active subscription found", ErrSubscriptionNotFound)
	}

	b.subscription = nil

	return nil
}

// AssociateDRTLogBucket associates an S3 log bucket with the DRT.
func (b *InMemoryBackend) AssociateDRTLogBucket(bucket string) error {
	if bucket == "" {
		return fmt.Errorf("%w: LogBucket is required", ErrValidation)
	}

	b.mu.Lock("AssociateDRTLogBucket")
	defer b.mu.Unlock()

	if b.subscription == nil {
		return fmt.Errorf("%w: Shield Advanced subscription is required", ErrSubscriptionRequired)
	}

	if b.drtAccess == nil {
		b.drtAccess = &DRTAccess{}
	}

	if slices.Contains(b.drtAccess.LogBucketList, bucket) {
		return nil
	}

	b.drtAccess.LogBucketList = append(b.drtAccess.LogBucketList, bucket)

	return nil
}

// DisassociateDRTLogBucket removes an S3 log bucket from the DRT.
func (b *InMemoryBackend) DisassociateDRTLogBucket(bucket string) error {
	b.mu.Lock("DisassociateDRTLogBucket")
	defer b.mu.Unlock()

	if b.drtAccess == nil {
		return fmt.Errorf("%w: bucket %q not associated", ErrProtectionNotFound, bucket)
	}

	idx := slices.Index(b.drtAccess.LogBucketList, bucket)
	if idx < 0 {
		return fmt.Errorf("%w: bucket %q not associated with DRT", ErrProtectionNotFound, bucket)
	}

	b.drtAccess.LogBucketList = slices.Delete(b.drtAccess.LogBucketList, idx, idx+1)

	return nil
}

// AssociateDRTRole associates an IAM role with the DRT.
func (b *InMemoryBackend) AssociateDRTRole(roleARN string) error {
	if roleARN == "" {
		return fmt.Errorf("%w: RoleArn is required", ErrValidation)
	}

	b.mu.Lock("AssociateDRTRole")
	defer b.mu.Unlock()

	if b.subscription == nil {
		return fmt.Errorf("%w: Shield Advanced subscription is required", ErrSubscriptionRequired)
	}

	if b.drtAccess == nil {
		b.drtAccess = &DRTAccess{}
	}

	b.drtAccess.RoleArn = roleARN

	return nil
}

// DisassociateDRTRole removes the IAM role association from the DRT. Idempotent per AWS.
func (b *InMemoryBackend) DisassociateDRTRole() error {
	b.mu.Lock("DisassociateDRTRole")
	defer b.mu.Unlock()

	if b.drtAccess != nil {
		b.drtAccess.RoleArn = ""
	}

	return nil
}

// DescribeDRTAccess returns the current DRT access configuration.
func (b *InMemoryBackend) DescribeDRTAccess() *DRTAccess {
	b.mu.RLock("DescribeDRTAccess")
	defer b.mu.RUnlock()

	if b.drtAccess == nil {
		return &DRTAccess{LogBucketList: []string{}}
	}

	cp := *b.drtAccess
	cp.LogBucketList = append([]string(nil), b.drtAccess.LogBucketList...)

	return &cp
}

// AssociateHealthCheck associates a Route 53 health check with a protection.
func (b *InMemoryBackend) AssociateHealthCheck(protectionID, healthCheckARN string) error {
	b.mu.Lock("AssociateHealthCheck")
	defer b.mu.Unlock()

	p, ok := b.protections[protectionID]
	if !ok {
		return fmt.Errorf("%w: protection %q not found", ErrProtectionNotFound, protectionID)
	}

	if slices.Contains(p.HealthCheckIDs, healthCheckARN) {
		return nil
	}

	p.HealthCheckIDs = append(p.HealthCheckIDs, healthCheckARN)

	return nil
}

// DisassociateHealthCheck removes a Route 53 health check from a protection.
func (b *InMemoryBackend) DisassociateHealthCheck(protectionID, healthCheckARN string) error {
	b.mu.Lock("DisassociateHealthCheck")
	defer b.mu.Unlock()

	p, ok := b.protections[protectionID]
	if !ok {
		return fmt.Errorf("%w: protection %q not found", ErrProtectionNotFound, protectionID)
	}

	idx := slices.Index(p.HealthCheckIDs, healthCheckARN)
	if idx < 0 {
		return fmt.Errorf(
			"%w: health check %q not associated with protection %q",
			ErrProtectionNotFound,
			healthCheckARN,
			protectionID,
		)
	}

	p.HealthCheckIDs = slices.Delete(p.HealthCheckIDs, idx, idx+1)

	return nil
}

// AssociateProactiveEngagementDetails stores emergency contact details for proactive engagement.
// Sets status to PENDING when transitioning from DISABLED or empty.
func (b *InMemoryBackend) AssociateProactiveEngagementDetails(contacts []EmergencyContact) error {
	b.mu.Lock("AssociateProactiveEngagementDetails")
	defer b.mu.Unlock()

	b.emergencyContacts = append([]EmergencyContact(nil), contacts...)

	if b.proactiveEngagementStatus == "" || b.proactiveEngagementStatus == ProactiveEngagementDisabled {
		b.proactiveEngagementStatus = ProactiveEngagementPending
	}

	return nil
}

const maxEmergencyContacts = 10

// UpdateEmergencyContactSettings replaces the emergency contact list.
// Enforces: max 10 contacts, non-empty EmailAddress required.
func (b *InMemoryBackend) UpdateEmergencyContactSettings(contacts []EmergencyContact) error {
	if len(contacts) > maxEmergencyContacts {
		return fmt.Errorf(
			"%w: EmergencyContactList cannot exceed %d contacts",
			ErrValidation,
			maxEmergencyContacts,
		)
	}

	for _, c := range contacts {
		if c.EmailAddress == "" {
			return fmt.Errorf("%w: EmailAddress is required for each emergency contact", ErrValidation)
		}
	}

	b.mu.Lock("UpdateEmergencyContactSettings")
	defer b.mu.Unlock()

	b.emergencyContacts = append([]EmergencyContact(nil), contacts...)

	return nil
}

// GetProactiveEngagementStatus returns the current proactive engagement status.
func (b *InMemoryBackend) GetProactiveEngagementStatus() string {
	b.mu.RLock("GetProactiveEngagementStatus")
	defer b.mu.RUnlock()

	return b.proactiveEngagementStatus
}

// DescribeEmergencyContactSettings returns the current emergency contacts.
func (b *InMemoryBackend) DescribeEmergencyContactSettings() []EmergencyContact {
	b.mu.RLock("DescribeEmergencyContactSettings")
	defer b.mu.RUnlock()

	return append([]EmergencyContact(nil), b.emergencyContacts...)
}

// EnableProactiveEngagement enables proactive engagement for the subscription.
// Requires at least one emergency contact to be configured.
func (b *InMemoryBackend) EnableProactiveEngagement() error {
	b.mu.Lock("EnableProactiveEngagement")
	defer b.mu.Unlock()

	if b.subscription == nil {
		return fmt.Errorf("%w: Shield Advanced subscription is required", ErrSubscriptionRequired)
	}

	if len(b.emergencyContacts) == 0 {
		return fmt.Errorf(
			"%w: EmergencyContactList must be populated before enabling proactive engagement",
			ErrValidation,
		)
	}

	b.proactiveEngagementStatus = ProactiveEngagementEnabled

	return nil
}

// DisableProactiveEngagement disables proactive engagement for the subscription.
func (b *InMemoryBackend) DisableProactiveEngagement() error {
	b.mu.Lock("DisableProactiveEngagement")
	defer b.mu.Unlock()

	if b.subscription == nil {
		return fmt.Errorf("%w: Shield Advanced subscription is required", ErrSubscriptionRequired)
	}

	b.proactiveEngagementStatus = ProactiveEngagementDisabled

	return nil
}

// UpdateSubscription updates the auto-renew setting of the active subscription.
func (b *InMemoryBackend) UpdateSubscription(autoRenew string) error {
	b.mu.Lock("UpdateSubscription")
	defer b.mu.Unlock()

	if b.subscription == nil {
		return fmt.Errorf("%w: no active subscription found", ErrSubscriptionNotFound)
	}

	b.subscription.AutoRenew = autoRenew

	return nil
}

// CreateProtectionGroup creates a new Shield Advanced protection group.
func (b *InMemoryBackend) CreateProtectionGroup(
	id, aggregation, pattern, resourceType string,
	members []string,
) (*ProtectionGroup, error) {
	b.mu.Lock("CreateProtectionGroup")
	defer b.mu.Unlock()

	if id == "" {
		return nil, fmt.Errorf("%w: ProtectionGroupId is required", ErrValidation)
	}

	if b.subscription == nil {
		return nil, fmt.Errorf("%w: Shield Advanced subscription is required", ErrSubscriptionRequired)
	}

	if _, valid := validAggregations()[aggregation]; !valid {
		return nil, fmt.Errorf("%w: Aggregation must be one of SUM, MEAN, MAX", ErrValidation)
	}

	if _, valid := validPatterns()[pattern]; !valid {
		return nil, fmt.Errorf("%w: Pattern must be one of ALL, ARBITRARY, BY_RESOURCE_TYPE", ErrValidation)
	}

	if pattern == PatternArbitrary && len(members) == 0 {
		return nil, fmt.Errorf("%w: Members is required when Pattern is ARBITRARY", ErrValidation)
	}

	if pattern == PatternByResourceType && resourceType == "" {
		return nil, fmt.Errorf("%w: ResourceType is required when Pattern is BY_RESOURCE_TYPE", ErrValidation)
	}

	if _, exists := b.protectionGroups[id]; exists {
		return nil, fmt.Errorf("%w: protection group %q already exists", ErrProtectionGroupAlreadyExists, id)
	}

	groupArn := protectionGroupARN(b.accountID, id)

	pg := &ProtectionGroup{
		ID:                 id,
		ProtectionGroupArn: groupArn,
		Aggregation:        aggregation,
		Pattern:            pattern,
		ResourceType:       resourceType,
		Members:            append([]string(nil), members...),
		CreationTime:       time.Now(),
	}
	b.protectionGroups[id] = pg

	return cloneProtectionGroup(pg), nil
}

// DescribeProtectionGroup returns a single protection group by ID.
func (b *InMemoryBackend) DescribeProtectionGroup(id string) (*ProtectionGroup, error) {
	b.mu.RLock("DescribeProtectionGroup")
	defer b.mu.RUnlock()

	pg, ok := b.protectionGroups[id]
	if !ok {
		return nil, fmt.Errorf("%w: protection group %q not found", ErrProtectionGroupNotFound, id)
	}

	return cloneProtectionGroup(pg), nil
}

// ListProtectionGroups returns all protection groups sorted by ID.
func (b *InMemoryBackend) ListProtectionGroups() []*ProtectionGroup {
	b.mu.RLock("ListProtectionGroups")
	defer b.mu.RUnlock()

	list := make([]*ProtectionGroup, 0, len(b.protectionGroups))

	for _, pg := range b.protectionGroups {
		list = append(list, cloneProtectionGroup(pg))
	}

	slices.SortFunc(list, func(a, b *ProtectionGroup) int {
		if a.ID < b.ID {
			return -1
		}

		if a.ID > b.ID {
			return 1
		}

		return 0
	})

	return list
}

// UpdateProtectionGroup updates the aggregation, pattern, resource type, and members of a group.
func (b *InMemoryBackend) UpdateProtectionGroup(
	id, aggregation, pattern, resourceType string,
	members []string,
) error {
	b.mu.Lock("UpdateProtectionGroup")
	defer b.mu.Unlock()

	pg, ok := b.protectionGroups[id]
	if !ok {
		return fmt.Errorf("%w: protection group %q not found", ErrProtectionGroupNotFound, id)
	}

	if _, valid := validAggregations()[aggregation]; !valid {
		return fmt.Errorf("%w: Aggregation must be one of SUM, MEAN, MAX", ErrValidation)
	}

	if _, valid := validPatterns()[pattern]; !valid {
		return fmt.Errorf("%w: Pattern must be one of ALL, ARBITRARY, BY_RESOURCE_TYPE", ErrValidation)
	}

	if pattern == PatternArbitrary && len(members) == 0 {
		return fmt.Errorf("%w: Members is required when Pattern is ARBITRARY", ErrValidation)
	}

	if pattern == PatternByResourceType && resourceType == "" {
		return fmt.Errorf("%w: ResourceType is required when Pattern is BY_RESOURCE_TYPE", ErrValidation)
	}

	pg.Aggregation = aggregation
	pg.Pattern = pattern
	pg.ResourceType = resourceType
	pg.Members = append([]string(nil), members...)

	return nil
}

// ListAttacks returns all attacks, optionally filtered by resource ARNs (match any).
// start and end are optional Unix epoch seconds (0 = not filtered).
func (b *InMemoryBackend) ListAttacks(resourceARNs []string, startTime, endTime int64) []*Attack {
	b.mu.RLock("ListAttacks")
	defer b.mu.RUnlock()

	arnSet := make(map[string]struct{}, len(resourceARNs))
	for _, a := range resourceARNs {
		arnSet[a] = struct{}{}
	}

	list := make([]*Attack, 0, len(b.attacks))

	for _, a := range b.attacks {
		if len(arnSet) > 0 {
			if _, ok := arnSet[a.ResourceARN]; !ok {
				continue
			}
		}

		ts := a.StartTime.Unix()
		if startTime > 0 && ts < startTime {
			continue
		}

		if endTime > 0 && ts > endTime {
			continue
		}

		cp := *a
		cp.AttackVectors = append([]AttackVector(nil), a.AttackVectors...)
		cp.AttackCounters = append([]AttackCounter(nil), a.AttackCounters...)
		cp.Mitigations = append([]Mitigation(nil), a.Mitigations...)
		list = append(list, &cp)
	}

	slices.SortFunc(list, func(a, b *Attack) int {
		if a.StartTime.Before(b.StartTime) {
			return -1
		}

		if a.StartTime.After(b.StartTime) {
			return 1
		}

		return 0
	})

	return list
}

// DeleteProtectionGroup removes a Shield Advanced protection group.
func (b *InMemoryBackend) DeleteProtectionGroup(protectionGroupID string) error {
	b.mu.Lock("DeleteProtectionGroup")
	defer b.mu.Unlock()

	if _, ok := b.protectionGroups[protectionGroupID]; !ok {
		return fmt.Errorf("%w: protection group %q not found", ErrProtectionGroupNotFound, protectionGroupID)
	}

	delete(b.protectionGroups, protectionGroupID)

	return nil
}

// AddAttackInternal creates an attack record directly (for tests).
func (b *InMemoryBackend) AddAttackInternal(attackID, resourceARN string) *Attack {
	b.mu.Lock("AddAttackInternal")
	defer b.mu.Unlock()

	now := time.Now()
	a := &Attack{
		AttackID:    attackID,
		ResourceARN: resourceARN,
		StartTime:   now.Add(-1 * time.Hour),
		EndTime:     now,
		AttackVectors: []AttackVector{
			{VectorType: "SYN_FLOOD"},
		},
		Mitigations: []Mitigation{
			{MitigationName: "Shield Advanced mitigation"},
		},
	}
	b.attacks[attackID] = a

	cp := *a

	return &cp
}

// Representative traffic magnitudes for simulated attack counters.
const (
	simBpsMax  = 1e9
	simBpsAvg  = 5e8
	simBpsSum  = 3e10
	simPpsMax  = 1e6
	simPpsAvg  = 5e5
	simPpsSum  = 3e7
	simSamples = 60
)

// simAttackCounters returns representative traffic counters for a simulated attack.
func simAttackCounters() []AttackCounter {
	return []AttackCounter{
		{Name: "Total bps", Max: simBpsMax, Average: simBpsAvg, Sum: simBpsSum, N: simSamples, Unit: "bits/second"},
		{Name: "Total pps", Max: simPpsMax, Average: simPpsAvg, Sum: simPpsSum, N: simSamples, Unit: "packets/second"},
	}
}

// SimulateAttack creates a synthetic attack record reachable via the API.
// attackVectorTypes may be empty; defaults to SYN_FLOOD if omitted.
func (b *InMemoryBackend) SimulateAttack(resourceARN string, attackVectorTypes []string) (*Attack, error) {
	b.mu.Lock("SimulateAttack")
	defer b.mu.Unlock()

	if _, ok := b.resourceARNIndex[resourceARN]; !ok {
		return nil, fmt.Errorf("%w: no protection found for resource %q", ErrProtectionNotFound, resourceARN)
	}

	if len(attackVectorTypes) == 0 {
		attackVectorTypes = []string{"SYN_FLOOD"}
	}

	vectors := make([]AttackVector, 0, len(attackVectorTypes))
	for _, vt := range attackVectorTypes {
		vectors = append(vectors, AttackVector{VectorType: vt})
	}

	id := newShieldID()
	now := time.Now()
	a := &Attack{
		AttackID:       id,
		ResourceARN:    resourceARN,
		StartTime:      now.Add(-5 * time.Minute),
		EndTime:        now,
		AttackVectors:  vectors,
		AttackCounters: simAttackCounters(),
		Mitigations: []Mitigation{
			{MitigationName: "Shield Advanced mitigation"},
		},
	}
	b.attacks[id] = a

	cp := *a
	cp.AttackVectors = append([]AttackVector(nil), a.AttackVectors...)
	cp.AttackCounters = append([]AttackCounter(nil), a.AttackCounters...)
	cp.Mitigations = append([]Mitigation(nil), a.Mitigations...)

	return &cp, nil
}

// AddProtectionGroupInternal creates a protection group directly (for tests).
func (b *InMemoryBackend) AddProtectionGroupInternal(id, aggregation, pattern string) *ProtectionGroup {
	b.mu.Lock("AddProtectionGroupInternal")
	defer b.mu.Unlock()

	groupArn := protectionGroupARN(b.accountID, id)

	pg := &ProtectionGroup{
		ID:                 id,
		ProtectionGroupArn: groupArn,
		Aggregation:        aggregation,
		Pattern:            pattern,
		Members:            []string{},
		CreationTime:       time.Now(),
	}
	b.protectionGroups[id] = pg

	return cloneProtectionGroup(pg)
}

// DescribeAttack returns the details of a specific attack.
func (b *InMemoryBackend) DescribeAttack(attackID string) (*Attack, error) {
	b.mu.RLock("DescribeAttack")
	defer b.mu.RUnlock()

	a, ok := b.attacks[attackID]
	if !ok {
		return nil, fmt.Errorf("%w: attack %q not found", ErrAttackNotFound, attackID)
	}

	atk := *a

	return &atk, nil
}

// attackStatsBucket holds per-month volume maxima.
type attackStatsBucket struct {
	count  int64
	maxBps float64
	maxPps float64
	maxRps float64
}

// accumulateCounters updates bucket maxima from attack counters.
func accumulateCounters(bk *attackStatsBucket, counters []AttackCounter) {
	for _, c := range counters {
		switch {
		case strings.Contains(c.Name, "bps"):
			if c.Max > bk.maxBps {
				bk.maxBps = c.Max
			}
		case strings.Contains(c.Name, "pps"):
			if c.Max > bk.maxPps {
				bk.maxPps = c.Max
			}
		case strings.Contains(c.Name, "rps"):
			if c.Max > bk.maxRps {
				bk.maxRps = c.Max
			}
		}
	}
}

// buildAttackVolumeFromBucket converts non-zero maxima to an AttackVolume.
func buildAttackVolumeFromBucket(bk *attackStatsBucket) *AttackVolume {
	if bk.maxBps == 0 && bk.maxPps == 0 && bk.maxRps == 0 {
		return nil
	}

	vol := &AttackVolume{}

	if bk.maxBps > 0 {
		vol.BitsPerSecond = &AttackVolumeStatistics{Max: bk.maxBps}
	}

	if bk.maxPps > 0 {
		vol.PacketsPerSecond = &AttackVolumeStatistics{Max: bk.maxPps}
	}

	if bk.maxRps > 0 {
		vol.RequestsPerSecond = &AttackVolumeStatistics{Max: bk.maxRps}
	}

	return vol
}

// DescribeAttackStatistics returns summary statistics about attacks, bucketed by month.
func (b *InMemoryBackend) DescribeAttackStatistics() *AttackStatistics {
	b.mu.RLock("DescribeAttackStatistics")
	defer b.mu.RUnlock()

	now := time.Now()
	from := now.AddDate(-1, 0, 0)

	buckets := make(map[string]*attackStatsBucket)

	for _, a := range b.attacks {
		if a.StartTime.Before(from) || a.StartTime.After(now) {
			continue
		}

		key := a.StartTime.Format("2006-01")
		bk := buckets[key]

		if bk == nil {
			bk = &attackStatsBucket{}
			buckets[key] = bk
		}

		bk.count++
		accumulateCounters(bk, a.AttackCounters)
	}

	keys := make([]string, 0, len(buckets))
	for k := range buckets {
		keys = append(keys, k)
	}

	slices.Sort(keys)

	items := make([]AttackStatisticsItem, 0, len(keys))

	for _, k := range keys {
		bk := buckets[k]
		item := AttackStatisticsItem{AttackCount: bk.count, AttackVolume: buildAttackVolumeFromBucket(bk)}
		items = append(items, item)
	}

	if len(items) == 0 {
		items = []AttackStatisticsItem{{AttackCount: 0}}
	}

	return &AttackStatistics{
		TimeRange: AttackTimeRange{
			FromInclusive: from.Unix(),
			ToExclusive:   now.Unix(),
		},
		DataItems: items,
	}
}
