package shield

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"maps"
	"slices"
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

// Attack represents a Shield Advanced attack event.
type Attack struct {
	StartTime   time.Time `json:"startTime"`
	EndTime     time.Time `json:"endTime"`
	AttackID    string    `json:"attackId"`
	ResourceARN string    `json:"resourceArn"`
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
	AttackCount int64 `json:"AttackCount"`
}

// InMemoryBackend is an in-memory store for Shield Advanced resources.
type InMemoryBackend struct {
	protections               map[string]*Protection
	protectionGroups          map[string]*ProtectionGroup
	attacks                   map[string]*Attack
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

// TagResource adds tags to a protection.
func (b *InMemoryBackend) TagResource(resourceARN string, tags map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	p, ok := b.protections[resourceARN]
	if !ok {
		return fmt.Errorf("%w: protection %q not found", ErrProtectionNotFound, resourceARN)
	}

	if p.Tags == nil {
		p.Tags = make(map[string]string)
	}

	maps.Copy(p.Tags, tags)

	return nil
}

// ListTagsForResource returns the tags for a protection.
func (b *InMemoryBackend) ListTagsForResource(resourceARN string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	p, ok := b.protections[resourceARN]
	if !ok {
		return nil, fmt.Errorf("%w: protection %q not found", ErrProtectionNotFound, resourceARN)
	}

	return maps.Clone(p.Tags), nil
}

// UntagResource removes tags from a protection.
func (b *InMemoryBackend) UntagResource(resourceARN string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	p, ok := b.protections[resourceARN]
	if !ok {
		return fmt.Errorf("%w: protection %q not found", ErrProtectionNotFound, resourceARN)
	}

	for _, k := range tagKeys {
		delete(p.Tags, k)
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

// Reset clears all Shield protections and subscription state.
func (b *InMemoryBackend) Reset() {
	b.mu.Close()
	b.mu = lockmetrics.New("shield")
	b.protections = make(map[string]*Protection)
	b.protectionGroups = make(map[string]*ProtectionGroup)
	b.attacks = make(map[string]*Attack)
	b.resourceARNIndex = make(map[string]string)
	b.nameIndex = make(map[string]string)
	b.subscription = nil
	b.drtAccess = nil
	b.emergencyContacts = nil
	b.proactiveEngagementStatus = ""
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

// DisassociateDRTRole removes the IAM role association from the DRT.
func (b *InMemoryBackend) DisassociateDRTRole() error {
	b.mu.Lock("DisassociateDRTRole")
	defer b.mu.Unlock()

	if b.drtAccess == nil || b.drtAccess.RoleArn == "" {
		return fmt.Errorf("%w: no DRT role is currently associated", ErrProtectionNotFound)
	}

	b.drtAccess.RoleArn = ""

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
func (b *InMemoryBackend) AssociateProactiveEngagementDetails(contacts []EmergencyContact) error {
	b.mu.Lock("AssociateProactiveEngagementDetails")
	defer b.mu.Unlock()

	b.emergencyContacts = append([]EmergencyContact(nil), contacts...)

	return nil
}

// UpdateEmergencyContactSettings replaces the emergency contact list.
func (b *InMemoryBackend) UpdateEmergencyContactSettings(contacts []EmergencyContact) error {
	b.mu.Lock("UpdateEmergencyContactSettings")
	defer b.mu.Unlock()

	b.emergencyContacts = append([]EmergencyContact(nil), contacts...)

	return nil
}

// DescribeEmergencyContactSettings returns the current emergency contacts.
func (b *InMemoryBackend) DescribeEmergencyContactSettings() []EmergencyContact {
	b.mu.RLock("DescribeEmergencyContactSettings")
	defer b.mu.RUnlock()

	return append([]EmergencyContact(nil), b.emergencyContacts...)
}

// EnableProactiveEngagement enables proactive engagement for the subscription.
func (b *InMemoryBackend) EnableProactiveEngagement() error {
	b.mu.Lock("EnableProactiveEngagement")
	defer b.mu.Unlock()

	if b.subscription == nil {
		return fmt.Errorf("%w: Shield Advanced subscription is required", ErrSubscriptionRequired)
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

// ListAttacks returns all attacks, optionally filtered by resource ARN.
// start and end are optional Unix epoch seconds (0 = not filtered).
func (b *InMemoryBackend) ListAttacks(resourceARN string, startTime, endTime int64) []*Attack {
	b.mu.RLock("ListAttacks")
	defer b.mu.RUnlock()

	list := make([]*Attack, 0, len(b.attacks))

	for _, a := range b.attacks {
		if resourceARN != "" && a.ResourceARN != resourceARN {
			continue
		}

		ts := a.StartTime.Unix()
		if startTime > 0 && ts < startTime {
			continue
		}

		if endTime > 0 && ts > endTime {
			continue
		}

		cp := *a
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
	}
	b.attacks[attackID] = a

	cp := *a

	return &cp
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

// DescribeAttackStatistics returns summary statistics about attacks.
func (b *InMemoryBackend) DescribeAttackStatistics() *AttackStatistics {
	b.mu.RLock("DescribeAttackStatistics")
	defer b.mu.RUnlock()

	now := time.Now()
	stats := &AttackStatistics{
		TimeRange: AttackTimeRange{
			FromInclusive: now.AddDate(-1, 0, 0).Unix(),
			ToExclusive:   now.Unix(),
		},
		DataItems: []AttackStatisticsItem{
			{AttackCount: int64(len(b.attacks))},
		},
	}

	return stats
}
