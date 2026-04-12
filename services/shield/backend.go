package shield

import (
	"fmt"
	"maps"
	"slices"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

// subscriptionCommitmentDays is the default Shield Advanced subscription commitment period.
const subscriptionCommitmentDays int64 = 365

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

// Protection represents an AWS Shield Advanced protection.
type Protection struct {
	CreationTime   time.Time         `json:"creationTime"`
	Tags           map[string]string `json:"tags,omitempty"`
	ID             string            `json:"id"`
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
	CreationTime time.Time `json:"creationTime"`
	ID           string    `json:"id"`
	Aggregation  string    `json:"aggregation"`
	Pattern      string    `json:"pattern"`
	ResourceType string    `json:"resourceType,omitempty"`
	Members      []string  `json:"members"`
}

// cloneProtectionGroup returns a deep copy of a ProtectionGroup.
func cloneProtectionGroup(pg *ProtectionGroup) *ProtectionGroup {
	cp := *pg
	cp.Members = append([]string(nil), pg.Members...)

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
	protections       map[string]*Protection
	protectionGroups  map[string]*ProtectionGroup
	attacks           map[string]*Attack
	subscription      *Subscription
	drtAccess         *DRTAccess
	resourceARNIndex  map[string]string
	nameIndex         map[string]string
	mu                *lockmetrics.RWMutex
	accountID         string
	region            string
	emergencyContacts []EmergencyContact
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

	protectionARN := arn.Build("shield", b.region, b.accountID, "protection/"+name)

	p := &Protection{
		ID:           protectionARN,
		Name:         name,
		ResourceARN:  resourceARN,
		CreationTime: time.Now(),
		Tags:         cloneTags(tags),
	}
	b.protections[protectionARN] = p
	b.resourceARNIndex[resourceARN] = protectionARN
	b.nameIndex[name] = protectionARN

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

	sort.Slice(list, func(i, j int) bool {
		return list[i].Name < list[j].Name
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
}

// AddProtectionInternal creates a protection directly (for tests).
func (b *InMemoryBackend) AddProtectionInternal(name, resourceARN string) *Protection {
	b.mu.Lock("AddProtectionInternal")
	defer b.mu.Unlock()

	protectionARN := arn.Build("shield", b.region, b.accountID, "protection/"+name)

	p := &Protection{
		ID:           protectionARN,
		Name:         name,
		ResourceARN:  resourceARN,
		CreationTime: time.Now(),
		Tags:         make(map[string]string),
	}
	b.protections[protectionARN] = p
	b.resourceARNIndex[resourceARN] = protectionARN
	b.nameIndex[name] = protectionARN

	return cloneProtection(p)
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
	b.mu.Lock("AssociateDRTLogBucket")
	defer b.mu.Unlock()

	if bucket == "" {
		return fmt.Errorf("%w: LogBucket is required", ErrValidation)
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

// AssociateDRTRole associates an IAM role with the DRT.
func (b *InMemoryBackend) AssociateDRTRole(roleARN string) error {
	b.mu.Lock("AssociateDRTRole")
	defer b.mu.Unlock()

	if roleARN == "" {
		return fmt.Errorf("%w: RoleArn is required", ErrValidation)
	}

	if b.drtAccess == nil {
		b.drtAccess = &DRTAccess{}
	}

	b.drtAccess.RoleArn = roleARN

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

	if protectionID == "" {
		return fmt.Errorf("%w: ProtectionId is required", ErrValidation)
	}

	if healthCheckARN == "" {
		return fmt.Errorf("%w: HealthCheckArn is required", ErrValidation)
	}

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

// AssociateProactiveEngagementDetails stores emergency contact details for proactive engagement.
func (b *InMemoryBackend) AssociateProactiveEngagementDetails(contacts []EmergencyContact) error {
	b.mu.Lock("AssociateProactiveEngagementDetails")
	defer b.mu.Unlock()

	if len(contacts) == 0 {
		return fmt.Errorf("%w: at least one EmergencyContact is required", ErrValidation)
	}

	b.emergencyContacts = append([]EmergencyContact(nil), contacts...)

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

	if aggregation == "" {
		return nil, fmt.Errorf("%w: Aggregation is required", ErrValidation)
	}

	if pattern == "" {
		return nil, fmt.Errorf("%w: Pattern is required", ErrValidation)
	}

	if _, exists := b.protectionGroups[id]; exists {
		return nil, fmt.Errorf("%w: protection group %q already exists", ErrProtectionGroupAlreadyExists, id)
	}

	pg := &ProtectionGroup{
		ID:           id,
		Aggregation:  aggregation,
		Pattern:      pattern,
		ResourceType: resourceType,
		Members:      append([]string(nil), members...),
		CreationTime: time.Now(),
	}
	b.protectionGroups[id] = pg

	return cloneProtectionGroup(pg), nil
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

	atk := *a

	return &atk
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
