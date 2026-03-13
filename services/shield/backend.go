package shield

import (
	"fmt"
	"maps"
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
)

// Protection represents an AWS Shield Advanced protection.
type Protection struct {
	CreationTime time.Time
	Tags         map[string]string
	ID           string
	Name         string
	ResourceARN  string
}

// cloneProtection returns a deep copy of p, including its Tags map.
func cloneProtection(p *Protection) *Protection {
	cp := *p
	cp.Tags = maps.Clone(p.Tags)

	return &cp
}

// Subscription represents an AWS Shield Advanced subscription.
type Subscription struct {
	StartTime            time.Time
	EndTime              time.Time
	AutoRenew            string
	TimeCommitmentInDays int64
}

// InMemoryBackend is an in-memory store for Shield Advanced resources.
type InMemoryBackend struct {
	protections  map[string]*Protection
	subscription *Subscription
	mu           *lockmetrics.RWMutex
	accountID    string
	region       string
}

// NewInMemoryBackend creates a new in-memory Shield backend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		protections: make(map[string]*Protection),
		accountID:   accountID,
		region:      region,
		mu:          lockmetrics.New("shield"),
	}
}

// Region returns the AWS region this backend is configured for.
func (b *InMemoryBackend) Region() string { return b.region }

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

	for _, p := range b.protections {
		if p.ResourceARN == resourceARN {
			return nil, fmt.Errorf(
				"%w: protection for resource %s already exists",
				ErrProtectionAlreadyExists,
				resourceARN,
			)
		}
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

	for _, p := range b.protections {
		if p.ResourceARN == resourceARN {
			return cloneProtection(p), nil
		}
	}

	return nil, fmt.Errorf("%w: protection for resource %q not found", ErrProtectionNotFound, resourceARN)
}

// DeleteProtection deletes a protection by ID.
func (b *InMemoryBackend) DeleteProtection(protectionID string) error {
	b.mu.Lock("DeleteProtection")
	defer b.mu.Unlock()

	if _, ok := b.protections[protectionID]; !ok {
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
