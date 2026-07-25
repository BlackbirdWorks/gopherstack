package shield

import (
	"fmt"
	"slices"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// protectionARN builds a Shield protection ARN. Shield ARNs are global (no region component in
// the resource path, like IAM), but the partition must still reflect the account's actual AWS
// partition -- aws / aws-us-gov / aws-cn / aws-iso / aws-iso-b -- derived from region. This is
// built directly here (rather than via arn.Build, whose only region-omitting special case is
// service=="iam") so GovCloud/China/ISO region backends produce wire-correct
// "arn:aws-us-gov:shield::..." ARNs instead of always defaulting to the "aws" partition.
func protectionARN(region, accountID, protectionID string) string {
	return fmt.Sprintf("arn:%s:shield::%s:protection/%s", arn.PartitionForRegion(region), accountID, protectionID)
}

// protectionResourceTypes lists every Shield-protectable resource type, used for the
// subscriptionMaxProtectionsPerType quota check in CreateProtection.
func protectionResourceTypes() []string {
	return []string{
		ResourceTypeCloudFrontDistribution,
		ResourceTypeRoute53HostedZone,
		ResourceTypeApplicationLoadBalancer,
		ResourceTypeClassicLoadBalancer,
		ResourceTypeElasticIPAllocation,
		ResourceTypeGlobalAccelerator,
	}
}

// protectionResourceType returns the Shield resource type resourceARN belongs to, or "" if it
// doesn't match any known Shield-protectable type.
func protectionResourceType(resourceARN string) string {
	for _, rt := range protectionResourceTypes() {
		if resourceARNMatchesType(resourceARN, rt) {
			return rt
		}
	}

	return ""
}

// checkProtectionQuotas enforces the Shield Advanced subscriptionMaxProtections and
// subscriptionMaxProtectionsPerType quotas that CreateProtection itself reports via
// DescribeSubscription (see handler_subscription.go's subscriptionLimits). Must be called with
// b.mu held.
func (b *InMemoryBackend) checkProtectionQuotas(resourceARN string) error {
	if b.protections.Len() >= subscriptionMaxProtections {
		return fmt.Errorf("%w: Type=Protections, Limit=%d", ErrLimitExceeded, subscriptionMaxProtections)
	}

	rt := protectionResourceType(resourceARN)
	if rt == "" {
		return nil
	}

	var count int

	b.protections.Range(func(p *Protection) bool {
		if resourceARNMatchesType(p.ResourceARN, rt) {
			count++
		}

		return true
	})

	if count >= subscriptionMaxProtectionsPerType {
		return fmt.Errorf("%w: Type=%s, Limit=%d", ErrLimitExceeded, rt, subscriptionMaxProtectionsPerType)
	}

	return nil
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

	if matches := b.protectionsByName.Get(name); len(matches) > 0 {
		return nil, fmt.Errorf("%w: protection %q already exists", ErrProtectionAlreadyExists, name)
	}

	if matches := b.protectionsByResourceARN.Get(resourceARN); len(matches) > 0 {
		return nil, fmt.Errorf("%w: protection for resource %s already exists", ErrProtectionAlreadyExists, resourceARN)
	}

	if err := b.checkProtectionQuotas(resourceARN); err != nil {
		return nil, err
	}

	pArn := protectionARN(b.region, b.accountID, id)

	p := &Protection{
		ID:            id,
		ProtectionArn: pArn,
		Name:          name,
		ResourceARN:   resourceARN,
		CreationTime:  time.Now(),
		Tags:          cloneTags(tags),
	}
	b.protections.Put(p)

	return cloneProtection(p), nil
}

// DescribeProtection returns a protection by ID or resource ARN.
func (b *InMemoryBackend) DescribeProtection(protectionID, resourceARN string) (*Protection, error) {
	b.mu.RLock("DescribeProtection")
	defer b.mu.RUnlock()

	if protectionID != "" {
		p, ok := b.protections.Get(protectionID)
		if !ok {
			return nil, fmt.Errorf("%w: protection %q not found", ErrProtectionNotFound, protectionID)
		}

		return cloneProtection(p), nil
	}

	if matches := b.protectionsByResourceARN.Get(resourceARN); len(matches) > 0 {
		return cloneProtection(matches[0]), nil
	}

	return nil, fmt.Errorf("%w: no protection for resource %q", ErrProtectionNotFound, resourceARN)
}

// DeleteProtection deletes a protection by ID. ApplicationLayerAutomaticResponseConfiguration is
// a field of the real AWS Protection object (see types.Protection), so gopherstack's separate
// alarConfigs table -- keyed by the protection's ResourceARN -- must be cascade-cleaned here to
// avoid an orphaned row that a future protection for the same resource ARN would incorrectly
// inherit.
func (b *InMemoryBackend) DeleteProtection(protectionID string) error {
	b.mu.Lock("DeleteProtection")
	defer b.mu.Unlock()

	p, ok := b.protections.Get(protectionID)
	if !ok {
		return fmt.Errorf("%w: protection %q not found", ErrProtectionNotFound, protectionID)
	}

	b.protections.Delete(protectionID)
	b.alarConfigs.Delete(p.ResourceARN)

	return nil
}

// ListProtections returns all protections sorted by name.
// Clones are built under RLock; sorting happens after the lock is released.
func (b *InMemoryBackend) ListProtections() []*Protection {
	var list []*Protection

	func() {
		b.mu.RLock("ListProtections")
		defer b.mu.RUnlock()

		items := b.protections.All()
		list = make([]*Protection, 0, len(items))

		for _, p := range items {
			list = append(list, cloneProtection(p))
		}
	}()

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

// AddProtectionInternal creates a protection directly (for tests).
func (b *InMemoryBackend) AddProtectionInternal(name, resourceARN string) *Protection {
	id := newShieldID()

	b.mu.Lock("AddProtectionInternal")
	defer b.mu.Unlock()

	pArn := protectionARN(b.region, b.accountID, id)

	p := &Protection{
		ID:            id,
		ProtectionArn: pArn,
		Name:          name,
		ResourceARN:   resourceARN,
		CreationTime:  time.Now(),
		Tags:          make(map[string]string),
	}
	b.protections.Put(p)

	return cloneProtection(p)
}
