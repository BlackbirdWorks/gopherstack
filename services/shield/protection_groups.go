package shield

import (
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// protectionGroupARN builds a Shield protection group ARN.
func protectionGroupARN(accountID, groupID string) string {
	return arn.Build("shield", "", accountID, fmt.Sprintf("protection-group/%s", groupID))
}

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

	pg, ok := b.protectionGroups.Get(protectionGroupID)
	if !ok {
		return nil, fmt.Errorf("%w: protection group %q not found", ErrProtectionGroupNotFound, protectionGroupID)
	}

	switch pg.Pattern {
	case PatternAll:
		items := b.protections.All()
		arns := make([]string, 0, len(items))

		for _, p := range items {
			arns = append(arns, p.ResourceARN)
		}

		slices.Sort(arns)

		return arns, nil

	case PatternByResourceType:
		arns := make([]string, 0)

		b.protections.Range(func(p *Protection) bool {
			if resourceARNMatchesType(p.ResourceARN, pg.ResourceType) {
				arns = append(arns, p.ResourceARN)
			}

			return true
		})

		slices.Sort(arns)

		return arns, nil
	}

	members := append([]string(nil), pg.Members...)

	return members, nil
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

	if b.protectionGroups.Has(id) {
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
	b.protectionGroups.Put(pg)

	return cloneProtectionGroup(pg), nil
}

// DescribeProtectionGroup returns a single protection group by ID.
func (b *InMemoryBackend) DescribeProtectionGroup(id string) (*ProtectionGroup, error) {
	b.mu.RLock("DescribeProtectionGroup")
	defer b.mu.RUnlock()

	pg, ok := b.protectionGroups.Get(id)
	if !ok {
		return nil, fmt.Errorf("%w: protection group %q not found", ErrProtectionGroupNotFound, id)
	}

	return cloneProtectionGroup(pg), nil
}

// ListProtectionGroups returns all protection groups sorted by ID.
// Clones are built under RLock; sorting happens after the lock is released.
func (b *InMemoryBackend) ListProtectionGroups() []*ProtectionGroup {
	b.mu.RLock("ListProtectionGroups")

	items := b.protectionGroups.All()
	list := make([]*ProtectionGroup, 0, len(items))

	for _, pg := range items {
		list = append(list, cloneProtectionGroup(pg))
	}

	b.mu.RUnlock()

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

	pg, ok := b.protectionGroups.Get(id)
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

// DeleteProtectionGroup removes a Shield Advanced protection group.
func (b *InMemoryBackend) DeleteProtectionGroup(protectionGroupID string) error {
	b.mu.Lock("DeleteProtectionGroup")
	defer b.mu.Unlock()

	if !b.protectionGroups.Delete(protectionGroupID) {
		return fmt.Errorf("%w: protection group %q not found", ErrProtectionGroupNotFound, protectionGroupID)
	}

	return nil
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
	b.protectionGroups.Put(pg)

	return cloneProtectionGroup(pg)
}
