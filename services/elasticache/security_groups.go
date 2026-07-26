package elasticache

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

func (b *InMemoryBackend) cacheSecurityGroupARN(region, name string) string {
	return arn.Build("elasticache", region, b.accountID, "securitygroup:"+name)
}

// CreateCacheSecurityGroup creates a new cache security group.
func (b *InMemoryBackend) CreateCacheSecurityGroup(
	ctx context.Context,
	name, description string,
) (*CacheSecurityGroup, error) {
	b.mu.Lock("CreateCacheSecurityGroup")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	tbl := b.cacheSecurityGroupsStore(region)
	if _, exists := tbl.Get(name); exists {
		return nil, ErrCacheSecurityGroupAlreadyExists
	}

	sg := &CacheSecurityGroup{
		Name:        name,
		Description: description,
		ARN:         b.cacheSecurityGroupARN(region, name),
		OwnerID:     b.accountID,
		Tags:        tags.New("elasticache.sg." + name + ".tags"),
	}
	tbl.Put(sg)

	return sg, nil
}

// AuthorizeCacheSecurityGroupIngress adds an EC2 security group authorization to the named cache security group.
func (b *InMemoryBackend) AuthorizeCacheSecurityGroupIngress(
	ctx context.Context,
	name, ec2SecurityGroupName, ec2SecurityGroupOwnerID string,
) (*CacheSecurityGroup, error) {
	b.mu.Lock("AuthorizeCacheSecurityGroupIngress")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	sg, ok := b.cacheSecurityGroupsStore(region).Get(name)
	if !ok {
		return nil, ErrCacheSecurityGroupNotFound
	}

	ingressStore := b.cacheSecurityGroupIngressStore(region)
	ingressStore[name] = append(ingressStore[name], EC2SecurityGroupMembership{
		EC2SecurityGroupName:    ec2SecurityGroupName,
		EC2SecurityGroupOwnerID: ec2SecurityGroupOwnerID,
		Status:                  "authorized",
	})

	result := *sg

	return &result, nil
}

// ----------------------------------------
// CreateGlobalReplicationGroup
// ----------------------------------------

// AddCacheSecurityGroupInternal seeds a cache security group for testing.
func (b *InMemoryBackend) AddCacheSecurityGroupInternal(sg *CacheSecurityGroup) {
	b.mu.Lock("AddCacheSecurityGroupInternal")
	defer b.mu.Unlock()
	b.cacheSecurityGroupsStore(b.region).Put(sg)
}

// DeleteCacheSecurityGroup deletes a cache security group.
func (b *InMemoryBackend) DeleteCacheSecurityGroup(ctx context.Context, name string) error {
	b.mu.Lock("DeleteCacheSecurityGroup")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	sgStore := b.cacheSecurityGroupsStore(region)
	if _, ok := sgStore.Get(name); !ok {
		return ErrCacheSecurityGroupNotFound
	}

	sgStore.Delete(name)
	delete(b.cacheSecurityGroupIngressStore(region), name)

	return nil
}

// DescribeCacheSecurityGroups returns a paginated list of cache security groups.
func (b *InMemoryBackend) DescribeCacheSecurityGroups(
	ctx context.Context,
	name, marker string,
	maxRecords int,
) (page.Page[CacheSecurityGroup], error) {
	b.mu.RLock("DescribeCacheSecurityGroups")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	return describePaged(b.cacheSecurityGroupsStoreRO(region), name, ErrCacheSecurityGroupNotFound, nil,
		func(sg CacheSecurityGroup) string { return sg.Name }, marker, maxRecords)
}

// RevokeCacheSecurityGroupIngress removes an EC2 security group authorization.
func (b *InMemoryBackend) RevokeCacheSecurityGroupIngress(
	ctx context.Context,
	name, ec2SecurityGroupName, ec2SecurityGroupOwnerID string,
) (*CacheSecurityGroup, error) {
	b.mu.Lock("RevokeCacheSecurityGroupIngress")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	sg, ok := b.cacheSecurityGroupsStore(region).Get(name)
	if !ok {
		return nil, ErrCacheSecurityGroupNotFound
	}

	ingressStore := b.cacheSecurityGroupIngressStore(region)
	ingress := ingressStore[name]
	filtered := make([]EC2SecurityGroupMembership, 0, len(ingress))

	for _, entry := range ingress {
		if entry.EC2SecurityGroupName == ec2SecurityGroupName &&
			entry.EC2SecurityGroupOwnerID == ec2SecurityGroupOwnerID {
			continue
		}

		filtered = append(filtered, entry)
	}

	ingressStore[name] = filtered
	result := *sg

	return &result, nil
}
