package route53resolver

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"strings"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

func (b *InMemoryBackend) queryLogConfigPoliciesStore(region string) map[string]string {
	if b.queryLogConfigPolicies[region] == nil {
		b.queryLogConfigPolicies[region] = make(map[string]string)
	}

	return b.queryLogConfigPolicies[region]
}

// CreateResolverQueryLogConfig creates a new query logging configuration.
func (b *InMemoryBackend) CreateResolverQueryLogConfig(
	ctx context.Context,
	name, creatorRequestID, destinationARN string,
) (*ResolverQueryLogConfig, error) {
	b.mu.Lock("CreateResolverQueryLogConfig")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if !isValidQueryLogDestination(destinationARN) {
		return nil, fmt.Errorf(
			"%w: DestinationArn must be an S3 bucket, CloudWatch Logs log group, or Kinesis Firehose stream ARN",
			ErrValidation,
		)
	}

	now := currentTime()
	id := "rqlc-" + uuid.New().String()[:8]
	configARN := arn.Build(
		"route53resolver",
		region,
		b.accountID,
		"resolver-query-log-config/"+id,
	)
	cfg := &ResolverQueryLogConfig{
		ID:               id,
		ARN:              configARN,
		Name:             name,
		CreatorRequestID: creatorRequestID,
		DestinationARN:   destinationARN,
		Status:           statusCreated,
		OwnerID:          b.accountID,
		ShareStatus:      shareStatusNotShared,
		CreationTime:     now,
		Region:           region,
	}
	b.queryLogConfigs.Put(cfg)
	cp := *cfg

	return &cp, nil
}

func isValidQueryLogDestination(destinationARN string) bool {
	for _, prefix := range []string{"arn:aws:s3:::", "arn:aws:logs:", "arn:aws:firehose:"} {
		if strings.HasPrefix(destinationARN, prefix) {
			return true
		}
	}

	return false
}

// AddQueryLogConfigInternal adds a query log config directly to the backend (test seed helper).
func (b *InMemoryBackend) AddQueryLogConfigInternal(
	name, destinationARN string,
) *ResolverQueryLogConfig {
	b.mu.Lock("AddQueryLogConfigInternal")
	defer b.mu.Unlock()

	id := "rqlc-" + uuid.New().String()[:8]
	configARN := arn.Build(
		"route53resolver",
		b.region,
		b.accountID,
		"resolver-query-log-config/"+id,
	)
	cfg := &ResolverQueryLogConfig{
		ID:             id,
		ARN:            configARN,
		Name:           name,
		DestinationARN: destinationARN,
		Status:         statusCreated,
		OwnerID:        b.accountID,
		Region:         b.region,
	}
	b.queryLogConfigs.Put(cfg)
	cp := *cfg

	return &cp
}

// DeleteResolverQueryLogConfig deletes a query log config and its associations.
func (b *InMemoryBackend) DeleteResolverQueryLogConfig(
	ctx context.Context,
	id string,
) (*ResolverQueryLogConfig, error) {
	b.mu.Lock("DeleteResolverQueryLogConfig")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	cfg, ok := b.queryLogConfigs.Get(regionalKey(region, id))
	if !ok {
		return nil, fmt.Errorf("%w: resolver query log config %s not found", ErrNotFound, id)
	}
	cp := *cfg

	// Clean up tags.
	delete(b.tagsStore(region), cfg.ARN)

	// Cascade: remove all associations referencing this config. slices.Clone
	// before deleting in the loop -- see DeleteResolverEndpoint's comment.
	for _, assoc := range slices.Clone(b.queryLogConfigAssociationsByRegion.Get(region)) {
		if assoc.ResolverQueryLogConfigID == id {
			b.queryLogConfigAssociations.Delete(regionalKey(region, assoc.ID))
		}
	}
	b.queryLogConfigs.Delete(regionalKey(region, id))

	return &cp, nil
}

// GetResolverQueryLogConfig retrieves a query log config by ID.
func (b *InMemoryBackend) GetResolverQueryLogConfig(ctx context.Context, id string) (*ResolverQueryLogConfig, error) {
	b.mu.RLock("GetResolverQueryLogConfig")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	cfg, ok := b.queryLogConfigs.Get(regionalKey(region, id))
	if !ok {
		return nil, fmt.Errorf("%w: resolver query log config %s not found", ErrNotFound, id)
	}
	cp := *cfg

	return &cp, nil
}

// ListResolverQueryLogConfigs lists all query log configs.
func (b *InMemoryBackend) ListResolverQueryLogConfigs(ctx context.Context) []*ResolverQueryLogConfig {
	b.mu.RLock("ListResolverQueryLogConfigs")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	regionConfigs := b.queryLogConfigsByRegion.Get(region)
	list := make([]*ResolverQueryLogConfig, 0, len(regionConfigs))
	for _, cfg := range regionConfigs {
		cp := *cfg
		list = append(list, &cp)
	}
	sort.Slice(list, func(i, j int) bool { return list[i].Name < list[j].Name })

	return list
}

// GetResolverQueryLogConfigPolicy retrieves a resource policy for a query log config ARN.
func (b *InMemoryBackend) GetResolverQueryLogConfigPolicy(ctx context.Context, arnStr string) string {
	b.mu.RLock("GetResolverQueryLogConfigPolicy")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	return b.queryLogConfigPoliciesStore(region)[arnStr]
}

// PutResolverQueryLogConfigPolicy stores a resource policy for a query log config ARN.
func (b *InMemoryBackend) PutResolverQueryLogConfigPolicy(ctx context.Context, arnStr, policy string) error {
	b.mu.Lock("PutResolverQueryLogConfigPolicy")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	b.queryLogConfigPoliciesStore(region)[arnStr] = policy

	return nil
}

// --- Resolver Rule Association operations ---
