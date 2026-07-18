package emr

import (
	"context"
	"fmt"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
)

// PutManagedScalingPolicy sets the managed scaling policy on a cluster.
func (b *InMemoryBackend) PutManagedScalingPolicy(
	ctx context.Context,
	clusterID string,
	policy ManagedScalingPolicy,
) error {
	if err := validateManagedScalingPolicy(policy); err != nil {
		return err
	}

	region := getRegion(ctx, b.region)

	b.mu.Lock("PutManagedScalingPolicy")
	defer b.mu.Unlock()

	cluster, ok := b.clusterGet(region, clusterID)
	if !ok {
		return fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterID)
	}

	cp := policy
	cluster.managedScalingPolicy = &cp

	return nil
}

func validateManagedScalingPolicy(policy ManagedScalingPolicy) error {
	cl := policy.ComputeLimits
	if cl.MinimumCapacityUnits > cl.MaximumCapacityUnits {
		return fmt.Errorf("%w: MinimumCapacityUnits must be <= MaximumCapacityUnits", ErrValidation)
	}

	if cl.MaximumOnDemandCapacityUnits > 0 &&
		cl.MaximumOnDemandCapacityUnits > cl.MaximumCapacityUnits {
		return fmt.Errorf(
			"%w: MaximumOnDemandCapacityUnits must be <= MaximumCapacityUnits",
			ErrValidation,
		)
	}

	return nil
}

// GetManagedScalingPolicy returns the managed scaling policy for a cluster.
func (b *InMemoryBackend) GetManagedScalingPolicy(
	ctx context.Context, clusterID string,
) (*ManagedScalingPolicy, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("GetManagedScalingPolicy")
	defer b.mu.RUnlock()

	cluster, ok := b.clusterGet(region, clusterID)
	if !ok {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterID)
	}

	if cluster.managedScalingPolicy == nil {
		empty := ManagedScalingPolicy{}

		return &empty, nil
	}

	cp := *cluster.managedScalingPolicy

	return &cp, nil
}

// RemoveManagedScalingPolicy clears the managed scaling policy on a cluster.
func (b *InMemoryBackend) RemoveManagedScalingPolicy(ctx context.Context, clusterID string) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("RemoveManagedScalingPolicy")
	defer b.mu.Unlock()

	cluster, ok := b.clusterGet(region, clusterID)
	if !ok {
		return fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterID)
	}

	cluster.managedScalingPolicy = nil

	return nil
}

// PutAutoTerminationPolicy sets the auto-termination policy on a cluster.
func (b *InMemoryBackend) PutAutoTerminationPolicy(
	ctx context.Context,
	clusterID string,
	policy AutoTerminationPolicy,
) error {
	if policy.IdleTimeout < minIdleTimeout || policy.IdleTimeout > maxIdleTimeout {
		return fmt.Errorf(
			"%w: IdleTimeout must be between %d and %d seconds",
			ErrValidation,
			minIdleTimeout,
			maxIdleTimeout,
		)
	}

	region := getRegion(ctx, b.region)

	b.mu.Lock("PutAutoTerminationPolicy")
	defer b.mu.Unlock()

	cluster, ok := b.clusterGet(region, clusterID)
	if !ok {
		return fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterID)
	}

	cp := policy
	cluster.autoTerminationPolicy = &cp

	return nil
}

// GetAutoTerminationPolicy returns the auto-termination policy for a cluster.
func (b *InMemoryBackend) GetAutoTerminationPolicy(
	ctx context.Context,
	clusterID string,
) (*AutoTerminationPolicy, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("GetAutoTerminationPolicy")
	defer b.mu.RUnlock()

	cluster, ok := b.clusterGet(region, clusterID)
	if !ok {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterID)
	}

	if cluster.autoTerminationPolicy == nil {
		empty := AutoTerminationPolicy{}

		return &empty, nil
	}

	cp := *cluster.autoTerminationPolicy

	return &cp, nil
}

// RemoveAutoTerminationPolicy clears the auto-termination policy on a cluster.
func (b *InMemoryBackend) RemoveAutoTerminationPolicy(ctx context.Context, clusterID string) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("RemoveAutoTerminationPolicy")
	defer b.mu.Unlock()

	cluster, ok := b.clusterGet(region, clusterID)
	if !ok {
		return fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterID)
	}

	cluster.autoTerminationPolicy = nil

	return nil
}

// PutAutoScalingPolicy persists an auto-scaling policy on an instance group.
func (b *InMemoryBackend) PutAutoScalingPolicy(
	ctx context.Context,
	clusterID, instanceGroupID string,
	policy AutoScalingPolicySpec,
) (*AutoScalingPolicyDetail, string, string, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("PutAutoScalingPolicy")
	defer b.mu.Unlock()

	cluster, ok := b.clusterGet(region, clusterID)
	if !ok {
		return nil, "", "", fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterID)
	}

	for i := range cluster.instanceGroups {
		if cluster.instanceGroups[i].ID == instanceGroupID {
			detail := &AutoScalingPolicyDetail{
				Status:      map[string]string{"State": "ATTACHED"},
				Constraints: policy.Constraints,
				Rules:       policy.Rules,
			}
			cluster.instanceGroups[i].AutoScalingPolicy = detail

			return detail, cluster.ARN, instanceGroupID, nil
		}
	}

	return nil, "", "", fmt.Errorf("%w: instance group %s not found", ErrNotFound, instanceGroupID)
}

// RemoveAutoScalingPolicy clears the auto-scaling policy on an instance group.
func (b *InMemoryBackend) RemoveAutoScalingPolicy(
	ctx context.Context, clusterID, instanceGroupID string,
) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("RemoveAutoScalingPolicy")
	defer b.mu.Unlock()

	cluster, ok := b.clusterGet(region, clusterID)
	if !ok {
		return fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterID)
	}

	for i := range cluster.instanceGroups {
		if cluster.instanceGroups[i].ID == instanceGroupID {
			cluster.instanceGroups[i].AutoScalingPolicy = nil

			return nil
		}
	}

	return fmt.Errorf("%w: instance group %s not found", ErrNotFound, instanceGroupID)
}

// GetBlockPublicAccessConfiguration returns the account-level block-public-access config.
func (b *InMemoryBackend) GetBlockPublicAccessConfiguration(
	ctx context.Context,
) (BlockPublicAccessConfiguration, blockPublicAccessMeta) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("GetBlockPublicAccessConfiguration")
	defer b.mu.RUnlock()

	cfg, ok := b.blockPublicAccess.Get(region)
	if !ok {
		return defaultBlockPublicAccess(), blockPublicAccessMeta{CreationDateTime: awstime.Epoch(time.Now())}
	}

	meta, _ := b.blockPublicAccessMeta.Get(region)

	return *cfg, *meta
}

func defaultBlockPublicAccess() BlockPublicAccessConfiguration {
	return BlockPublicAccessConfiguration{
		BlockPublicSecurityGroupRules: true,
		PermittedPublicSecurityGroupRuleRanges: []PortRange{
			{MinRange: defaultSSHPort, MaxRange: defaultSSHPort},
		},
	}
}

// PutBlockPublicAccessConfiguration sets the account-level block-public-access config.
func (b *InMemoryBackend) PutBlockPublicAccessConfiguration(
	ctx context.Context,
	config BlockPublicAccessConfiguration,
) error {
	if err := validatePortRanges(config.PermittedPublicSecurityGroupRuleRanges); err != nil {
		return err
	}

	region := getRegion(ctx, b.region)

	b.mu.Lock("PutBlockPublicAccessConfiguration")
	defer b.mu.Unlock()

	cp := config
	cp.region = region
	b.blockPublicAccess.Put(&cp)
	b.blockPublicAccessMeta.Put(&blockPublicAccessMeta{
		CreationDateTime: awstime.Epoch(time.Now()),
		CreatedByArn:     arn.Build("iam", "", b.accountID, "root"),
		region:           region,
	})

	return nil
}

func validatePortRanges(ranges []PortRange) error {
	for _, r := range ranges {
		if r.MinRange < 0 || r.MaxRange > 65535 || r.MinRange > r.MaxRange {
			return fmt.Errorf("%w: invalid port range %d-%d", ErrValidation, r.MinRange, r.MaxRange)
		}
	}

	return nil
}
