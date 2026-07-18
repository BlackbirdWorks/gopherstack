package efs

import (
	"context"
	"fmt"
)

// DescribeMountTargetSecurityGroups returns the security groups for a mount target.
func (b *InMemoryBackend) DescribeMountTargetSecurityGroups(
	ctx context.Context,
	mountTargetID string,
) ([]string, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("DescribeMountTargetSecurityGroups")
	defer b.mu.RUnlock()

	mt, ok := b.mountTargets.Get(regionKey(region, mountTargetID))
	if !ok {
		return nil, fmt.Errorf(
			"%w: mount target %s not found",
			ErrMountTargetNotFound,
			mountTargetID,
		)
	}

	if mt.SecurityGroups == nil {
		return []string{}, nil
	}

	result := make([]string, len(mt.SecurityGroups))
	copy(result, mt.SecurityGroups)

	return result, nil
}

// ModifyMountTargetSecurityGroups replaces the security groups for a mount target.
// Enforces a maximum of 5 security groups.
func (b *InMemoryBackend) ModifyMountTargetSecurityGroups(
	ctx context.Context,
	mountTargetID string,
	securityGroups []string,
) error {
	if len(securityGroups) > maxSecurityGroups {
		return fmt.Errorf(
			"%w: too many security groups: %d (max %d)",
			ErrSecurityGroupLimitExceeded,
			len(securityGroups),
			maxSecurityGroups,
		)
	}

	region := getRegion(ctx, b.region)

	b.mu.Lock("ModifyMountTargetSecurityGroups")
	defer b.mu.Unlock()

	mt, ok := b.mountTargets.Get(regionKey(region, mountTargetID))
	if !ok {
		return fmt.Errorf("%w: mount target %s not found", ErrMountTargetNotFound, mountTargetID)
	}

	groups := make([]string, len(securityGroups))
	copy(groups, securityGroups)
	mt.SecurityGroups = groups

	return nil
}
