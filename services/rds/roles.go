package rds

import (
	"fmt"
	"slices"
)

// AddRoleToDBInstance associates an IAM role with the given DB instance for the given feature
// (e.g. S3_INTEGRATION, SQLSERVER_AUDIT). FeatureName selects the feature slot: real AWS allows
// at most one role per feature per instance, so re-adding the same (feature, role) pair is a
// no-op and adding a different role for a feature already in use replaces it.
func (b *InMemoryBackend) AddRoleToDBInstance(instanceID, roleARN, featureName string) error {
	if instanceID == "" {
		return fmt.Errorf("%w: DBInstanceIdentifier must not be empty", ErrInvalidParameter)
	}
	if roleARN == "" {
		return fmt.Errorf("%w: RoleArn must not be empty", ErrInvalidParameter)
	}
	if featureName == "" {
		return fmt.Errorf("%w: FeatureName must not be empty", ErrInvalidParameter)
	}

	b.mu.Lock("AddRoleToDBInstance")
	defer b.mu.Unlock()

	inst, exists := b.instances.Get(normalizeID(instanceID))
	if !exists {
		return fmt.Errorf("%w: instance %s not found", ErrInstanceNotFound, instanceID)
	}

	// Key b.instanceRoles off inst.DBInstanceIdentifier (the stored,
	// creation-time casing), not the raw instanceID argument: they can
	// differ purely in case (see normalizeID), and instanceRoles is a plain
	// map with no normalization of its own.
	canonicalID := inst.DBInstanceIdentifier
	if b.instanceRoles[canonicalID] == nil {
		b.instanceRoles[canonicalID] = make(map[string]string)
	}

	b.instanceRoles[canonicalID][featureName] = roleARN

	return nil
}

// RemoveRoleFromDBInstance disassociates an IAM role from the given instance's feature slot.
// Returns an error if the instance does not exist. Removing a role that is not associated, or
// whose ARN doesn't match what's currently associated with that feature, is a no-op -- it must
// only remove the matching (feature, role) association, not any role sharing the instance.
func (b *InMemoryBackend) RemoveRoleFromDBInstance(instanceID, roleARN, featureName string) error {
	if instanceID == "" {
		return fmt.Errorf("%w: DBInstanceIdentifier must not be empty", ErrInvalidParameter)
	}
	if roleARN == "" {
		return fmt.Errorf("%w: RoleArn must not be empty", ErrInvalidParameter)
	}
	if featureName == "" {
		return fmt.Errorf("%w: FeatureName must not be empty", ErrInvalidParameter)
	}

	b.mu.Lock("RemoveRoleFromDBInstance")
	defer b.mu.Unlock()

	inst, exists := b.instances.Get(normalizeID(instanceID))
	if !exists {
		return fmt.Errorf("%w: instance %s not found", ErrInstanceNotFound, instanceID)
	}

	canonicalID := inst.DBInstanceIdentifier
	if b.instanceRoles[canonicalID][featureName] == roleARN {
		delete(b.instanceRoles[canonicalID], featureName)
	}

	return nil
}

// InstanceAssociatedRoles returns the IAM roles associated with the given DB
// instance (AssociatedRoles on DescribeDBInstances' DBInstance, rds@v1.124.1
// types.go:1956). Returns nil if the instance does not exist or has no
// associated roles. Every entry in b.instanceRoles is applied synchronously
// by AddRoleToDBInstance, so Status is always ACTIVE -- there is no
// PENDING/INVALID state this backend ever produces.
func (b *InMemoryBackend) InstanceAssociatedRoles(instanceID string) []DBInstanceRole {
	b.mu.RLock("InstanceAssociatedRoles")
	defer b.mu.RUnlock()

	inst, exists := b.instances.Get(normalizeID(instanceID))
	if !exists {
		return nil
	}

	roles := b.instanceRoles[inst.DBInstanceIdentifier]
	if len(roles) == 0 {
		return nil
	}

	result := make([]DBInstanceRole, 0, len(roles))
	for feature, roleARN := range roles {
		result = append(result, DBInstanceRole{RoleArn: roleARN, FeatureName: feature, Status: clusterRoleStatusActive})
	}
	slices.SortFunc(result, func(a, b DBInstanceRole) int {
		switch {
		case a.FeatureName < b.FeatureName:
			return -1
		case a.FeatureName > b.FeatureName:
			return 1
		default:
			return 0
		}
	})

	return result
}
