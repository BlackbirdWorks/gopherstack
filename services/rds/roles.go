package rds

import (
	"fmt"
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
