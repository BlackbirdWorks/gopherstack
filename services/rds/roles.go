package rds

import (
	"fmt"
	"slices"
)

// AddRoleToDBInstance associates an IAM role with the given DB instance.
func (b *InMemoryBackend) AddRoleToDBInstance(instanceID, roleARN string) error {
	if instanceID == "" {
		return fmt.Errorf("%w: DBInstanceIdentifier must not be empty", ErrInvalidParameter)
	}
	if roleARN == "" {
		return fmt.Errorf("%w: RoleArn must not be empty", ErrInvalidParameter)
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
	if slices.Contains(b.instanceRoles[canonicalID], roleARN) {
		return nil
	}

	b.instanceRoles[canonicalID] = append(b.instanceRoles[canonicalID], roleARN)

	return nil
}

// RemoveRoleFromDBInstance disassociates an IAM role from the given instance.
// Returns an error if the instance does not exist. Removing a role that is not associated is a no-op.
func (b *InMemoryBackend) RemoveRoleFromDBInstance(instanceID, roleARN string) error {
	if instanceID == "" {
		return fmt.Errorf("%w: DBInstanceIdentifier must not be empty", ErrInvalidParameter)
	}
	if roleARN == "" {
		return fmt.Errorf("%w: RoleArn must not be empty", ErrInvalidParameter)
	}

	b.mu.Lock("RemoveRoleFromDBInstance")
	defer b.mu.Unlock()

	inst, exists := b.instances.Get(normalizeID(instanceID))
	if !exists {
		return fmt.Errorf("%w: instance %s not found", ErrInstanceNotFound, instanceID)
	}

	canonicalID := inst.DBInstanceIdentifier
	roles := b.instanceRoles[canonicalID]
	idx := slices.Index(roles, roleARN)
	if idx >= 0 {
		b.instanceRoles[canonicalID] = slices.Delete(roles, idx, idx+1)
	}

	return nil
}
