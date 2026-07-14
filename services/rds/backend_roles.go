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

	if _, exists := b.instances.Get(instanceID); !exists {
		return fmt.Errorf("%w: instance %s not found", ErrInstanceNotFound, instanceID)
	}

	if slices.Contains(b.instanceRoles[instanceID], roleARN) {
		return nil
	}

	b.instanceRoles[instanceID] = append(b.instanceRoles[instanceID], roleARN)

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

	if _, exists := b.instances.Get(instanceID); !exists {
		return fmt.Errorf("%w: instance %s not found", ErrInstanceNotFound, instanceID)
	}

	roles := b.instanceRoles[instanceID]
	idx := slices.Index(roles, roleARN)
	if idx >= 0 {
		b.instanceRoles[instanceID] = slices.Delete(roles, idx, idx+1)
	}

	return nil
}
