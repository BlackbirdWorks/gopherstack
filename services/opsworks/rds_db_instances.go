package opsworks

import (
	"fmt"
	"strings"
)

// RegisterRdsDBInstance registers an RDS DB instance with a stack.
func (b *InMemoryBackend) RegisterRdsDBInstance(stackID, rdsDBInstanceArn, dbUser, _ string) error {
	if rdsDBInstanceArn == "" {
		return ErrValidation
	}

	b.mu.Lock("RegisterRdsDBInstance")
	defer b.mu.Unlock()

	if !b.stacks.Has(stackID) {
		return ErrStackNotFound
	}

	// Extract a readable identifier from the ARN.
	id := rdsDBInstanceArn
	if idx := strings.LastIndex(rdsDBInstanceArn, ":"); idx >= 0 {
		id = rdsDBInstanceArn[idx+1:]
	}

	b.rdsDBInstances.Put(&storedRdsDBInstance{
		RdsDBInstanceArn:     rdsDBInstanceArn,
		DBInstanceIdentifier: id,
		DBUser:               dbUser,
		StackID:              stackID,
		Region:               b.region,
		Address:              fmt.Sprintf("%s.%s.rds.amazonaws.com", id, b.region),
	})

	return nil
}

// DeregisterRdsDBInstance removes a registered RDS DB instance.
func (b *InMemoryBackend) DeregisterRdsDBInstance(rdsDBInstanceArn string) error {
	b.mu.Lock("DeregisterRdsDBInstance")
	defer b.mu.Unlock()

	if !b.rdsDBInstances.Delete(rdsDBInstanceArn) {
		return ErrRdsDBInstanceNotFound
	}

	return nil
}

// DescribeRdsDBInstances returns RDS DB instances filtered by stack or ARN list.
func (b *InMemoryBackend) DescribeRdsDBInstances(stackID string, rdsDBInstanceArns []string) ([]*RdsDBInstance, error) {
	b.mu.RLock("DescribeRdsDBInstances")
	defer b.mu.RUnlock()

	if len(rdsDBInstanceArns) > 0 {
		result := make([]*RdsDBInstance, 0, len(rdsDBInstanceArns))
		for _, rArn := range rdsDBInstanceArns {
			r, ok := b.rdsDBInstances.Get(rArn)
			if !ok {
				return nil, ErrRdsDBInstanceNotFound
			}
			result = append(result, r.toRdsDBInstance())
		}

		return result, nil
	}

	source := stackScoped(stackID, b.rdsDBInstances.All, b.rdsDBInstancesByStack.Get)

	result := make([]*RdsDBInstance, 0, len(source))
	for _, r := range source {
		result = append(result, r.toRdsDBInstance())
	}

	return result, nil
}

// UpdateRdsDBInstance updates the DB user/password for a registered RDS instance.
func (b *InMemoryBackend) UpdateRdsDBInstance(rdsDBInstanceArn, dbUser, _ string) error {
	b.mu.Lock("UpdateRdsDBInstance")
	defer b.mu.Unlock()

	r, ok := b.rdsDBInstances.Get(rdsDBInstanceArn)
	if !ok {
		return ErrRdsDBInstanceNotFound
	}

	if dbUser != "" {
		r.DBUser = dbUser
	}

	return nil
}
