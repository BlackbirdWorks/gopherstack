// Package dynamodb implements the AWS DynamoDB mock service.
// resource_policy.go implements Get/Put/DeleteResourcePolicy (resource-based
// access policies attached to a table by ARN).
package dynamodb

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

// --- GetResourcePolicy ---

// GetResourcePolicy returns the resource-based policy stored on the table.
func (db *InMemoryDB) GetResourcePolicy(
	_ context.Context,
	input *dynamodb.GetResourcePolicyInput,
) (*dynamodb.GetResourcePolicyOutput, error) {
	if input == nil || input.ResourceArn == nil || *input.ResourceArn == "" {
		return nil, NewValidationException("ResourceArn is required")
	}

	table := db.getTableByARN(*input.ResourceArn)
	if table == nil {
		return nil, NewResourceNotFoundException("Table not found for ARN: " + *input.ResourceArn)
	}

	policy := resourcePolicyRLocked(table)
	if policy == "" {
		return &dynamodb.GetResourcePolicyOutput{}, nil
	}

	return &dynamodb.GetResourcePolicyOutput{
		Policy:     aws.String(policy),
		RevisionId: aws.String("1"),
	}, nil
}

// --- PutResourcePolicy ---

// PutResourcePolicy stores a resource-based policy on the table.
func (db *InMemoryDB) PutResourcePolicy(
	_ context.Context,
	input *dynamodb.PutResourcePolicyInput,
) (*dynamodb.PutResourcePolicyOutput, error) {
	if input == nil || input.ResourceArn == nil || *input.ResourceArn == "" {
		return nil, NewValidationException("ResourceArn is required")
	}

	if input.Policy == nil || *input.Policy == "" {
		return nil, NewValidationException("Policy is required")
	}

	table := db.getTableByARN(*input.ResourceArn)
	if table == nil {
		return nil, NewResourceNotFoundException("Table not found for ARN: " + *input.ResourceArn)
	}

	setResourcePolicyLocked(table, "PutResourcePolicy", *input.Policy)

	return &dynamodb.PutResourcePolicyOutput{
		RevisionId: aws.String("1"),
	}, nil
}

// --- DeleteResourcePolicy ---

// DeleteResourcePolicy removes the resource-based policy from the table.
func (db *InMemoryDB) DeleteResourcePolicy(
	_ context.Context,
	input *dynamodb.DeleteResourcePolicyInput,
) (*dynamodb.DeleteResourcePolicyOutput, error) {
	if input == nil || input.ResourceArn == nil || *input.ResourceArn == "" {
		return nil, NewValidationException("ResourceArn is required")
	}

	table := db.getTableByARN(*input.ResourceArn)
	if table == nil {
		// idempotent: nonexistent resource is a no-op
		return &dynamodb.DeleteResourcePolicyOutput{RevisionId: aws.String("1")}, nil
	}

	setResourcePolicyLocked(table, "DeleteResourcePolicy", "")

	return &dynamodb.DeleteResourcePolicyOutput{
		RevisionId: aws.String("1"),
	}, nil
}

// resourcePolicyRLocked returns table.ResourcePolicy under a defer-protected
// table.mu.RLock.
func resourcePolicyRLocked(table *Table) string {
	table.mu.RLock("GetResourcePolicy")
	defer table.mu.RUnlock()

	return table.ResourcePolicy
}

// setResourcePolicyLocked sets table.ResourcePolicy under a defer-protected
// table.mu.Lock, using op as the lock's metrics label.
func setResourcePolicyLocked(table *Table, op, policy string) {
	table.mu.Lock(op)
	defer table.mu.Unlock()

	table.ResourcePolicy = policy
}

// getTableByARN looks up a table by its ARN, restricting the search to the
// region encoded in the ARN itself. Returns nil if not found.
func (db *InMemoryDB) getTableByARN(resourceARN string) *Table {
	region := db.regionFromARN(resourceARN)

	db.mu.RLock("getTableByARN")
	defer db.mu.RUnlock()

	for _, table := range db.tablesByRegion.Get(region) {
		if table.TableArn == resourceARN {
			return table
		}
	}

	return nil
}
