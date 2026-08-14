// Package dynamodb implements the AWS DynamoDB mock service.
// resource_policy.go implements Get/Put/DeleteResourcePolicy (resource-based
// access policies attached to a table by ARN).
package dynamodb

import (
	"context"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

// resourcePolicyNoPolicySentinel is the literal ExpectedRevisionId value AWS
// defines for "I expect no policy to exist yet on this resource".
const resourcePolicyNoPolicySentinel = "NO_POLICY"

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

	policy, revision := resourcePolicyRLocked(table)
	if policy == "" {
		return &dynamodb.GetResourcePolicyOutput{}, nil
	}

	return &dynamodb.GetResourcePolicyOutput{
		Policy:     aws.String(policy),
		RevisionId: aws.String(revision),
	}, nil
}

// --- PutResourcePolicy ---

// PutResourcePolicy stores a resource-based policy on the table. When
// input.ExpectedRevisionId is set, the write is rejected with
// PolicyNotFoundException unless it matches the policy's current revision (or
// the resource has no policy at all, and the sentinel "NO_POLICY" was given).
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

	newRevision, err := setResourcePolicyLocked(
		table,
		"PutResourcePolicy",
		*input.Policy,
		input.ExpectedRevisionId,
	)
	if err != nil {
		return nil, err
	}

	return &dynamodb.PutResourcePolicyOutput{
		RevisionId: aws.String(newRevision),
	}, nil
}

// --- DeleteResourcePolicy ---

// DeleteResourcePolicy removes the resource-based policy from the table. When
// input.ExpectedRevisionId is set, the delete is rejected with
// PolicyNotFoundException unless it matches the policy's current revision.
func (db *InMemoryDB) DeleteResourcePolicy(
	_ context.Context,
	input *dynamodb.DeleteResourcePolicyInput,
) (*dynamodb.DeleteResourcePolicyOutput, error) {
	if input == nil || input.ResourceArn == nil || *input.ResourceArn == "" {
		return nil, NewValidationException("ResourceArn is required")
	}

	table := db.getTableByARN(*input.ResourceArn)
	if table == nil {
		// idempotent: nonexistent resource is a no-op, and never had a policy.
		return &dynamodb.DeleteResourcePolicyOutput{}, nil
	}

	priorRevision, err := clearResourcePolicyLocked(table, input.ExpectedRevisionId)
	if err != nil {
		return nil, err
	}

	out := &dynamodb.DeleteResourcePolicyOutput{}
	if priorRevision != "" {
		out.RevisionId = aws.String(priorRevision)
	}

	return out, nil
}

// resourcePolicyRLocked returns table.ResourcePolicy and its revision under a
// defer-protected table.mu.RLock.
func resourcePolicyRLocked(table *Table) (string, string) {
	table.mu.RLock("GetResourcePolicy")
	defer table.mu.RUnlock()

	return table.ResourcePolicy, table.ResourcePolicyRevision
}

// setResourcePolicyLocked stores policy on table under a defer-protected
// table.mu.Lock, enforcing expectedRevision first. Returns the new revision ID.
func setResourcePolicyLocked(
	table *Table,
	op, policy string,
	expectedRevision *string,
) (string, error) {
	table.mu.Lock(op)
	defer table.mu.Unlock()

	if err := checkExpectedRevision(table.ResourcePolicyRevision, expectedRevision); err != nil {
		return "", err
	}

	next := nextResourcePolicyRevision(table.ResourcePolicyRevision)
	table.ResourcePolicy = policy
	table.ResourcePolicyRevision = next

	return next, nil
}

// clearResourcePolicyLocked removes the policy from table under a
// defer-protected table.mu.Lock, enforcing expectedRevision first. Returns
// the revision that was in effect before deletion (empty if there was none).
func clearResourcePolicyLocked(table *Table, expectedRevision *string) (string, error) {
	table.mu.Lock("DeleteResourcePolicy")
	defer table.mu.Unlock()

	if err := checkExpectedRevision(table.ResourcePolicyRevision, expectedRevision); err != nil {
		return "", err
	}

	prior := table.ResourcePolicyRevision
	table.ResourcePolicy = ""
	table.ResourcePolicyRevision = ""

	return prior, nil
}

// checkExpectedRevision enforces Put/DeleteResourcePolicy's optional
// optimistic-concurrency check. expected == nil means the caller didn't ask
// for one. currentRevision == "" means no policy is currently attached.
func checkExpectedRevision(currentRevision string, expected *string) error {
	if expected == nil {
		return nil
	}

	want := *expected
	if want == resourcePolicyNoPolicySentinel {
		if currentRevision == "" {
			return nil
		}

		return NewPolicyNotFoundException(
			"ExpectedRevisionId is NO_POLICY but a policy is already attached",
		)
	}

	if want != currentRevision {
		return NewPolicyNotFoundException(
			"ExpectedRevisionId does not match the current policy revision",
		)
	}

	return nil
}

// nextResourcePolicyRevision returns the next revision ID after current, an
// incrementing decimal counter. AWS revision IDs are opaque strings; this
// satisfies equality-comparison round-tripping without a separate persisted
// counter field.
func nextResourcePolicyRevision(current string) string {
	n, _ := strconv.Atoi(current)

	return strconv.Itoa(n + 1)
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
