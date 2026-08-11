package ssoadmin

import (
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
)

// CreateAccountAssignment assigns a permission set to a principal in an account.
func (b *InMemoryBackend) CreateAccountAssignment(
	instanceArn, permissionSetArn, accountID, principalID, principalType string,
) (string, error) {
	b.mu.Lock("CreateAccountAssignment")
	defer b.mu.Unlock()

	if !b.instances.Has(instanceArn) {
		return "", ErrInstanceNotFound
	}
	if !b.permissionSets.Has(permissionSetArn) {
		return "", ErrPermissionSetNotFound
	}

	assignment := &AccountAssignment{
		AccountID:        accountID,
		PermissionSetArn: permissionSetArn,
		PrincipalID:      principalID,
		PrincipalType:    principalType,
	}
	key := assignmentKey(instanceArn, permissionSetArn)

	// Build a deterministic idempotency key for this specific assignment.
	idempotencyKey := assignmentIdempotencyKey(key, accountID, principalType, principalID)

	// Idempotency: if the same assignment already exists, return the original request ID.
	if existingRequestID, exists := b.assignmentCreationIDs[idempotencyKey]; exists {
		return existingRequestID, nil
	}

	b.assignments[key] = append(b.assignments[key], assignment)

	requestID := uuid.NewString()
	now := time.Now().UTC()
	pruneOldestStatus(b.creationStatuses)
	b.creationStatuses.Put(&ProvisioningStatus{
		RequestID:        requestID,
		Status:           statusInProgress,
		CreatedDate:      now,
		AccountID:        accountID,
		PermissionSetArn: permissionSetArn,
		PrincipalID:      principalID,
		PrincipalType:    principalType,
		TargetType:       targetTypeAWSAccount,
	})
	b.assignmentCreationIDs[idempotencyKey] = requestID

	// CreateAccountAssignment provisions the permission set to the account as
	// a side effect, same as an explicit ProvisionPermissionSet call -- real
	// AWS documents this ("this operation also provisions the permission set
	// to the account if it isn't already provisioned").
	b.provisionedAt[provisionedAtKey(instanceArn, permissionSetArn, accountID)] = now

	return requestID, nil
}

// DescribeAccountAssignmentCreationStatus returns the status of a creation request.
// Lazily transitions IN_PROGRESS → SUCCEEDED on first poll.
func (b *InMemoryBackend) DescribeAccountAssignmentCreationStatus(
	_ string,
	requestID string,
) (*ProvisioningStatus, error) {
	b.mu.Lock("DescribeAccountAssignmentCreationStatus")
	defer b.mu.Unlock()

	status, ok := b.creationStatuses.Get(requestID)
	if !ok {
		return nil, ErrRequestNotFound
	}
	if status.Status == statusInProgress {
		status.Status = statusSucceeded
	}

	cp := *status

	return &cp, nil
}

// ListAccountAssignmentCreationStatus returns creation statuses sorted by creation date descending.
// filterStatus filters by status when non-empty.
func (b *InMemoryBackend) ListAccountAssignmentCreationStatus(_, filterStatus string) []*ProvisioningStatus {
	b.mu.RLock("ListAccountAssignmentCreationStatus")
	defer b.mu.RUnlock()

	result := make([]*ProvisioningStatus, 0, b.creationStatuses.Len())
	for _, status := range b.creationStatuses.All() {
		if filterStatus != "" && status.Status != filterStatus {
			continue
		}
		cp := *status
		result = append(result, &cp)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].CreatedDate.After(result[j].CreatedDate)
	})

	return result
}

// ListAccountAssignments returns assignments for a permission set in an instance, optionally filtered by account.
func (b *InMemoryBackend) ListAccountAssignments(instanceArn, permissionSetArn, accountID string) []*AccountAssignment {
	b.mu.RLock("ListAccountAssignments")
	defer b.mu.RUnlock()

	key := assignmentKey(instanceArn, permissionSetArn)
	all := b.assignments[key]

	var result []*AccountAssignment
	for _, a := range all {
		if accountID == "" || a.AccountID == accountID {
			cp := *a
			result = append(result, &cp)
		}
	}

	return result
}

// DeleteAccountAssignment removes an account assignment.
func (b *InMemoryBackend) DeleteAccountAssignment(
	instanceArn, permissionSetArn, accountID, principalID, principalType string,
) (string, error) {
	b.mu.Lock("DeleteAccountAssignment")
	defer b.mu.Unlock()

	key := assignmentKey(instanceArn, permissionSetArn)
	all := b.assignments[key]

	found := false
	var remaining []*AccountAssignment
	for _, a := range all {
		if a.AccountID == accountID && a.PrincipalID == principalID && a.PrincipalType == principalType {
			found = true
		} else {
			remaining = append(remaining, a)
		}
	}
	if !found {
		return "", ErrAssignmentNotFound
	}
	b.assignments[key] = remaining

	// Once no assignment for this account+permission-set pair remains, the
	// account is no longer provisioned with it (real AWS's DeleteAccountAssignment
	// deprovisions when the last assignment is removed) -- drop the ghost
	// provisionedAt row so a future re-CreateAccountAssignment starts clean
	// rather than inheriting a stale timestamp.
	stillAssigned := false

	for _, a := range remaining {
		if a.AccountID == accountID {
			stillAssigned = true

			break
		}
	}

	if !stillAssigned {
		delete(b.provisionedAt, provisionedAtKey(instanceArn, permissionSetArn, accountID))
	}

	// Remove the idempotency index entry and associated creation status to prevent unbounded growth.
	idempotencyKey := key + "|" + accountID + "|" + principalType + "|" + principalID
	if oldRequestID, exists := b.assignmentCreationIDs[idempotencyKey]; exists {
		b.creationStatuses.Delete(oldRequestID)
	}
	delete(b.assignmentCreationIDs, idempotencyKey)

	requestID := uuid.NewString()
	pruneOldestStatus(b.deletionStatuses)
	b.deletionStatuses.Put(&ProvisioningStatus{
		RequestID:        requestID,
		Status:           statusInProgress,
		AccountID:        accountID,
		PermissionSetArn: permissionSetArn,
		PrincipalID:      principalID,
		PrincipalType:    principalType,
		TargetType:       targetTypeAWSAccount,
		CreatedDate:      time.Now().UTC(),
	})

	return requestID, nil
}

// DescribeAccountAssignmentDeletionStatus returns the status of a deletion request.
// Lazily transitions IN_PROGRESS → SUCCEEDED on first poll.
func (b *InMemoryBackend) DescribeAccountAssignmentDeletionStatus(
	_ string,
	requestID string,
) (*ProvisioningStatus, error) {
	b.mu.Lock("DescribeAccountAssignmentDeletionStatus")
	defer b.mu.Unlock()

	status, ok := b.deletionStatuses.Get(requestID)
	if !ok {
		return nil, ErrRequestNotFound
	}
	if status.Status == statusInProgress {
		status.Status = statusSucceeded
	}

	cp := *status

	return &cp, nil
}

// ListAccountAssignmentDeletionStatus returns deletion statuses sorted by creation date descending.
// filterStatus filters by status when non-empty.
func (b *InMemoryBackend) ListAccountAssignmentDeletionStatus(_, filterStatus string) []*ProvisioningStatus {
	b.mu.RLock("ListAccountAssignmentDeletionStatus")
	defer b.mu.RUnlock()

	result := make([]*ProvisioningStatus, 0, b.deletionStatuses.Len())
	for _, status := range b.deletionStatuses.All() {
		if filterStatus != "" && status.Status != filterStatus {
			continue
		}
		cp := *status
		result = append(result, &cp)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].CreatedDate.After(result[j].CreatedDate)
	})

	return result
}

func assignmentKey(instanceArn, permissionSetArn string) string {
	return instanceArn + "|" + permissionSetArn
}

// assignmentIdempotencyKey builds a deterministic key for CreateAccountAssignment idempotency.
// The key encodes all fields that uniquely identify an assignment.
func assignmentIdempotencyKey(baseKey, accountID, principalType, principalID string) string {
	return baseKey + "|" + accountID + "|" + principalType + "|" + principalID
}

// ListAccountAssignmentsForPrincipal returns account assignments for a specific principal.
func (b *InMemoryBackend) ListAccountAssignmentsForPrincipal(
	instanceArn, principalID, principalType string,
) []*AccountAssignment {
	b.mu.RLock("ListAccountAssignmentsForPrincipal")
	defer b.mu.RUnlock()

	var result []*AccountAssignment
	for key, assignments := range b.assignments {
		if !strings.HasPrefix(key, instanceArn+"|") {
			continue
		}
		for _, a := range assignments {
			if a.PrincipalID == principalID && a.PrincipalType == principalType {
				cp := *a
				result = append(result, &cp)
			}
		}
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].AccountID != result[j].AccountID {
			return result[i].AccountID < result[j].AccountID
		}

		return result[i].PermissionSetArn < result[j].PermissionSetArn
	})

	return result
}
