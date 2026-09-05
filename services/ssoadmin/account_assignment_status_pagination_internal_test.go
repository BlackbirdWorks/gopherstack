package ssoadmin

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func verifyAllPagesCovered(
	t *testing.T,
	numItems int,
	wantIDs map[string]bool,
	listFn func() []accountAssignmentStatusMetadataView,
) {
	t.Helper()

	got := make(map[string]int, numItems)
	var next string

	for {
		statuses := listFn()
		page, rawNext := paginateOrdered(statuses, 3, next, func(v accountAssignmentStatusMetadataView) string {
			return v.RequestID
		})

		for _, v := range page {
			got[v.RequestID]++
		}

		if rawNext == nil {
			break
		}

		next, _ = rawNext.(string)
	}

	require.Len(t, got, numItems)
	for id, count := range got {
		assert.Equal(t, 1, count, "request %q appeared %d times", id, count)
	}
	for id := range wantIDs {
		assert.Equal(t, 1, got[id], "request %q missing from paginated results", id)
	}
}

func setupSSOAdminTestInstance(t *testing.T, b *InMemoryBackend) (*Instance, *PermissionSet) {
	t.Helper()

	inst, err := b.CreateInstance("test", "111111111111", "", nil)
	require.NoError(t, err)

	ps, err := b.CreatePermissionSet(inst.InstanceArn, "TestPS", "", "PT1H", "", nil)
	require.NoError(t, err)

	return inst, ps
}

// TestListAccountAssignmentCreationStatus_PaginationStableAcrossTiedCreatedDate
// proves that paginating ListAccountAssignmentCreationStatus is reproducible
// when several ProvisioningStatus records share an identical CreatedDate.
func TestListAccountAssignmentCreationStatus_PaginationStableAcrossTiedCreatedDate(t *testing.T) {
	t.Parallel()

	const numAssignments = 8
	tied := time.Now().UTC()

	tests := []struct {
		name string
	}{
		{name: "creation_status_tied_dates"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			for range 30 {
				b := NewInMemoryBackend("111111111111", "us-east-1")
				inst, ps := setupSSOAdminTestInstance(t, b)
				wantIDs := make(map[string]bool, numAssignments)

				for i := range numAssignments {
					reqID, createErr := b.CreateAccountAssignment(
						inst.InstanceArn, ps.PermissionSetArn,
						fmt.Sprintf("%012d", i), fmt.Sprintf("principal-%d", i), "USER",
					)
					require.NoError(t, createErr)

					status, ok := b.creationStatuses.Get(reqID)
					require.True(t, ok)

					cp := *status
					cp.CreatedDate = tied
					b.creationStatuses.Put(&cp)

					wantIDs[reqID] = true
				}

				verifyAllPagesCovered(t, numAssignments, wantIDs, func() []accountAssignmentStatusMetadataView {
					statuses := b.ListAccountAssignmentCreationStatus(inst.InstanceArn, "")
					out := make([]accountAssignmentStatusMetadataView, 0, len(statuses))
					for _, s := range statuses {
						out = append(out, toAccountAssignmentStatusMetadataView(s))
					}

					return out
				})
			}
		})
	}
}

// TestListAccountAssignmentDeletionStatus_PaginationStableAcrossTiedCreatedDate
// is the DeleteAccountAssignment sibling of the CreationStatus proof above.
func TestListAccountAssignmentDeletionStatus_PaginationStableAcrossTiedCreatedDate(t *testing.T) {
	t.Parallel()

	const numAssignments = 8
	tied := time.Now().UTC()

	tests := []struct {
		name string
	}{
		{name: "deletion_status_tied_dates"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			for range 30 {
				b := NewInMemoryBackend("111111111111", "us-east-1")
				inst, ps := setupSSOAdminTestInstance(t, b)
				wantIDs := make(map[string]bool, numAssignments)

				for i := range numAssignments {
					accountID := fmt.Sprintf("%012d", i)
					principalID := fmt.Sprintf("principal-%d", i)

					_, createErr := b.CreateAccountAssignment(
						inst.InstanceArn, ps.PermissionSetArn, accountID, principalID, "USER",
					)
					require.NoError(t, createErr)

					reqID, deleteErr := b.DeleteAccountAssignment(
						inst.InstanceArn, ps.PermissionSetArn, accountID, principalID, "USER",
					)
					require.NoError(t, deleteErr)

					status, ok := b.deletionStatuses.Get(reqID)
					require.True(t, ok)

					cp := *status
					cp.CreatedDate = tied
					b.deletionStatuses.Put(&cp)

					wantIDs[reqID] = true
				}

				verifyAllPagesCovered(t, numAssignments, wantIDs, func() []accountAssignmentStatusMetadataView {
					statuses := b.ListAccountAssignmentDeletionStatus(inst.InstanceArn, "")
					out := make([]accountAssignmentStatusMetadataView, 0, len(statuses))
					for _, s := range statuses {
						out = append(out, toAccountAssignmentStatusMetadataView(s))
					}

					return out
				})
			}
		})
	}
}

// TestListPermissionSetProvisioningStatus_PaginationStableAcrossTiedCreatedDate
// is the ProvisionPermissionSet sibling of the two proofs above.
func TestListPermissionSetProvisioningStatus_PaginationStableAcrossTiedCreatedDate(t *testing.T) {
	t.Parallel()

	const numStatuses = 8
	tied := time.Now().UTC()

	tests := []struct {
		name string
	}{
		{name: "provisioning_status_tied_dates"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			for range 30 {
				b := NewInMemoryBackend("111111111111", "us-east-1")
				inst, ps := setupSSOAdminTestInstance(t, b)
				wantIDs := make(map[string]bool, numStatuses)

				for i := range numStatuses {
					reqID, provisionErr := b.ProvisionPermissionSet(
						inst.InstanceArn, ps.PermissionSetArn, targetTypeAWSAccount, fmt.Sprintf("%012d", i),
					)
					require.NoError(t, provisionErr)

					status, ok := b.provisioningStatuses.Get(reqID)
					require.True(t, ok)

					cp := *status
					cp.CreatedDate = tied
					b.provisioningStatuses.Put(&cp)

					wantIDs[reqID] = true
				}

				verifyAllPagesCovered(t, numStatuses, wantIDs, func() []accountAssignmentStatusMetadataView {
					statuses := b.ListPermissionSetProvisioningStatus(inst.InstanceArn, "")
					out := make([]accountAssignmentStatusMetadataView, 0, len(statuses))
					for _, s := range statuses {
						out = append(out, toAccountAssignmentStatusMetadataView(s))
					}

					return out
				})
			}
		})
	}
}
