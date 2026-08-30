package ssoadmin

import (
	"fmt"
	"testing"
	"time"
)

// TestListAccountAssignmentCreationStatus_PaginationStableAcrossTiedCreatedDate
// proves that paginating ListAccountAssignmentCreationStatus is reproducible
// when several ProvisioningStatus records share an identical CreatedDate (a
// realistic tie: e.g. bulk-provisioning several account assignments in the
// same instant). The RequestID resume cursor (paginateOrdered's keyFn) is
// itself genuinely unique, but the sort feeding it -- CreatedDate descending
// over b.creationStatuses.All(), a store.Table map walk -- is not: real
// requests re-fetch and re-sort on every call (see
// listProvisioningStatusMetadata), so ties can resolve to a different
// relative order between two separate calls even though nothing changed.
// The resume-by-key scan in paginateOrdered then finds a different split
// point, dropping or duplicating a tied record at the page boundary. This
// mirrors the elbv2 listener/rule bug: a unique marker fed by a tie-prone sort.
func TestListAccountAssignmentCreationStatus_PaginationStableAcrossTiedCreatedDate(t *testing.T) {
	t.Parallel()

	const numAssignments = 8

	tied := time.Now().UTC()

	for iter := range 30 {
		b := NewInMemoryBackend("111111111111", "us-east-1")

		inst, err := b.CreateInstance("test", "111111111111", "", nil)
		if err != nil {
			t.Fatalf("iter %d: CreateInstance: %v", iter, err)
		}

		ps, err := b.CreatePermissionSet(inst.InstanceArn, "TestPS", "", "PT1H", "", nil)
		if err != nil {
			t.Fatalf("iter %d: CreatePermissionSet: %v", iter, err)
		}

		wantIDs := make(map[string]bool, numAssignments)

		for i := range numAssignments {
			reqID, createErr := b.CreateAccountAssignment(
				inst.InstanceArn, ps.PermissionSetArn,
				fmt.Sprintf("%012d", i), fmt.Sprintf("principal-%d", i), "USER",
			)
			if createErr != nil {
				t.Fatalf("iter %d: CreateAccountAssignment: %v", iter, createErr)
			}

			status, ok := b.creationStatuses.Get(reqID)
			if !ok {
				t.Fatalf("iter %d: creation status %q not found", iter, reqID)
			}

			cp := *status
			cp.CreatedDate = tied
			b.creationStatuses.Put(&cp)

			wantIDs[reqID] = true
		}

		got := make(map[string]int, numAssignments)

		var next string

		for {
			statuses := b.ListAccountAssignmentCreationStatus(inst.InstanceArn, "")

			out := make([]accountAssignmentStatusMetadataView, 0, len(statuses))
			for _, s := range statuses {
				out = append(out, toAccountAssignmentStatusMetadataView(s))
			}

			page, rawNext := paginateOrdered(out, 3, next, func(v accountAssignmentStatusMetadataView) string {
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

		if len(got) != numAssignments {
			t.Fatalf(
				"iter %d: got %d distinct request IDs across pages, want %d: %v",
				iter, len(got), numAssignments, got,
			)
		}

		for id, count := range got {
			if count != 1 {
				t.Fatalf("iter %d: request %q appeared %d times across pages (want exactly 1)", iter, id, count)
			}
		}

		for id := range wantIDs {
			if got[id] != 1 {
				t.Fatalf("iter %d: request %q missing from paginated results", iter, id)
			}
		}
	}
}

// TestListAccountAssignmentDeletionStatus_PaginationStableAcrossTiedCreatedDate
// is the DeleteAccountAssignment sibling of the CreationStatus proof above --
// same paginateOrdered resume-by-RequestID cursor, same CreatedDate-descending
// sort over b.deletionStatuses.All() (a store.Table map walk).
func TestListAccountAssignmentDeletionStatus_PaginationStableAcrossTiedCreatedDate(t *testing.T) {
	t.Parallel()

	const numAssignments = 8

	tied := time.Now().UTC()

	for iter := range 30 {
		b := NewInMemoryBackend("111111111111", "us-east-1")

		inst, err := b.CreateInstance("test", "111111111111", "", nil)
		if err != nil {
			t.Fatalf("iter %d: CreateInstance: %v", iter, err)
		}

		ps, err := b.CreatePermissionSet(inst.InstanceArn, "TestPS", "", "PT1H", "", nil)
		if err != nil {
			t.Fatalf("iter %d: CreatePermissionSet: %v", iter, err)
		}

		wantIDs := make(map[string]bool, numAssignments)

		for i := range numAssignments {
			accountID := fmt.Sprintf("%012d", i)
			principalID := fmt.Sprintf("principal-%d", i)

			if _, createErr := b.CreateAccountAssignment(
				inst.InstanceArn, ps.PermissionSetArn, accountID, principalID, "USER",
			); createErr != nil {
				t.Fatalf("iter %d: CreateAccountAssignment: %v", iter, createErr)
			}

			reqID, deleteErr := b.DeleteAccountAssignment(
				inst.InstanceArn, ps.PermissionSetArn, accountID, principalID, "USER",
			)
			if deleteErr != nil {
				t.Fatalf("iter %d: DeleteAccountAssignment: %v", iter, deleteErr)
			}

			status, ok := b.deletionStatuses.Get(reqID)
			if !ok {
				t.Fatalf("iter %d: deletion status %q not found", iter, reqID)
			}

			cp := *status
			cp.CreatedDate = tied
			b.deletionStatuses.Put(&cp)

			wantIDs[reqID] = true
		}

		got := make(map[string]int, numAssignments)

		var next string

		for {
			statuses := b.ListAccountAssignmentDeletionStatus(inst.InstanceArn, "")

			out := make([]accountAssignmentStatusMetadataView, 0, len(statuses))
			for _, s := range statuses {
				out = append(out, toAccountAssignmentStatusMetadataView(s))
			}

			page, rawNext := paginateOrdered(out, 3, next, func(v accountAssignmentStatusMetadataView) string {
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

		if len(got) != numAssignments {
			t.Fatalf(
				"iter %d: got %d distinct request IDs across pages, want %d: %v",
				iter, len(got), numAssignments, got,
			)
		}

		for id, count := range got {
			if count != 1 {
				t.Fatalf("iter %d: request %q appeared %d times across pages (want exactly 1)", iter, id, count)
			}
		}

		for id := range wantIDs {
			if got[id] != 1 {
				t.Fatalf("iter %d: request %q missing from paginated results", iter, id)
			}
		}
	}
}

// TestListPermissionSetProvisioningStatus_PaginationStableAcrossTiedCreatedDate
// is the ProvisionPermissionSet sibling of the two proofs above -- same
// paginateOrdered resume-by-RequestID cursor, same CreatedDate-descending
// sort over b.provisioningStatuses.All() (a store.Table map walk).
func TestListPermissionSetProvisioningStatus_PaginationStableAcrossTiedCreatedDate(t *testing.T) {
	t.Parallel()

	const numStatuses = 8

	tied := time.Now().UTC()

	for iter := range 30 {
		b := NewInMemoryBackend("111111111111", "us-east-1")

		inst, err := b.CreateInstance("test", "111111111111", "", nil)
		if err != nil {
			t.Fatalf("iter %d: CreateInstance: %v", iter, err)
		}

		ps, err := b.CreatePermissionSet(inst.InstanceArn, "TestPS", "", "PT1H", "", nil)
		if err != nil {
			t.Fatalf("iter %d: CreatePermissionSet: %v", iter, err)
		}

		wantIDs := make(map[string]bool, numStatuses)

		for i := range numStatuses {
			reqID, provisionErr := b.ProvisionPermissionSet(
				inst.InstanceArn, ps.PermissionSetArn, targetTypeAWSAccount, fmt.Sprintf("%012d", i),
			)
			if provisionErr != nil {
				t.Fatalf("iter %d: ProvisionPermissionSet: %v", iter, provisionErr)
			}

			status, ok := b.provisioningStatuses.Get(reqID)
			if !ok {
				t.Fatalf("iter %d: provisioning status %q not found", iter, reqID)
			}

			cp := *status
			cp.CreatedDate = tied
			b.provisioningStatuses.Put(&cp)

			wantIDs[reqID] = true
		}

		got := make(map[string]int, numStatuses)

		var next string

		for {
			statuses := b.ListPermissionSetProvisioningStatus(inst.InstanceArn, "")

			out := make([]accountAssignmentStatusMetadataView, 0, len(statuses))
			for _, s := range statuses {
				out = append(out, toAccountAssignmentStatusMetadataView(s))
			}

			page, rawNext := paginateOrdered(out, 3, next, func(v accountAssignmentStatusMetadataView) string {
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

		if len(got) != numStatuses {
			t.Fatalf(
				"iter %d: got %d distinct request IDs across pages, want %d: %v",
				iter, len(got), numStatuses, got,
			)
		}

		for id, count := range got {
			if count != 1 {
				t.Fatalf("iter %d: request %q appeared %d times across pages (want exactly 1)", iter, id, count)
			}
		}

		for id := range wantIDs {
			if got[id] != 1 {
				t.Fatalf("iter %d: request %q missing from paginated results", iter, id)
			}
		}
	}
}
