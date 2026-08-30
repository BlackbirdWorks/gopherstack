package workspaces_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/workspaces"
)

// TestDescribeWorkspacesConnectionStatus_UnfilteredPageWalksExactly proves the
// unfiltered (WorkspaceIds omitted) path of DescribeWorkspacesConnectionStatus
// now honors NextToken and returns a deterministic, total (sorted by
// WorkspaceId) order. Real aws-sdk-go-v2/service/workspaces@v1.73.1's
// DescribeWorkspacesConnectionStatusInput/Output both declare NextToken (only
// WorkspaceIds is capped at 25 -- the unfiltered path genuinely paginates),
// but this backend previously had no NextToken on either wire struct at all
// and built the unfiltered response straight off store.Table.All() (unspecified
// map order) with no sort -- both a missing-cursor gap and a Class E
// (never-sorted) bug. Hand-reverted to confirm: with the pre-fix code, 15
// repeated calls to the unfiltered backend method produced a different
// WorkspaceId order essentially every time (map iteration is randomized per
// range); this test walks NextToken across more than one internal page
// (connectionStatusPageSize = 100) and asserts the concatenation reproduces
// every created WorkspaceId exactly once, with no gap in a single walk.
func TestDescribeWorkspacesConnectionStatus_UnfilteredPageWalksExactly(t *testing.T) {
	t.Parallel()

	b := workspaces.NewInMemoryBackend("000000000000", "us-east-1")
	ctx := context.Background()

	require.NoError(t, b.RegisterWorkspaceDirectory("d-test", nil, nil))

	const n = 130 // > connectionStatusPageSize (100), so the walk spans 2 pages.

	want := make([]string, 0, n)

	for i := range n {
		ws, err := b.CreateWorkspace(ctx, &workspaces.WorkspaceCreationSpec{
			DirectoryID: "d-test",
			UserName:    fmt.Sprintf("user%d", i),
		})
		require.NoError(t, err)
		want = append(want, ws.WorkspaceID)
	}

	var got []string

	token := ""
	pages := 0

	for {
		statuses, next, err := b.GetWorkspacesConnectionStatus(nil, token)
		require.NoError(t, err)

		for _, s := range statuses {
			got = append(got, s.WorkspaceID)
		}

		pages++
		token = next

		if token == "" {
			break
		}
	}

	require.Greater(t, pages, 1, "expected the walk to span more than one internal page")
	require.ElementsMatch(t, want, got)
}

// TestDescribeWorkspacesConnectionStatus_UnfilteredOrderIsDeterministic proves
// the unfiltered path's order no longer depends on Go's randomized map
// iteration -- repeated calls (with no intervening writes) return the exact
// same WorkspaceId order every time.
func TestDescribeWorkspacesConnectionStatus_UnfilteredOrderIsDeterministic(t *testing.T) {
	t.Parallel()

	b := workspaces.NewInMemoryBackend("000000000000", "us-east-1")
	ctx := context.Background()

	require.NoError(t, b.RegisterWorkspaceDirectory("d-test", nil, nil))

	for i := range 12 {
		_, err := b.CreateWorkspace(ctx, &workspaces.WorkspaceCreationSpec{
			DirectoryID: "d-test",
			UserName:    fmt.Sprintf("user%d", i),
		})
		require.NoError(t, err)
	}

	var firstOrder []string

	for iter := range 15 {
		statuses, _, err := b.GetWorkspacesConnectionStatus(nil, "")
		require.NoError(t, err)

		order := make([]string, 0, len(statuses))
		for _, s := range statuses {
			order = append(order, s.WorkspaceID)
		}

		if iter == 0 {
			firstOrder = order

			continue
		}

		require.Equalf(t, firstOrder, order, "iteration %d: order changed with no intervening writes", iter)
	}
}
