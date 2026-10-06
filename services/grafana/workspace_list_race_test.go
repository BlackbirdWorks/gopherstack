package grafana_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Listing while the async transition fires must hand out copies, not live records.
func TestListWorkspaces_ReturnsCopies(t *testing.T) {
	t.Parallel()

	h, client := newTestHandlerAndClient(t)

	_, err := client.CreateWorkspace(t.Context(), minimalCreateWorkspaceInput())
	require.NoError(t, err)

	first := h.Backend.ListWorkspaces()
	require.Len(t, first, 1)
	assert.NotSame(t, first[0], h.Backend.ListWorkspaces()[0])

	require.Eventually(t, func() bool {
		all := h.Backend.ListWorkspaces()

		return len(all) == 1 && all[0].Status == "ACTIVE" && !all[0].Modified.IsZero()
	}, 10*workspaceTransitionWait, 5*time.Millisecond)

	assert.Equal(t, "CREATING", first[0].Status)
}
