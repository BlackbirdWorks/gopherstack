package quicksight_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/quicksight"
)

// TestQuickSight_FinalStub_Persistence verifies that every subsystem added in
// the final canned-stub batch (action connectors, automation jobs, flows)
// survives a Snapshot/Restore round-trip.
func TestQuickSight_FinalStub_Persistence(t *testing.T) {
	t.Parallel()

	backend := newTestBackend(t)
	h := quicksight.NewHandler(backend)

	require.Equal(
		t, http.StatusOK,
		doRequest(t, h, http.MethodPost, accountPath("/action-connectors"), map[string]any{
			"ActionConnectorId": "ac1",
			"Name":              "AC1",
			"Type":              "GENERIC_HTTP",
			"AuthenticationConfig": map[string]any{
				"AuthenticationType":     "NO_AUTH",
				"AuthenticationMetadata": map[string]any{},
			},
		}).Code,
	)

	startRec := doRequest(
		t, h, http.MethodPost,
		accountPath("/automation-groups/group1/automations/automation1/jobs"),
		map[string]any{},
	)
	require.Equal(t, http.StatusOK, startRec.Code)
	jobID, ok := parseBody(t, startRec)["JobId"].(string)
	require.True(t, ok)
	require.NotEmpty(t, jobID)

	seedTestFlow(backend, "flow1", "Flow One")

	// Snapshot, then restore onto a brand-new backend/handler.
	snapshot := backend.Snapshot(t.Context())
	restoredBackend := quicksight.NewInMemoryBackend(testAccountID, testRegion)
	require.NoError(t, restoredBackend.Restore(t.Context(), snapshot))
	restoredHandler := quicksight.NewHandler(restoredBackend)

	acRec := doRequest(t, restoredHandler, http.MethodGet, accountPath("/action-connectors/ac1"), nil)
	require.Equal(t, http.StatusOK, acRec.Code)

	jobRec := doRequest(
		t, restoredHandler, http.MethodGet,
		accountPath("/automation-groups/group1/automations/automation1/jobs/"+jobID),
		nil,
	)
	require.Equal(t, http.StatusOK, jobRec.Code)

	flowRec := doRequest(t, restoredHandler, http.MethodGet, accountPath("/flows/flow1/metadata"), nil)
	require.Equal(t, http.StatusOK, flowRec.Code)
}
