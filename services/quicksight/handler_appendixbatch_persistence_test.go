package quicksight_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/quicksight"
)

// TestQuickSight_AppendixBatch_Persistence verifies that every subsystem added in
// the final Appendix-A batch (brands, custom permissions, role/user custom
// permissions, role memberships, OAuth apps, identity propagation, asset bundle
// jobs, dashboard snapshot jobs, and dataset refresh schedules/properties)
// survives a Snapshot/Restore round-trip.
func TestQuickSight_AppendixBatch_Persistence(t *testing.T) {
	t.Parallel()

	backend := newTestBackend(t)
	h := quicksight.NewHandler(backend)

	require.Equal(t, http.StatusOK, doRequest(t, h, http.MethodPost, accountPath("/brands/b1"), map[string]any{
		"BrandDefinition": map[string]any{"BrandName": "Brand One"},
	}).Code)

	require.Equal(
		t, http.StatusOK,
		doRequest(t, h, http.MethodPost, accountPath("/custom-permissions"), map[string]any{
			"CustomPermissionsName": "cp1",
		}).Code,
	)

	require.Equal(
		t, http.StatusOK,
		doRequest(t, h, http.MethodPut, nsPath("/roles/AUTHOR/custom-permission"), map[string]any{
			"CustomPermissionsName": "cp1",
		}).Code,
	)

	require.Equal(t, http.StatusOK, doRequest(t, h, http.MethodPost, nsPath("/roles/AUTHOR/members/group1"), nil).Code)

	require.Equal(
		t, http.StatusOK,
		doRequest(t, h, http.MethodPost, accountPath("/oauth-client-applications"), map[string]any{
			"OAuthClientApplicationId": "app1", "Name": "App1",
		}).Code,
	)

	require.Equal(
		t, http.StatusOK,
		doRequest(t, h, http.MethodPut, accountPath("/identity-propagation-config/REDSHIFT"), map[string]any{
			"AuthorizedTargets": []string{"arn:aws:sso::000000000000:application/foo"},
		}).Code,
	)

	require.Equal(
		t, http.StatusOK,
		doRequest(t, h, http.MethodPost, accountPath("/asset-bundle-export-jobs"), map[string]any{
			"AssetBundleExportJobId": "job1",
			"ResourceArns":           []string{"arn:aws:quicksight:us-east-1:000000000000:dashboard/dash1"},
		}).Code,
	)

	require.Equal(t, http.StatusOK, doRequest(t, h, http.MethodPost, accountPath("/dashboards/dash1"), map[string]any{
		"Name": "Dashboard1",
	}).Code)
	require.Equal(
		t, http.StatusOK,
		doRequest(t, h, http.MethodPost, accountPath("/dashboards/dash1/snapshot-jobs"), map[string]any{
			"SnapshotJobId": "snap1",
		}).Code,
	)

	require.Equal(t, http.StatusCreated, doRequest(t, h, http.MethodPost, accountPath("/data-sets"), map[string]any{
		"DataSetId": "ds1", "Name": "DataSet1", "ImportMode": "SPICE",
	}).Code)
	require.Equal(
		t, http.StatusOK,
		doRequest(t, h, http.MethodPost, accountPath("/data-sets/ds1/refresh-schedules"), map[string]any{
			"Schedule": map[string]any{"ScheduleId": "sched1", "RefreshType": "FULL_REFRESH"},
		}).Code,
	)
	require.Equal(
		t, http.StatusOK,
		doRequest(t, h, http.MethodPut, accountPath("/data-sets/ds1/refresh-properties"), map[string]any{
			"DataSetRefreshProperties": map[string]any{"RefreshConfiguration": map[string]any{}},
		}).Code,
	)

	// Snapshot, then restore onto a brand-new backend/handler.
	snapshot := backend.Snapshot(t.Context())
	restoredBackend := quicksight.NewInMemoryBackend(testAccountID, testRegion)
	require.NoError(t, restoredBackend.Restore(t.Context(), snapshot))
	restoredHandler := quicksight.NewHandler(restoredBackend)

	brandRec := doRequest(t, restoredHandler, http.MethodGet, accountPath("/brands/b1"), nil)
	require.Equal(t, http.StatusOK, brandRec.Code)

	cpRec := doRequest(t, restoredHandler, http.MethodGet, accountPath("/custom-permissions/cp1"), nil)
	require.Equal(t, http.StatusOK, cpRec.Code)

	roleCPRec := doRequest(t, restoredHandler, http.MethodGet, nsPath("/roles/AUTHOR/custom-permission"), nil)
	require.Equal(t, http.StatusOK, roleCPRec.Code)
	assert.Equal(t, "cp1", parseBody(t, roleCPRec)["CustomPermissionsName"])

	membersRec := doRequest(t, restoredHandler, http.MethodGet, nsPath("/roles/AUTHOR/members"), nil)
	require.Equal(t, http.StatusOK, membersRec.Code)
	assert.Contains(t, parseBody(t, membersRec)["MembersList"], "group1")

	oauthRec := doRequest(t, restoredHandler, http.MethodGet, accountPath("/oauth-client-applications/app1"), nil)
	require.Equal(t, http.StatusOK, oauthRec.Code)

	idPropRec := doRequest(t, restoredHandler, http.MethodGet, accountPath("/identity-propagation-config"), nil)
	require.Equal(t, http.StatusOK, idPropRec.Code)
	assert.Len(t, parseBody(t, idPropRec)["Services"], 1)

	exportJobRec := doRequest(t, restoredHandler, http.MethodGet, accountPath("/asset-bundle-export-jobs/job1"), nil)
	require.Equal(t, http.StatusOK, exportJobRec.Code)

	snapshotJobRec := doRequest(
		t, restoredHandler, http.MethodGet, accountPath("/dashboards/dash1/snapshot-jobs/snap1"), nil,
	)
	require.Equal(t, http.StatusOK, snapshotJobRec.Code)

	scheduleRec := doRequest(
		t, restoredHandler, http.MethodGet, accountPath("/data-sets/ds1/refresh-schedules/sched1"), nil,
	)
	require.Equal(t, http.StatusOK, scheduleRec.Code)

	propsRec := doRequest(t, restoredHandler, http.MethodGet, accountPath("/data-sets/ds1/refresh-properties"), nil)
	require.Equal(t, http.StatusOK, propsRec.Code)
}
