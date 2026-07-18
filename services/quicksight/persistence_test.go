package quicksight_test

import (
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
	"github.com/blackbirdworks/gopherstack/services/quicksight"
)

// Test_Handler_SnapshotRestore verifies Handler.Snapshot/Restore
// (persistence.go) delegate to the backend -- the shape persistence.Manager
// actually drives. cli.go's setupPersistence registers a service.Registerable
// (the *Handler returned by Provider.Init) in the persistence.Manager only if
// that Handler itself satisfies Snapshot(ctx)/Restore(ctx, []byte);
// InMemoryBackend implementing the same two methods (exercised directly by
// TestQuickSight_Phase3_3_StoreRoundTrip) is not enough on its own, since
// Handler.Backend is the StorageBackend interface and does not promote them.
// Mirrors services/securityhub's Test_Handler_SnapshotRestore.
func Test_Handler_SnapshotRestore(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	backend := quicksight.NewInMemoryBackend("000000000000", "us-east-1")
	h := quicksight.NewHandler(backend)

	// Compile-time proof Handler satisfies the persistence layer's contract.
	var _ persistence.Persistable = h

	_, err := backend.CreateGroup("000000000000", "default", "handler-group", "desc")
	require.NoError(t, err)

	data := h.Snapshot(ctx)
	require.NotEmpty(t, data)

	restoredBackend := quicksight.NewInMemoryBackend("000000000000", "us-east-1")
	restoredHandler := quicksight.NewHandler(restoredBackend)
	require.NoError(t, restoredHandler.Restore(ctx, data))

	assert.Equal(t, 1, quicksight.GroupCount(restoredBackend))

	got, err := restoredBackend.DescribeGroup("000000000000", "default", "handler-group")
	require.NoError(t, err)
	assert.Equal(t, "handler-group", got.GroupName)
}

// TestQuickSight_ExtendedResourcesPersistence verifies that every subsystem added in
// the final Appendix-A batch (brands, custom permissions, role/user custom
// permissions, role memberships, OAuth apps, identity propagation, asset bundle
// jobs, dashboard snapshot jobs, and dataset refresh schedules/properties)
// survives a Snapshot/Restore round-trip.
func TestQuickSight_ExtendedResourcesPersistence(t *testing.T) {
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

// TestQuickSight_ActionConnectorAutomationFlowPersistence verifies that every subsystem added in
// the final canned-stub batch (action connectors, automation jobs, flows)
// survives a Snapshot/Restore round-trip.
func TestQuickSight_ActionConnectorAutomationFlowPersistence(t *testing.T) {
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

// ---- Persistence ----

// TestQuickSight_DashboardVersionSelfUpgradePersistence verifies that the state introduced in
// this batch (dashboard published version/links, self-upgrade config/requests)
// survives a Snapshot/Restore round-trip.
func TestQuickSight_DashboardVersionSelfUpgradePersistence(t *testing.T) {
	t.Parallel()

	backend := newTestBackend(t)
	h := quicksight.NewHandler(backend)

	require.Equal(t, http.StatusOK,
		doRequest(t, h, http.MethodPost, accountPath("/dashboards/dash1"), map[string]any{"Name": "D1"}).Code)
	require.Equal(t, http.StatusOK,
		doRequest(t, h, http.MethodPut, accountPath("/dashboards/dash1"), map[string]any{"Name": "D1v2"}).Code)
	require.Equal(t, http.StatusOK,
		doRequest(t, h, http.MethodPut, accountPath("/dashboards/dash1/versions/2"), nil).Code)
	require.Equal(t, http.StatusOK,
		doRequest(t, h, http.MethodPut, accountPath("/dashboards/dash1/linked-entities"), map[string]any{
			"LinkEntities": []any{"arn:aws:quicksight:us-east-1:000000000000:analysis/a1"},
		}).Code)
	require.Equal(t, http.StatusOK,
		doRequest(t, h, http.MethodPut, nsPath("/self-upgrade-configuration"), map[string]any{
			"SelfUpgradeStatus": "AUTO_APPROVAL",
		}).Code)

	quicksight.SeedSelfUpgradeRequest(backend, testAccountID, testNamespace, &quicksight.SelfUpgradeRequestDetail{
		CreationTime:     time.Now().UTC().Unix(),
		UpgradeRequestID: "req1",
		OriginalRole:     "READER",
		RequestedRole:    "AUTHOR",
		RequestStatus:    "PENDING",
	})

	snapshot := backend.Snapshot(t.Context())
	restoredBackend := quicksight.NewInMemoryBackend(testAccountID, testRegion)
	require.NoError(t, restoredBackend.Restore(t.Context(), snapshot))
	restoredHandler := quicksight.NewHandler(restoredBackend)

	rec := doRequest(t, restoredHandler, http.MethodPut, accountPath("/dashboards/dash1/versions/2"), nil)
	require.Equal(t, http.StatusOK, rec.Code)

	linksRec := doRequest(
		t, restoredHandler, http.MethodPut, accountPath("/dashboards/dash1/linked-entities"),
		map[string]any{"LinkEntities": []any{"arn:aws:quicksight:us-east-1:000000000000:analysis/a1"}},
	)
	require.Equal(t, http.StatusOK, linksRec.Code)
	linkEntities, ok := parseBody(t, linksRec)["LinkEntities"].([]any)
	require.True(t, ok)
	assert.Equal(t, []any{"arn:aws:quicksight:us-east-1:000000000000:analysis/a1"}, linkEntities)

	cfgRec := doRequest(t, restoredHandler, http.MethodGet, nsPath("/self-upgrade-configuration"), nil)
	require.Equal(t, http.StatusOK, cfgRec.Code)
	cfg, ok := parseBody(t, cfgRec)["SelfUpgradeConfiguration"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "AUTO_APPROVAL", cfg["SelfUpgradeStatus"])

	listRec := doRequest(t, restoredHandler, http.MethodGet, nsPath("/self-upgrade-requests"), nil)
	require.Equal(t, http.StatusOK, listRec.Code)
	details, ok := parseBody(t, listRec)["SelfUpgradeRequestDetails"].([]any)
	require.True(t, ok)
	require.Len(t, details, 1)
}
