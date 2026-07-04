package quicksight_test

import (
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/quicksight"
)

// ---- SearchTopics ----

func TestQuickSight_SearchTopics(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, accountPath("/topics"), map[string]any{
		"TopicId": "t1",
		"Name":    "Sales",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, http.MethodPost, accountPath("/topics"), map[string]any{
		"TopicId": "t2",
		"Name":    "Marketing",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// No filters: both topics come back.
	rec = doRequest(t, h, http.MethodPost, accountPath("/search/topics"), map[string]any{"Filters": []any{}})
	require.Equal(t, http.StatusOK, rec.Code)
	body := parseBody(t, rec)
	list, ok := body["TopicSummaryList"].([]any)
	require.True(t, ok)
	assert.Len(t, list, 2)

	summary, ok := list[0].(map[string]any)
	require.True(t, ok)
	assert.Contains(t, summary, "Arn")
	assert.Contains(t, summary, "TopicId")
	assert.Contains(t, summary, "Name")
	assert.Contains(t, summary, "UserExperienceVersion")
	assert.NotContains(t, summary, "Description")
	assert.NotContains(t, summary, "CreatedTime")

	// TOPIC_NAME StringEquals filter narrows to a single match.
	rec = doRequest(t, h, http.MethodPost, accountPath("/search/topics"), map[string]any{
		"Filters": []any{
			map[string]any{"Name": "TOPIC_NAME", "Operator": "StringEquals", "Value": "Sales"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	body = parseBody(t, rec)
	list, ok = body["TopicSummaryList"].([]any)
	require.True(t, ok)
	require.Len(t, list, 1)
	summary, ok = list[0].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "Sales", summary["Name"])
}

// ---- UpdateDashboardPublishedVersion ----

func TestQuickSight_UpdateDashboardPublishedVersion(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, accountPath("/dashboards/dash1"), map[string]any{"Name": "D1"})
	require.Equal(t, http.StatusOK, rec.Code)

	// Bump to version 2.
	rec = doRequest(t, h, http.MethodPut, accountPath("/dashboards/dash1"), map[string]any{"Name": "D1v2"})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, http.MethodPut, accountPath("/dashboards/dash1/versions/2"), nil)
	require.Equal(t, http.StatusOK, rec.Code)
	body := parseBody(t, rec)
	assert.Equal(t, "dash1", body["DashboardId"])
	assert.Contains(t, body["DashboardArn"], "dashboard/dash1")

	// A version number that was never created (3) doesn't exist.
	rec = doRequest(t, h, http.MethodPut, accountPath("/dashboards/dash1/versions/3"), nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
	body = parseBody(t, rec)
	assert.Equal(t, "ResourceNotFoundException", body["Code"])

	// Unknown dashboard.
	rec = doRequest(t, h, http.MethodPut, accountPath("/dashboards/nope/versions/1"), nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// ---- UpdateDashboardLinks ----

func TestQuickSight_UpdateDashboardLinks(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, accountPath("/dashboards/dash1"), map[string]any{"Name": "D1"})
	require.Equal(t, http.StatusOK, rec.Code)

	linkArn := "arn:aws:quicksight:us-east-1:000000000000:analysis/a1"
	rec = doRequest(t, h, http.MethodPut, accountPath("/dashboards/dash1/linked-entities"), map[string]any{
		"LinkEntities": []any{linkArn},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	body := parseBody(t, rec)
	assert.Contains(t, body["DashboardArn"], "dashboard/dash1")
	linkEntities, ok := body["LinkEntities"].([]any)
	require.True(t, ok)
	assert.Equal(t, []any{linkArn}, linkEntities)
	assert.NotContains(t, body, "DashboardId")
}

// ---- GetIdentityContext ----

func TestQuickSight_GetIdentityContext(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, accountPath("/identity-context"), map[string]any{
		"UserIdentifier": map[string]any{"UserName": "u1"},
		"Namespace":      testNamespace,
	})
	require.Equal(t, http.StatusOK, rec.Code)
	body := parseBody(t, rec)
	ctx1, ok := body["Context"].(string)
	require.True(t, ok)
	assert.NotEmpty(t, ctx1)
	assert.NotContains(t, body, "IdentityContextDomains")

	// Each call mints a fresh token.
	rec = doRequest(t, h, http.MethodPost, accountPath("/identity-context"), map[string]any{
		"UserIdentifier": map[string]any{"UserName": "u1"},
		"Namespace":      testNamespace,
	})
	require.Equal(t, http.StatusOK, rec.Code)
	ctx2 := parseBody(t, rec)["Context"].(string) //nolint:forcetypeassert // asserted above
	assert.NotEqual(t, ctx1, ctx2)

	// Email/UserName identifiers require a Namespace.
	rec = doRequest(t, h, http.MethodPost, accountPath("/identity-context"), map[string]any{
		"UserIdentifier": map[string]any{"Email": "u1@example.com"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	// Missing UserIdentifier entirely.
	rec = doRequest(t, h, http.MethodPost, accountPath("/identity-context"), map[string]any{})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	// Unknown namespace.
	rec = doRequest(t, h, http.MethodPost, accountPath("/identity-context"), map[string]any{
		"UserIdentifier": map[string]any{"UserName": "u1"},
		"Namespace":      "no-such-ns",
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// ---- Namespace self-upgrade ----

func TestQuickSight_SelfUpgradeConfiguration(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Defaults to ADMIN_APPROVAL when never configured.
	rec := doRequest(t, h, http.MethodGet, nsPath("/self-upgrade-configuration"), nil)
	require.Equal(t, http.StatusOK, rec.Code)
	body := parseBody(t, rec)
	cfg, ok := body["SelfUpgradeConfiguration"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "ADMIN_APPROVAL", cfg["SelfUpgradeStatus"])

	// Invalid status is rejected.
	rec = doRequest(t, h, http.MethodPut, nsPath("/self-upgrade-configuration"), map[string]any{
		"SelfUpgradeStatus": "NOT_A_STATUS",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	// Update to AUTO_APPROVAL and read it back.
	rec = doRequest(t, h, http.MethodPut, nsPath("/self-upgrade-configuration"), map[string]any{
		"SelfUpgradeStatus": "AUTO_APPROVAL",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, http.MethodGet, nsPath("/self-upgrade-configuration"), nil)
	require.Equal(t, http.StatusOK, rec.Code)
	body = parseBody(t, rec)
	cfg, ok = body["SelfUpgradeConfiguration"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "AUTO_APPROVAL", cfg["SelfUpgradeStatus"])

	// Unknown namespace.
	rec = doRequest(t, h, http.MethodGet, accountPath("/namespaces/no-such-ns/self-upgrade-configuration"), nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestQuickSight_ListAndUpdateSelfUpgrades(t *testing.T) {
	t.Parallel()

	backend := newTestBackend(t)
	h := quicksight.NewHandler(backend)

	// No requests yet.
	rec := doRequest(t, h, http.MethodGet, nsPath("/self-upgrade-requests"), nil)
	require.Equal(t, http.StatusOK, rec.Code)
	body := parseBody(t, rec)
	details, ok := body["SelfUpgradeRequestDetails"].([]any)
	require.True(t, ok)
	assert.Empty(t, details)

	// Seed a pending request (no CreateSelfUpgradeRequest API exists).
	quicksight.SeedSelfUpgradeRequest(backend, testAccountID, testNamespace, &quicksight.SelfUpgradeRequestDetail{
		CreationTime:     time.Now().UTC().Unix(),
		UpgradeRequestID: "req1",
		OriginalRole:     "READER",
		RequestedRole:    "AUTHOR",
		RequestStatus:    "PENDING",
	})

	rec = doRequest(t, h, http.MethodGet, nsPath("/self-upgrade-requests"), nil)
	require.Equal(t, http.StatusOK, rec.Code)
	body = parseBody(t, rec)
	details, ok = body["SelfUpgradeRequestDetails"].([]any)
	require.True(t, ok)
	require.Len(t, details, 1)
	detail, ok := details[0].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "req1", detail["UpgradeRequestId"])
	assert.Equal(t, "PENDING", detail["RequestStatus"])

	// Approve it.
	rec = doRequest(t, h, http.MethodPost, nsPath("/update-self-upgrade-request"), map[string]any{
		"Action":           "APPROVE",
		"UpgradeRequestId": "req1",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	body = parseBody(t, rec)
	detail, ok = body["SelfUpgradeRequestDetail"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "APPROVED", detail["RequestStatus"])

	// Unknown request ID.
	rec = doRequest(t, h, http.MethodPost, nsPath("/update-self-upgrade-request"), map[string]any{
		"Action":           "APPROVE",
		"UpgradeRequestId": "no-such-request",
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)

	// Invalid action.
	rec = doRequest(t, h, http.MethodPost, nsPath("/update-self-upgrade-request"), map[string]any{
		"Action":           "NOT_AN_ACTION",
		"UpgradeRequestId": "req1",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ---- Persistence ----

// TestQuickSight_ParitySweep_Persistence verifies that the state introduced in
// this batch (dashboard published version/links, self-upgrade config/requests)
// survives a Snapshot/Restore round-trip.
func TestQuickSight_ParitySweep_Persistence(t *testing.T) {
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
