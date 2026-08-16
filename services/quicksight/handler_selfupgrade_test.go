package quicksight_test

import (
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/quicksight"
)

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
		UserName:         "alice",
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
	// SelfUpgradeRequestDetail.UserName (aws-sdk-go-v2/service/quicksight@v1.123.1/
	// types/types.go:18620) identifies who requested the upgrade.
	assert.Equal(t, "alice", detail["UserName"])

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
