package redshift_test

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/redshift"
)

// TestServerless_UpdateNamespace_DBNameNotMutated is a regression test for
// gopherstack-8v8v: UpdateNamespaceInput has no dbName member in the real
// SDK, so a client sending one must not change the stored namespace.
func TestServerless_UpdateNamespace_DBNameNotMutated(t *testing.T) {
	t.Parallel()

	h := newServerlessHandler()

	rec := doServerlessOp(t, h, "CreateNamespace", map[string]any{
		"namespaceName": "phantom-ns",
		"dbName":        "original",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doServerlessOp(t, h, "UpdateNamespace", map[string]any{
		"namespaceName": "phantom-ns",
		"dbName":        "attempted-rename",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doServerlessOp(t, h, "GetNamespace", map[string]any{"namespaceName": "phantom-ns"})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	ns, _ := resp["namespace"].(map[string]any)
	require.NotNil(t, ns)
	assert.Equal(t, "original", ns["dbName"], "UpdateNamespace's dbName must be silently ignored, not applied")
}

// TestServerless_Namespace_AdminUserPassword_NeverEchoed covers
// gopherstack-mbcq: AdminUserPassword is accepted on both CreateNamespace and
// UpdateNamespace (the only way to set an explicit admin password outside
// ManageAdminPassword), but as a credential it must never appear in a
// response.
func TestServerless_Namespace_AdminUserPassword_NeverEchoed(t *testing.T) {
	t.Parallel()

	h := newServerlessHandler()

	rec := doServerlessOp(t, h, "CreateNamespace", map[string]any{
		"namespaceName":             "pw-ns",
		"adminUsername":             "admin",
		"adminUserPassword":         "s3cr3t-Passw0rd",
		"redshiftIdcApplicationArn": "arn:aws:sso::000000000000:application/idc-app-1",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	body := rec.Body.String()
	assert.NotContains(t, body, "s3cr3t-Passw0rd")

	var createResp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&createResp))
	ns, _ := createResp["namespace"].(map[string]any)
	require.NotNil(t, ns)
	_, hasPassword := ns["adminUserPassword"]
	assert.False(t, hasPassword)
	_, hasIdcArn := ns["redshiftIdcApplicationArn"]
	assert.False(t, hasIdcArn, "real Namespace has no redshiftIdcApplicationArn member either")

	rec = doServerlessOp(t, h, "UpdateNamespace", map[string]any{
		"namespaceName":     "pw-ns",
		"adminUserPassword": "another-S3cret1",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.NotContains(t, rec.Body.String(), "another-S3cret1")
}

// TestServerless_RestoreFromSnapshot_MaintainIntegration covers
// gopherstack-mbcq: MaintainIntegration must be accepted on the wire even
// though this backend has no integration state to gate.
func TestServerless_RestoreFromSnapshot_MaintainIntegration(t *testing.T) {
	t.Parallel()

	h := newServerlessHandler()
	doServerlessOp(t, h, "CreateNamespace", map[string]any{"namespaceName": "mi-ns"})
	doServerlessOp(t, h, "CreateWorkgroup", map[string]any{"workgroupName": "mi-wg", "namespaceName": "mi-ns"})
	rec := doServerlessOp(t, h, "CreateSnapshot", map[string]any{"snapshotName": "mi-snap", "namespaceName": "mi-ns"})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doServerlessOp(t, h, "RestoreFromSnapshot", map[string]any{
		"namespaceName":       "mi-ns",
		"workgroupName":       "mi-wg",
		"snapshotName":        "mi-snap",
		"maintainIntegration": false,
	})
	require.Equal(t, http.StatusOK, rec.Code)
}

// TestServerless_TableRestore_ActivateCaseSensitiveIdentifier covers
// gopherstack-mbcq for both RestoreTableFromSnapshot and
// RestoreTableFromRecoveryPoint, which share slTableRestoreReq.
func TestServerless_TableRestore_ActivateCaseSensitiveIdentifier(t *testing.T) {
	t.Parallel()

	h, rp := seedNamespaceAndWorkgroup(t, "cs-ns", "cs-wg")
	recoveryPointID, _ := rp["recoveryPointId"].(string)
	require.NotEmpty(t, recoveryPointID)

	rec := doServerlessOp(t, h, "CreateSnapshot", map[string]any{
		"snapshotName": "cs-snap", "namespaceName": "cs-ns",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doServerlessOp(t, h, "RestoreTableFromSnapshot", map[string]any{
		"namespaceName":                   "cs-ns",
		"workgroupName":                   "cs-wg",
		"newTableName":                    "new_tbl",
		"snapshotName":                    "cs-snap",
		"sourceDatabaseName":              "srcdb",
		"sourceTableName":                 "srctbl",
		"activateCaseSensitiveIdentifier": true,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doServerlessOp(t, h, "RestoreTableFromRecoveryPoint", map[string]any{
		"namespaceName":                   "cs-ns",
		"workgroupName":                   "cs-wg",
		"newTableName":                    "new_tbl2",
		"recoveryPointId":                 recoveryPointID,
		"sourceDatabaseName":              "srcdb",
		"sourceTableName":                 "srctbl2",
		"activateCaseSensitiveIdentifier": true,
	})
	require.Equal(t, http.StatusOK, rec.Code)
}

// TestServerless_ListSnapshots_Filters proves NamespaceArn actually narrows a
// multi-item result set, not just that the field parses.
func TestServerless_ListSnapshots_Filters(t *testing.T) {
	t.Parallel()

	h := newServerlessHandler()
	doServerlessOp(t, h, "CreateNamespace", map[string]any{"namespaceName": "lsf-ns-a"})
	doServerlessOp(t, h, "CreateNamespace", map[string]any{"namespaceName": "lsf-ns-b"})
	doServerlessOp(t, h, "CreateSnapshot", map[string]any{"snapshotName": "lsf-snap-a1", "namespaceName": "lsf-ns-a"})
	doServerlessOp(t, h, "CreateSnapshot", map[string]any{"snapshotName": "lsf-snap-a2", "namespaceName": "lsf-ns-a"})
	doServerlessOp(t, h, "CreateSnapshot", map[string]any{"snapshotName": "lsf-snap-b1", "namespaceName": "lsf-ns-b"})

	rec := doServerlessOp(t, h, "GetNamespace", map[string]any{"namespaceName": "lsf-ns-a"})
	require.Equal(t, http.StatusOK, rec.Code)

	var nsResp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&nsResp))
	ns, _ := nsResp["namespace"].(map[string]any)
	require.NotNil(t, ns)
	nsArn, _ := ns["namespaceArn"].(string)
	require.NotEmpty(t, nsArn)

	rec = doServerlessOp(t, h, "ListSnapshots", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var allResp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&allResp))
	allSnaps, _ := allResp["snapshots"].([]any)
	require.Len(t, allSnaps, 3, "sanity: three snapshots exist before filtering")

	rec = doServerlessOp(t, h, "ListSnapshots", map[string]any{"namespaceArn": nsArn})
	require.Equal(t, http.StatusOK, rec.Code)

	var filteredResp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&filteredResp))
	filtered, _ := filteredResp["snapshots"].([]any)
	assert.Len(t, filtered, 2, "namespaceArn filter must narrow to only lsf-ns-a's snapshots")

	for _, s := range filtered {
		snap, _ := s.(map[string]any)
		assert.Equal(t, "lsf-ns-a", snap["namespaceName"])
	}

	rec = doServerlessOp(t, h, "ListSnapshots", map[string]any{"ownerAccount": "999999999999"})
	require.Equal(t, http.StatusOK, rec.Code)

	var otherAcctResp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&otherAcctResp))
	otherAcctSnaps, _ := otherAcctResp["snapshots"].([]any)
	assert.Empty(t, otherAcctSnaps, "no snapshot in a single-account backend can be owned by another account")

	rec = doServerlessOp(t, h, "ListSnapshots", map[string]any{"ownerAccount": "000000000000"})
	require.Equal(t, http.StatusOK, rec.Code)

	var ownAcctResp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&ownAcctResp))
	ownAcctSnaps, _ := ownAcctResp["snapshots"].([]any)
	assert.Len(t, ownAcctSnaps, 3)
}

// TestServerless_GetSnapshot_OwnerAccount proves OwnerAccount is compared
// honestly against this backend's single emulated account.
func TestServerless_GetSnapshot_OwnerAccount(t *testing.T) {
	t.Parallel()

	h := newServerlessHandler()
	doServerlessOp(t, h, "CreateNamespace", map[string]any{"namespaceName": "gso-ns"})
	rec := doServerlessOp(t, h, "CreateSnapshot", map[string]any{"snapshotName": "gso-snap", "namespaceName": "gso-ns"})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doServerlessOp(t, h, "GetSnapshot", map[string]any{
		"snapshotName": "gso-snap", "ownerAccount": "000000000000",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doServerlessOp(t, h, "GetSnapshot", map[string]any{
		"snapshotName": "gso-snap", "ownerAccount": "999999999999",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var errResp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&errResp))
	assert.Equal(t, "ResourceNotFoundException", errResp["__type"])
}

// TestServerless_ListWorkgroups_OwnerAccount proves the OwnerAccount filter
// narrows a multi-item result set to zero for any account other than this
// backend's own.
func TestServerless_ListWorkgroups_OwnerAccount(t *testing.T) {
	t.Parallel()

	h := newServerlessHandler()
	doServerlessOp(t, h, "CreateNamespace", map[string]any{"namespaceName": "lwo-ns"})
	doServerlessOp(t, h, "CreateWorkgroup", map[string]any{"workgroupName": "lwo-wg-1", "namespaceName": "lwo-ns"})
	doServerlessOp(t, h, "CreateWorkgroup", map[string]any{"workgroupName": "lwo-wg-2", "namespaceName": "lwo-ns"})

	rec := doServerlessOp(t, h, "ListWorkgroups", map[string]any{"ownerAccount": "000000000000"})
	require.Equal(t, http.StatusOK, rec.Code)

	var ownResp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&ownResp))
	own, _ := ownResp["workgroups"].([]any)
	require.Len(t, own, 2, "sanity: two workgroups exist and match this backend's own account")

	rec = doServerlessOp(t, h, "ListWorkgroups", map[string]any{"ownerAccount": "999999999999"})
	require.Equal(t, http.StatusOK, rec.Code)

	var otherResp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&otherResp))
	other, _ := otherResp["workgroups"].([]any)
	assert.Empty(t, other, "ownerAccount filter must narrow to zero for an account this backend never owns")
}

// TestServerless_ListUsageLimits_UsageType proves UsageType narrows a
// multi-item result set.
func TestServerless_ListUsageLimits_UsageType(t *testing.T) {
	t.Parallel()

	h := newServerlessHandler()
	doServerlessOp(t, h, "CreateNamespace", map[string]any{"namespaceName": "lut-ns"})
	doServerlessOp(t, h, "CreateWorkgroup", map[string]any{"workgroupName": "lut-wg", "namespaceName": "lut-ns"})

	resourceArn := "arn:aws:redshift-serverless:us-east-1:000000000000:workgroup/lut-wg"
	rec := doServerlessOp(t, h, "CreateUsageLimit", map[string]any{
		"resourceArn": resourceArn, "usageType": "serverless-compute", "amount": 100, "breachAction": "log",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	rec = doServerlessOp(t, h, "CreateUsageLimit", map[string]any{
		"resourceArn": resourceArn, "usageType": "cross-region-datasharing", "amount": 50, "breachAction": "log",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doServerlessOp(t, h, "ListUsageLimits", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var allResp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&allResp))
	all, _ := allResp["usageLimits"].([]any)
	require.Len(t, all, 2, "sanity: two usage limits exist before filtering")

	rec = doServerlessOp(t, h, "ListUsageLimits", map[string]any{"usageType": "serverless-compute"})
	require.Equal(t, http.StatusOK, rec.Code)

	var filteredResp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&filteredResp))
	filtered, _ := filteredResp["usageLimits"].([]any)
	require.Len(t, filtered, 1, "usageType filter must narrow to the single matching limit")
	got, _ := filtered[0].(map[string]any)
	assert.Equal(t, "serverless-compute", got["usageType"])
}

// TestServerless_ListRecoveryPoints_TimeRange proves StartTime/EndTime narrow
// a multi-item result set, exercised at the backend layer with seeded
// timestamps rather than real elapsed time (no sleeps: this backend has no
// wire-reachable way to control a recovery point's creation time).
func TestServerless_ListRecoveryPoints_TimeRange(t *testing.T) {
	t.Parallel()

	b := redshift.NewInMemoryBackend("000000000000", "us-east-1")

	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	b.AddRecoveryPointInternal(&redshift.RecoveryPoint{
		RecoveryPointID:         "rp-early",
		NamespaceName:           "trr-ns",
		RecoveryPointCreateTime: base,
	})
	b.AddRecoveryPointInternal(&redshift.RecoveryPoint{
		RecoveryPointID:         "rp-mid",
		NamespaceName:           "trr-ns",
		RecoveryPointCreateTime: base.Add(time.Hour),
	})
	b.AddRecoveryPointInternal(&redshift.RecoveryPoint{
		RecoveryPointID:         "rp-late",
		NamespaceName:           "trr-ns",
		RecoveryPointCreateTime: base.Add(2 * time.Hour),
	})

	all, _ := b.ListRecoveryPointsSL(redshift.ListRecoveryPointsParams{})
	require.Len(t, all, 3, "sanity: three recovery points exist before filtering")

	windowed, _ := b.ListRecoveryPointsSL(redshift.ListRecoveryPointsParams{
		StartTime: base.Add(30 * time.Minute),
		EndTime:   base.Add(90 * time.Minute),
	})
	require.Len(t, windowed, 1, "start/end window must narrow to only rp-mid")
	assert.Equal(t, "rp-mid", windowed[0].RecoveryPointID)
}
