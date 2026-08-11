package redshift_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/redshift"
)

func newServerlessHandler() *redshift.ServerlessHandler {
	return redshift.NewServerlessHandler(redshift.NewInMemoryBackend("000000000000", "us-east-1"))
}

// doServerlessOp drives a request the way a real aws-sdk-go-v2 client for
// redshiftserverless does: POST "/" with an X-Amz-Target: RedshiftServerless.<op>
// header and the operation's parameters (including any resource identifier)
// in the JSON body -- there is no REST path/verb routing on the wire.
func doServerlessOp(
	t *testing.T,
	h *redshift.ServerlessHandler,
	op string,
	body any,
) *httptest.ResponseRecorder {
	t.Helper()

	var bodyReader *bytes.Reader

	if body != nil {
		b, err := json.Marshal(body)
		require.NoError(t, err)
		bodyReader = bytes.NewReader(b)
	} else {
		bodyReader = bytes.NewReader(nil)
	}

	req := httptest.NewRequest(http.MethodPost, "/", bodyReader)
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "RedshiftServerless."+op)

	rec := httptest.NewRecorder()
	e := echo.New()
	c := e.NewContext(req, rec)

	require.NoError(t, h.Handler()(c))

	return rec
}

func TestServerless_RouteMatcher(t *testing.T) {
	t.Parallel()

	h := newServerlessHandler()

	t.Run("matches real X-Amz-Target header", func(t *testing.T) {
		t.Parallel()

		req := httptest.NewRequest(http.MethodPost, "/", nil)
		req.Header.Set("X-Amz-Target", "RedshiftServerless.CreateNamespace")
		e := echo.New()
		c := e.NewContext(req, httptest.NewRecorder())

		assert.True(t, h.RouteMatcher()(c))
	})

	t.Run("does not match the old REST-style path with no header", func(t *testing.T) {
		t.Parallel()

		req := httptest.NewRequest(http.MethodPost, "/redshift-serverless/namespaces", nil)
		e := echo.New()
		c := e.NewContext(req, httptest.NewRecorder())

		assert.False(t, h.RouteMatcher()(c), "a real SDK client never sends this path")
	})

	t.Run("does not match an unrelated target prefix", func(t *testing.T) {
		t.Parallel()

		req := httptest.NewRequest(http.MethodPost, "/", nil)
		req.Header.Set("X-Amz-Target", "RedshiftData.ExecuteStatement")
		e := echo.New()
		c := e.NewContext(req, httptest.NewRecorder())

		assert.False(t, h.RouteMatcher()(c))
	})
}

// TestServerless_NamespaceCRUD covers CreateNamespace, GetNamespace, ListNamespaces,
// UpdateNamespace, DeleteNamespace.
func TestServerless_NamespaceCRUD(t *testing.T) {
	t.Parallel()

	h := newServerlessHandler()

	rec := doServerlessOp(t, h, "CreateNamespace", map[string]any{
		"namespaceName":     "test-namespace",
		"dbName":            "testdb",
		"adminUsername":     "admin",
		"defaultIamRoleArn": "arn:aws:iam::000000000000:role/default",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&createResp))
	nsData, _ := createResp["namespace"].(map[string]any)
	require.NotNil(t, nsData)
	assert.Equal(t, "arn:aws:iam::000000000000:role/default", nsData["defaultIamRoleArn"])

	rec = doServerlessOp(t, h, "ListNamespaces", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doServerlessOp(t, h, "GetNamespace", map[string]any{"namespaceName": "test-namespace"})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doServerlessOp(t, h, "UpdateNamespace", map[string]any{
		"namespaceName": "test-namespace",
		"dbName":        "updateddb",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doServerlessOp(t, h, "DeleteNamespace", map[string]any{"namespaceName": "test-namespace"})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doServerlessOp(t, h, "GetNamespace", map[string]any{"namespaceName": "test-namespace"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var errResp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&errResp))
	assert.Equal(t, "ResourceNotFoundException", errResp["__type"])
}

func TestServerless_DeleteNamespace_WithFinalSnapshot(t *testing.T) {
	t.Parallel()

	h := newServerlessHandler()

	rec := doServerlessOp(t, h, "CreateNamespace", map[string]any{"namespaceName": "fs-ns"})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doServerlessOp(t, h, "DeleteNamespace", map[string]any{
		"namespaceName":                "fs-ns",
		"finalSnapshotName":            "fs-ns-final",
		"finalSnapshotRetentionPeriod": 14,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doServerlessOp(t, h, "GetSnapshot", map[string]any{"snapshotName": "fs-ns-final"})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	snap, _ := resp["snapshot"].(map[string]any)
	require.NotNil(t, snap)
	assert.InEpsilon(t, float64(14), snap["snapshotRetentionPeriod"], 0)
}

// TestServerless_WorkgroupCRUD covers CreateWorkgroup, GetWorkgroup, ListWorkgroups,
// UpdateWorkgroup, DeleteWorkgroup, GetCredentials.
func TestServerless_WorkgroupCRUD(t *testing.T) {
	t.Parallel()

	h := newServerlessHandler()

	rec := doServerlessOp(t, h, "CreateNamespace", map[string]any{"namespaceName": "wg-ns"})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doServerlessOp(t, h, "CreateWorkgroup", map[string]any{
		"workgroupName": "test-workgroup",
		"namespaceName": "wg-ns",
		"maxCapacity":   64,
		"configParameters": []map[string]any{
			{"parameterKey": "max_query_execution_time", "parameterValue": "100"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&createResp))
	wgData, _ := createResp["workgroup"].(map[string]any)
	require.NotNil(t, wgData)
	assert.InEpsilon(t, float64(64), wgData["maxCapacity"], 0)

	rec = doServerlessOp(t, h, "ListWorkgroups", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doServerlessOp(t, h, "GetWorkgroup", map[string]any{"workgroupName": "test-workgroup"})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doServerlessOp(t, h, "GetCredentials", map[string]any{
		"workgroupName":   "test-workgroup",
		"durationSeconds": 1800,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var credResp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&credResp))
	// Expiration is wire-encoded as a JSON number (epoch seconds), not an
	// RFC3339 string -- see the wire-shape audit note in handler_serverless.go.
	_, isNumber := credResp["expiration"].(float64)
	assert.True(t, isNumber, "expiration should decode as a JSON number, got %T", credResp["expiration"])
	assert.Contains(t, credResp, "nextRefreshTime")

	rec = doServerlessOp(t, h, "UpdateWorkgroup", map[string]any{
		"workgroupName": "test-workgroup",
		"baseCapacity":  16,
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doServerlessOp(t, h, "DeleteWorkgroup", map[string]any{"workgroupName": "test-workgroup"})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestServerless_SnapshotCRUD covers CreateServerlessSnapshot, GetServerlessSnapshot,
// ListServerlessSnapshots, DeleteServerlessSnapshot.
func TestServerless_SnapshotCRUD(t *testing.T) {
	t.Parallel()

	h := newServerlessHandler()

	rec := doServerlessOp(t, h, "CreateNamespace", map[string]any{"namespaceName": "snap-ns"})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doServerlessOp(t, h, "CreateSnapshot", map[string]any{
		"snapshotName":    "test-snapshot",
		"namespaceName":   "snap-ns",
		"retentionPeriod": 7,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doServerlessOp(t, h, "ListSnapshots", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doServerlessOp(t, h, "GetSnapshot", map[string]any{"snapshotName": "test-snapshot"})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doServerlessOp(t, h, "DeleteSnapshot", map[string]any{"snapshotName": "test-snapshot"})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestServerless_UsageLimitsCRUD covers CreateUsageLimit, GetUsageLimit, ListUsageLimits,
// UpdateUsageLimit, DeleteUsageLimit.
func TestServerless_UsageLimitsCRUD(t *testing.T) {
	t.Parallel()

	h := newServerlessHandler()

	doServerlessOp(t, h, "CreateNamespace", map[string]any{"namespaceName": "ul-ns"})
	doServerlessOp(t, h, "CreateWorkgroup", map[string]any{"workgroupName": "ul-wg", "namespaceName": "ul-ns"})

	rec := doServerlessOp(t, h, "CreateUsageLimit", map[string]any{
		"resourceArn":  "arn:aws:redshift-serverless:us-east-1:000000000000:workgroup/ul-wg",
		"usageType":    "serverless-compute",
		"amount":       100,
		"breachAction": "log",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&createResp))
	ulData, _ := createResp["usageLimit"].(map[string]any)
	ulID := ""

	if ulData != nil {
		ulID, _ = ulData["usageLimitId"].(string)
	}

	rec = doServerlessOp(t, h, "ListUsageLimits", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	require.NotEmpty(t, ulID)

	rec = doServerlessOp(t, h, "GetUsageLimit", map[string]any{"usageLimitId": ulID})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doServerlessOp(t, h, "UpdateUsageLimit", map[string]any{"usageLimitId": ulID, "amount": 200})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doServerlessOp(t, h, "DeleteUsageLimit", map[string]any{"usageLimitId": ulID})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestServerless_ScheduledActionCRUD covers CreateScheduledAction, GetScheduledAction,
// ListScheduledActions, UpdateScheduledAction, DeleteScheduledAction, including the
// wire-shape fixes: schedule/targetAction are nested objects (not strings), and
// startTime/endTime/state use the real epoch-seconds/"state" wire shape.
func TestServerless_ScheduledActionCRUD(t *testing.T) {
	t.Parallel()

	h := newServerlessHandler()

	rec := doServerlessOp(t, h, "CreateNamespace", map[string]any{"namespaceName": "sa-ns"})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doServerlessOp(t, h, "CreateScheduledAction", map[string]any{
		"scheduledActionName": "test-action",
		"namespaceName":       "sa-ns",
		"roleArn":             "arn:aws:iam::000000000000:role/scheduler",
		"schedule":            map[string]any{"cron": "0 10 ? * MON *"},
		"targetAction": map[string]any{
			"createSnapshot": map[string]any{"namespaceName": "sa-ns", "snapshotName": "scheduled-snap"},
		},
		"enabled": false,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&createResp))
	saData, _ := createResp["scheduledAction"].(map[string]any)
	require.NotNil(t, saData)
	assert.Equal(t, "DISABLED", saData["state"], "state (not status) is the real wire key")
	assert.NotContains(t, saData, "status", "status is not a real ScheduledActionResponse field")

	sched, _ := saData["schedule"].(map[string]any)
	require.NotNil(t, sched, "schedule must be a nested object, not a string")
	assert.Equal(t, "0 10 ? * MON *", sched["cron"])

	target, _ := saData["targetAction"].(map[string]any)
	require.NotNil(t, target, "targetAction must be a nested object, not a string")
	assert.Contains(t, target, "createSnapshot")

	assert.NotEmpty(t, saData["scheduledActionUuid"])

	rec = doServerlessOp(t, h, "GetScheduledAction", map[string]any{"scheduledActionName": "test-action"})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doServerlessOp(t, h, "ListScheduledActions", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doServerlessOp(t, h, "UpdateScheduledAction", map[string]any{
		"scheduledActionName": "test-action",
		"enabled":             true,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var updateResp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&updateResp))
	updated, _ := updateResp["scheduledAction"].(map[string]any)
	require.NotNil(t, updated)
	assert.Equal(t, "ACTIVE", updated["state"])

	rec = doServerlessOp(t, h, "DeleteScheduledAction", map[string]any{"scheduledActionName": "test-action"})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestServerless_CreateScheduledAction_RequiresRoleArn(t *testing.T) {
	t.Parallel()

	h := newServerlessHandler()

	doServerlessOp(t, h, "CreateNamespace", map[string]any{"namespaceName": "sa-ns2"})

	rec := doServerlessOp(t, h, "CreateScheduledAction", map[string]any{
		"scheduledActionName": "no-role",
		"namespaceName":       "sa-ns2",
		"schedule":            map[string]any{"cron": "0 10 ? * MON *"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code, "roleArn is required by the real CreateScheduledActionInput")
}

// TestServerless_ModifyUsageLimit covers ModifyUsageLimit in backend_refinement3.go
// via the HTTP handler. This is classic (query-protocol) Redshift, not Serverless.
func TestServerless_ModifyUsageLimit(t *testing.T) {
	t.Parallel()

	h := newRedshiftHandler()

	rec := postRedshiftForm(t, h, "Action=CreateCluster&Version=2012-12-01"+
		"&ClusterIdentifier=mul-cluster&NodeType=dc2.large&DBName=db&MasterUsername=admin")
	require.Equal(t, http.StatusOK, rec.Code)

	rec = postRedshiftForm(t, h, "Action=CreateUsageLimit&Version=2012-12-01"+
		"&ClusterIdentifier=mul-cluster&FeatureType=spectrum&LimitType=time&Amount=100&BreachAction=log")
	require.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	ulID := extractXMLVal(body, "UsageLimitId")
	require.NotEmpty(t, ulID)

	rec = postRedshiftForm(t, h, "Action=ModifyUsageLimit&Version=2012-12-01"+
		"&UsageLimitId="+ulID+"&Amount=200&BreachAction=emit-metric")
	assert.Equal(t, http.StatusOK, rec.Code)

	_ = strings.TrimSpace("") // suppress unused import
}

// extractXMLVal is a simple XML value extractor for testing.
func extractXMLVal(body, tag string) string {
	open := "<" + tag + ">"
	closeTag := "</" + tag + ">"

	start := strings.Index(body, open)
	if start < 0 {
		return ""
	}

	start += len(open)
	end := strings.Index(body[start:], closeTag)

	if end < 0 {
		return ""
	}

	return body[start : start+end]
}
