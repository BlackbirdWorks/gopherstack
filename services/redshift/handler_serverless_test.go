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

func doServerlessRequest(
	t *testing.T,
	h *redshift.ServerlessHandler,
	method, path string,
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

	req := httptest.NewRequest(method, path, bodyReader)

	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	rec := httptest.NewRecorder()
	e := echo.New()
	c := e.NewContext(req, rec)

	require.NoError(t, h.Handler()(c))

	return rec
}

// TestServerless_NamespaceCRUD covers CreateNamespace, GetNamespace, ListNamespaces,
// UpdateNamespace, DeleteNamespace.
func TestServerless_NamespaceCRUD(t *testing.T) {
	t.Parallel()

	h := newServerlessHandler()

	// CreateNamespace.
	rec := doServerlessRequest(t, h, http.MethodPost, "/redshift-serverless/namespaces",
		map[string]any{
			"namespaceName": "test-namespace",
			"dbName":        "testdb",
			"adminUsername": "admin",
		})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&createResp))
	nsData, _ := createResp["namespace"].(map[string]any)
	require.NotNil(t, nsData)

	// ListNamespaces.
	rec = doServerlessRequest(t, h, http.MethodGet, "/redshift-serverless/namespaces", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// GetNamespace.
	rec = doServerlessRequest(t, h, http.MethodGet, "/redshift-serverless/namespaces/test-namespace", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// UpdateNamespace.
	rec = doServerlessRequest(t, h, http.MethodPatch, "/redshift-serverless/namespaces/test-namespace",
		map[string]any{"dbName": "updateddb"})
	assert.Equal(t, http.StatusOK, rec.Code)

	// DeleteNamespace.
	rec = doServerlessRequest(t, h, http.MethodDelete, "/redshift-serverless/namespaces/test-namespace", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// GetNamespace not found.
	rec = doServerlessRequest(t, h, http.MethodGet, "/redshift-serverless/namespaces/test-namespace", nil)
	assert.NotEqual(t, http.StatusOK, rec.Code)
}

// TestServerless_WorkgroupCRUD covers CreateWorkgroup, GetWorkgroup, ListWorkgroups,
// UpdateWorkgroup, DeleteWorkgroup, GetCredentials.
func TestServerless_WorkgroupCRUD(t *testing.T) {
	t.Parallel()

	h := newServerlessHandler()

	// CreateNamespace first (workgroup needs one).
	rec := doServerlessRequest(t, h, http.MethodPost, "/redshift-serverless/namespaces",
		map[string]any{"namespaceName": "wg-ns"})
	require.Equal(t, http.StatusOK, rec.Code)

	// CreateWorkgroup.
	rec = doServerlessRequest(t, h, http.MethodPost, "/redshift-serverless/workgroups",
		map[string]any{
			"workgroupName": "test-workgroup",
			"namespaceName": "wg-ns",
		})
	require.Equal(t, http.StatusOK, rec.Code)

	// ListWorkgroups.
	rec = doServerlessRequest(t, h, http.MethodGet, "/redshift-serverless/workgroups", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// GetWorkgroup.
	rec = doServerlessRequest(t, h, http.MethodGet, "/redshift-serverless/workgroups/test-workgroup", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// GetCredentials.
	rec = doServerlessRequest(t, h, http.MethodGet, "/redshift-serverless/workgroups/test-workgroup/credentials", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// UpdateWorkgroup.
	rec = doServerlessRequest(t, h, http.MethodPatch, "/redshift-serverless/workgroups/test-workgroup",
		map[string]any{"baseCapacity": 16})
	assert.Equal(t, http.StatusOK, rec.Code)

	// DeleteWorkgroup.
	rec = doServerlessRequest(t, h, http.MethodDelete, "/redshift-serverless/workgroups/test-workgroup", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestServerless_SnapshotCRUD covers CreateServerlessSnapshot, GetServerlessSnapshot,
// ListServerlessSnapshots, DeleteServerlessSnapshot.
func TestServerless_SnapshotCRUD(t *testing.T) {
	t.Parallel()

	h := newServerlessHandler()

	// CreateNamespace.
	rec := doServerlessRequest(t, h, http.MethodPost, "/redshift-serverless/namespaces",
		map[string]any{"namespaceName": "snap-ns"})
	require.Equal(t, http.StatusOK, rec.Code)

	// CreateSnapshot.
	rec = doServerlessRequest(t, h, http.MethodPost, "/redshift-serverless/snapshots",
		map[string]any{
			"snapshotName":  "test-snapshot",
			"namespaceName": "snap-ns",
		})
	require.Equal(t, http.StatusOK, rec.Code)

	// ListSnapshots.
	rec = doServerlessRequest(t, h, http.MethodGet, "/redshift-serverless/snapshots", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// GetSnapshot.
	rec = doServerlessRequest(t, h, http.MethodGet, "/redshift-serverless/snapshots/test-snapshot", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// DeleteSnapshot.
	rec = doServerlessRequest(t, h, http.MethodDelete, "/redshift-serverless/snapshots/test-snapshot", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestServerless_UsageLimitsCRUD covers CreateUsageLimit, GetUsageLimit, ListUsageLimits,
// UpdateUsageLimit, DeleteUsageLimit.
func TestServerless_UsageLimitsCRUD(t *testing.T) {
	t.Parallel()

	h := newServerlessHandler()

	// CreateNamespace and workgroup for usage limits.
	doServerlessRequest(t, h, http.MethodPost, "/redshift-serverless/namespaces",
		map[string]any{"namespaceName": "ul-ns"})
	doServerlessRequest(t, h, http.MethodPost, "/redshift-serverless/workgroups",
		map[string]any{"workgroupName": "ul-wg", "namespaceName": "ul-ns"})

	// CreateUsageLimit.
	rec := doServerlessRequest(t, h, http.MethodPost, "/redshift-serverless/usagelimits",
		map[string]any{
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

	// ListUsageLimits.
	rec = doServerlessRequest(t, h, http.MethodGet, "/redshift-serverless/usagelimits", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	if ulID != "" {
		// GetUsageLimit.
		rec = doServerlessRequest(t, h, http.MethodGet, "/redshift-serverless/usagelimits/"+ulID, nil)
		assert.Equal(t, http.StatusOK, rec.Code)

		// UpdateUsageLimit.
		rec = doServerlessRequest(t, h, http.MethodPatch, "/redshift-serverless/usagelimits/"+ulID,
			map[string]any{"amount": 200})
		assert.Equal(t, http.StatusOK, rec.Code)

		// DeleteUsageLimit.
		rec = doServerlessRequest(t, h, http.MethodDelete, "/redshift-serverless/usagelimits/"+ulID, nil)
		assert.Equal(t, http.StatusOK, rec.Code)
	}
}

// TestServerless_ModifyUsageLimit covers ModifyUsageLimit in backend_refinement3.go
// via the HTTP handler.
func TestServerless_ModifyUsageLimit(t *testing.T) {
	t.Parallel()

	h := newRedshiftHandler()

	// Create a cluster via handler.
	rec := postRedshiftForm(t, h, "Action=CreateCluster&Version=2012-12-01"+
		"&ClusterIdentifier=mul-cluster&NodeType=dc2.large&DBName=db&MasterUsername=admin")
	require.Equal(t, http.StatusOK, rec.Code)

	// Create usage limit.
	rec = postRedshiftForm(t, h, "Action=CreateUsageLimit&Version=2012-12-01"+
		"&ClusterIdentifier=mul-cluster&FeatureType=spectrum&LimitType=time&Amount=100&BreachAction=log")
	require.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	ulID := extractXMLVal(body, "UsageLimitId")
	require.NotEmpty(t, ulID)

	// ModifyUsageLimit.
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
