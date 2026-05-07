package dax_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/dax"
)

// newTestHandler creates a handler backed by a fresh in-memory backend.
func newTestHandler() *dax.Handler {
	return dax.NewHandler(dax.NewInMemoryBackend("123456789012", "us-east-1"))
}

// daxRequest sends a JSON request to the handler with the given target and body.
func daxRequest(t *testing.T, h *dax.Handler, target string, body any) *httptest.ResponseRecorder {
	t.Helper()

	var buf bytes.Buffer

	if body != nil {
		require.NoError(t, json.NewEncoder(&buf).Encode(body))
	}

	req := httptest.NewRequest(http.MethodPost, "/", &buf)
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "AmazonDAXV3."+target)

	rec := httptest.NewRecorder()

	e := echo.New()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

// ---- CreateCluster ----

func TestHandlerCreateCluster(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	rec := daxRequest(t, h, "CreateCluster", map[string]any{
		"ClusterName":       "test-cluster",
		"NodeType":          "dax.r5.large",
		"IamRoleArn":        "arn:aws:iam::123456789012:role/DAXRole",
		"ReplicationFactor": 1,
	})

	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	clusterAny := resp["Cluster"]
	require.NotNil(t, clusterAny)

	cluster := clusterAny.(map[string]any)
	assert.Equal(t, "test-cluster", cluster["ClusterName"])
	assert.Equal(t, "available", cluster["ClusterStatus"])
}

func TestHandlerCreateCluster_Duplicate(t *testing.T) {
	t.Parallel()
	h := newTestHandler()

	body := map[string]any{
		"ClusterName":       "dup-cluster",
		"NodeType":          "dax.r5.large",
		"IamRoleArn":        "arn:aws:iam::123456789012:role/DAXRole",
		"ReplicationFactor": 1,
	}

	rec := daxRequest(t, h, "CreateCluster", body)
	assert.Equal(t, http.StatusOK, rec.Code)

	rec2 := daxRequest(t, h, "CreateCluster", body)
	assert.Equal(t, http.StatusBadRequest, rec2.Code)
}

// ---- DescribeClusters ----

func TestHandlerDescribeClusters_Empty(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	rec := daxRequest(t, h, "DescribeClusters", map[string]any{})

	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	clusters := resp["Clusters"].([]any)
	assert.Empty(t, clusters)
}

func TestHandlerDescribeClusters_AfterCreate(t *testing.T) {
	t.Parallel()
	h := newTestHandler()

	daxRequest(t, h, "CreateCluster", map[string]any{
		"ClusterName":       "c1",
		"NodeType":          "dax.r5.large",
		"IamRoleArn":        "arn:aws:iam::123456789012:role/DAXRole",
		"ReplicationFactor": 1,
	})

	rec := daxRequest(t, h, "DescribeClusters", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	clusters := resp["Clusters"].([]any)
	assert.Len(t, clusters, 1)
}

// ---- UpdateCluster ----

func TestHandlerUpdateCluster(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	daxRequest(t, h, "CreateCluster", map[string]any{
		"ClusterName":       "upd-cluster",
		"NodeType":          "dax.r5.large",
		"IamRoleArn":        "arn:aws:iam::123456789012:role/DAXRole",
		"ReplicationFactor": 1,
	})

	rec := daxRequest(t, h, "UpdateCluster", map[string]any{
		"ClusterName": "upd-cluster",
		"Description": "Updated",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	cluster := resp["Cluster"].(map[string]any)
	assert.Equal(t, "Updated", cluster["Description"])
}

// ---- DeleteCluster ----

func TestHandlerDeleteCluster(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	daxRequest(t, h, "CreateCluster", map[string]any{
		"ClusterName":       "del-cluster",
		"NodeType":          "dax.r5.large",
		"IamRoleArn":        "arn:aws:iam::123456789012:role/DAXRole",
		"ReplicationFactor": 1,
	})

	rec := daxRequest(t, h, "DeleteCluster", map[string]any{
		"ClusterName": "del-cluster",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandlerDeleteCluster_NotFound(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	rec := daxRequest(t, h, "DeleteCluster", map[string]any{
		"ClusterName": "no-such",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ---- Tags ----

func TestHandlerTagResource_ListTags(t *testing.T) {
	t.Parallel()
	h := newTestHandler()

	// First, create a cluster to get the ARN.
	createRec := daxRequest(t, h, "CreateCluster", map[string]any{
		"ClusterName":       "tagged-cluster",
		"NodeType":          "dax.r5.large",
		"IamRoleArn":        "arn:aws:iam::123456789012:role/DAXRole",
		"ReplicationFactor": 1,
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	clusterArn := createResp["Cluster"].(map[string]any)["ClusterArn"].(string)

	// Tag it.
	tagRec := daxRequest(t, h, "TagResource", map[string]any{
		"ResourceName": clusterArn,
		"Tags":         []map[string]string{{"Key": "env", "Value": "prod"}},
	})
	assert.Equal(t, http.StatusOK, tagRec.Code)

	// List tags.
	listRec := daxRequest(t, h, "ListTags", map[string]any{
		"ResourceName": clusterArn,
	})
	assert.Equal(t, http.StatusOK, listRec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
	tags := listResp["Tags"].([]any)
	assert.NotEmpty(t, tags)
}

// ---- Parameter Groups ----

func TestHandlerCreateParameterGroup(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	rec := daxRequest(t, h, "CreateParameterGroup", map[string]any{
		"ParameterGroupName": "my-pg",
		"Description":        "My parameter group",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	pg := resp["ParameterGroup"].(map[string]any)
	assert.Equal(t, "my-pg", pg["ParameterGroupName"])
}

func TestHandlerDescribeParameterGroups(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	rec := daxRequest(t, h, "DescribeParameterGroups", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	groups := resp["ParameterGroups"].([]any)
	assert.NotEmpty(t, groups)
}

func TestHandlerDeleteParameterGroup(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	daxRequest(t, h, "CreateParameterGroup", map[string]any{
		"ParameterGroupName": "pg-del",
	})

	rec := daxRequest(t, h, "DeleteParameterGroup", map[string]any{
		"ParameterGroupName": "pg-del",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// ---- Subnet Groups ----

func TestHandlerCreateSubnetGroup(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	rec := daxRequest(t, h, "CreateSubnetGroup", map[string]any{
		"SubnetGroupName": "my-sg",
		"Description":     "My subnet group",
		"SubnetIds":       []string{"subnet-abc123"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	sg := resp["SubnetGroup"].(map[string]any)
	assert.Equal(t, "my-sg", sg["SubnetGroupName"])
}

func TestHandlerDescribeSubnetGroups(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	rec := daxRequest(t, h, "DescribeSubnetGroups", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	groups := resp["SubnetGroups"].([]any)
	assert.NotEmpty(t, groups)
}

func TestHandlerDeleteSubnetGroup(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	daxRequest(t, h, "CreateSubnetGroup", map[string]any{
		"SubnetGroupName": "sg-del",
		"SubnetIds":       []string{"subnet-1"},
	})

	rec := daxRequest(t, h, "DeleteSubnetGroup", map[string]any{
		"SubnetGroupName": "sg-del",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// ---- Unknown action ----

func TestHandlerUnknownAction(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	rec := daxRequest(t, h, "UnknownAction", nil)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ---- Reset ----

func TestHandlerReset(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	daxRequest(t, h, "CreateCluster", map[string]any{
		"ClusterName":       "r-cluster",
		"NodeType":          "dax.r5.large",
		"IamRoleArn":        "arn:aws:iam::123456789012:role/DAXRole",
		"ReplicationFactor": 1,
	})

	h.Reset()

	rec := daxRequest(t, h, "DescribeClusters", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	clusters := resp["Clusters"].([]any)
	assert.Empty(t, clusters)
}

// ---- GetSupportedOperations ----

func TestHandlerGetSupportedOperations(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	ops := h.GetSupportedOperations()
	assert.Contains(t, ops, "CreateCluster")
	assert.Contains(t, ops, "DescribeClusters")
	assert.Contains(t, ops, "UpdateCluster")
	assert.Contains(t, ops, "DeleteCluster")
	assert.Contains(t, ops, "TagResource")
	assert.Contains(t, ops, "ListTags")
	assert.Contains(t, ops, "CreateParameterGroup")
	assert.Contains(t, ops, "CreateSubnetGroup")
}
