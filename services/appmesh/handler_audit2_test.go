package appmesh_test

import (
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestAppMesh_Batch2Accuracy covers AWS-accuracy gaps found in the batch-2 audit:
//
//  1. metadata.arn format: "arn:aws:appmesh:{region}:{account}:mesh/{name}"
//  2. metadata timestamps: epoch seconds (float64), not strings or millis
//  3. metadata.version: starts at 1, increments on update
//  4. spec field: {} not null when created without spec
//  5. status: {"status":"ACTIVE"} object, not bare string
//  6. list responses: [] not null for empty collections
//  7. lastUpdatedAt advances after update; createdAt unchanged
//  8. Route ARN: "virtualRouter/{vr}/route/{r}" path segments
//  9. GatewayRoute ARN: "virtualGateway/{vg}/gatewayRoute/{r}" path segments
// 10. creation-time tags appear in ListTagsForResource
// 11. TagResource on unknown ARN → NotFoundException (404)
// 12. UntagResource on unknown ARN → NotFoundException (404)

// TestAppMesh_Batch2ARNFormat verifies all resource ARN shapes.
func TestAppMesh_Batch2ARNFormat(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	doRequest(t, h, http.MethodPut, "/meshes", map[string]any{"meshName": "arns"})
	doRequest(t, h, http.MethodPut, "/meshes/arns/virtualNodes",
		map[string]any{"virtualNodeName": "vn1"})
	doRequest(t, h, http.MethodPut, "/meshes/arns/virtualRouters",
		map[string]any{"virtualRouterName": "vr1"})
	doRequest(t, h, http.MethodPut, "/meshes/arns/virtualRouter/vr1/routes",
		map[string]any{"routeName": "rt1"})
	doRequest(t, h, http.MethodPut, "/meshes/arns/virtualServices",
		map[string]any{"virtualServiceName": "svc1"})
	doRequest(t, h, http.MethodPut, "/meshes/arns/virtualGateways",
		map[string]any{"virtualGatewayName": "gw1"})
	doRequest(t, h, http.MethodPut, "/meshes/arns/virtualGateway/gw1/gatewayRoutes",
		map[string]any{"gatewayRouteName": "gr1"})

	base := "arn:aws:appmesh:us-east-1:000000000000:mesh/arns"

	checks := []struct {
		method  string
		path    string
		bodyKey string
		wantARN string
	}{
		{http.MethodGet, "/meshes/arns", "mesh", base},
		{http.MethodGet, "/meshes/arns/virtualNodes/vn1", "virtualNode", base + "/virtualNode/vn1"},
		{http.MethodGet, "/meshes/arns/virtualRouters/vr1", "virtualRouter", base + "/virtualRouter/vr1"},
		{
			http.MethodGet, "/meshes/arns/virtualRouter/vr1/routes/rt1", "route",
			base + "/virtualRouter/vr1/route/rt1",
		},
		{
			http.MethodGet, "/meshes/arns/virtualServices/svc1", "virtualService",
			base + "/virtualService/svc1",
		},
		{http.MethodGet, "/meshes/arns/virtualGateways/gw1", "virtualGateway", base + "/virtualGateway/gw1"},
		{
			http.MethodGet, "/meshes/arns/virtualGateway/gw1/gatewayRoutes/gr1", "gatewayRoute",
			base + "/virtualGateway/gw1/gatewayRoute/gr1",
		},
	}
	for _, c := range checks {
		rec := doRequest(t, h, c.method, c.path, nil)
		require.Equal(t, http.StatusOK, rec.Code, "path: %s", c.path)
		body := getBody(t, rec)
		resource := body[c.bodyKey].(map[string]any)
		arn := resource["metadata"].(map[string]any)["arn"].(string)
		assert.Equal(t, c.wantARN, arn, "ARN mismatch for %s", c.bodyKey)
	}
}

// TestAppMesh_Batch2Timestamps verifies epoch-second timestamps and update semantics.
func TestAppMesh_Batch2Timestamps(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	rec := doRequest(t, h, http.MethodPut, "/meshes", map[string]any{"meshName": "ts-mesh"})
	require.Equal(t, http.StatusOK, rec.Code)
	body := getBody(t, rec)
	meta := body["mesh"].(map[string]any)["metadata"].(map[string]any)

	// Timestamps must be JSON numbers (epoch seconds).
	createdAt1, ok := meta["createdAt"].(float64)
	require.True(t, ok, "createdAt must be JSON number (epoch seconds)")
	assert.Greater(t, createdAt1, float64(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).Unix()))

	lastUpdated1, ok := meta["lastUpdatedAt"].(float64)
	require.True(t, ok, "lastUpdatedAt must be JSON number (epoch seconds)")

	// On creation, createdAt == lastUpdatedAt.
	assert.InDelta(t, createdAt1, lastUpdated1, 1e-9, "createdAt and lastUpdatedAt must be equal on creation")

	// Version starts at 1.
	assert.Equal(t, int64(1), int64(meta["version"].(float64)), "version must start at 1")

	time.Sleep(10 * time.Millisecond)

	rec = doRequest(t, h, http.MethodPut, "/meshes/ts-mesh", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)
	body = getBody(t, rec)
	meta = body["mesh"].(map[string]any)["metadata"].(map[string]any)

	createdAt2 := meta["createdAt"].(float64)
	lastUpdated2 := meta["lastUpdatedAt"].(float64)

	assert.InDelta(t, createdAt1, createdAt2, 1e-9, "createdAt must not change after update")
	assert.GreaterOrEqual(t, lastUpdated2, lastUpdated1, "lastUpdatedAt must advance after update")
	assert.Equal(t, int64(2), int64(meta["version"].(float64)), "version must increment on update")
}

// TestAppMesh_Batch2SpecNotNull verifies spec is {} not null for all resource types.
func TestAppMesh_Batch2SpecNotNull(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	doRequest(t, h, http.MethodPut, "/meshes", map[string]any{"meshName": "m1"})
	doRequest(t, h, http.MethodPut, "/meshes/m1/virtualNodes",
		map[string]any{"virtualNodeName": "vn1"})
	doRequest(t, h, http.MethodPut, "/meshes/m1/virtualRouters",
		map[string]any{"virtualRouterName": "vr1"})
	doRequest(t, h, http.MethodPut, "/meshes/m1/virtualRouter/vr1/routes",
		map[string]any{"routeName": "rt1"})
	doRequest(t, h, http.MethodPut, "/meshes/m1/virtualServices",
		map[string]any{"virtualServiceName": "vs1"})
	doRequest(t, h, http.MethodPut, "/meshes/m1/virtualGateways",
		map[string]any{"virtualGatewayName": "gw1"})
	doRequest(t, h, http.MethodPut, "/meshes/m1/virtualGateway/gw1/gatewayRoutes",
		map[string]any{"gatewayRouteName": "gr1"})

	checks := []struct {
		method  string
		path    string
		bodyKey string
	}{
		{http.MethodGet, "/meshes/m1", "mesh"},
		{http.MethodGet, "/meshes/m1/virtualNodes/vn1", "virtualNode"},
		{http.MethodGet, "/meshes/m1/virtualRouters/vr1", "virtualRouter"},
		{http.MethodGet, "/meshes/m1/virtualRouter/vr1/routes/rt1", "route"},
		{http.MethodGet, "/meshes/m1/virtualServices/vs1", "virtualService"},
		{http.MethodGet, "/meshes/m1/virtualGateways/gw1", "virtualGateway"},
		{http.MethodGet, "/meshes/m1/virtualGateway/gw1/gatewayRoutes/gr1", "gatewayRoute"},
	}
	for _, c := range checks {
		rec := doRequest(t, h, c.method, c.path, nil)
		require.Equal(t, http.StatusOK, rec.Code)
		body := getBody(t, rec)
		resource := body[c.bodyKey].(map[string]any)
		_, ok := resource["spec"].(map[string]any)
		assert.True(t, ok, "%s: spec must be a JSON object {}, not null", c.bodyKey)
	}
}

// TestAppMesh_Batch2StatusObject verifies status is {"status":"ACTIVE"} not a bare string.
func TestAppMesh_Batch2StatusObject(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	doRequest(t, h, http.MethodPut, "/meshes", map[string]any{"meshName": "m1"})
	doRequest(t, h, http.MethodPut, "/meshes/m1/virtualNodes",
		map[string]any{"virtualNodeName": "vn1"})
	doRequest(t, h, http.MethodPut, "/meshes/m1/virtualRouters",
		map[string]any{"virtualRouterName": "vr1"})
	doRequest(t, h, http.MethodPut, "/meshes/m1/virtualRouter/vr1/routes",
		map[string]any{"routeName": "rt1"})
	doRequest(t, h, http.MethodPut, "/meshes/m1/virtualServices",
		map[string]any{"virtualServiceName": "vs1"})
	doRequest(t, h, http.MethodPut, "/meshes/m1/virtualGateways",
		map[string]any{"virtualGatewayName": "gw1"})
	doRequest(t, h, http.MethodPut, "/meshes/m1/virtualGateway/gw1/gatewayRoutes",
		map[string]any{"gatewayRouteName": "gr1"})

	checks := []struct {
		method  string
		path    string
		bodyKey string
	}{
		{http.MethodGet, "/meshes/m1", "mesh"},
		{http.MethodGet, "/meshes/m1/virtualNodes/vn1", "virtualNode"},
		{http.MethodGet, "/meshes/m1/virtualRouters/vr1", "virtualRouter"},
		{http.MethodGet, "/meshes/m1/virtualRouter/vr1/routes/rt1", "route"},
		{http.MethodGet, "/meshes/m1/virtualServices/vs1", "virtualService"},
		{http.MethodGet, "/meshes/m1/virtualGateways/gw1", "virtualGateway"},
		{http.MethodGet, "/meshes/m1/virtualGateway/gw1/gatewayRoutes/gr1", "gatewayRoute"},
	}
	for _, c := range checks {
		rec := doRequest(t, h, c.method, c.path, nil)
		require.Equal(t, http.StatusOK, rec.Code)
		body := getBody(t, rec)
		resource := body[c.bodyKey].(map[string]any)
		status, ok := resource["status"].(map[string]any)
		require.True(t, ok, "%s: status must be a JSON object", c.bodyKey)
		assert.Equal(t, "ACTIVE", status["status"])
	}
}

// TestAppMesh_Batch2ListsReturnArrayNotNull verifies empty list fields are [] not null.
func TestAppMesh_Batch2ListsReturnArrayNotNull(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	doRequest(t, h, http.MethodPut, "/meshes", map[string]any{"meshName": "empty"})
	doRequest(t, h, http.MethodPut, "/meshes/empty/virtualRouters",
		map[string]any{"virtualRouterName": "vr1"})
	doRequest(t, h, http.MethodPut, "/meshes/empty/virtualGateways",
		map[string]any{"virtualGatewayName": "gw1"})

	checks := []struct {
		path    string
		listKey string
	}{
		{"/meshes/empty/virtualNodes", "virtualNodes"},
		{"/meshes/empty/virtualRouters", "virtualRouters"},
		{"/meshes/empty/virtualRouter/vr1/routes", "routes"},
		{"/meshes/empty/virtualServices", "virtualServices"},
		{"/meshes/empty/virtualGateways", "virtualGateways"},
		{"/meshes/empty/virtualGateway/gw1/gatewayRoutes", "gatewayRoutes"},
	}
	for _, c := range checks {
		rec := doRequest(t, h, http.MethodGet, c.path, nil)
		require.Equal(t, http.StatusOK, rec.Code, "path: %s", c.path)
		body := getBody(t, rec)
		_, ok := body[c.listKey].([]any)
		assert.True(t, ok, "%s: %q must be JSON array [], not null", c.path, c.listKey)
	}
}

// TestAppMesh_Batch2TagsCreatedWith verifies creation-time tags appear in list.
func TestAppMesh_Batch2TagsCreatedWith(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	rec := doRequest(t, h, http.MethodPut, "/meshes", map[string]any{
		"meshName": "tagged",
		"tags": []map[string]string{
			{"key": "env", "value": "prod"},
			{"key": "team", "value": "platform"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	arn := getBody(t, rec)["mesh"].(map[string]any)["metadata"].(map[string]any)["arn"].(string)

	// Creation-time tags appear in ListTagsForResource.
	rec = doRequest(t, h, http.MethodGet, fmt.Sprintf("/tags?resourceArn=%s", arn), nil)
	require.Equal(t, http.StatusOK, rec.Code)
	tags := getBody(t, rec)["tags"].([]any)
	assert.Len(t, tags, 2, "creation-time tags must appear in ListTagsForResource")

	// TagResource merges with existing tags, does not replace.
	rec = doRequest(t, h, http.MethodPut, "/tag", map[string]any{
		"resourceArn": arn,
		"tags":        []map[string]string{{"key": "new-key", "value": "new-val"}},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, http.MethodGet, fmt.Sprintf("/tags?resourceArn=%s", arn), nil)
	tags = getBody(t, rec)["tags"].([]any)
	assert.Len(t, tags, 3, "TagResource must merge, not replace existing tags")
}

// TestAppMesh_Batch2TagUnknownARN verifies tag ops on unknown ARN return NotFoundException.
func TestAppMesh_Batch2TagUnknownARN(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	unknownARN := "arn:aws:appmesh:us-east-1:000000000000:mesh/nonexistent"

	// TagResource → 404 NotFoundException
	rec := doRequest(t, h, http.MethodPut, "/tag", map[string]any{
		"resourceArn": unknownARN,
		"tags":        []map[string]string{{"key": "k", "value": "v"}},
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
	body := getBody(t, rec)
	assert.Equal(t, "NotFoundException", body["code"])

	// UntagResource → 404 NotFoundException
	rec = doRequest(t, h, http.MethodPut, "/untag", map[string]any{
		"resourceArn": unknownARN,
		"tagKeys":     []string{"k"},
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
	body = getBody(t, rec)
	assert.Equal(t, "NotFoundException", body["code"])

	// ListTagsForResource → 404 NotFoundException
	rec = doRequest(t, h, http.MethodGet, fmt.Sprintf("/tags?resourceArn=%s", unknownARN), nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
	body = getBody(t, rec)
	assert.Equal(t, "NotFoundException", body["code"])
}
