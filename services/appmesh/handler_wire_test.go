package appmesh_test

// handler_wire_test.go verifies AWS wire-format accuracy for every resource
// type's HTTP response:
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
// 13. single-resource responses put fields at the response root -- there is
//     no "mesh"/"virtualNode"/etc. wrapper key.

import (
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/appmesh"
)

// TestAppMesh_ARNFormat verifies all resource ARN shapes.
func TestAppMesh_ARNFormat(t *testing.T) {
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
		resource := getBody(t, rec)
		arn := resource["metadata"].(map[string]any)["arn"].(string)
		assert.Equal(t, c.wantARN, arn, "ARN mismatch for %s", c.bodyKey)
	}
}

// TestAppMesh_Timestamps verifies epoch-second timestamps and update semantics.
func TestAppMesh_Timestamps(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	rec := doRequest(t, h, http.MethodPut, "/meshes", map[string]any{"meshName": "ts-mesh"})
	require.Equal(t, http.StatusOK, rec.Code)
	body := getBody(t, rec)
	meta := body["metadata"].(map[string]any)

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
	meta = body["metadata"].(map[string]any)

	createdAt2 := meta["createdAt"].(float64)
	lastUpdated2 := meta["lastUpdatedAt"].(float64)

	assert.InDelta(t, createdAt1, createdAt2, 1e-9, "createdAt must not change after update")
	assert.GreaterOrEqual(t, lastUpdated2, lastUpdated1, "lastUpdatedAt must advance after update")
	assert.Equal(t, int64(2), int64(meta["version"].(float64)), "version must increment on update")
}

// TestAppMesh_SpecNotNull verifies spec is {} not null for all resource types.
func TestAppMesh_SpecNotNull(t *testing.T) {
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
		resource := getBody(t, rec)
		_, ok := resource["spec"].(map[string]any)
		assert.True(t, ok, "%s: spec must be a JSON object {}, not null", c.bodyKey)
	}
}

// TestAppMesh_StatusObject verifies status is {"status":"ACTIVE"} not a bare string.
func TestAppMesh_StatusObject(t *testing.T) {
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
		resource := getBody(t, rec)
		status, ok := resource["status"].(map[string]any)
		require.True(t, ok, "%s: status must be a JSON object", c.bodyKey)
		assert.Equal(t, "ACTIVE", status["status"])
	}
}

// TestAppMesh_ListsReturnArrayNotNull verifies empty list fields are [] not null.
func TestAppMesh_ListsReturnArrayNotNull(t *testing.T) {
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

// TestAppMesh_TagsCreatedWith verifies creation-time tags appear in list.
func TestAppMesh_TagsCreatedWith(t *testing.T) {
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
	arn := getBody(t, rec)["metadata"].(map[string]any)["arn"].(string)

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

// TestAppMesh_TagUnknownARN verifies tag ops on unknown ARN return NotFoundException.
func TestAppMesh_TagUnknownARN(t *testing.T) {
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

// TestAppMesh_ResponseFieldsAtTopLevel verifies that every single-resource
// response returns data at the top level (no resource-type wrapper key).
//
// Real AWS App Mesh (restjson1, httpPayload trait) puts the resource fields
// directly at the response root -- there is no "mesh"/"virtualNode"/etc.
// wrapper. Confirmed against aws-sdk-go-v2@v1.38.4's actual invoked
// deserializer: each op's own `awsRestjson1_deserializeOp<Op>.HandleDeserialize`
// (e.g. deserializers.go:244 for CreateMesh) calls
// `awsRestjson1_deserializeDocumentMeshData(&output.Mesh, shape)` directly on
// the raw decoded body -- not the generic `awsRestjson1_deserializeOpDocument
// <Op>Output` helper that reads a `"mesh"` key. That helper function exists
// in the generated file (smithy-go codegen artifact for the httpPayload
// path) but is dead code: nothing calls it. Grepping for it and trusting its
// `case "mesh":` line is the trap -- it looks authoritative but is never
// exercised by a real client. List ops are the one place the wrapper
// function IS live (see ListMeshesOutput, which has two members and no
// single httpPayload field), which is why `listResp` correctly wraps under
// a plural key while these single-resource ops must not.
func TestAppMesh_ResponseFieldsAtTopLevel(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		setup    func(h *appmesh.Handler)
		method   string
		path     string
		body     any
		wrapKey  string // the resource-type key the response must NOT be wrapped under
		topField string // a field expected at the response root
	}{
		// ── Mesh ──────────────────────────────────────────────────────────────────
		{
			name:     "CreateMesh",
			setup:    func(_ *appmesh.Handler) {},
			method:   http.MethodPut,
			path:     "/meshes",
			body:     map[string]any{"meshName": "wrap-test"},
			wrapKey:  "mesh",
			topField: "meshName",
		},
		{
			name: "DescribeMesh",
			setup: func(h *appmesh.Handler) {
				doRequest(t, h, http.MethodPut, "/meshes", map[string]any{"meshName": "wrap-test"})
			},
			method:   http.MethodGet,
			path:     "/meshes/wrap-test",
			wrapKey:  "mesh",
			topField: "meshName",
		},
		{
			name: "UpdateMesh",
			setup: func(h *appmesh.Handler) {
				doRequest(t, h, http.MethodPut, "/meshes", map[string]any{"meshName": "wrap-test"})
			},
			method:   http.MethodPut,
			path:     "/meshes/wrap-test",
			body:     map[string]any{},
			wrapKey:  "mesh",
			topField: "meshName",
		},
		{
			name: "DeleteMesh",
			setup: func(h *appmesh.Handler) {
				doRequest(t, h, http.MethodPut, "/meshes", map[string]any{"meshName": "wrap-test"})
			},
			method:   http.MethodDelete,
			path:     "/meshes/wrap-test",
			wrapKey:  "mesh",
			topField: "meshName",
		},
		// ── VirtualNode ───────────────────────────────────────────────────────────
		{
			name: "CreateVirtualNode",
			setup: func(h *appmesh.Handler) {
				doRequest(t, h, http.MethodPut, "/meshes", map[string]any{"meshName": "m"})
			},
			method:   http.MethodPut,
			path:     "/meshes/m/virtualNodes",
			body:     map[string]any{"virtualNodeName": "vn1"},
			wrapKey:  "virtualNode",
			topField: "virtualNodeName",
		},
		{
			name: "DescribeVirtualNode",
			setup: func(h *appmesh.Handler) {
				doRequest(t, h, http.MethodPut, "/meshes", map[string]any{"meshName": "m"})
				doRequest(t, h, http.MethodPut, "/meshes/m/virtualNodes",
					map[string]any{"virtualNodeName": "vn1"})
			},
			method:   http.MethodGet,
			path:     "/meshes/m/virtualNodes/vn1",
			wrapKey:  "virtualNode",
			topField: "virtualNodeName",
		},
		{
			name: "UpdateVirtualNode",
			setup: func(h *appmesh.Handler) {
				doRequest(t, h, http.MethodPut, "/meshes", map[string]any{"meshName": "m"})
				doRequest(t, h, http.MethodPut, "/meshes/m/virtualNodes",
					map[string]any{"virtualNodeName": "vn1"})
			},
			method:   http.MethodPut,
			path:     "/meshes/m/virtualNodes/vn1",
			body:     map[string]any{"spec": map[string]any{}},
			wrapKey:  "virtualNode",
			topField: "virtualNodeName",
		},
		{
			name: "DeleteVirtualNode",
			setup: func(h *appmesh.Handler) {
				doRequest(t, h, http.MethodPut, "/meshes", map[string]any{"meshName": "m"})
				doRequest(t, h, http.MethodPut, "/meshes/m/virtualNodes",
					map[string]any{"virtualNodeName": "vn1"})
			},
			method:   http.MethodDelete,
			path:     "/meshes/m/virtualNodes/vn1",
			wrapKey:  "virtualNode",
			topField: "virtualNodeName",
		},
		// ── VirtualRouter ─────────────────────────────────────────────────────────
		{
			name: "CreateVirtualRouter",
			setup: func(h *appmesh.Handler) {
				doRequest(t, h, http.MethodPut, "/meshes", map[string]any{"meshName": "m"})
			},
			method:   http.MethodPut,
			path:     "/meshes/m/virtualRouters",
			body:     map[string]any{"virtualRouterName": "vr1"},
			wrapKey:  "virtualRouter",
			topField: "virtualRouterName",
		},
		{
			name: "DescribeVirtualRouter",
			setup: func(h *appmesh.Handler) {
				doRequest(t, h, http.MethodPut, "/meshes", map[string]any{"meshName": "m"})
				doRequest(t, h, http.MethodPut, "/meshes/m/virtualRouters",
					map[string]any{"virtualRouterName": "vr1"})
			},
			method:   http.MethodGet,
			path:     "/meshes/m/virtualRouters/vr1",
			wrapKey:  "virtualRouter",
			topField: "virtualRouterName",
		},
		{
			name: "UpdateVirtualRouter",
			setup: func(h *appmesh.Handler) {
				doRequest(t, h, http.MethodPut, "/meshes", map[string]any{"meshName": "m"})
				doRequest(t, h, http.MethodPut, "/meshes/m/virtualRouters",
					map[string]any{"virtualRouterName": "vr1"})
			},
			method:   http.MethodPut,
			path:     "/meshes/m/virtualRouters/vr1",
			body:     map[string]any{"spec": map[string]any{}},
			wrapKey:  "virtualRouter",
			topField: "virtualRouterName",
		},
		{
			name: "DeleteVirtualRouter",
			setup: func(h *appmesh.Handler) {
				doRequest(t, h, http.MethodPut, "/meshes", map[string]any{"meshName": "m"})
				doRequest(t, h, http.MethodPut, "/meshes/m/virtualRouters",
					map[string]any{"virtualRouterName": "vr1"})
			},
			method:   http.MethodDelete,
			path:     "/meshes/m/virtualRouters/vr1",
			wrapKey:  "virtualRouter",
			topField: "virtualRouterName",
		},
		// ── Route ─────────────────────────────────────────────────────────────────
		{
			name: "CreateRoute",
			setup: func(h *appmesh.Handler) {
				doRequest(t, h, http.MethodPut, "/meshes", map[string]any{"meshName": "m"})
				doRequest(t, h, http.MethodPut, "/meshes/m/virtualRouters",
					map[string]any{"virtualRouterName": "vr1"})
			},
			method:   http.MethodPut,
			path:     "/meshes/m/virtualRouter/vr1/routes",
			body:     map[string]any{"routeName": "rt1", "spec": map[string]any{}},
			wrapKey:  "route",
			topField: "routeName",
		},
		{
			name: "DescribeRoute",
			setup: func(h *appmesh.Handler) {
				doRequest(t, h, http.MethodPut, "/meshes", map[string]any{"meshName": "m"})
				doRequest(t, h, http.MethodPut, "/meshes/m/virtualRouters",
					map[string]any{"virtualRouterName": "vr1"})
				doRequest(t, h, http.MethodPut, "/meshes/m/virtualRouter/vr1/routes",
					map[string]any{"routeName": "rt1", "spec": map[string]any{}})
			},
			method:   http.MethodGet,
			path:     "/meshes/m/virtualRouter/vr1/routes/rt1",
			wrapKey:  "route",
			topField: "routeName",
		},
		{
			name: "UpdateRoute",
			setup: func(h *appmesh.Handler) {
				doRequest(t, h, http.MethodPut, "/meshes", map[string]any{"meshName": "m"})
				doRequest(t, h, http.MethodPut, "/meshes/m/virtualRouters",
					map[string]any{"virtualRouterName": "vr1"})
				doRequest(t, h, http.MethodPut, "/meshes/m/virtualRouter/vr1/routes",
					map[string]any{"routeName": "rt1", "spec": map[string]any{}})
			},
			method:   http.MethodPut,
			path:     "/meshes/m/virtualRouter/vr1/routes/rt1",
			body:     map[string]any{"spec": map[string]any{}},
			wrapKey:  "route",
			topField: "routeName",
		},
		{
			name: "DeleteRoute",
			setup: func(h *appmesh.Handler) {
				doRequest(t, h, http.MethodPut, "/meshes", map[string]any{"meshName": "m"})
				doRequest(t, h, http.MethodPut, "/meshes/m/virtualRouters",
					map[string]any{"virtualRouterName": "vr1"})
				doRequest(t, h, http.MethodPut, "/meshes/m/virtualRouter/vr1/routes",
					map[string]any{"routeName": "rt1"})
			},
			method:   http.MethodDelete,
			path:     "/meshes/m/virtualRouter/vr1/routes/rt1",
			wrapKey:  "route",
			topField: "routeName",
		},
		// ── VirtualService ────────────────────────────────────────────────────────
		{
			name: "CreateVirtualService",
			setup: func(h *appmesh.Handler) {
				doRequest(t, h, http.MethodPut, "/meshes", map[string]any{"meshName": "m"})
			},
			method:   http.MethodPut,
			path:     "/meshes/m/virtualServices",
			body:     map[string]any{"virtualServiceName": "svc.local"},
			wrapKey:  "virtualService",
			topField: "virtualServiceName",
		},
		{
			name: "DescribeVirtualService",
			setup: func(h *appmesh.Handler) {
				doRequest(t, h, http.MethodPut, "/meshes", map[string]any{"meshName": "m"})
				doRequest(t, h, http.MethodPut, "/meshes/m/virtualServices",
					map[string]any{"virtualServiceName": "svc.local"})
			},
			method:   http.MethodGet,
			path:     "/meshes/m/virtualServices/svc.local",
			wrapKey:  "virtualService",
			topField: "virtualServiceName",
		},
		{
			name: "UpdateVirtualService",
			setup: func(h *appmesh.Handler) {
				doRequest(t, h, http.MethodPut, "/meshes", map[string]any{"meshName": "m"})
				doRequest(t, h, http.MethodPut, "/meshes/m/virtualServices",
					map[string]any{"virtualServiceName": "svc.local"})
			},
			method:   http.MethodPut,
			path:     "/meshes/m/virtualServices/svc.local",
			body:     map[string]any{"spec": map[string]any{}},
			wrapKey:  "virtualService",
			topField: "virtualServiceName",
		},
		{
			name: "DeleteVirtualService",
			setup: func(h *appmesh.Handler) {
				doRequest(t, h, http.MethodPut, "/meshes", map[string]any{"meshName": "m"})
				doRequest(t, h, http.MethodPut, "/meshes/m/virtualServices",
					map[string]any{"virtualServiceName": "svc.local"})
			},
			method:   http.MethodDelete,
			path:     "/meshes/m/virtualServices/svc.local",
			wrapKey:  "virtualService",
			topField: "virtualServiceName",
		},
		// ── VirtualGateway ────────────────────────────────────────────────────────
		{
			name: "CreateVirtualGateway",
			setup: func(h *appmesh.Handler) {
				doRequest(t, h, http.MethodPut, "/meshes", map[string]any{"meshName": "m"})
			},
			method:   http.MethodPut,
			path:     "/meshes/m/virtualGateways",
			body:     map[string]any{"virtualGatewayName": "gw1"},
			wrapKey:  "virtualGateway",
			topField: "virtualGatewayName",
		},
		{
			name: "DescribeVirtualGateway",
			setup: func(h *appmesh.Handler) {
				doRequest(t, h, http.MethodPut, "/meshes", map[string]any{"meshName": "m"})
				doRequest(t, h, http.MethodPut, "/meshes/m/virtualGateways",
					map[string]any{"virtualGatewayName": "gw1"})
			},
			method:   http.MethodGet,
			path:     "/meshes/m/virtualGateways/gw1",
			wrapKey:  "virtualGateway",
			topField: "virtualGatewayName",
		},
		{
			name: "UpdateVirtualGateway",
			setup: func(h *appmesh.Handler) {
				doRequest(t, h, http.MethodPut, "/meshes", map[string]any{"meshName": "m"})
				doRequest(t, h, http.MethodPut, "/meshes/m/virtualGateways",
					map[string]any{"virtualGatewayName": "gw1"})
			},
			method:   http.MethodPut,
			path:     "/meshes/m/virtualGateways/gw1",
			body:     map[string]any{"spec": map[string]any{}},
			wrapKey:  "virtualGateway",
			topField: "virtualGatewayName",
		},
		{
			name: "DeleteVirtualGateway",
			setup: func(h *appmesh.Handler) {
				doRequest(t, h, http.MethodPut, "/meshes", map[string]any{"meshName": "m"})
				doRequest(t, h, http.MethodPut, "/meshes/m/virtualGateways",
					map[string]any{"virtualGatewayName": "gw1"})
			},
			method:   http.MethodDelete,
			path:     "/meshes/m/virtualGateways/gw1",
			wrapKey:  "virtualGateway",
			topField: "virtualGatewayName",
		},
		// ── GatewayRoute ──────────────────────────────────────────────────────────
		{
			name: "CreateGatewayRoute",
			setup: func(h *appmesh.Handler) {
				doRequest(t, h, http.MethodPut, "/meshes", map[string]any{"meshName": "m"})
				doRequest(t, h, http.MethodPut, "/meshes/m/virtualGateways",
					map[string]any{"virtualGatewayName": "gw1"})
			},
			method:   http.MethodPut,
			path:     "/meshes/m/virtualGateway/gw1/gatewayRoutes",
			body:     map[string]any{"gatewayRouteName": "gr1", "spec": map[string]any{}},
			wrapKey:  "gatewayRoute",
			topField: "gatewayRouteName",
		},
		{
			name: "DescribeGatewayRoute",
			setup: func(h *appmesh.Handler) {
				doRequest(t, h, http.MethodPut, "/meshes", map[string]any{"meshName": "m"})
				doRequest(t, h, http.MethodPut, "/meshes/m/virtualGateways",
					map[string]any{"virtualGatewayName": "gw1"})
				doRequest(t, h, http.MethodPut, "/meshes/m/virtualGateway/gw1/gatewayRoutes",
					map[string]any{"gatewayRouteName": "gr1", "spec": map[string]any{}})
			},
			method:   http.MethodGet,
			path:     "/meshes/m/virtualGateway/gw1/gatewayRoutes/gr1",
			wrapKey:  "gatewayRoute",
			topField: "gatewayRouteName",
		},
		{
			name: "UpdateGatewayRoute",
			setup: func(h *appmesh.Handler) {
				doRequest(t, h, http.MethodPut, "/meshes", map[string]any{"meshName": "m"})
				doRequest(t, h, http.MethodPut, "/meshes/m/virtualGateways",
					map[string]any{"virtualGatewayName": "gw1"})
				doRequest(t, h, http.MethodPut, "/meshes/m/virtualGateway/gw1/gatewayRoutes",
					map[string]any{"gatewayRouteName": "gr1", "spec": map[string]any{}})
			},
			method:   http.MethodPut,
			path:     "/meshes/m/virtualGateway/gw1/gatewayRoutes/gr1",
			body:     map[string]any{"spec": map[string]any{}},
			wrapKey:  "gatewayRoute",
			topField: "gatewayRouteName",
		},
		{
			name: "DeleteGatewayRoute",
			setup: func(h *appmesh.Handler) {
				doRequest(t, h, http.MethodPut, "/meshes", map[string]any{"meshName": "m"})
				doRequest(t, h, http.MethodPut, "/meshes/m/virtualGateways",
					map[string]any{"virtualGatewayName": "gw1"})
				doRequest(t, h, http.MethodPut, "/meshes/m/virtualGateway/gw1/gatewayRoutes",
					map[string]any{"gatewayRouteName": "gr1", "spec": map[string]any{}})
			},
			method:   http.MethodDelete,
			path:     "/meshes/m/virtualGateway/gw1/gatewayRoutes/gr1",
			wrapKey:  "gatewayRoute",
			topField: "gatewayRouteName",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			tt.setup(h)
			rec := doRequest(t, h, tt.method, tt.path, tt.body)
			require.Equal(t, http.StatusOK, rec.Code, "%s: unexpected status", tt.name)

			body := getBody(t, rec)
			_, wrapped := body[tt.wrapKey].(map[string]any)
			assert.False(t, wrapped,
				"%s: response must NOT be wrapped under %q; got keys: %v",
				tt.name, tt.wrapKey, mapKeys(body))
			_, ok := body[tt.topField]
			assert.True(t, ok,
				"%s: response must have top-level %q; got keys: %v",
				tt.name, tt.topField, mapKeys(body))
		})
	}
}

// mapKeys returns the top-level keys of a map for use in error messages.
func mapKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}

	return keys
}
