package appsync_test

// This file covers routing bugs found in the fresh AppSync parity audit: the real AWS
// SDK (aws-sdk-go-v2/service/appsync, restjson1) sends Update* operations as POST (not
// PUT/PATCH), and several ops live at entirely different paths than this handler
// previously implemented (tags, ApiCache update/flush, code/template evaluation,
// ListSourceApiAssociations). Each test below exercises the *real* wire path through
// h.Handler(), not just the business-logic method, and TestRouteMatcher_RealPaths
// additionally verifies h.RouteMatcher() (not just Handler()) accepts them — a request
// that Handler() can serve but RouteMatcher() rejects never reaches this service from a
// real SDK client.

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/appsync"
)

// Test_UpdateOps_UsePOSTMethod verifies that every AppSync Update* operation is
// reachable via POST — the method the real AWS SDK actually sends — not just the
// PUT/PATCH this handler previously required exclusively.
func Test_UpdateOps_UsePOSTMethod(t *testing.T) {
	t.Parallel()

	t.Run("UpdateGraphqlApi", func(t *testing.T) {
		t.Parallel()

		h, b := newTestHandler()
		api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
		require.NoError(t, err)

		rec := doRequest(t, h, http.MethodPost, "/v1/apis/"+api.APIID, map[string]any{"name": "Renamed"})
		require.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("UpdateApi_v2", func(t *testing.T) {
		t.Parallel()

		h, b := newTestHandler()
		api, err := b.CreateAPI("TestEventAPI", "", nil, nil)
		require.NoError(t, err)

		rec := doRequest(t, h, http.MethodPost, "/v2/apis/"+api.APIID, map[string]any{"name": "Renamed"})
		require.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("UpdateDataSource", func(t *testing.T) {
		t.Parallel()

		h, b := newTestHandler()
		api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
		require.NoError(t, err)
		_, err = b.CreateDataSource(api.APIID, &appsync.DataSource{Name: "myds", Type: "NONE"})
		require.NoError(t, err)

		rec := doRequest(t, h, http.MethodPost, "/v1/apis/"+api.APIID+"/datasources/myds",
			map[string]any{"name": "myds", "type": "NONE", "description": "updated"})
		require.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("UpdateFunction", func(t *testing.T) {
		t.Parallel()

		h, b := newTestHandler()
		api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
		require.NoError(t, err)
		_, err = b.CreateDataSource(api.APIID, &appsync.DataSource{Name: "ds", Type: "NONE"})
		require.NoError(t, err)
		fn, err := b.CreateFunction(api.APIID, &appsync.Function{Name: "fn", DataSourceName: "ds"})
		require.NoError(t, err)

		rec := doRequest(t, h, http.MethodPost, "/v1/apis/"+api.APIID+"/functions/"+fn.FunctionID,
			map[string]any{"name": "fn", "dataSourceName": "ds", "description": "updated"})
		require.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("UpdateType", func(t *testing.T) {
		t.Parallel()

		h, b := newTestHandler()
		api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
		require.NoError(t, err)
		_, err = b.CreateType(api.APIID, "type MyType { id: ID! }", appsync.TypeFormatSDL)
		require.NoError(t, err)

		rec := doRequest(t, h, http.MethodPost, "/v1/apis/"+api.APIID+"/types/MyType",
			map[string]any{"definition": "type MyType { id: ID! name: String! }", "format": "SDL"})
		require.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("UpdateResolver", func(t *testing.T) {
		t.Parallel()

		h, b := newTestHandler()
		api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
		require.NoError(t, err)
		_, err = b.CreateDataSource(api.APIID, &appsync.DataSource{Name: "ds", Type: "NONE"})
		require.NoError(t, err)
		_, err = b.CreateResolver(api.APIID, "Query", &appsync.Resolver{
			FieldName: "hello", DataSourceName: "ds", Kind: "UNIT",
		})
		require.NoError(t, err)

		rec := doRequest(t, h, http.MethodPost, "/v1/apis/"+api.APIID+"/types/Query/resolvers/hello",
			map[string]any{"dataSourceName": "ds", "kind": "UNIT"})
		require.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("UpdateApiKey", func(t *testing.T) {
		t.Parallel()

		h, b := newTestHandler()
		api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
		require.NoError(t, err)
		key, err := b.CreateAPIKey(api.APIID, "orig", 0)
		require.NoError(t, err)

		rec := doRequest(t, h, http.MethodPost, "/v1/apis/"+api.APIID+"/apikeys/"+key.ID,
			map[string]any{"description": "updated"})
		require.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("UpdateDomainName", func(t *testing.T) {
		t.Parallel()

		h, b := newTestHandler()
		_, err := b.CreateDomainName("api.example.com", "arn:aws:acm:us-east-1:000000000000:cert/abc", "", nil)
		require.NoError(t, err)

		rec := doRequest(t, h, http.MethodPost, "/v1/domainnames/api.example.com",
			map[string]any{"description": "updated"})
		require.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("UpdateChannelNamespace", func(t *testing.T) {
		t.Parallel()

		h, b := newTestHandler()
		api, err := b.CreateAPI("TestEventAPI", "", nil, nil)
		require.NoError(t, err)
		_, err = b.CreateChannelNamespace(api.APIID, "ns1", nil, nil)
		require.NoError(t, err)

		rec := doRequest(t, h, http.MethodPost, "/v2/apis/"+api.APIID+"/channelNamespaces/ns1",
			map[string]any{"codeHandlers": "export const handler = () => {}"})
		require.Equal(t, http.StatusOK, rec.Code)
	})
}

// Test_TagResource_RealPath verifies tag operations are reachable at the real AWS SDK
// path "/v1/tags/{resourceArn}" — not the previously (and still, for back-compat)
// supported "/v1/apis/{apiId}/tags" alias. The ARN itself contains "/" (between the
// "apis" resource-type segment and the api ID), matching the ARN-with-slash routing
// bug class flagged for this sweep.
func Test_TagResource_RealPath(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	tagPath := "/v1/tags/" + api.ARN

	// TagResource (POST).
	rec := doRequest(t, h, http.MethodPost, tagPath, map[string]any{"tags": map[string]any{"env": "prod"}})
	require.Equal(t, http.StatusNoContent, rec.Code)

	// ListTagsForResource (GET).
	rec2 := doRequest(t, h, http.MethodGet, tagPath, nil)
	require.Equal(t, http.StatusOK, rec2.Code)

	var listResp map[string]any
	require.NoError(t, json.NewDecoder(rec2.Body).Decode(&listResp))
	tagMap, ok := listResp["tags"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "prod", tagMap["env"])

	// UntagResource (DELETE).
	rec3 := doRequest(t, h, http.MethodDelete, tagPath+"?tagKeys=env", nil)
	require.Equal(t, http.StatusNoContent, rec3.Code)

	rec4 := doRequest(t, h, http.MethodGet, tagPath, nil)
	require.NoError(t, json.NewDecoder(rec4.Body).Decode(&listResp))
	assert.Empty(t, listResp["tags"])
}

// Test_TagResource_RealPath_EventAPI verifies tagging works for v2 Api (Event API)
// resources too — both v1 GraphqlApi and v2 Api share the "apis/{id}" ARN shape.
func Test_TagResource_RealPath_EventAPI(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler()
	api, err := b.CreateAPI("TestEventAPI", "", nil, nil)
	require.NoError(t, err)

	tagPath := "/v1/tags/" + api.ARN

	rec := doRequest(t, h, http.MethodPost, tagPath, map[string]any{"tags": map[string]any{"team": "core"}})
	require.Equal(t, http.StatusNoContent, rec.Code)

	rec2 := doRequest(t, h, http.MethodGet, tagPath, nil)
	require.Equal(t, http.StatusOK, rec2.Code)

	var resp map[string]any
	require.NoError(t, json.NewDecoder(rec2.Body).Decode(&resp))
	tagMap, ok := resp["tags"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "core", tagMap["team"])
}

// Test_TagResource_RealPath_NotFound verifies an ARN for a nonexistent api resolves to
// NotFoundException, not a silent success or an InternalFailure.
func Test_TagResource_RealPath_NotFound(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler()

	rec := doRequest(t, h, http.MethodGet,
		"/v1/tags/arn:aws:appsync:us-east-1:000000000000:apis/nonexistent", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// Test_ApiCache_RealPaths verifies UpdateApiCache and FlushApiCache at their real AWS
// SDK paths ("/v1/apis/{apiId}/ApiCaches/update" and "/v1/apis/{apiId}/FlushCache")
// rather than only the previously-implemented (and still supported) aliases.
func Test_ApiCache_RealPaths(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)
	_, err = b.CreateAPICache(
		api.APIID,
		&appsync.APICache{TTL: 60, Type: "SMALL", APICachingBehavior: "FULL_REQUEST_CACHING"},
	)
	require.NoError(t, err)

	rec := doRequest(t, h, http.MethodPost, "/v1/apis/"+api.APIID+"/ApiCaches/update",
		map[string]any{"ttl": 120, "type": "LARGE"})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	cache := resp["apiCache"].(map[string]any)
	assert.Equal(t, "LARGE", cache["type"])

	rec2 := doRequest(t, h, http.MethodDelete, "/v1/apis/"+api.APIID+"/FlushCache", nil)
	assert.Equal(t, http.StatusNoContent, rec2.Code)
}

// Test_Evaluate_RealPaths verifies EvaluateCode/EvaluateMappingTemplate at their real
// AWS SDK paths ("/v1/dataplane-evaluatecode" and "/v1/dataplane-evaluatetemplate") —
// two standalone top-level paths, not "/v1/dataplane-evaluations/{code,template}".
func Test_Evaluate_RealPaths(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler()

	rec := doRequest(t, h, http.MethodPost, "/v1/dataplane-evaluatecode", map[string]any{
		"code":    `export function request(ctx) { return {}; }`,
		"context": "",
		"runtime": map[string]any{"name": "APPSYNC_JS"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec2 := doRequest(t, h, http.MethodPost, "/v1/dataplane-evaluatetemplate", map[string]any{
		"template": `{"version": "2017-02-28", "payload": {}}`,
		"context":  "",
	})
	require.Equal(t, http.StatusOK, rec2.Code)
}

// Test_ListSourceApiAssociations_ByAPIID verifies the real AWS SDK path
// "/v1/apis/{apiId}/sourceApiAssociations" (keyed by the merged API's own apiId) works
// alongside the existing "/v1/mergedApis/{mergedApiIdentifier}/sourceApiAssociations"
// path used by Associate/Get/Update/DisassociateSourceGraphqlApi.
func Test_ListSourceApiAssociations_ByAPIID(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler()
	merged, err := b.CreateGraphqlAPI("MergedAPI", appsync.AuthTypeAPIKey, false, "MERGED", "", nil, nil, nil)
	require.NoError(t, err)
	source, err := b.CreateGraphqlAPI("SourceAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)
	_, err = b.AssociateSourceGraphqlAPI(merged.APIID, source.APIID, "", "")
	require.NoError(t, err)

	rec := doRequest(t, h, http.MethodGet, "/v1/apis/"+merged.APIID+"/sourceApiAssociations", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	// The real AWS SDK's ListSourceApiAssociationsOutput wraps the list under
	// "sourceApiAssociationSummaries", not "sourceApiAssociations" (the latter is only
	// the URL path segment name).
	assocs, ok := resp["sourceApiAssociationSummaries"].([]any)
	require.True(t, ok)
	assert.Len(t, assocs, 1)
}

// Test_ListApis_ResponseKey verifies ListApis wraps its list under "apis" — the real
// AWS SDK's ListApisOutput field name — not "items" (a disguised no-op: a real client
// would always see an empty list back, since it deserializes strictly by field name).
func Test_ListApis_ResponseKey(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler()
	_, err := b.CreateAPI("TestEventAPI", "", nil, nil)
	require.NoError(t, err)

	rec := doRequest(t, h, http.MethodGet, "/v2/apis", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	apis, ok := resp["apis"].([]any)
	require.True(t, ok, "response must wrap the list under \"apis\", got keys %v", resp)
	assert.Len(t, apis, 1)
	assert.NotContains(t, resp, "items")
}

// TestRouteMatcher_RealPaths verifies h.RouteMatcher() — the entry point real SDK
// requests are dispatched through — accepts every path fixed in this sweep. A path that
// only Handler() recognizes but RouteMatcher() rejects never reaches this service.
func TestRouteMatcher_RealPaths(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler()
	matcher := h.RouteMatcher()

	tests := []struct {
		method    string
		path      string
		userAgent string
	}{
		{method: http.MethodPost, path: "/v1/tags/arn:aws:appsync:us-east-1:000000000000:apis/abc123"},
		{method: http.MethodGet, path: "/v1/tags/arn:aws:appsync:us-east-1:000000000000:apis/abc123"},
		{method: http.MethodDelete, path: "/v1/tags/arn:aws:appsync:us-east-1:000000000000:apis/abc123"},
		{method: http.MethodPost, path: "/v1/dataplane-evaluatecode"},
		{method: http.MethodPost, path: "/v1/dataplane-evaluatetemplate"},
		{method: http.MethodPost, path: "/v1/apis/abc123"}, // UpdateGraphqlApi
		{
			// UpdateApi; /v2/apis is shared with API Gateway V2, so the real SDK's
			// distinguishing User-Agent is required for a match (see RouteMatcher doc).
			method: http.MethodPost, path: "/v2/apis/abc123",
			userAgent: "aws-sdk-go-v2/1.0 api/appsync/1.55.0",
		},
		{method: http.MethodPost, path: "/v1/apis/abc123/ApiCaches/update"},
		{method: http.MethodDelete, path: "/v1/apis/abc123/FlushCache"},
		{method: http.MethodGet, path: "/v1/apis/abc123/sourceApiAssociations"},
	}

	e := echo.New()

	for _, tt := range tests {
		t.Run(tt.method+" "+tt.path, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(tt.method, tt.path, nil)
			if tt.userAgent != "" {
				req.Header.Set("User-Agent", tt.userAgent)
			}

			c := e.NewContext(req, httptest.NewRecorder())
			assert.True(t, matcher(c), "RouteMatcher rejected %s %s", tt.method, tt.path)
		})
	}
}
