package quicksight_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/quicksight"
)

// ---- Search tests ---- //nolint:godot // existing issue.
func TestQuickSight_Search(t *testing.T) { //nolint:paralleltest // existing issue.
	h := newTestHandler(t)

	tests := []struct {
		body       any
		name       string
		method     string
		path       string
		wantKey    string
		wantStatus int
	}{
		{
			name:       "search analyses",
			method:     http.MethodPost,
			path:       accountPath("/search/analyses"),
			body:       map[string]any{"Filters": []any{}},
			wantStatus: http.StatusOK,
			wantKey:    "AnalysisSummaryList",
		},
		{
			name:       "search dashboards",
			method:     http.MethodPost,
			path:       accountPath("/search/dashboards"),
			body:       map[string]any{"Filters": []any{}},
			wantStatus: http.StatusOK,
			wantKey:    "DashboardSummaryList",
		},
		{
			name:       "search datasets",
			method:     http.MethodPost,
			path:       accountPath("/search/data-sets"),
			body:       map[string]any{"Filters": []any{}},
			wantStatus: http.StatusOK,
			wantKey:    "DataSetSummaries",
		},
		{
			name:       "search data sources",
			method:     http.MethodPost,
			path:       accountPath("/search/data-sources"),
			body:       map[string]any{"Filters": []any{}},
			wantStatus: http.StatusOK,
			wantKey:    "DataSources",
		},
		{
			name:       "search topics",
			method:     http.MethodPost,
			path:       accountPath("/search/topics"),
			body:       map[string]any{"Filters": []any{}},
			wantStatus: http.StatusOK,
			wantKey:    "TopicSummaryList",
		},
	}

	for _, tc := range tests { //nolint:paralleltest // existing issue.
		t.Run(tc.name, func(t *testing.T) {
			rec := doRequest(t, h, tc.method, tc.path, tc.body)
			assert.Equal(t, tc.wantStatus, rec.Code, "status")
			if tc.wantKey != "" {
				body := parseBody(t, rec)
				assert.Contains(t, body, tc.wantKey)
			}
		})
	}
}

// roundTripStep is one HTTP call in a stateful round-trip scenario.
type roundTripStep struct {
	body         any
	name         string
	method       string
	path         string
	wantContains string // top-level key expected in the response body
	wantStatus   int
}

func runRoundTrip(t *testing.T, h *quicksight.Handler, steps []roundTripStep) {
	t.Helper()

	for _, st := range steps {
		t.Run(st.name, func(t *testing.T) {
			rec := doRequest(t, h, st.method, st.path, st.body)
			assert.Equal(t, st.wantStatus, rec.Code, "status for %s", st.name)
			if st.wantContains != "" {
				body := parseBody(t, rec)
				assert.Contains(t, body, st.wantContains, "key for %s", st.name)
			}
		})
	}
}

func TestQuickSight_DuplicateCreateConflicts(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body any
		name string
		path string
	}{
		{name: "folder", path: accountPath("/folders/dup1"), body: map[string]any{"Name": "Dup"}},
		{name: "template", path: accountPath("/templates/dup1"), body: map[string]any{"Name": "Dup"}},
		{name: "theme", path: accountPath("/themes/dup1"), body: map[string]any{"Name": "Dup"}},
		{name: "brand", path: accountPath("/brands/dup1"), body: map[string]any{"BrandName": "Dup"}},
		{
			name: "vpc",
			path: accountPath("/vpc-connections"),
			body: map[string]any{"VPCConnectionId": "dup1", "Name": "Dup"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			rec := doRequest(t, h, http.MethodPost, tc.path, tc.body)
			require.Equal(t, http.StatusOK, rec.Code, "first create")

			rec = doRequest(t, h, http.MethodPost, tc.path, tc.body)
			assert.Equal(t, http.StatusConflict, rec.Code, "duplicate create conflicts")
		})
	}
}

const (
	testAccountID = "000000000000"
	testRegion    = "us-east-1"
	testNamespace = "default"
)

// testPhysicalTableMap returns a minimal but real PhysicalTableMap body
// fragment (one RelationalTable entry) for tests that only need a dataset to
// exist, not to exercise PhysicalTableMap itself. CreateDataSet/UpdateDataSet
// reject a request with no physical tables (quicksight@v1.123.1
// api_op_CreateDataSet.go:55, api_op_UpdateDataSet.go:55: PhysicalTableMap is
// required), so every test that creates/updates a dataset over HTTP needs one.
func testPhysicalTableMap() map[string]any {
	return map[string]any{
		"pt1": map[string]any{
			"RelationalTable": map[string]any{
				"DataSourceArn": "arn:aws:quicksight:us-east-1:000000000000:datasource/ds1",
				"Name":          "table1",
				"InputColumns": []any{
					map[string]any{"Name": "col1", "Type": "STRING"},
				},
			},
		},
	}
}

func newTestBackend(t *testing.T) *quicksight.InMemoryBackend {
	t.Helper()

	return quicksight.NewInMemoryBackend(testAccountID, testRegion)
}

func newTestHandler(t *testing.T) *quicksight.Handler {
	t.Helper()

	return quicksight.NewHandler(newTestBackend(t))
}

func doRequest(t *testing.T, h *quicksight.Handler, method, path string, body any) *httptest.ResponseRecorder {
	t.Helper()

	var bodyBytes []byte
	if body != nil {
		var marshalErr error
		bodyBytes, marshalErr = json.Marshal(body)
		require.NoError(t, marshalErr)
	}

	req := httptest.NewRequest(method, path, bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=AKID/20240101/us-east-1/quicksight/aws4_request")

	rec := httptest.NewRecorder()
	e := echo.New()
	c := e.NewContext(req, rec)

	handlerErr := h.Handler()(c)
	require.NoError(t, handlerErr)

	return rec
}

func parseBody(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	return resp
}

func accountPath(sub string) string {
	return fmt.Sprintf("/accounts/%s%s", testAccountID, sub)
}

// oauthAppBody returns a CreateOAuthClientApplicationInput body carrying
// every field aws-sdk-go-v2/service/quicksight@v1.123.1/validators.go's
// validateOpCreateOAuthClientApplicationInput marks required
// (OAuthClientApplicationId, Name, OAuthClientAuthenticationType, ClientId,
// ClientSecret, OAuthTokenEndpointUrl), for tests that only care about an
// OAuth client application existing rather than exercising these fields
// directly.
func oauthAppBody(id, name string) map[string]any {
	return map[string]any{
		"OAuthClientApplicationId":      id,
		"Name":                          name,
		"ClientId":                      "idp-client-id-" + id,
		"ClientSecret":                  "idp-client-secret-" + id,
		"OAuthClientAuthenticationType": "TOKEN",
		"OAuthTokenEndpointUrl":         "https://idp.example.com/token",
	}
}

func nsPath(sub string) string {
	return fmt.Sprintf("/accounts/%s/namespaces/%s%s", testAccountID, testNamespace, sub)
}

// ---- RouteMatcher tests ----

func TestQuickSight_RouteMatcher(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	matcher := h.RouteMatcher()
	e := echo.New()

	withAuth := func(path string) *echo.Context {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=AKID/20240101/us-east-1/quicksight/aws4_request")
		c := e.NewContext(req, httptest.NewRecorder())

		return c
	}

	withoutAuth := func(path string) *echo.Context {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=AKID/20240101/us-east-1/securityhub/aws4_request")
		c := e.NewContext(req, httptest.NewRecorder())

		return c
	}

	assert.True(t, matcher(withAuth("/accounts/123/data-sources")), "quicksight auth on /accounts/ should match")
	assert.True(
		t,
		matcher(withAuth("/resources/arn:aws:quicksight:us-east-1:123:dashboard/d1/tags")),
		"tag path should match",
	)
	assert.False(t, matcher(withoutAuth("/accounts/123/data-sources")), "securityhub auth should not match")
	assert.False(t, matcher(withAuth("/other/path")), "non-quicksight path should not match")
}

// TestRequestIDsAreUnique verifies that successive responses carry distinct RequestId values.
func TestRequestIDsAreUnique(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
	h := quicksight.NewHandler(b)

	ids := make(map[string]bool)
	path := accountPath("/namespaces")

	for range 5 {
		rec := doRequest(t, h, http.MethodGet, path, nil)
		require.Equal(t, http.StatusOK, rec.Code)
		body := parseBody(t, rec)
		reqID, _ := body["RequestId"].(string)
		assert.NotEmpty(t, reqID)
		assert.False(t, ids[reqID], "RequestId %q appeared more than once", reqID)
		ids[reqID] = true
	}
}
