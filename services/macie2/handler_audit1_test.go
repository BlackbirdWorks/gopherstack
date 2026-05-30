package macie2_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/macie2"
)

func newTestHandler(t *testing.T) *macie2.Handler {
	t.Helper()
	backend := macie2.NewInMemoryBackend("000000000000", "us-east-1")

	return macie2.NewHandler(backend)
}

func doRequest(t *testing.T, h *macie2.Handler, method, path string, body any) *httptest.ResponseRecorder {
	t.Helper()

	var bodyBytes []byte

	if body != nil {
		var err error

		bodyBytes, err = json.Marshal(body)
		require.NoError(t, err)
	}

	req := httptest.NewRequest(method, path, bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	e := echo.New()
	c := e.NewContext(req, rec)
	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

func TestMacie2_Session(t *testing.T) {
	t.Parallel()

	tests := []struct { //nolint:govet // function field in anonymous struct causes false fieldalignment positive
		name     string
		setup    func(h *macie2.Handler)
		method   string
		path     string
		body     any
		wantCode int
		check    func(t *testing.T, body []byte)
	}{
		{
			name:     "GetMacieSession when not enabled returns 403",
			method:   http.MethodGet,
			path:     "/macie",
			wantCode: http.StatusForbidden,
		},
		{
			name:   "EnableMacie returns 200",
			method: http.MethodPost,
			path:   "/macie",
			body: map[string]string{
				"findingPublishingFrequency": "FIFTEEN_MINUTES",
				"status":                     "ENABLED",
			},
			wantCode: http.StatusOK,
		},
		{
			name: "GetMacieSession after enable returns session details",
			setup: func(h *macie2.Handler) {
				doRequest(t, h, http.MethodPost, "/macie", map[string]string{
					"findingPublishingFrequency": "ONE_HOUR",
				})
			},
			method:   http.MethodGet,
			path:     "/macie",
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.Equal(t, "ONE_HOUR", resp["findingPublishingFrequency"])
				assert.Equal(t, "ENABLED", resp["status"])
				assert.NotEmpty(t, resp["serviceRole"])
				assert.NotEmpty(t, resp["createdAt"])
			},
		},
		{
			name: "UpdateMacieSession changes frequency",
			setup: func(h *macie2.Handler) {
				doRequest(t, h, http.MethodPost, "/macie", nil)
			},
			method:   http.MethodPatch,
			path:     "/macie",
			body:     map[string]string{"findingPublishingFrequency": "SIX_HOURS"},
			wantCode: http.StatusOK,
		},
		{
			name: "DisableMacie returns 200",
			setup: func(h *macie2.Handler) {
				doRequest(t, h, http.MethodPost, "/macie", nil)
			},
			method:   http.MethodDelete,
			path:     "/macie",
			wantCode: http.StatusOK,
		},
		{
			name:     "DisableMacie when not enabled returns 200 (idempotent)",
			method:   http.MethodDelete,
			path:     "/macie",
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.setup != nil {
				tt.setup(h)
			}

			rec := doRequest(t, h, tt.method, tt.path, tt.body)

			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.check != nil {
				tt.check(t, rec.Body.Bytes())
			}
		})
	}
}

func TestMacie2_AllowLists(t *testing.T) {
	t.Parallel()

	tests := []struct { //nolint:govet // function field in anonymous struct causes false fieldalignment positive
		name     string
		setup    func(h *macie2.Handler) string
		method   string
		pathFn   func(id string) string
		body     any
		wantCode int
		check    func(t *testing.T, body []byte)
	}{
		{
			name:   "CreateAllowList returns arn and id",
			method: http.MethodPost,
			pathFn: func(_ string) string { return "/allow-lists" },
			body: map[string]any{
				"clientToken": "tok1",
				"name":        "test-list",
				"criteria":    map[string]any{"regex": "\\d+"},
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]string
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.NotEmpty(t, resp["arn"])
				assert.NotEmpty(t, resp["id"])
				assert.Contains(t, resp["arn"], "arn:aws:macie2:")
			},
		},
		{
			name:   "GetAllowList returns full detail with criteria",
			method: http.MethodGet,
			setup: func(h *macie2.Handler) string {
				rec := doRequest(t, h, http.MethodPost, "/allow-lists", map[string]any{
					"clientToken": "tok2",
					"name":        "my-list",
					"criteria":    map[string]any{"regex": "test-\\w+"},
					"description": "my desc",
				})
				var resp map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

				return resp["id"]
			},
			pathFn:   func(id string) string { return "/allow-lists/" + id },
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.Equal(t, "my-list", resp["name"])
				assert.Equal(t, "my desc", resp["description"])
				assert.NotNil(t, resp["criteria"])
				assert.NotNil(t, resp["status"])
			},
		},
		{
			name:     "GetAllowList not found returns 404",
			method:   http.MethodGet,
			pathFn:   func(_ string) string { return "/allow-lists/nonexistent" },
			wantCode: http.StatusNotFound,
		},
		{
			name:     "ListAllowLists returns allowLists key",
			method:   http.MethodGet,
			pathFn:   func(_ string) string { return "/allow-lists" },
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.Contains(t, resp, "allowLists")
			},
		},
		{
			name:   "DeleteAllowList returns 200",
			method: http.MethodDelete,
			setup: func(h *macie2.Handler) string {
				rec := doRequest(t, h, http.MethodPost, "/allow-lists", map[string]any{
					"clientToken": "tok3",
					"name":        "del-list",
					"criteria":    map[string]any{"regex": ".+"},
				})
				var resp map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

				return resp["id"]
			},
			pathFn:   func(id string) string { return "/allow-lists/" + id },
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			id := ""
			if tt.setup != nil {
				id = tt.setup(h)
			}

			path := tt.pathFn(id)
			rec := doRequest(t, h, tt.method, path, tt.body)

			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.check != nil {
				tt.check(t, rec.Body.Bytes())
			}
		})
	}
}

func TestMacie2_CustomDataIdentifiers(t *testing.T) {
	t.Parallel()

	tests := []struct { //nolint:govet // function field in anonymous struct causes false fieldalignment positive
		name     string
		setup    func(h *macie2.Handler) string
		method   string
		pathFn   func(id string) string
		body     any
		wantCode int
		check    func(t *testing.T, body []byte)
	}{
		{
			name:   "CreateCustomDataIdentifier returns id",
			method: http.MethodPost,
			pathFn: func(_ string) string { return "/custom-data-identifiers" },
			body: map[string]any{
				"name":  "ssn-detector",
				"regex": `\d{3}-\d{2}-\d{4}`,
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]string
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.NotEmpty(t, resp["customDataIdentifierId"])
			},
		},
		{
			name:   "GetCustomDataIdentifier returns full detail",
			method: http.MethodGet,
			setup: func(h *macie2.Handler) string {
				rec := doRequest(t, h, http.MethodPost, "/custom-data-identifiers", map[string]any{
					"name":        "phone-detector",
					"regex":       `\d{3}-\d{3}-\d{4}`,
					"description": "detects phone numbers",
					"keywords":    []string{"phone", "tel"},
				})
				var resp map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

				return resp["customDataIdentifierId"]
			},
			pathFn:   func(id string) string { return "/custom-data-identifiers/" + id },
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.Equal(t, "phone-detector", resp["name"])
				assert.Equal(t, "detects phone numbers", resp["description"])
				assert.NotEmpty(t, resp["arn"])
				assert.InDelta(t, float64(50), resp["maximumMatchDistance"], 0.001)
			},
		},
		{
			name:   "TestCustomDataIdentifier returns match count",
			method: http.MethodPost,
			pathFn: func(_ string) string { return "/custom-data-identifiers/test" },
			body: map[string]any{
				"regex":      `\d{3}-\d{2}-\d{4}`,
				"sampleText": "my ssn is 123-45-6789 and another 987-65-4321",
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.InDelta(t, float64(2), resp["matchCount"], 0.001)
			},
		},
		{
			name:     "ListCustomDataIdentifiers returns items key",
			method:   http.MethodPost,
			pathFn:   func(_ string) string { return "/custom-data-identifiers/list" },
			body:     map[string]any{},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.Contains(t, resp, "items")
			},
		},
		{
			name:   "DeleteCustomDataIdentifier marks as deleted",
			method: http.MethodDelete,
			setup: func(h *macie2.Handler) string {
				rec := doRequest(t, h, http.MethodPost, "/custom-data-identifiers", map[string]any{
					"name":  "del-detector",
					"regex": `.+`,
				})
				var resp map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

				return resp["customDataIdentifierId"]
			},
			pathFn:   func(id string) string { return "/custom-data-identifiers/" + id },
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			id := ""
			if tt.setup != nil {
				id = tt.setup(h)
			}

			path := tt.pathFn(id)
			rec := doRequest(t, h, tt.method, path, tt.body)

			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.check != nil {
				tt.check(t, rec.Body.Bytes())
			}
		})
	}
}

func TestMacie2_FindingsFilters(t *testing.T) {
	t.Parallel()

	tests := []struct { //nolint:govet // function field in anonymous struct causes false fieldalignment positive
		name     string
		setup    func(h *macie2.Handler) string
		method   string
		pathFn   func(id string) string
		body     any
		wantCode int
		check    func(t *testing.T, body []byte)
	}{
		{
			name:   "CreateFindingsFilter returns arn and id",
			method: http.MethodPost,
			pathFn: func(_ string) string { return "/findingsfilters" },
			body: map[string]any{
				"name":            "high-severity",
				"action":          "ARCHIVE",
				"findingCriteria": map[string]any{"criterion": map[string]any{}},
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]string
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.NotEmpty(t, resp["arn"])
				assert.NotEmpty(t, resp["id"])
				assert.Contains(t, resp["arn"], "findings-filter")
			},
		},
		{
			name:   "GetFindingsFilter returns full detail",
			method: http.MethodGet,
			setup: func(h *macie2.Handler) string {
				rec := doRequest(t, h, http.MethodPost, "/findingsfilters", map[string]any{
					"name":        "test-filter",
					"action":      "NOOP",
					"description": "test desc",
					"position":    int32(3),
				})
				var resp map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

				return resp["id"]
			},
			pathFn:   func(id string) string { return "/findingsfilters/" + id },
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.Equal(t, "test-filter", resp["name"])
				assert.Equal(t, "NOOP", resp["action"])
				assert.Equal(t, "test desc", resp["description"])
				assert.InDelta(t, float64(3), resp["position"], 0.001)
			},
		},
		{
			name:     "ListFindingsFilters returns findingsFilterListItems key",
			method:   http.MethodGet,
			pathFn:   func(_ string) string { return "/findingsfilters" },
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.Contains(t, resp, "findingsFilterListItems")
			},
		},
		{
			name:   "DeleteFindingsFilter returns 200",
			method: http.MethodDelete,
			setup: func(h *macie2.Handler) string {
				rec := doRequest(t, h, http.MethodPost, "/findingsfilters", map[string]any{
					"name":   "del-filter",
					"action": "ARCHIVE",
				})
				var resp map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

				return resp["id"]
			},
			pathFn:   func(id string) string { return "/findingsfilters/" + id },
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			id := ""
			if tt.setup != nil {
				id = tt.setup(h)
			}

			path := tt.pathFn(id)
			rec := doRequest(t, h, tt.method, path, tt.body)

			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.check != nil {
				tt.check(t, rec.Body.Bytes())
			}
		})
	}
}

func TestMacie2_Findings(t *testing.T) {
	t.Parallel()

	tests := []struct { //nolint:govet // function field in anonymous struct causes false fieldalignment positive
		name     string
		setup    func(h *macie2.Handler)
		method   string
		path     string
		body     any
		wantCode int
		check    func(t *testing.T, body []byte)
	}{
		{
			name:   "CreateSampleFindings returns 200",
			method: http.MethodPost,
			path:   "/findings/sample",
			body: map[string]any{
				"findingTypes": []string{"SensitiveData:S3Object/Personal"},
			},
			wantCode: http.StatusOK,
		},
		{
			name:   "ListFindings returns findingIds key",
			method: http.MethodPost,
			path:   "/findings",
			setup: func(h *macie2.Handler) {
				doRequest(t, h, http.MethodPost, "/findings/sample", map[string]any{
					"findingTypes": []string{"Policy:IAMUser/S3BucketPublic"},
				})
			},
			body:     map[string]any{},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.Contains(t, resp, "findingIds")

				ids, ok := resp["findingIds"].([]any)
				require.True(t, ok)
				assert.Len(t, ids, 1)
			},
		},
		{
			name:   "GetFindings returns findings array",
			method: http.MethodPost,
			path:   "/findings/describe",
			setup: func(h *macie2.Handler) {
				doRequest(t, h, http.MethodPost, "/findings/sample", map[string]any{
					"findingTypes": []string{"SensitiveData:S3Object/Financial"},
				})
			},
			body:     nil, // will be set in test body below
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.Contains(t, resp, "findings")
			},
		},
		{
			name:   "GetFindingStatistics returns countsByGroup",
			method: http.MethodPost,
			path:   "/findings/statistics",
			setup: func(h *macie2.Handler) {
				doRequest(t, h, http.MethodPost, "/findings/sample", map[string]any{
					"findingTypes": []string{"SensitiveData:S3Object/Credentials"},
				})
			},
			body:     map[string]any{"groupBy": "type"},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.Contains(t, resp, "countsByGroup")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.setup != nil {
				tt.setup(h)
			}

			// For GetFindings, get actual finding IDs first
			body := tt.body
			if tt.name == "GetFindings returns findings array" {
				listRec := doRequest(t, h, http.MethodPost, "/findings", map[string]any{})
				var listResp map[string]any
				require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
				ids := listResp["findingIds"].([]any)
				body = map[string]any{"findingIds": ids}
			}

			rec := doRequest(t, h, tt.method, tt.path, body)

			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.check != nil {
				tt.check(t, rec.Body.Bytes())
			}
		})
	}
}

func TestMacie2_Tags(t *testing.T) {
	t.Parallel()

	const testARN = "arn:aws:macie2:us-east-1:000000000000:allow-list/test-id"

	tests := []struct { //nolint:govet // function field in anonymous struct causes false fieldalignment positive
		name     string
		setup    func(h *macie2.Handler)
		method   string
		path     string
		query    string
		body     any
		wantCode int
		check    func(t *testing.T, body []byte)
	}{
		{
			name:     "TagResource returns 200",
			method:   http.MethodPost,
			path:     "/tags/" + testARN,
			body:     map[string]any{"tags": map[string]string{"env": "test"}},
			wantCode: http.StatusOK,
		},
		{
			name:   "ListTagsForResource returns tags",
			method: http.MethodGet,
			path:   "/tags/" + testARN,
			setup: func(h *macie2.Handler) {
				doRequest(t, h, http.MethodPost, "/tags/"+testARN,
					map[string]any{"tags": map[string]string{"env": "prod", "team": "security"}})
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				tags, ok := resp["tags"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "prod", tags["env"])
				assert.Equal(t, "security", tags["team"])
			},
		},
		{
			name:   "UntagResource removes tags",
			method: http.MethodDelete,
			path:   "/tags/" + testARN,
			setup: func(h *macie2.Handler) {
				doRequest(t, h, http.MethodPost, "/tags/"+testARN,
					map[string]any{"tags": map[string]string{"env": "prod", "team": "security"}})
			},
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.setup != nil {
				tt.setup(h)
			}

			rec := doRequest(t, h, tt.method, tt.path, tt.body)

			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.check != nil {
				tt.check(t, rec.Body.Bytes())
			}
		})
	}
}

func TestMacie2_RouteMatching(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	matcher := h.RouteMatcher()

	tests := []struct {
		path string
		want bool
	}{
		{"/macie", true},
		{"/allow-lists", true},
		{"/allow-lists/some-id", true},
		{"/custom-data-identifiers", true},
		{"/findingsfilters", true},
		{"/findings", true},
		{"/findings/describe", true},
		{"/tags/arn:aws:macie2:us-east-1:000000000000:allow-list/id", true},
		{"/tags/arn:aws:guardduty:us-east-1:000000000000:detector/id", false},
		{"/s3", false},
		{"/iam/roles", false},
	}

	e := echo.New()

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(http.MethodGet, tt.path, nil)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)
			assert.Equal(t, tt.want, matcher(c))
		})
	}
}
