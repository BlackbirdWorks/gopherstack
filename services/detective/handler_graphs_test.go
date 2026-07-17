package detective_test

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/detective"
)

func TestDetective_Graph(t *testing.T) { //nolint:paralleltest // existing issue.
	tests := []struct {
		name     string
		setup    func(h *detective.Handler)
		body     any
		check    func(t *testing.T, body []byte)
		method   string
		path     string
		wantCode int
	}{
		{
			name:     "CreateGraph returns graphArn",
			method:   http.MethodPost,
			path:     "/graph",
			body:     map[string]any{"Tags": map[string]string{"env": "test"}},
			wantCode: http.StatusOK,
			check: func(t *testing.T, respBody []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(respBody, &resp))
				assert.Contains(t, resp["GraphArn"], "arn:aws:detective:")
			},
		},
		{
			name:   "CreateGraph idempotent returns same arn",
			method: http.MethodPost,
			path:   "/graph",
			body:   map[string]any{},
			setup: func(h *detective.Handler) {
				doRequest(t, h, http.MethodPost, "/graph", map[string]any{})
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, respBody []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(respBody, &resp))
				assert.Contains(t, resp["GraphArn"], "arn:aws:detective:")
			},
		},
		{
			name:   "DeleteGraph returns 200",
			method: http.MethodPost,
			path:   "/graph/removal",
			setup: func(h *detective.Handler) {
				doRequest(t, h, http.MethodPost, "/graph", map[string]any{})
			},
			wantCode: http.StatusOK,
		},
		{
			name:     "DeleteGraph unknown arn returns 404",
			method:   http.MethodPost,
			path:     "/graph/removal",
			body:     map[string]any{"GraphArn": "arn:aws:detective:us-east-1:000000000000:graph:doesnotexist"},
			wantCode: http.StatusNotFound,
		},
		{
			name:     "DeleteGraph missing arn returns 400",
			method:   http.MethodPost,
			path:     "/graph/removal",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "ListGraphs empty returns empty list",
			method:   http.MethodPost,
			path:     "/graphs/list",
			body:     map[string]any{},
			wantCode: http.StatusOK,
			check: func(t *testing.T, respBody []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(respBody, &resp))
				list, ok := resp["GraphList"].([]any)
				require.True(t, ok)
				assert.Empty(t, list)
			},
		},
		{
			name:   "ListGraphs after create returns graph",
			method: http.MethodPost,
			path:   "/graphs/list",
			body:   map[string]any{},
			setup: func(h *detective.Handler) {
				doRequest(t, h, http.MethodPost, "/graph", map[string]any{})
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, respBody []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(respBody, &resp))
				list, ok := resp["GraphList"].([]any)
				require.True(t, ok)
				assert.Len(t, list, 1)
			},
		},
	}

	for _, tc := range tests { //nolint:paralleltest // existing issue.
		t.Run(tc.name, func(t *testing.T) {
			h := newTestHandler(t)

			if tc.setup != nil {
				tc.setup(h)
			}

			body := tc.body
			if body == nil && tc.setup != nil && tc.method == http.MethodPost && tc.path == "/graph/removal" {
				// get graphArn from listing
				listRec := doRequest(t, h, http.MethodPost, "/graphs/list", map[string]any{})
				var listResp map[string]any
				require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
				list := listResp["GraphList"].([]any)
				if len(list) > 0 {
					graph := list[0].(map[string]any)
					body = map[string]any{"GraphArn": graph["Arn"]}
				}
			}

			rec := doRequest(t, h, tc.method, tc.path, body)
			assert.Equal(t, tc.wantCode, rec.Code)

			if tc.check != nil {
				tc.check(t, rec.Body.Bytes())
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Graph ARN format
// ---------------------------------------------------------------------------

var reDetectiveARN = regexp.MustCompile(`^arn:aws:detective:[a-z0-9-]+:\d{12}:graph:[0-9a-f]{32}$`)

func TestGraphARN_Shape(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/graph", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	arn, _ := resp["GraphArn"].(string)

	require.NotEmpty(t, arn, "CreateGraph must return a GraphArn")
	assert.True(t, reDetectiveARN.MatchString(arn),
		"GraphArn must match arn:aws:detective:{region}:{account}:graph:{32hexchars}, got %q", arn)
}

// ---------------------------------------------------------------------------
// ListGraphs CreatedTime present
// ---------------------------------------------------------------------------

func TestListGraphs_CreatedTime_Present(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doRequest(t, h, http.MethodPost, "/graph", map[string]any{})

	rec := doRequest(t, h, http.MethodPost, "/graphs/list", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		GraphList []struct {
			Arn         string `json:"Arn"`
			CreatedTime string `json:"CreatedTime"`
		} `json:"GraphList"`
	}

	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	require.Len(t, resp.GraphList, 1)
	assert.NotEmpty(t, resp.GraphList[0].CreatedTime, "ListGraphs must include CreatedTime")
	assert.Contains(t, resp.GraphList[0].CreatedTime, "T",
		"CreatedTime must be ISO 8601, got %q", resp.GraphList[0].CreatedTime)
}

// ---------------------------------------------------------------------------
// ListGraphs pagination
// ---------------------------------------------------------------------------

func TestListGraphs_Pagination_NextToken_Absent_When_All_Fit(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doRequest(t, h, http.MethodPost, "/graph", map[string]any{})

	rec := doRequest(t, h, http.MethodPost, "/graphs/list", map[string]any{"MaxResults": 200})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	_, hasToken := resp["NextToken"]
	assert.False(t, hasToken, "NextToken must be absent when all results fit on one page")
}

// ---- ListGraphs: opaque pagination token ----

func TestListGraphsOpaqueToken(t *testing.T) {
	t.Parallel()
	b := detective.NewInMemoryBackend("000000000000", "us-east-1")
	h := detective.NewHandler(b)

	// Detective allows only one graph per account via the API, so seed a second
	// graph directly into the backend to test multi-graph pagination.
	rec := doRequest(t, h, http.MethodPost, "/graph", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)
	detective.SeedGraph(b, "arn:aws:detective:us-east-1:000000000000:graph:aaaabbbbcccc00001111222233334444")

	// Request page 1 of 1.
	rec = doRequest(t, h, http.MethodPost, "/graphs/list", map[string]any{"MaxResults": 1})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	parseJSON(t, rec.Body.Bytes(), &resp)

	tok, hasTok := resp["NextToken"].(string)
	require.True(t, hasTok, "NextToken must be present when more results exist")
	assert.NotEmpty(t, tok)

	// Token must be base64 — not a raw ARN.
	_, err := base64.StdEncoding.DecodeString(tok)
	require.NoError(t, err, "NextToken must be opaque base64, not a raw resource identifier")

	// Second page should exhaust results.
	rec2 := doRequest(t, h, http.MethodPost, "/graphs/list", map[string]any{"MaxResults": 1, "NextToken": tok})
	require.Equal(t, http.StatusOK, rec2.Code)

	var resp2 map[string]any
	parseJSON(t, rec2.Body.Bytes(), &resp2)
	_, hasTok2 := resp2["NextToken"]
	assert.False(t, hasTok2, "NextToken must be absent on the last page")
}
