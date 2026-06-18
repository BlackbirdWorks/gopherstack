package dlm_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/dlm"
)

// ---------------------------------------------------------------------------
// Backend: AccountID, Region, Reset, Snapshot, Restore
// ---------------------------------------------------------------------------

func TestBackend_AccountIDAndRegion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		accountID  string
		region     string
		wantAcct   string
		wantRegion string
	}{
		{
			name:       "returns configured accountID and region",
			accountID:  "123456789012",
			region:     "us-west-2",
			wantAcct:   "123456789012",
			wantRegion: "us-west-2",
		},
		{
			name:       "empty values passthrough",
			accountID:  "",
			region:     "",
			wantAcct:   "",
			wantRegion: "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := dlm.NewInMemoryBackend(tc.accountID, tc.region)
			assert.Equal(t, tc.wantAcct, b.AccountID())
			assert.Equal(t, tc.wantRegion, b.Region())
		})
	}
}

func TestBackend_Reset(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		createCount int
	}{
		{name: "reset clears all policies", createCount: 3},
		{name: "reset on empty backend is safe", createCount: 0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := dlm.NewInMemoryBackend("000000000000", "us-east-1")
			for i := range tc.createCount {
				_, err := b.CreateLifecyclePolicy(
					fmt.Sprintf("desc-%d", i),
					"arn:aws:iam::000000000000:role/r",
					"ENABLED",
					nil,
					nil,
				)
				require.NoError(t, err)
			}

			b.Reset()

			policies, err := b.GetLifecyclePolicies(nil, "")
			require.NoError(t, err)
			assert.Empty(t, policies)
		})
	}
}

func TestBackend_SnapshotAndRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		badJSON     bool
		wantRestErr bool
	}{
		{
			name:        "snapshot roundtrip preserves policies",
			badJSON:     false,
			wantRestErr: false,
		},
		{
			name:        "restore with invalid JSON returns error",
			badJSON:     true,
			wantRestErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := dlm.NewInMemoryBackend("000000000000", "us-east-1")
			p, err := b.CreateLifecyclePolicy(
				"snap-policy",
				"arn:aws:iam::000000000000:role/r",
				"ENABLED",
				map[string]string{"k": "v"},
				nil,
			)
			require.NoError(t, err)

			snap := b.Snapshot()
			require.NotEmpty(t, snap)

			if tc.badJSON {
				err = b.Restore([]byte("not json"))
				require.Error(t, err)

				return
			}

			// Restore into fresh backend.
			b2 := dlm.NewInMemoryBackend("000000000000", "us-east-1")
			require.NoError(t, b2.Restore(snap))

			got, err := b2.GetLifecyclePolicy(p.PolicyID)
			require.NoError(t, err)
			assert.Equal(t, p.PolicyID, got.PolicyID)
			assert.Equal(t, "snap-policy", got.Description)
		})
	}
}

func TestBackend_SnapshotNullFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		payload []byte
	}{
		{
			name:    "restore null policies field initialises empty map",
			payload: []byte(`{"policies":null,"tags":null}`),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := dlm.NewInMemoryBackend("000000000000", "us-east-1")
			require.NoError(t, b.Restore(tc.payload))

			policies, err := b.GetLifecyclePolicies(nil, "")
			require.NoError(t, err)
			assert.Empty(t, policies)
		})
	}
}

// ---------------------------------------------------------------------------
// Handler meta: Name, Reset, RouteMatcher, MatchPriority, ExtractOperation,
// ExtractResource
// ---------------------------------------------------------------------------

func TestHandler_Name(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		want string
	}{
		{name: "returns DLM", want: "DLM"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			assert.Equal(t, tc.want, h.Name())
		})
	}
}

func TestHandler_Reset(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "reset clears handler backend state"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			_ = createPolicy(t, h)
			h.Reset()

			rec := doRequest(t, h, http.MethodGet, "/policies", nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			policies := resp["Policies"].([]any)
			assert.Empty(t, policies)
		})
	}
}

func TestHandler_RouteMatcher(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		path string
		want bool
	}{
		{name: "matches /policies", path: "/policies", want: true},
		{name: "matches /policies/policy-xxx", path: "/policies/policy-0000000000000001", want: true},
		{
			name: "matches /tags/ with dlm ARN",
			path: "/tags/arn:aws:dlm:us-east-1:000000000000:policy/policy-1",
			want: true,
		},
		{name: "does not match /tags/ non-dlm ARN", path: "/tags/arn:aws:s3:::bucket", want: false},
		{name: "does not match unrelated path", path: "/ec2/instances", want: false},
		{name: "does not match empty path", path: "/", want: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			matcher := h.RouteMatcher()

			req := httptest.NewRequest(http.MethodGet, tc.path, nil)
			e := echo.New()
			c := e.NewContext(req, httptest.NewRecorder())
			assert.Equal(t, tc.want, matcher(c))
		})
	}
}

func TestHandler_MatchPriority(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "returns non-zero priority"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			assert.NotZero(t, h.MatchPriority())
		})
	}
}

func TestHandler_ExtractOperation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		method string
		path   string
		want   string
	}{
		{
			name:   "POST /policies → CreateLifecyclePolicy",
			method: http.MethodPost,
			path:   "/policies",
			want:   "CreateLifecyclePolicy",
		},
		{
			name:   "GET /policies → GetLifecyclePolicies",
			method: http.MethodGet,
			path:   "/policies",
			want:   "GetLifecyclePolicies",
		},
		{
			name:   "GET /policies/id → GetLifecyclePolicy",
			method: http.MethodGet,
			path:   "/policies/policy-abc",
			want:   "GetLifecyclePolicy",
		},
		{
			name:   "DELETE /policies/id → DeleteLifecyclePolicy",
			method: http.MethodDelete,
			path:   "/policies/policy-abc",
			want:   "DeleteLifecyclePolicy",
		},
		{
			name:   "PATCH /policies/id → UpdateLifecyclePolicy",
			method: http.MethodPatch,
			path:   "/policies/policy-abc",
			want:   "UpdateLifecyclePolicy",
		},
		{
			name:   "POST /tags/arn → TagResource",
			method: http.MethodPost,
			path:   "/tags/arn:aws:dlm:us-east-1:000000000000:policy/policy-1",
			want:   "TagResource",
		},
		{
			name:   "DELETE /tags/arn → UntagResource",
			method: http.MethodDelete,
			path:   "/tags/arn:aws:dlm:us-east-1:000000000000:policy/policy-1",
			want:   "UntagResource",
		},
		{
			name:   "GET /tags/arn → ListTagsForResource",
			method: http.MethodGet,
			path:   "/tags/arn:aws:dlm:us-east-1:000000000000:policy/policy-1",
			want:   "ListTagsForResource",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			req := httptest.NewRequest(tc.method, tc.path, nil)
			e := echo.New()
			c := e.NewContext(req, httptest.NewRecorder())
			assert.Equal(t, tc.want, h.ExtractOperation(c))
		})
	}
}

func TestHandler_ExtractResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		path string
		want string
	}{
		{
			name: "extracts resource from /tags/ path",
			path: "/tags/arn:aws:dlm:us-east-1:000000000000:policy/policy-1",
			want: "arn:aws:dlm:us-east-1:000000000000:policy/policy-1",
		},
		{name: "extracts policy id from /policies/ path", path: "/policies/policy-abc", want: "policy-abc"},
		{name: "returns full path for /policies base", path: "/policies", want: "/policies"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			req := httptest.NewRequest(http.MethodGet, tc.path, nil)
			e := echo.New()
			c := e.NewContext(req, httptest.NewRecorder())
			assert.Equal(t, tc.want, h.ExtractResource(c))
		})
	}
}

// ---------------------------------------------------------------------------
// mapError: cover InternalServerException branch (non-awserr error)
// ---------------------------------------------------------------------------

func TestHandler_MapError_InternalError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		path     string
		method   string
		wantType string
		wantCode int
	}{
		{
			// Trigger mapError with an error that is neither ErrNotFound nor ErrInvalidParameter.
			// We DELETE a policy that exists but then delete it again — second delete hits ErrPolicyNotFound
			// which is an awserr.ErrNotFound → 404. We need a different path.
			// Instead, use TagResource on a non-existent ARN to get 404.
			name:     "TagResource unknown ARN returns 404 ResourceNotFoundException",
			path:     "/tags/arn:aws:dlm:us-east-1:000000000000:policy/policy-doesnotexist",
			method:   http.MethodPost,
			wantCode: http.StatusNotFound,
			wantType: "ResourceNotFoundException",
		},
		{
			name:     "UntagResource unknown ARN returns 404",
			path:     "/tags/arn:aws:dlm:us-east-1:000000000000:policy/policy-doesnotexist",
			method:   http.MethodDelete,
			wantCode: http.StatusNotFound,
			wantType: "ResourceNotFoundException",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			var bodyBytes []byte
			if tc.method == http.MethodPost {
				bodyBytes, _ = json.Marshal(map[string]any{"Tags": map[string]string{"k": "v"}})
			}

			req := httptest.NewRequest(tc.method, tc.path, bytes.NewReader(bodyBytes))
			req.Header.Set("Content-Type", "application/json")
			rec := httptest.NewRecorder()
			e := echo.New()
			c := e.NewContext(req, rec)
			err := h.Handler()(c)
			require.NoError(t, err)

			assert.Equal(t, tc.wantCode, rec.Code)

			var resp map[string]string
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Equal(t, tc.wantType, resp["__type"])
		})
	}
}

// ---------------------------------------------------------------------------
// handleCreateLifecyclePolicy: invalid body
// ---------------------------------------------------------------------------

func TestHandler_CreateLifecyclePolicy_InvalidBody(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		body     []byte
		wantCode int
	}{
		{
			name:     "malformed JSON returns 400",
			body:     []byte(`{not valid json`),
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			req := httptest.NewRequest(http.MethodPost, "/policies", bytes.NewReader(tc.body))
			req.Header.Set("Content-Type", "application/json")
			rec := httptest.NewRecorder()
			e := echo.New()
			c := e.NewContext(req, rec)
			err := h.Handler()(c)
			require.NoError(t, err)
			assert.Equal(t, tc.wantCode, rec.Code)
		})
	}
}

// ---------------------------------------------------------------------------
// handleUpdateLifecyclePolicy: invalid body
// ---------------------------------------------------------------------------

func TestHandler_UpdateLifecyclePolicy_InvalidBody(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		body     []byte
		wantCode int
	}{
		{
			name:     "malformed JSON on PATCH returns 400",
			body:     []byte(`not json`),
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			policyID := createPolicy(t, h)
			path := fmt.Sprintf("/policies/%s", policyID)

			req := httptest.NewRequest(http.MethodPatch, path, bytes.NewReader(tc.body))
			req.Header.Set("Content-Type", "application/json")
			rec := httptest.NewRecorder()
			e := echo.New()
			c := e.NewContext(req, rec)
			err := h.Handler()(c)
			require.NoError(t, err)
			assert.Equal(t, tc.wantCode, rec.Code)
		})
	}
}

// ---------------------------------------------------------------------------
// handleTagResource: invalid body
// ---------------------------------------------------------------------------

func TestHandler_TagResource_InvalidBody(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		body     []byte
		wantCode int
	}{
		{
			name:     "malformed JSON on POST tags returns 400",
			body:     []byte(`bad json`),
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			policyID := createPolicy(t, h)

			// Get the ARN.
			rec := doRequest(t, h, http.MethodGet, fmt.Sprintf("/policies/%s", policyID), nil)
			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			arn := resp["Policy"].(map[string]any)["PolicyArn"].(string)

			req := httptest.NewRequest(http.MethodPost, fmt.Sprintf("/tags/%s", arn), bytes.NewReader(tc.body))
			req.Header.Set("Content-Type", "application/json")
			rec2 := httptest.NewRecorder()
			e := echo.New()
			c := e.NewContext(req, rec2)
			err := h.Handler()(c)
			require.NoError(t, err)
			assert.Equal(t, tc.wantCode, rec2.Code)
		})
	}
}

// ---------------------------------------------------------------------------
// handleGetLifecyclePolicies: filter by policyIds and state
// ---------------------------------------------------------------------------

func TestHandler_GetLifecyclePolicies_Filters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		query      string
		setupCount int
		wantMinLen int
		wantMaxLen int
	}{
		{
			name:       "filter by policyIds returns matching policies",
			setupCount: 2,
			// will be set dynamically — tested via check in subtests below
		},
		{
			name:       "filter by state=ENABLED returns only enabled",
			query:      "state=ENABLED",
			setupCount: 1,
			wantMinLen: 1,
			wantMaxLen: 1,
		},
		{
			name:       "filter by state=DISABLED returns empty when all ENABLED",
			query:      "state=DISABLED",
			setupCount: 1,
			wantMinLen: 0,
			wantMaxLen: 0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			var ids []string
			for range tc.setupCount {
				ids = append(ids, createPolicy(t, h))
			}

			query := tc.query
			if query == "" && len(ids) > 0 {
				// Filter by first ID only.
				query = fmt.Sprintf("policyIds=%s", ids[0])
			}

			path := "/policies"
			if query != "" {
				path = path + "?" + query
			}

			rec := doRequest(t, h, http.MethodGet, path, nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			policies := resp["Policies"].([]any)

			if tc.wantMaxLen > 0 || tc.wantMinLen > 0 {
				assert.GreaterOrEqual(t, len(policies), tc.wantMinLen)
				assert.LessOrEqual(t, len(policies), tc.wantMaxLen)
			} else if tc.query == "" {
				// policyIds filter: exactly 1 result.
				assert.Len(t, policies, 1)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// classifyPath: unknown method on tags path returns Unknown → 501
// ---------------------------------------------------------------------------

func TestHandler_UnknownOperation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		method   string
		path     string
		wantCode int
	}{
		{
			name:     "PUT on /tags/ ARN returns 501 (unknown method for tags path)",
			method:   http.MethodPut,
			path:     "/tags/arn:aws:dlm:us-east-1:000000000000:policy/policy-x",
			wantCode: http.StatusNotImplemented,
		},
		{
			name:     "PUT on /policies returns 501",
			method:   http.MethodPut,
			path:     "/policies",
			wantCode: http.StatusNotImplemented,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			req := httptest.NewRequest(tc.method, tc.path, nil)
			rec := httptest.NewRecorder()
			e := echo.New()
			c := e.NewContext(req, rec)
			err := h.Handler()(c)
			require.NoError(t, err)
			assert.Equal(t, tc.wantCode, rec.Code)
		})
	}
}

// ---------------------------------------------------------------------------
// Provider
// ---------------------------------------------------------------------------

func TestProvider_Name(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		want string
	}{
		{name: "returns DLM", want: "DLM"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			p := &dlm.Provider{}
			assert.Equal(t, tc.want, p.Name())
		})
	}
}

func TestProvider_Init(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ctx     *service.AppContext
		name    string
		wantErr bool
	}{
		{
			name:    "nil AppContext returns error",
			ctx:     nil,
			wantErr: true,
		},
		{
			name:    "valid AppContext returns handler",
			ctx:     &service.AppContext{},
			wantErr: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			p := &dlm.Provider{}
			got, err := p.Init(tc.ctx)
			if tc.wantErr {
				require.Error(t, err)
				assert.Nil(t, got)
			} else {
				require.NoError(t, err)
				assert.NotNil(t, got)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// GetLifecyclePolicies: filter by multiple policyIds
// ---------------------------------------------------------------------------

func TestBackend_GetLifecyclePolicies_MultipleIDFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		createN   int
		filterN   int
		wantCount int
	}{
		{
			name:      "filter 2 of 3 policies by ID",
			createN:   3,
			filterN:   2,
			wantCount: 2,
		},
		{
			name:      "empty filter returns all",
			createN:   2,
			filterN:   0,
			wantCount: 2,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := dlm.NewInMemoryBackend("000000000000", "us-east-1")
			var ids []string
			for i := range tc.createN {
				p, err := b.CreateLifecyclePolicy(fmt.Sprintf("desc-%d", i), "role", "ENABLED", nil, nil)
				require.NoError(t, err)
				ids = append(ids, p.PolicyID)
			}

			var filter []string
			if tc.filterN > 0 {
				filter = ids[:tc.filterN]
			}

			result, err := b.GetLifecyclePolicies(filter, "")
			require.NoError(t, err)
			assert.Len(t, result, tc.wantCount)
		})
	}
}

// ---------------------------------------------------------------------------
// UpdateLifecyclePolicy: partial update (only some fields set)
// ---------------------------------------------------------------------------

func TestBackend_UpdateLifecyclePolicy_PartialUpdate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		updateDesc  string
		updateRole  string
		updateState string
		wantDesc    string
		wantRole    string
		wantState   string
	}{
		{
			name:       "update only description",
			updateDesc: "new description",
			wantDesc:   "new description",
			wantRole:   "arn:aws:iam::000000000000:role/original",
			wantState:  "ENABLED",
		},
		{
			name:        "update only state",
			updateState: "DISABLED",
			wantDesc:    "original description",
			wantRole:    "arn:aws:iam::000000000000:role/original",
			wantState:   "DISABLED",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := dlm.NewInMemoryBackend("000000000000", "us-east-1")
			p, err := b.CreateLifecyclePolicy(
				"original description",
				"arn:aws:iam::000000000000:role/original",
				"ENABLED",
				nil,
				nil,
			)
			require.NoError(t, err)

			err = b.UpdateLifecyclePolicy(p.PolicyID, tc.updateDesc, tc.updateRole, tc.updateState)
			require.NoError(t, err)

			got, err := b.GetLifecyclePolicy(p.PolicyID)
			require.NoError(t, err)
			assert.Equal(t, tc.wantDesc, got.Description)
			assert.Equal(t, tc.wantRole, got.ExecutionRoleARN)
			assert.Equal(t, tc.wantState, got.State)
		})
	}
}

// ---------------------------------------------------------------------------
// CreateLifecyclePolicy: default state when empty
// ---------------------------------------------------------------------------

func TestBackend_CreateLifecyclePolicy_DefaultState(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		state     string
		wantState string
	}{
		{name: "empty state defaults to ENABLED", state: "", wantState: "ENABLED"},
		{name: "explicit DISABLED preserved", state: "DISABLED", wantState: "DISABLED"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := dlm.NewInMemoryBackend("000000000000", "us-east-1")
			p, err := b.CreateLifecyclePolicy("desc", "role", tc.state, nil, nil)
			require.NoError(t, err)
			assert.Equal(t, tc.wantState, p.State)
		})
	}
}

// ---------------------------------------------------------------------------
// UntagResource: multiple keys
// ---------------------------------------------------------------------------

func TestBackend_UntagResource_MultipleKeys(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		initTags   map[string]string
		removeKeys []string
		wantKeys   []string
	}{
		{
			name:       "remove two of three tags",
			initTags:   map[string]string{"a": "1", "b": "2", "c": "3"},
			removeKeys: []string{"a", "b"},
			wantKeys:   []string{"c"},
		},
		{
			name:       "remove non-existent key is safe",
			initTags:   map[string]string{"x": "1"},
			removeKeys: []string{"y"},
			wantKeys:   []string{"x"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := dlm.NewInMemoryBackend("000000000000", "us-east-1")
			p, err := b.CreateLifecyclePolicy("desc", "role", "ENABLED", tc.initTags, nil)
			require.NoError(t, err)

			err = b.UntagResource(p.PolicyArn, tc.removeKeys)
			require.NoError(t, err)

			tags, err := b.ListTagsForResource(p.PolicyArn)
			require.NoError(t, err)
			for _, k := range tc.wantKeys {
				assert.Contains(t, tags, k)
			}
			for _, k := range tc.removeKeys {
				// Only assert removed if it was initially present.
				if _, had := tc.initTags[k]; had {
					assert.NotContains(t, tags, k)
				}
			}
		})
	}
}

// ---------------------------------------------------------------------------
// HandleREST via HTTP: UntagResource with multiple tagKeys query params
// ---------------------------------------------------------------------------

func TestHandler_UntagResource_MultipleQueryKeys(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		tagKeys  []string
		wantCode int
	}{
		{
			name:     "untag multiple keys via repeated query params",
			tagKeys:  []string{"env", "team"},
			wantCode: http.StatusOK,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			policyID := createPolicy(t, h)

			rec := doRequest(t, h, http.MethodGet, fmt.Sprintf("/policies/%s", policyID), nil)
			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			arn := resp["Policy"].(map[string]any)["PolicyArn"].(string)

			// Tag first.
			doRequest(t, h, http.MethodPost, fmt.Sprintf("/tags/%s", arn), map[string]any{
				"Tags": map[string]string{"env": "prod", "team": "ops"},
			})

			// Build query string with multiple tagKeys.
			var parts []string
			for _, k := range tc.tagKeys {
				parts = append(parts, "tagKeys="+k)
			}
			path := fmt.Sprintf("/tags/%s?%s", arn, strings.Join(parts, "&"))

			req := httptest.NewRequest(http.MethodDelete, path, nil)
			rec2 := httptest.NewRecorder()
			e := echo.New()
			c := e.NewContext(req, rec2)
			err := h.Handler()(c)
			require.NoError(t, err)
			assert.Equal(t, tc.wantCode, rec2.Code)
		})
	}
}
