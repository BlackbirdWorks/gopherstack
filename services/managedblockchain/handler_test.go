package managedblockchain_test

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

	"github.com/blackbirdworks/gopherstack/services/managedblockchain"
)

func newTestHandler(t *testing.T) *managedblockchain.Handler {
	t.Helper()

	b := managedblockchain.NewInMemoryBackend()
	h := managedblockchain.NewHandler(b)
	h.AccountID = testAccountID
	h.DefaultRegion = testRegion

	return h
}

func doRequest(t *testing.T, h *managedblockchain.Handler, method, path string, body any) *httptest.ResponseRecorder {
	t.Helper()

	var bodyBytes []byte

	if body != nil {
		var err error
		bodyBytes, err = json.Marshal(body)
		require.NoError(t, err)
	}

	e := echo.New()

	var req *http.Request

	if len(bodyBytes) > 0 {
		req = httptest.NewRequest(method, path, bytes.NewReader(bodyBytes))
		req.Header.Set("Content-Type", "application/json")
	} else {
		req = httptest.NewRequest(method, path, http.NoBody)
	}

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

func TestHandler_CreateNetwork(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantStatus int
		wantKey    string
	}{
		{
			name:       "creates network",
			body:       map[string]any{"Name": "my-net", "MemberConfiguration": map[string]any{"Name": "m1"}},
			wantStatus: http.StatusOK,
			wantKey:    "NetworkId",
		},
		{
			name:       "missing network name",
			body:       map[string]any{"MemberConfiguration": map[string]any{"Name": "m1"}},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing member name",
			body:       map[string]any{"Name": "net1"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "duplicate network returns conflict",
			body:       map[string]any{"Name": "dup-net", "MemberConfiguration": map[string]any{"Name": "m1"}},
			wantStatus: http.StatusConflict,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.name == "duplicate network returns conflict" {
				rec := doRequest(t, h, http.MethodPost, "/networks", tt.body)
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doRequest(t, h, http.MethodPost, "/networks", tt.body)

			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantKey != "" {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Contains(t, resp, tt.wantKey)
			}
		})
	}
}

func TestHandler_GetNetwork(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		networkID  string
		wantStatus int
	}{
		{
			name:       "get existing network",
			wantStatus: http.StatusOK,
		},
		{
			name:       "not found",
			networkID:  "does-not-exist",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			rec := doRequest(t, h, http.MethodPost, "/networks",
				map[string]any{"Name": "net1", "MemberConfiguration": map[string]any{"Name": "m1"}})
			require.Equal(t, http.StatusOK, rec.Code)

			var createResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
			networkID := createResp["NetworkId"].(string)

			id := tt.networkID
			if id == "" {
				id = networkID
			}

			rec = doRequest(t, h, http.MethodGet, "/networks/"+id, nil)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Contains(t, resp, "Network")
			}
		})
	}
}

func TestHandler_ListNetworks(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		createN    int
		wantStatus int
	}{
		{
			name:       "lists zero networks",
			createN:    0,
			wantStatus: http.StatusOK,
		},
		{
			name:       "lists created networks",
			createN:    2,
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			for i := range tt.createN {
				rec := doRequest(t, h, http.MethodPost, "/networks",
					map[string]any{
						"Name":                fmt.Sprintf("net-%d", i),
						"MemberConfiguration": map[string]any{"Name": "m1"},
					})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doRequest(t, h, http.MethodGet, "/networks", nil)
			assert.Equal(t, tt.wantStatus, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			networks, ok := resp["Networks"].([]any)
			require.True(t, ok)
			assert.Len(t, networks, tt.createN)
		})
	}
}

func TestHandler_MemberLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "create get list delete member"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			// Create network
			rec := doRequest(t, h, http.MethodPost, "/networks",
				map[string]any{"Name": "net1", "MemberConfiguration": map[string]any{"Name": "initial"}})
			require.Equal(t, http.StatusOK, rec.Code)

			var createNetResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createNetResp))
			networkID := createNetResp["NetworkId"].(string)

			// Create member
			rec = doRequest(t, h, http.MethodPost, "/networks/"+networkID+"/members",
				map[string]any{"MemberConfiguration": map[string]any{"Name": "new-member"}})
			require.Equal(t, http.StatusOK, rec.Code)

			var createMemResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createMemResp))
			memberID := createMemResp["MemberId"].(string)
			assert.NotEmpty(t, memberID)

			// Get member
			rec = doRequest(t, h, http.MethodGet, "/networks/"+networkID+"/members/"+memberID, nil)
			assert.Equal(t, http.StatusOK, rec.Code)

			// List members - initial + new
			rec = doRequest(t, h, http.MethodGet, "/networks/"+networkID+"/members", nil)
			assert.Equal(t, http.StatusOK, rec.Code)

			var listResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
			members, ok := listResp["Members"].([]any)
			require.True(t, ok)
			assert.Len(t, members, 2)

			// Delete member
			rec = doRequest(t, h, http.MethodDelete, "/networks/"+networkID+"/members/"+memberID, nil)
			assert.Equal(t, http.StatusNoContent, rec.Code)

			// Verify not found
			rec = doRequest(t, h, http.MethodGet, "/networks/"+networkID+"/members/"+memberID, nil)
			assert.Equal(t, http.StatusNotFound, rec.Code)
		})
	}
}

func TestHandler_TagOperations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "list tag and untag network"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			// Create network
			rec := doRequest(t, h, http.MethodPost, "/networks",
				map[string]any{"Name": "tagged-net", "MemberConfiguration": map[string]any{"Name": "m1"}})
			require.Equal(t, http.StatusOK, rec.Code)

			var createResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))

			// Get the network to find its ARN
			networkID := createResp["NetworkId"].(string)
			rec = doRequest(t, h, http.MethodGet, "/networks/"+networkID, nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var netResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &netResp))
			network := netResp["Network"].(map[string]any)
			arn := network["Arn"].(string)
			assert.NotEmpty(t, arn)

			// TagResource
			rec = doRequest(t, h, http.MethodPost, "/tags/"+arn,
				map[string]any{"Tags": map[string]string{"env": "test"}})
			assert.Equal(t, http.StatusNoContent, rec.Code)

			// ListTagsForResource
			rec = doRequest(t, h, http.MethodGet, "/tags/"+arn, nil)
			assert.Equal(t, http.StatusOK, rec.Code)

			var tagsResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &tagsResp))
			tags := tagsResp["Tags"].(map[string]any)
			assert.Equal(t, "test", tags["env"])

			// UntagResource
			rec = doRequest(t, h, http.MethodDelete, "/tags/"+arn+"?tagKeys=env", nil)
			assert.Equal(t, http.StatusNoContent, rec.Code)
		})
	}
}

func TestHandler_MemberErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
		op         string
	}{
		{
			name:       "create member missing member name",
			wantStatus: http.StatusBadRequest,
			op:         "create_missing_name",
		},
		{
			name:       "create member in nonexistent network",
			wantStatus: http.StatusNotFound,
			op:         "create_bad_network",
		},
		{
			name:       "list members bad network",
			wantStatus: http.StatusNotFound,
			op:         "list_bad_network",
		},
		{
			name:       "delete member bad network",
			wantStatus: http.StatusNotFound,
			op:         "delete_bad_network",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			var rec *httptest.ResponseRecorder

			switch tt.op {
			case "create_missing_name":
				rec = doRequest(t, h, http.MethodPost, "/networks/net1/members",
					map[string]any{"MemberConfiguration": map[string]any{}})
			case "create_bad_network":
				rec = doRequest(t, h, http.MethodPost, "/networks/nonexistent/members",
					map[string]any{"MemberConfiguration": map[string]any{"Name": "m1"}})
			case "list_bad_network":
				rec = doRequest(t, h, http.MethodGet, "/networks/nonexistent/members", nil)
			case "delete_bad_network":
				rec = doRequest(t, h, http.MethodDelete, "/networks/nonexistent/members/mid", nil)
			}

			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_HandlerMetadata(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "metadata methods"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			assert.Equal(t, "ManagedBlockchain", h.Name())
			assert.NotEmpty(t, h.GetSupportedOperations())
			assert.Equal(t, "managedblockchain", h.ChaosServiceName())
			assert.NotEmpty(t, h.ChaosOperations())
			assert.NotEmpty(t, h.ChaosRegions())
			assert.Greater(t, h.MatchPriority(), 0)
		})
	}
}

func TestHandler_RouteMatcher(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		path        string
		serviceName string
		wantMatch   bool
	}{
		{
			name:        "matches networks path with correct service",
			path:        "/networks",
			serviceName: "managedblockchain",
			wantMatch:   true,
		},
		{
			name:        "matches networks sub-path",
			path:        "/networks/abc/members",
			serviceName: "managedblockchain",
			wantMatch:   true,
		},
		{
			name:        "does not match networks with wrong service",
			path:        "/networks",
			serviceName: "iotwireless",
			wantMatch:   false,
		},
		{
			name:        "does not match unknown path",
			path:        "/unknown",
			serviceName: "managedblockchain",
			wantMatch:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			matcher := h.RouteMatcher()

			e := echo.New()
			req := httptest.NewRequest(http.MethodGet, tt.path, http.NoBody)

			if tt.serviceName != "" {
				req.Header.Set("Authorization",
					"AWS4-HMAC-SHA256 Credential=test/20230101/us-east-1/"+tt.serviceName+"/aws4_request")
			}

			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			assert.Equal(t, tt.wantMatch, matcher(c))
		})
	}
}

func TestHandler_ExtractOperationAndResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		method         string
		path           string
		wantOperation  string
		wantResource   string
	}{
		{
			name:          "create network",
			method:        http.MethodPost,
			path:          "/networks",
			wantOperation: "CreateNetwork",
			wantResource:  "",
		},
		{
			name:          "get network",
			method:        http.MethodGet,
			path:          "/networks/net123",
			wantOperation: "GetNetwork",
			wantResource:  "net123",
		},
		{
			name:          "get member",
			method:        http.MethodGet,
			path:          "/networks/net123/members/mem456",
			wantOperation: "GetMember",
			wantResource:  "net123/mem456",
		},
		{
			name:          "unknown path",
			method:        http.MethodGet,
			path:          "/unknown",
			wantOperation: "",
			wantResource:  "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			e := echo.New()
			req := httptest.NewRequest(tt.method, tt.path, http.NoBody)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			assert.Equal(t, tt.wantOperation, h.ExtractOperation(c))
			assert.Equal(t, tt.wantResource, h.ExtractResource(c))
		})
	}
}

func TestHandler_TagErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		method     string
		path       string
		body       map[string]any
		wantStatus int
	}{
		{
			name:       "list tags not found",
			method:     http.MethodGet,
			path:       "/tags/arn:aws:managedblockchain:us-east-1:000000000000:networks/nonexistent",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "tag resource not found",
			method:     http.MethodPost,
			path:       "/tags/arn:aws:managedblockchain:us-east-1:000000000000:networks/nonexistent",
			body:       map[string]any{"Tags": map[string]string{"k": "v"}},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, tt.method, tt.path, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_CreateNetworkInvalidJSON(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "invalid json body"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			b := managedblockchain.NewInMemoryBackend()
			h := managedblockchain.NewHandler(b)
			h.AccountID = testAccountID
			h.DefaultRegion = testRegion

			req := httptest.NewRequest(http.MethodPost, "/networks",
				bytes.NewReader([]byte("{invalid json")))
			req.Header.Set("Content-Type", "application/json")

			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			err := h.Handler()(c)
			require.NoError(t, err)
			assert.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}
}

func TestHandler_UnknownPath(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		method     string
		path       string
		wantStatus int
	}{
		{
			name:       "unknown path",
			method:     http.MethodGet,
			path:       "/unknown-resource",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, tt.method, tt.path, nil)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}
