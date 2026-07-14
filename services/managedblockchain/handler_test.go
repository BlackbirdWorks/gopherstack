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
		wantKey    string
		wantStatus int
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
		op         string
		wantStatus int
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
			assert.Positive(t, h.MatchPriority())
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
		name          string
		method        string
		path          string
		wantOperation string
		wantResource  string
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
			// Real aws-sdk-go-v2 wire shape: node paths nest directly under the
			// network, NOT under the member (see serializers.go's opPath for
			// CreateNode/GetNode/ListNodes/DeleteNode/UpdateNode, which all
			// resolve to "/networks/{NetworkId}/nodes[/{NodeId}]" -- MemberId
			// travels as a query parameter or body field, never in the URI).
			name:          "create node",
			method:        http.MethodPost,
			path:          "/networks/net123/nodes",
			wantOperation: "CreateNode",
			wantResource:  "net123",
		},
		{
			name:          "list nodes",
			method:        http.MethodGet,
			path:          "/networks/net123/nodes",
			wantOperation: "ListNodes",
			wantResource:  "net123",
		},
		{
			name:          "get node",
			method:        http.MethodGet,
			path:          "/networks/net123/nodes/node456",
			wantOperation: "GetNode",
			wantResource:  "net123/node456",
		},
		{
			name:          "delete node",
			method:        http.MethodDelete,
			path:          "/networks/net123/nodes/node456",
			wantOperation: "DeleteNode",
			wantResource:  "net123/node456",
		},
		{
			name:          "update node",
			method:        http.MethodPatch,
			path:          "/networks/net123/nodes/node456",
			wantOperation: "UpdateNode",
			wantResource:  "net123/node456",
		},
		{
			// The old (never-real) member-nested node shape must NOT resolve
			// to any operation -- it never matches a real SDK request.
			name:          "member-nested node shape is not a valid route",
			method:        http.MethodPost,
			path:          "/networks/net123/members/mem456/nodes",
			wantOperation: "",
			wantResource:  "",
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
		body       map[string]any
		name       string
		method     string
		path       string
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

func createTestNetwork(t *testing.T, h *managedblockchain.Handler) (string, string) {
	t.Helper()

	rec := doRequest(t, h, http.MethodPost, "/networks", map[string]any{
		"Name":                "test-net",
		"MemberConfiguration": map[string]any{"Name": "member-1"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		NetworkID string `json:"NetworkId"`
		MemberID  string `json:"MemberId"`
	}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))

	return out.NetworkID, out.MemberID
}

// doRoutedRequest sends a request through both h.RouteMatcher() (with a
// managedblockchain Authorization header, as a real SDK client would send)
// and h.Handler(), proving the route is accepted by the matcher AND
// resolves to the right operation -- not just that Handler()'s internal
// parsePath accepts it directly. See .claude/memories/parity-principles.md's
// route-matcher bug class: unit tests calling h.Handler() alone can miss a
// matcher/parser mismatch that a real client would hit.
func doRoutedRequest(
	t *testing.T, h *managedblockchain.Handler, method, path string, body any,
) *httptest.ResponseRecorder {
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

	req.Header.Set("Authorization",
		"AWS4-HMAC-SHA256 Credential=test/20230101/us-east-1/managedblockchain/aws4_request")

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	require.True(t, h.RouteMatcher()(c),
		"RouteMatcher rejected a real managedblockchain wire path: %s %s", method, path)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

// TestHandler_NodeLifecycle_RealWireShape drives CreateNode/GetNode/ListNodes/
// UpdateNode/DeleteNode through the exact path+body/query shape a real
// aws-sdk-go-v2 managedblockchain client sends (MemberId in the CreateNode
// body, "memberId" as a query parameter everywhere else, node paths nested
// directly under the network -- never under the member). Regressing to the
// old, never-real "/networks/{id}/members/{id}/nodes" shape would 404 every
// case here.
func TestHandler_NodeLifecycle_RealWireShape(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	netID, memID := createTestNetwork(t, h)

	createRec := doRoutedRequest(t, h, http.MethodPost, "/networks/"+netID+"/nodes", map[string]any{
		"MemberId": memID,
		"NodeConfiguration": map[string]any{
			"InstanceType":     "bc.t3.small",
			"AvailabilityZone": "us-east-1a",
		},
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createOut struct {
		NodeID string `json:"NodeId"`
	}
	require.NoError(t, json.NewDecoder(createRec.Body).Decode(&createOut))
	require.NotEmpty(t, createOut.NodeID)

	nodePath := "/networks/" + netID + "/nodes/" + createOut.NodeID + "?memberId=" + memID

	getRec := doRoutedRequest(t, h, http.MethodGet, nodePath, nil)
	assert.Equal(t, http.StatusOK, getRec.Code)
	assert.Contains(t, getRec.Body.String(), createOut.NodeID)

	listRec := doRoutedRequest(t, h, http.MethodGet, "/networks/"+netID+"/nodes?memberId="+memID, nil)
	assert.Equal(t, http.StatusOK, listRec.Code)

	patchRec := doRoutedRequest(t, h, http.MethodPatch, nodePath, map[string]any{
		"LogPublishingConfiguration": map[string]any{
			"Fabric": map[string]any{
				"ChaincodeLogs": map[string]any{"CloudWatch": map[string]any{"Enabled": true}},
			},
		},
	})
	assert.Equal(t, http.StatusNoContent, patchRec.Code)

	delRec := doRoutedRequest(t, h, http.MethodDelete, nodePath, nil)
	assert.Equal(t, http.StatusNoContent, delRec.Code)

	confirmRec := doRoutedRequest(t, h, http.MethodGet, nodePath, nil)
	assert.Equal(t, http.StatusNotFound, confirmRec.Code)
}

func TestHandler_CreateNode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		nodeConfig map[string]any
		name       string
		networkID  string
		wantKey    string
		wantStatus int
		omitMember bool
	}{
		{
			name: "success",
			nodeConfig: map[string]any{
				"InstanceType":     "bc.t3.small",
				"AvailabilityZone": "us-east-1a",
			},
			wantStatus: http.StatusOK,
			wantKey:    "NodeId",
		},
		{
			name:       "network_not_found",
			nodeConfig: map[string]any{"InstanceType": "bc.t3.small"},
			networkID:  "nonexistent-network-id",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "missing_member_id",
			nodeConfig: map[string]any{"InstanceType": "bc.t3.small"},
			omitMember: true,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			netID, memID := createTestNetwork(t, h)

			if tt.networkID != "" {
				netID = tt.networkID
			}

			body := map[string]any{"NodeConfiguration": tt.nodeConfig}
			if !tt.omitMember {
				body["MemberId"] = memID
			}

			path := fmt.Sprintf("/networks/%s/nodes", netID)
			rec := doRequest(t, h, http.MethodPost, path, body)

			assert.Equal(t, tt.wantStatus, rec.Code)
			if tt.wantKey != "" {
				assert.Contains(t, rec.Body.String(), tt.wantKey)
			}
		})
	}
}

func TestHandler_GetNode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
	}{
		{
			name:       "success",
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			netID, memID := createTestNetwork(t, h)
			nodesPath := fmt.Sprintf("/networks/%s/nodes", netID)

			createRec := doRequest(t, h, http.MethodPost, nodesPath, map[string]any{
				"MemberId": memID,
				"NodeConfiguration": map[string]any{
					"InstanceType":     "bc.t3.small",
					"AvailabilityZone": "us-east-1a",
				},
			})
			require.Equal(t, http.StatusOK, createRec.Code)

			var createOut struct {
				NodeID string `json:"NodeId"`
			}
			require.NoError(t, json.NewDecoder(createRec.Body).Decode(&createOut))

			getPath := fmt.Sprintf("%s/%s?memberId=%s", nodesPath, createOut.NodeID, memID)
			rec := doRequest(t, h, http.MethodGet, getPath, nil)
			assert.Equal(t, tt.wantStatus, rec.Code)
			assert.Contains(t, rec.Body.String(), "Node")
		})
	}
}

func TestHandler_ListNodes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		nodeCount int
		wantLen   int
	}{
		{name: "empty", nodeCount: 0, wantLen: 0},
		{name: "two_nodes", nodeCount: 2, wantLen: 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			netID, memID := createTestNetwork(t, h)
			nodesPath := fmt.Sprintf("/networks/%s/nodes", netID)

			for range tt.nodeCount {
				rec := doRequest(t, h, http.MethodPost, nodesPath, map[string]any{
					"MemberId": memID,
					"NodeConfiguration": map[string]any{
						"InstanceType": "bc.t3.small",
					},
				})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			listRec := doRequest(t, h, http.MethodGet, nodesPath+"?memberId="+memID, nil)
			require.Equal(t, http.StatusOK, listRec.Code)

			var out struct {
				Nodes []map[string]any `json:"Nodes"`
			}
			require.NoError(t, json.NewDecoder(listRec.Body).Decode(&out))
			assert.Len(t, out.Nodes, tt.wantLen)
		})
	}
}

func TestHandler_DeleteNode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
	}{
		{name: "success", wantStatus: http.StatusNoContent},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			netID, memID := createTestNetwork(t, h)
			nodesPath := fmt.Sprintf("/networks/%s/nodes", netID)

			createRec := doRequest(t, h, http.MethodPost, nodesPath, map[string]any{
				"MemberId":          memID,
				"NodeConfiguration": map[string]any{"InstanceType": "bc.t3.small"},
			})
			require.Equal(t, http.StatusOK, createRec.Code)

			var createOut struct {
				NodeID string `json:"NodeId"`
			}
			require.NoError(t, json.NewDecoder(createRec.Body).Decode(&createOut))

			delPath := fmt.Sprintf("%s/%s?memberId=%s", nodesPath, createOut.NodeID, memID)
			rec := doRequest(t, h, http.MethodDelete, delPath, nil)
			assert.Equal(t, tt.wantStatus, rec.Code)

			// Confirm node is gone.
			getRec := doRequest(t, h, http.MethodGet, delPath, nil)
			assert.Equal(t, http.StatusNotFound, getRec.Code)
		})
	}
}

func TestHandler_CreateAccessor(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantKey    string
		wantStatus int
	}{
		{
			name: "success",
			body: map[string]any{
				"AccessorType": "BILLING_TOKEN",
				"NetworkType":  "ETHEREUM_MAINNET",
			},
			wantStatus: http.StatusOK,
			wantKey:    "AccessorId",
		},
		{
			name:       "empty body still creates accessor",
			body:       map[string]any{},
			wantStatus: http.StatusOK,
			wantKey:    "AccessorId",
		},
		{
			name:       "invalid json",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			var rec *httptest.ResponseRecorder
			if tt.body == nil {
				e := echo.New()
				req := httptest.NewRequest(http.MethodPost, "/accessors", bytes.NewReader([]byte("{bad json")))
				req.Header.Set("Content-Type", "application/json")
				w := httptest.NewRecorder()
				c := e.NewContext(req, w)
				err := h.Handler()(c)
				require.NoError(t, err)
				rec = w
			} else {
				rec = doRequest(t, h, http.MethodPost, "/accessors", tt.body)
			}

			assert.Equal(t, tt.wantStatus, rec.Code)
			if tt.wantKey != "" {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Contains(t, resp, tt.wantKey)
			}
		})
	}
}

func TestHandler_GetAccessor(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		accessorID string
		wantStatus int
	}{
		{
			name:       "get existing accessor",
			wantStatus: http.StatusOK,
		},
		{
			name:       "not found",
			accessorID: "nonexistent-accessor-id",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			createRec := doRequest(t, h, http.MethodPost, "/accessors", map[string]any{
				"AccessorType": "BILLING_TOKEN",
			})
			require.Equal(t, http.StatusOK, createRec.Code)

			var out struct {
				AccessorID string `json:"AccessorId"`
			}
			require.NoError(t, json.NewDecoder(createRec.Body).Decode(&out))

			id := tt.accessorID
			if id == "" {
				id = out.AccessorID
			}

			rec := doRequest(t, h, http.MethodGet, "/accessors/"+id, nil)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Contains(t, resp, "Accessor")
			}
		})
	}
}

func TestHandler_DeleteAccessor(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
	}{
		{name: "success", wantStatus: http.StatusNoContent},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			createRec := doRequest(t, h, http.MethodPost, "/accessors", map[string]any{
				"AccessorType": "BILLING_TOKEN",
			})
			require.Equal(t, http.StatusOK, createRec.Code)

			var out struct {
				AccessorID string `json:"AccessorId"`
			}
			require.NoError(t, json.NewDecoder(createRec.Body).Decode(&out))

			rec := doRequest(t, h, http.MethodDelete, "/accessors/"+out.AccessorID, nil)
			assert.Equal(t, tt.wantStatus, rec.Code)

			// Verify deleted.
			getRec := doRequest(t, h, http.MethodGet, "/accessors/"+out.AccessorID, nil)
			assert.Equal(t, http.StatusNotFound, getRec.Code)
		})
	}
}

func TestHandler_ListAccessors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		createCount int
		wantLen     int
		wantStatus  int
	}{
		{name: "empty list", createCount: 0, wantLen: 0, wantStatus: http.StatusOK},
		{name: "two accessors", createCount: 2, wantLen: 2, wantStatus: http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			for range tt.createCount {
				rec := doRequest(t, h, http.MethodPost, "/accessors", map[string]any{
					"AccessorType": "BILLING_TOKEN",
				})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doRequest(t, h, http.MethodGet, "/accessors", nil)
			assert.Equal(t, tt.wantStatus, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			accessors, ok := resp["Accessors"].([]any)
			require.True(t, ok)
			assert.Len(t, accessors, tt.wantLen)
		})
	}
}

func TestHandler_CreateProposal(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		networkID  string
		wantKey    string
		wantStatus int
	}{
		{
			name: "success",
			body: map[string]any{
				"MemberId":    "placeholder",
				"Description": "add member",
			},
			wantStatus: http.StatusOK,
			wantKey:    "ProposalId",
		},
		{
			name:       "missing member id",
			body:       map[string]any{"Description": "no member"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "network not found",
			body:       map[string]any{"MemberId": "some-member"},
			networkID:  "nonexistent-network",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			netID, memID := createTestNetwork(t, h)

			if tt.networkID != "" {
				netID = tt.networkID
			}

			// Use the real member ID for success cases.
			body := tt.body
			if body["MemberId"] == "placeholder" {
				body = map[string]any{
					"MemberId":    memID,
					"Description": tt.body["Description"],
				}
			}

			rec := doRequest(t, h, http.MethodPost, "/networks/"+netID+"/proposals", body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantKey != "" {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Contains(t, resp, tt.wantKey)
			}
		})
	}
}

func TestHandler_GetProposal(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		proposalID string
		wantStatus int
	}{
		{
			name:       "get existing proposal",
			wantStatus: http.StatusOK,
		},
		{
			name:       "not found",
			proposalID: "nonexistent-proposal",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			netID, memID := createTestNetwork(t, h)

			createRec := doRequest(t, h, http.MethodPost, "/networks/"+netID+"/proposals", map[string]any{
				"MemberId":    memID,
				"Description": "test proposal",
			})
			require.Equal(t, http.StatusOK, createRec.Code)

			var createOut struct {
				ProposalID string `json:"ProposalId"`
			}
			require.NoError(t, json.NewDecoder(createRec.Body).Decode(&createOut))

			pid := tt.proposalID
			if pid == "" {
				pid = createOut.ProposalID
			}

			rec := doRequest(t, h, http.MethodGet, "/networks/"+netID+"/proposals/"+pid, nil)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Contains(t, resp, "Proposal")
			}
		})
	}
}

func TestHandler_ListProposals(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		createCount int
		wantLen     int
	}{
		{name: "empty", createCount: 0, wantLen: 0},
		{name: "two proposals", createCount: 2, wantLen: 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			netID, memID := createTestNetwork(t, h)

			for range tt.createCount {
				rec := doRequest(t, h, http.MethodPost, "/networks/"+netID+"/proposals", map[string]any{
					"MemberId": memID,
				})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doRequest(t, h, http.MethodGet, "/networks/"+netID+"/proposals", nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp struct {
				Proposals []map[string]any `json:"Proposals"`
			}
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
			assert.Len(t, resp.Proposals, tt.wantLen)
		})
	}
}

func TestHandler_ListProposalVotes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
		wantLen    int
	}{
		{name: "empty votes", wantStatus: http.StatusOK, wantLen: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			netID, memID := createTestNetwork(t, h)

			createRec := doRequest(t, h, http.MethodPost, "/networks/"+netID+"/proposals", map[string]any{
				"MemberId": memID,
			})
			require.Equal(t, http.StatusOK, createRec.Code)

			var createOut struct {
				ProposalID string `json:"ProposalId"`
			}
			require.NoError(t, json.NewDecoder(createRec.Body).Decode(&createOut))

			path := fmt.Sprintf("/networks/%s/proposals/%s/votes", netID, createOut.ProposalID)
			rec := doRequest(t, h, http.MethodGet, path, nil)
			assert.Equal(t, tt.wantStatus, rec.Code)

			var resp struct {
				ProposalVotes []map[string]any `json:"ProposalVotes"`
			}
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
			assert.Len(t, resp.ProposalVotes, tt.wantLen)
		})
	}
}

func TestHandler_ListInvitations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		addSeed bool
		wantLen int
	}{
		{name: "empty", addSeed: false, wantLen: 0},
		{name: "with seed invitation", addSeed: true, wantLen: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := managedblockchain.NewInMemoryBackend()
			h := managedblockchain.NewHandler(b)
			h.AccountID = testAccountID
			h.DefaultRegion = testRegion

			if tt.addSeed {
				b.AddInvitationInternal(testRegion, testAccountID, "net-id", "net-name")
			}

			rec := doRequest(t, h, http.MethodGet, "/invitations", nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp struct {
				Invitations []map[string]any `json:"Invitations"`
			}
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
			assert.Len(t, resp.Invitations, tt.wantLen)
		})
	}
}

func TestHandler_RejectInvitation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		invitationID string
		wantStatus   int
	}{
		{
			name:       "success",
			wantStatus: http.StatusNoContent,
		},
		{
			name:         "not found",
			invitationID: "nonexistent-invitation",
			wantStatus:   http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := managedblockchain.NewInMemoryBackend()
			h := managedblockchain.NewHandler(b)
			h.AccountID = testAccountID
			h.DefaultRegion = testRegion

			inv := b.AddInvitationInternal(testRegion, testAccountID, "net-id", "net-name")

			id := tt.invitationID
			if id == "" {
				id = inv.InvitationID
			}

			rec := doRequest(t, h, http.MethodDelete, "/invitations/"+id, nil)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_DeleteNodeErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		nodeID     string
		wantStatus int
	}{
		{
			name:       "delete nonexistent node",
			nodeID:     "nonexistent-node",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			netID, memID := createTestNetwork(t, h)
			path := fmt.Sprintf("/networks/%s/nodes/%s?memberId=%s", netID, tt.nodeID, memID)

			rec := doRequest(t, h, http.MethodDelete, path, nil)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_UntagResourceQuery(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
	}{
		{
			name:       "untag resource with tags",
			wantStatus: http.StatusNoContent,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			netID, _ := createTestNetwork(t, h)

			// Get network ARN.
			netRec := doRequest(t, h, http.MethodGet, "/networks/"+netID, nil)
			require.Equal(t, http.StatusOK, netRec.Code)

			var netResp map[string]any
			require.NoError(t, json.Unmarshal(netRec.Body.Bytes(), &netResp))
			network := netResp["Network"].(map[string]any)
			arn := network["Arn"].(string)

			// Tag it.
			tagRec := doRequest(t, h, http.MethodPost, "/tags/"+arn,
				map[string]any{"Tags": map[string]string{"key1": "val1", "key2": "val2"}})
			require.Equal(t, http.StatusNoContent, tagRec.Code)

			// Untag.
			e := echo.New()
			req := httptest.NewRequest(http.MethodDelete, "/tags/"+arn+"?tagKeys=key1", http.NoBody)
			w := httptest.NewRecorder()
			c := e.NewContext(req, w)
			err := h.Handler()(c)
			require.NoError(t, err)
			assert.Equal(t, tt.wantStatus, w.Code)
		})
	}
}

func TestHandler_CreateProposalInvalidJSON(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "invalid json for create proposal"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			netID, _ := createTestNetwork(t, h)

			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/networks/"+netID+"/proposals",
				bytes.NewReader([]byte("{bad json")))
			req.Header.Set("Content-Type", "application/json")
			w := httptest.NewRecorder()
			c := e.NewContext(req, w)
			err := h.Handler()(c)
			require.NoError(t, err)
			assert.Equal(t, http.StatusBadRequest, w.Code)
		})
	}
}

func TestHandler_AccessorRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "create get list delete accessor"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			// Create.
			createRec := doRequest(t, h, http.MethodPost, "/accessors", map[string]any{
				"AccessorType": "BILLING_TOKEN",
				"NetworkType":  "ETHEREUM_MAINNET",
				"Tags":         map[string]string{"env": "test"},
			})
			require.Equal(t, http.StatusOK, createRec.Code)

			var createOut struct {
				AccessorID   string `json:"AccessorId"`
				BillingToken string `json:"BillingToken"`
				NetworkType  string `json:"NetworkType"`
			}
			require.NoError(t, json.NewDecoder(createRec.Body).Decode(&createOut))
			assert.NotEmpty(t, createOut.AccessorID)
			assert.NotEmpty(t, createOut.BillingToken)
			assert.Equal(t, "ETHEREUM_MAINNET", createOut.NetworkType)

			// Get.
			getRec := doRequest(t, h, http.MethodGet, "/accessors/"+createOut.AccessorID, nil)
			require.Equal(t, http.StatusOK, getRec.Code)

			var getOut struct {
				Accessor map[string]any `json:"Accessor"`
			}
			require.NoError(t, json.NewDecoder(getRec.Body).Decode(&getOut))
			assert.Equal(t, "AVAILABLE", getOut.Accessor["Status"])

			// List.
			listRec := doRequest(t, h, http.MethodGet, "/accessors", nil)
			require.Equal(t, http.StatusOK, listRec.Code)

			var listOut struct {
				Accessors []map[string]any `json:"Accessors"`
			}
			require.NoError(t, json.NewDecoder(listRec.Body).Decode(&listOut))
			assert.Len(t, listOut.Accessors, 1)

			// Delete.
			delRec := doRequest(t, h, http.MethodDelete, "/accessors/"+createOut.AccessorID, nil)
			assert.Equal(t, http.StatusNoContent, delRec.Code)

			// Verify gone.
			listRec2 := doRequest(t, h, http.MethodGet, "/accessors", nil)
			require.Equal(t, http.StatusOK, listRec2.Code)

			var listOut2 struct {
				Accessors []map[string]any `json:"Accessors"`
			}
			require.NoError(t, json.NewDecoder(listRec2.Body).Decode(&listOut2))
			assert.Empty(t, listOut2.Accessors)
		})
	}
}

func TestHandler_ProposalRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "create get list proposal with votes"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			netID, memID := createTestNetwork(t, h)

			// Create proposal.
			createRec := doRequest(t, h, http.MethodPost, "/networks/"+netID+"/proposals", map[string]any{
				"MemberId":    memID,
				"Description": "test governance proposal",
			})
			require.Equal(t, http.StatusOK, createRec.Code)

			var createOut struct {
				ProposalID string `json:"ProposalId"`
			}
			require.NoError(t, json.NewDecoder(createRec.Body).Decode(&createOut))
			assert.NotEmpty(t, createOut.ProposalID)

			// Get.
			getRec := doRequest(t, h, http.MethodGet,
				"/networks/"+netID+"/proposals/"+createOut.ProposalID, nil)
			require.Equal(t, http.StatusOK, getRec.Code)

			var getOut struct {
				Proposal map[string]any `json:"Proposal"`
			}
			require.NoError(t, json.NewDecoder(getRec.Body).Decode(&getOut))
			assert.Equal(t, "IN_PROGRESS", getOut.Proposal["Status"])

			// List votes.
			votesRec := doRequest(t, h, http.MethodGet,
				"/networks/"+netID+"/proposals/"+createOut.ProposalID+"/votes", nil)
			require.Equal(t, http.StatusOK, votesRec.Code)

			var votesOut struct {
				ProposalVotes []map[string]any `json:"ProposalVotes"`
			}
			require.NoError(t, json.NewDecoder(votesRec.Body).Decode(&votesOut))
			assert.Empty(t, votesOut.ProposalVotes)
		})
	}
}

func TestHandler_ProposalNotFoundErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		op         string
		wantStatus int
	}{
		{
			name:       "get proposal bad network",
			op:         "get_bad_network",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "list proposals bad network",
			op:         "list_bad_network",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "list proposal votes bad network",
			op:         "votes_bad_network",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			var rec *httptest.ResponseRecorder

			switch tt.op {
			case "get_bad_network":
				rec = doRequest(t, h, http.MethodGet, "/networks/nonexistent/proposals/pid", nil)
			case "list_bad_network":
				rec = doRequest(t, h, http.MethodGet, "/networks/nonexistent/proposals", nil)
			case "votes_bad_network":
				rec = doRequest(t, h, http.MethodGet, "/networks/nonexistent/proposals/pid/votes", nil)
			}

			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_AccessorDeleteNotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
	}{
		{name: "delete nonexistent accessor", wantStatus: http.StatusNotFound},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodDelete, "/accessors/nonexistent", nil)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_RouteMatcherNewPaths(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		path        string
		serviceName string
		wantMatch   bool
	}{
		{
			name:        "matches accessors path",
			path:        "/accessors",
			serviceName: "managedblockchain",
			wantMatch:   true,
		},
		{
			name:        "matches accessors sub-path",
			path:        "/accessors/some-id",
			serviceName: "managedblockchain",
			wantMatch:   true,
		},
		{
			name:        "matches invitations path",
			path:        "/invitations",
			serviceName: "managedblockchain",
			wantMatch:   true,
		},
		{
			name:        "matches invitations sub-path",
			path:        "/invitations/inv-id",
			serviceName: "managedblockchain",
			wantMatch:   true,
		},
		{
			name:        "does not match accessors with wrong service",
			path:        "/accessors",
			serviceName: "kms",
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
