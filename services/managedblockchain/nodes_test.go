package managedblockchain_test

import (
	"encoding/json"
	"fmt"
	"maps"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/managedblockchain"
)

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

func TestHandler_UpdateNode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		nodeID     string
		wantStatus int
	}{
		{
			name:       "happy path returns 204",
			wantStatus: http.StatusNoContent,
		},
		{
			name:       "node not found returns 404",
			nodeID:     "nonexistent-node",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := managedblockchain.NewInMemoryBackend()
			h := managedblockchain.NewHandler(b)
			h.AccountID = testAccountID
			h.DefaultRegion = testRegion

			net := b.AddNetworkInternal(testRegion, testAccountID, "test-network")
			mem := b.AddMemberInternal(testRegion, testAccountID, net.ID, "test-member")
			node := b.AddNodeInternal(testRegion, testAccountID, net.ID, mem.ID, "bc.t3.small")

			nodeID := node.ID
			if tt.nodeID != "" {
				nodeID = tt.nodeID
			}

			path := fmt.Sprintf("/networks/%s/nodes/%s?memberId=%s", net.ID, nodeID, mem.ID)
			rec := doRequest(t, h, http.MethodPatch, path, map[string]any{})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestHandler_ListNodesFilters verifies status filter for ListNodes.
func TestHandler_ListNodesFilters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		query     map[string]string
		name      string
		wantCount int
	}{
		{
			name:      "no filter returns all",
			query:     map[string]string{},
			wantCount: 2,
		},
		{
			name:      "filter by AVAILABLE status returns all",
			query:     map[string]string{"status": "AVAILABLE"},
			wantCount: 2,
		},
		{
			name:      "filter by nonexistent status returns none",
			query:     map[string]string{"status": "CREATING"},
			wantCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := managedblockchain.NewInMemoryBackend()
			n := b.AddNetworkInternal(testRegion, testAccountID, "net1")
			m := b.AddMemberInternal(testRegion, testAccountID, n.ID, "m1")
			b.AddNodeInternal(testRegion, testAccountID, n.ID, m.ID, "bc.t3.small.ethereum")
			b.AddNodeInternal(testRegion, testAccountID, n.ID, m.ID, "bc.t3.large.ethereum")

			h := managedblockchain.NewHandler(b)
			h.AccountID = testAccountID
			h.DefaultRegion = testRegion

			query := map[string]string{"memberId": m.ID}
			maps.Copy(query, tt.query)

			rec := doRequestWithQuery(t, h, fmt.Sprintf("/networks/%s/nodes", n.ID), query)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			nodes := resp["Nodes"].([]any)
			assert.Len(t, nodes, tt.wantCount)
		})
	}
}

// TestInMemoryBackend_ListNodesFilter verifies backend-level filtering for ListNodes.
func TestInMemoryBackend_ListNodesFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		filter    managedblockchain.ListNodesFilter
		wantCount int
	}{
		{
			name:      "empty filter returns all",
			filter:    managedblockchain.ListNodesFilter{},
			wantCount: 2,
		},
		{
			name:      "filter AVAILABLE returns all",
			filter:    managedblockchain.ListNodesFilter{Status: "AVAILABLE"},
			wantCount: 2,
		},
		{
			name:      "filter FAILED returns none",
			filter:    managedblockchain.ListNodesFilter{Status: "FAILED"},
			wantCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := managedblockchain.NewInMemoryBackend()
			n := b.AddNetworkInternal(testRegion, testAccountID, "net1")
			m := b.AddMemberInternal(testRegion, testAccountID, n.ID, "m1")
			b.AddNodeInternal(testRegion, testAccountID, n.ID, m.ID, "bc.t3.small.ethereum")
			b.AddNodeInternal(testRegion, testAccountID, n.ID, m.ID, "bc.t3.large.ethereum")

			nodes, err := b.ListNodes(n.ID, m.ID, tt.filter)
			require.NoError(t, err)
			assert.Len(t, nodes, tt.wantCount)
		})
	}
}

// TestHandler_NodeSummaryAvailabilityZone verifies AvailabilityZone in ListNodes response.
func TestHandler_NodeSummaryAvailabilityZone(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		instanceType string
		wantAZ       bool
	}{
		{
			name:         "node summary includes availability zone",
			instanceType: "bc.t3.small.ethereum",
			wantAZ:       true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := managedblockchain.NewInMemoryBackend()
			n := b.AddNetworkInternal(testRegion, testAccountID, "net1")
			m := b.AddMemberInternal(testRegion, testAccountID, n.ID, "m1")

			h := managedblockchain.NewHandler(b)
			h.AccountID = testAccountID
			h.DefaultRegion = testRegion

			// Create node with AZ
			rec := doRequest(
				t, h, http.MethodPost,
				fmt.Sprintf("/networks/%s/nodes", n.ID),
				map[string]any{
					"MemberId": m.ID,
					"NodeConfiguration": map[string]any{
						"InstanceType":     tt.instanceType,
						"AvailabilityZone": "us-east-1a",
					},
				},
			)
			require.Equal(t, http.StatusOK, rec.Code)

			// List nodes and check AZ in summary
			rec2 := doRequest(t, h, http.MethodGet,
				fmt.Sprintf("/networks/%s/nodes?memberId=%s", n.ID, m.ID), nil)
			require.Equal(t, http.StatusOK, rec2.Code)

			var listResp map[string]any
			require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &listResp))

			nodes := listResp["Nodes"].([]any)
			require.Len(t, nodes, 1)

			node := nodes[0].(map[string]any)

			if tt.wantAZ {
				az, ok := node["AvailabilityZone"]
				assert.True(t, ok, "AvailabilityZone should be in node summary")
				assert.Equal(t, "us-east-1a", az)
			}
		})
	}
}

// TestHandler_UpdateNodeLogPublishingConfig verifies node log config is stored and returned.
func TestHandler_UpdateNodeLogPublishingConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		chaincodeEnabled bool
		peerEnabled      bool
	}{
		{
			name:             "enable chaincode and peer logs",
			chaincodeEnabled: true,
			peerEnabled:      true,
		},
		{
			name:             "disable both logs",
			chaincodeEnabled: false,
			peerEnabled:      false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := managedblockchain.NewInMemoryBackend()
			n := b.AddNetworkInternal(testRegion, testAccountID, "net1")
			m := b.AddMemberInternal(testRegion, testAccountID, n.ID, "m1")
			node := b.AddNodeInternal(testRegion, testAccountID, n.ID, m.ID, "bc.t3.small.ethereum")

			h := managedblockchain.NewHandler(b)
			h.AccountID = testAccountID
			h.DefaultRegion = testRegion

			patchPath := fmt.Sprintf("/networks/%s/nodes/%s?memberId=%s", n.ID, node.ID, m.ID)

			rec := doRequest(t, h, http.MethodPatch, patchPath, map[string]any{
				"LogPublishingConfiguration": map[string]any{
					"Fabric": map[string]any{
						"ChaincodeLogs": map[string]any{
							"CloudWatch": map[string]any{"Enabled": tt.chaincodeEnabled},
						},
						"PeerLogs": map[string]any{
							"CloudWatch": map[string]any{"Enabled": tt.peerEnabled},
						},
					},
				},
			})
			require.Equal(t, http.StatusNoContent, rec.Code)

			// GetNode and verify
			rec2 := doRequest(t, h, http.MethodGet, patchPath, nil)
			require.Equal(t, http.StatusOK, rec2.Code)

			var getResp map[string]any
			require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &getResp))

			nodeResp := getResp["Node"].(map[string]any)
			logConfig, ok := nodeResp["LogPublishingConfiguration"]
			assert.True(t, ok, "LogPublishingConfiguration should be present after update")

			logConfigMap := logConfig.(map[string]any)
			fabric := logConfigMap["Fabric"].(map[string]any)

			ccLogs := fabric["ChaincodeLogs"].(map[string]any)
			ccCw := ccLogs["CloudWatch"].(map[string]any)
			assert.Equal(t, tt.chaincodeEnabled, ccCw["Enabled"])

			peerLogs := fabric["PeerLogs"].(map[string]any)
			peerCw := peerLogs["CloudWatch"].(map[string]any)
			assert.Equal(t, tt.peerEnabled, peerCw["Enabled"])
		})
	}
}

// TestHandler_CreateNodeWithTags verifies tags are persisted on CreateNode.
func TestHandler_CreateNodeWithTags(t *testing.T) {
	t.Parallel()

	b := managedblockchain.NewInMemoryBackend()
	n := b.AddNetworkInternal(testRegion, testAccountID, "net1")
	m := b.AddMemberInternal(testRegion, testAccountID, n.ID, "member1")
	h := managedblockchain.NewHandler(b)
	h.AccountID = testAccountID
	h.DefaultRegion = testRegion

	rec := doRequest(
		t, h, http.MethodPost,
		"/networks/"+n.ID+"/nodes",
		map[string]any{
			"MemberId": m.ID,
			"NodeConfiguration": map[string]any{
				"InstanceType":     "bc.t3.small.ethereum",
				"AvailabilityZone": "us-east-1a",
			},
			"Tags": map[string]string{"purpose": "mining"},
		},
	)

	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	nodeID, _ := resp["NodeId"].(string)
	require.NotEmpty(t, nodeID)

	// GetNode and verify tags
	rec2 := doRequest(t, h, http.MethodGet,
		"/networks/"+n.ID+"/nodes/"+nodeID+"?memberId="+m.ID, nil)
	require.Equal(t, http.StatusOK, rec2.Code)

	var nodeResp map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &nodeResp))

	node := nodeResp["Node"].(map[string]any)
	tags := node["Tags"].(map[string]any)
	assert.Equal(t, "mining", tags["purpose"])
}

// TestInMemoryBackend_GetNodeMemberNotFound verifies GetNode on deleted member returns not found.
func TestInMemoryBackend_GetNodeMemberNotFound(t *testing.T) {
	t.Parallel()

	b := managedblockchain.NewInMemoryBackend()
	n := b.AddNetworkInternal(testRegion, testAccountID, "net1")

	_, err := b.GetNode(n.ID, "nonexistent-member", "nonexistent-node")
	require.Error(t, err)
}

// TestHandler_NodeLifecycleBasicViaHTTP exercises Create/Get/List/Delete node over HTTP.
func TestHandler_NodeLifecycleBasicViaHTTP(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		instanceType string
		wantStatus   int
	}{
		{
			name:         "creates and retrieves node",
			instanceType: "bc.t3.small.ethereum",
			wantStatus:   http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := managedblockchain.NewInMemoryBackend()
			n := b.AddNetworkInternal(testRegion, testAccountID, "net1")
			m := b.AddMemberInternal(testRegion, testAccountID, n.ID, "member1")

			h := managedblockchain.NewHandler(b)
			h.AccountID = testAccountID
			h.DefaultRegion = testRegion

			rec := doRequest(
				t, h, http.MethodPost,
				"/networks/"+n.ID+"/nodes",
				map[string]any{
					"MemberId": m.ID,
					"NodeConfiguration": map[string]any{
						"InstanceType":     tt.instanceType,
						"AvailabilityZone": "us-east-1a",
					},
				},
			)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus != http.StatusOK {
				return
			}

			var createResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))

			nodeID, _ := createResp["NodeId"].(string)
			require.NotEmpty(t, nodeID)

			// GetNode
			rec2 := doRequest(t, h, http.MethodGet,
				"/networks/"+n.ID+"/nodes/"+nodeID+"?memberId="+m.ID, nil)
			require.Equal(t, http.StatusOK, rec2.Code)

			var getResp map[string]any
			require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &getResp))

			node := getResp["Node"].(map[string]any)
			assert.Equal(t, nodeID, node["Id"])
			assert.Equal(t, tt.instanceType, node["InstanceType"])

			// ListNodes
			rec3 := doRequest(t, h, http.MethodGet,
				"/networks/"+n.ID+"/nodes?memberId="+m.ID, nil)
			require.Equal(t, http.StatusOK, rec3.Code)

			var listResp map[string]any
			require.NoError(t, json.Unmarshal(rec3.Body.Bytes(), &listResp))
			nodes := listResp["Nodes"].([]any)
			assert.Len(t, nodes, 1)

			// DeleteNode
			rec4 := doRequest(t, h, http.MethodDelete,
				"/networks/"+n.ID+"/nodes/"+nodeID+"?memberId="+m.ID, nil)
			require.Equal(t, http.StatusNoContent, rec4.Code)

			// Verify deleted
			rec5 := doRequest(t, h, http.MethodGet,
				"/networks/"+n.ID+"/nodes/"+nodeID+"?memberId="+m.ID, nil)
			assert.Equal(t, http.StatusNotFound, rec5.Code)
		})
	}
}
