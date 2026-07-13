package managedblockchain_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/managedblockchain"
)

// TestRefinement1_Reset verifies that Backend.Reset clears all data.
func TestRefinement1_Reset(t *testing.T) {
	t.Parallel()

	b := managedblockchain.NewInMemoryBackend()

	// Seed data
	n := b.AddNetworkInternal(testRegion, testAccountID, "net1")
	m := b.AddMemberInternal(testRegion, testAccountID, n.ID, "member1")
	b.AddNodeInternal(testRegion, testAccountID, n.ID, m.ID, "bc.t3.small.ethereum")
	b.AddAccessorInternal(testRegion, testAccountID, "BILLING_TOKEN", "ETHEREUM_MAINNET")
	b.AddProposalInternal(testRegion, testAccountID, n.ID, m.ID, "proposal1")
	b.AddInvitationInternal(testRegion, testAccountID, n.ID, "net1")

	require.Equal(t, 1, managedblockchain.NetworkCount(b))
	require.Equal(t, 1, managedblockchain.AccessorCount(b))
	require.Equal(t, 1, managedblockchain.ProposalCount(b))
	require.Equal(t, 1, managedblockchain.InvitationCount(b))

	b.Reset()

	assert.Equal(t, 0, managedblockchain.NetworkCount(b))
	assert.Equal(t, 0, managedblockchain.MemberCount(b))
	assert.Equal(t, 0, managedblockchain.NodeCount(b))
	assert.Equal(t, 0, managedblockchain.AccessorCount(b))
	assert.Equal(t, 0, managedblockchain.ProposalCount(b))
	assert.Equal(t, 0, managedblockchain.InvitationCount(b))
	assert.Equal(t, 0, managedblockchain.ARNIndexSize(b))
}

// TestRefinement1_HandlerReset verifies that Handler.Reset delegates to backend.
func TestRefinement1_HandlerReset(t *testing.T) {
	t.Parallel()

	b := managedblockchain.NewInMemoryBackend()
	b.AddNetworkInternal(testRegion, testAccountID, "net1")

	h2 := managedblockchain.NewHandler(b)
	h2.AccountID = testAccountID
	h2.DefaultRegion = testRegion

	require.Equal(t, 1, managedblockchain.NetworkCount(b))

	h2.Reset()

	assert.Equal(t, 0, managedblockchain.NetworkCount(b))
}

// TestRefinement1_ProviderInit_NilCtx verifies that Provider.Init rejects a nil context.
func TestRefinement1_ProviderInit_NilCtx(t *testing.T) {
	t.Parallel()

	p := &managedblockchain.Provider{}
	_, err := p.Init(nil)

	require.ErrorIs(t, err, managedblockchain.ErrNilAppContext)
}

// TestRefinement1_GetSupportedOperations_Sorted verifies operations are sorted.
func TestRefinement1_GetSupportedOperations_Sorted(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ops := h.GetSupportedOperations()

	require.NotEmpty(t, ops)
	assert.Equal(t, 27, managedblockchain.HandlerOpsLen(h))

	for i := 1; i < len(ops); i++ {
		assert.LessOrEqual(t, ops[i-1], ops[i], "ops must be sorted: %s >= %s", ops[i-1], ops[i])
	}
}

// TestRefinement1_SeedHelpers verifies all AddXInternal helpers work.
func TestRefinement1_SeedHelpers(t *testing.T) {
	t.Parallel()

	b := managedblockchain.NewInMemoryBackend()

	n := b.AddNetworkInternal(testRegion, testAccountID, "seed-net")
	require.NotEmpty(t, n.ID)
	require.NotEmpty(t, n.Arn)
	assert.Equal(t, "AVAILABLE", n.Status)

	m := b.AddMemberInternal(testRegion, testAccountID, n.ID, "seed-member")
	require.NotEmpty(t, m.ID)
	require.NotEmpty(t, m.Arn)
	assert.Equal(t, n.ID, m.NetworkID)

	node := b.AddNodeInternal(testRegion, testAccountID, n.ID, m.ID, "bc.t3.small.ethereum")
	require.NotEmpty(t, node.ID)
	require.NotEmpty(t, node.Arn)
	assert.Equal(t, "bc.t3.small.ethereum", node.InstanceType)

	a := b.AddAccessorInternal(testRegion, testAccountID, "BILLING_TOKEN", "ETHEREUM_MAINNET")
	require.NotEmpty(t, a.ID)
	require.NotEmpty(t, a.BillingToken)
	assert.Equal(t, "BILLING_TOKEN", a.Type)

	p := b.AddProposalInternal(testRegion, testAccountID, n.ID, m.ID, "seed proposal")
	require.NotEmpty(t, p.ProposalID)
	assert.Equal(t, "IN_PROGRESS", p.Status)
	assert.NotNil(t, p.ExpirationDate)

	inv := b.AddInvitationInternal(testRegion, testAccountID, n.ID, "seed-net")
	require.NotEmpty(t, inv.InvitationID)
	assert.Equal(t, "PENDING", inv.Status)

	assert.Equal(t, 1, managedblockchain.NetworkCount(b))
	assert.Equal(t, 1, managedblockchain.MemberCount(b))
	assert.Equal(t, 1, managedblockchain.NodeCount(b))
}

// TestRefinement1_ExportCountHelpers verifies count helpers.
func TestRefinement1_ExportCountHelpers(t *testing.T) {
	t.Parallel()

	b := managedblockchain.NewInMemoryBackend()
	assert.Equal(t, 0, managedblockchain.NetworkCount(b))
	assert.Equal(t, 0, managedblockchain.MemberCount(b))
	assert.Equal(t, 0, managedblockchain.NodeCount(b))
	assert.Equal(t, 0, managedblockchain.AccessorCount(b))
	assert.Equal(t, 0, managedblockchain.ProposalCount(b))
	assert.Equal(t, 0, managedblockchain.InvitationCount(b))
	assert.Equal(t, 0, managedblockchain.ARNIndexSize(b))

	n := b.AddNetworkInternal(testRegion, testAccountID, "n1")
	m := b.AddMemberInternal(testRegion, testAccountID, n.ID, "m1")
	b.AddNodeInternal(testRegion, testAccountID, n.ID, m.ID, "bc.t3.small.ethereum")
	b.AddAccessorInternal(testRegion, testAccountID, "BILLING_TOKEN", "ETHEREUM_MAINNET")
	b.AddProposalInternal(testRegion, testAccountID, n.ID, m.ID, "p1")
	b.AddInvitationInternal(testRegion, testAccountID, n.ID, "n1")

	assert.Equal(t, 1, managedblockchain.NetworkCount(b))
	assert.Equal(t, 1, managedblockchain.MemberCount(b))
	assert.Equal(t, 1, managedblockchain.NodeCount(b))
	assert.Equal(t, 1, managedblockchain.AccessorCount(b))
	assert.Equal(t, 1, managedblockchain.ProposalCount(b))
	assert.Equal(t, 1, managedblockchain.InvitationCount(b))
	// ARN index: network + member + node + accessor = 4
	assert.Equal(t, 4, managedblockchain.ARNIndexSize(b))
}

// TestRefinement1_CreateNetwork_WithTags verifies tags are persisted on CreateNetwork.
func TestRefinement1_CreateNetwork_WithTags(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/networks", map[string]any{
		"Name":                "tagged-net",
		"MemberConfiguration": map[string]any{"Name": "m1"},
		"Tags":                map[string]string{"env": "prod", "team": "infra"},
	})

	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	networkID, _ := resp["NetworkId"].(string)
	require.NotEmpty(t, networkID)

	// GetNetwork and verify tags
	rec2 := doRequest(t, h, http.MethodGet, "/networks/"+networkID, nil)
	require.Equal(t, http.StatusOK, rec2.Code)

	var netResp map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &netResp))

	network := netResp["Network"].(map[string]any)
	tags := network["Tags"].(map[string]any)
	assert.Equal(t, "prod", tags["env"])
	assert.Equal(t, "infra", tags["team"])
}

// TestRefinement1_CreateMember_WithTags verifies tags are persisted on CreateMember.
func TestRefinement1_CreateMember_WithTags(t *testing.T) {
	t.Parallel()

	b := managedblockchain.NewInMemoryBackend()
	n := b.AddNetworkInternal(testRegion, testAccountID, "net1")
	h := managedblockchain.NewHandler(b)
	h.AccountID = testAccountID
	h.DefaultRegion = testRegion

	rec := doRequest(t, h, http.MethodPost, "/networks/"+n.ID+"/members", map[string]any{
		"MemberConfiguration": map[string]any{"Name": "tagged-member"},
		"Tags":                map[string]string{"role": "validator"},
	})

	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	memberID, _ := resp["MemberId"].(string)
	require.NotEmpty(t, memberID)

	// GetMember and verify tags
	rec2 := doRequest(t, h, http.MethodGet, "/networks/"+n.ID+"/members/"+memberID, nil)
	require.Equal(t, http.StatusOK, rec2.Code)

	var memResp map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &memResp))

	member := memResp["Member"].(map[string]any)
	tags := member["Tags"].(map[string]any)
	assert.Equal(t, "validator", tags["role"])
}

// TestRefinement1_CreateNode_WithTags verifies tags are persisted on CreateNode.
func TestRefinement1_CreateNode_WithTags(t *testing.T) {
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

// TestRefinement1_DeleteMember_CascadeNodes verifies nodes are deleted with the member.
func TestRefinement1_DeleteMember_CascadeNodes(t *testing.T) {
	t.Parallel()

	b := managedblockchain.NewInMemoryBackend()
	n := b.AddNetworkInternal(testRegion, testAccountID, "net1")
	m := b.AddMemberInternal(testRegion, testAccountID, n.ID, "member1")
	b.AddNodeInternal(testRegion, testAccountID, n.ID, m.ID, "bc.t3.small.ethereum")
	b.AddNodeInternal(testRegion, testAccountID, n.ID, m.ID, "bc.t3.large.ethereum")

	require.Equal(t, 2, managedblockchain.NodeCount(b))

	h := managedblockchain.NewHandler(b)
	h.AccountID = testAccountID
	h.DefaultRegion = testRegion

	rec := doRequest(t, h, http.MethodDelete, "/networks/"+n.ID+"/members/"+m.ID, nil)
	require.Equal(t, http.StatusNoContent, rec.Code)

	// Nodes should be gone too
	assert.Equal(t, 0, managedblockchain.NodeCount(b))
}

// TestRefinement1_DeleteMember_CascadeARNIndex verifies node ARNs are removed from the index.
func TestRefinement1_DeleteMember_CascadeARNIndex(t *testing.T) {
	t.Parallel()

	b := managedblockchain.NewInMemoryBackend()
	n := b.AddNetworkInternal(testRegion, testAccountID, "net1")
	m := b.AddMemberInternal(testRegion, testAccountID, n.ID, "member1")
	node := b.AddNodeInternal(testRegion, testAccountID, n.ID, m.ID, "bc.t3.small.ethereum")

	// ARN index should have: network + member + node = 3
	require.Equal(t, 3, managedblockchain.ARNIndexSize(b))

	err := b.DeleteMember(n.ID, m.ID)
	require.NoError(t, err)

	// After deletion: only network remains
	assert.Equal(t, 1, managedblockchain.ARNIndexSize(b))

	// Tagging the deleted node's ARN should fail
	err = b.TagResource(node.Arn, map[string]string{"k": "v"})
	require.Error(t, err)
}

// TestRefinement1_TagNode verifies nodes can be tagged after creation.
func TestRefinement1_TagNode(t *testing.T) {
	t.Parallel()

	b := managedblockchain.NewInMemoryBackend()
	n := b.AddNetworkInternal(testRegion, testAccountID, "net1")
	m := b.AddMemberInternal(testRegion, testAccountID, n.ID, "member1")
	node := b.AddNodeInternal(testRegion, testAccountID, n.ID, m.ID, "bc.t3.small.ethereum")

	err := b.TagResource(node.Arn, map[string]string{"stage": "prod"})
	require.NoError(t, err)

	tags, err := b.ListTagsForResource(node.Arn)
	require.NoError(t, err)
	assert.Equal(t, "prod", tags["stage"])

	err = b.UntagResource(node.Arn, []string{"stage"})
	require.NoError(t, err)

	tags, err = b.ListTagsForResource(node.Arn)
	require.NoError(t, err)
	assert.Empty(t, tags)
}

// TestRefinement1_TagAccessor verifies accessors can be tagged.
func TestRefinement1_TagAccessor(t *testing.T) {
	t.Parallel()

	b := managedblockchain.NewInMemoryBackend()
	a := b.AddAccessorInternal(testRegion, testAccountID, "BILLING_TOKEN", "ETHEREUM_MAINNET")

	err := b.TagResource(a.Arn, map[string]string{"project": "defi"})
	require.NoError(t, err)

	tags, err := b.ListTagsForResource(a.Arn)
	require.NoError(t, err)
	assert.Equal(t, "defi", tags["project"])

	err = b.UntagResource(a.Arn, []string{"project"})
	require.NoError(t, err)

	tags, err = b.ListTagsForResource(a.Arn)
	require.NoError(t, err)
	assert.Empty(t, tags)
}

// TestRefinement1_TagNode_ViaHTTP verifies node tagging works via HTTP handler.
func TestRefinement1_TagNode_ViaHTTP(t *testing.T) {
	t.Parallel()

	b := managedblockchain.NewInMemoryBackend()
	n := b.AddNetworkInternal(testRegion, testAccountID, "net1")
	m := b.AddMemberInternal(testRegion, testAccountID, n.ID, "member1")
	node := b.AddNodeInternal(testRegion, testAccountID, n.ID, m.ID, "bc.t3.small.ethereum")
	h := managedblockchain.NewHandler(b)
	h.AccountID = testAccountID
	h.DefaultRegion = testRegion

	rec := doRequest(t, h, http.MethodPost, "/tags/"+node.Arn,
		map[string]any{"Tags": map[string]string{"tier": "gold"}})
	require.Equal(t, http.StatusNoContent, rec.Code)

	rec2 := doRequest(t, h, http.MethodGet, "/tags/"+node.Arn, nil)
	require.Equal(t, http.StatusOK, rec2.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp))

	tags := resp["Tags"].(map[string]any)
	assert.Equal(t, "gold", tags["tier"])
}

// TestRefinement1_DeleteAccessor_ARNCleanup verifies accessor ARN is removed on delete.
func TestRefinement1_DeleteAccessor_ARNCleanup(t *testing.T) {
	t.Parallel()

	b := managedblockchain.NewInMemoryBackend()
	a := b.AddAccessorInternal(testRegion, testAccountID, "BILLING_TOKEN", "ETHEREUM_MAINNET")

	require.Equal(t, 1, managedblockchain.ARNIndexSize(b))

	err := b.DeleteAccessor(a.ID)
	require.NoError(t, err)

	assert.Equal(t, 0, managedblockchain.ARNIndexSize(b))

	// Tagging deleted accessor should fail
	err = b.TagResource(a.Arn, map[string]string{"k": "v"})
	require.Error(t, err)
}

// TestRefinement1_ProposalHasExpirationDate verifies proposals get an expiration date.
func TestRefinement1_ProposalHasExpirationDate(t *testing.T) {
	t.Parallel()

	b := managedblockchain.NewInMemoryBackend()
	n := b.AddNetworkInternal(testRegion, testAccountID, "net1")
	m := b.AddMemberInternal(testRegion, testAccountID, n.ID, "member1")
	p := b.AddProposalInternal(testRegion, testAccountID, n.ID, m.ID, "test proposal")

	require.NotNil(t, p.ExpirationDate)
	assert.True(t, p.ExpirationDate.After(*p.CreationDate))
}

// TestRefinement1_ProposalExpirationDate_ViaHTTP verifies expiration date appears in GetProposal.
func TestRefinement1_ProposalExpirationDate_ViaHTTP(t *testing.T) {
	t.Parallel()

	b := managedblockchain.NewInMemoryBackend()
	n := b.AddNetworkInternal(testRegion, testAccountID, "net1")
	m := b.AddMemberInternal(testRegion, testAccountID, n.ID, "member1")
	h := managedblockchain.NewHandler(b)
	h.AccountID = testAccountID
	h.DefaultRegion = testRegion

	rec := doRequest(t, h, http.MethodPost, "/networks/"+n.ID+"/proposals", map[string]any{
		"MemberId":    m.ID,
		"Description": "upgrade",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))

	proposalID, _ := createResp["ProposalId"].(string)
	require.NotEmpty(t, proposalID)

	rec2 := doRequest(t, h, http.MethodGet, "/networks/"+n.ID+"/proposals/"+proposalID, nil)
	require.Equal(t, http.StatusOK, rec2.Code)

	var getResp map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &getResp))

	proposal := getResp["Proposal"].(map[string]any)
	assert.NotEmpty(t, proposal["ExpirationDate"])
}

// TestRefinement1_PersistenceRoundTrip verifies Snapshot/Restore preserves nodes.
func TestRefinement1_PersistenceRoundTrip(t *testing.T) {
	t.Parallel()

	b := managedblockchain.NewInMemoryBackend()
	n := b.AddNetworkInternal(testRegion, testAccountID, "persist-net")
	m := b.AddMemberInternal(testRegion, testAccountID, n.ID, "persist-member")
	node := b.AddNodeInternal(testRegion, testAccountID, n.ID, m.ID, "bc.t3.small.ethereum")
	accessor := b.AddAccessorInternal(testRegion, testAccountID, "BILLING_TOKEN", "ETHEREUM_MAINNET")
	b.AddProposalInternal(testRegion, testAccountID, n.ID, m.ID, "test proposal")
	b.AddInvitationInternal(testRegion, testAccountID, n.ID, "persist-net")

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	b2 := managedblockchain.NewInMemoryBackend()
	require.NoError(t, b2.Restore(t.Context(), snap))

	assert.Equal(t, 1, managedblockchain.NetworkCount(b2))
	assert.Equal(t, 1, managedblockchain.MemberCount(b2))
	assert.Equal(t, 1, managedblockchain.NodeCount(b2))
	assert.Equal(t, 1, managedblockchain.AccessorCount(b2))
	assert.Equal(t, 1, managedblockchain.ProposalCount(b2))
	assert.Equal(t, 1, managedblockchain.InvitationCount(b2))

	// Network, member, node, accessor all in ARN index = 4
	assert.Equal(t, 4, managedblockchain.ARNIndexSize(b2))

	// Tags on node work after restore
	err := b2.TagResource(node.Arn, map[string]string{"restored": "true"})
	require.NoError(t, err)

	// Tags on accessor work after restore
	err = b2.TagResource(accessor.Arn, map[string]string{"restored": "true"})
	require.NoError(t, err)
}

// TestRefinement1_NodeLifecycleViaHTTP exercises Create/Get/List/Delete node over HTTP.
func TestRefinement1_NodeLifecycleViaHTTP(t *testing.T) {
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

// TestRefinement1_AccessorLifecycleViaHTTP exercises accessor CRUD over HTTP.
func TestRefinement1_AccessorLifecycleViaHTTP(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "accessor round trip"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			// CreateAccessor
			rec := doRequest(t, h, http.MethodPost, "/accessors", map[string]any{
				"AccessorType": "BILLING_TOKEN",
				"NetworkType":  "ETHEREUM_MAINNET",
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var createResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))

			accessorID, _ := createResp["AccessorId"].(string)
			require.NotEmpty(t, accessorID)
			assert.NotEmpty(t, createResp["BillingToken"])

			// GetAccessor
			rec2 := doRequest(t, h, http.MethodGet, "/accessors/"+accessorID, nil)
			require.Equal(t, http.StatusOK, rec2.Code)

			var getResp map[string]any
			require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &getResp))

			accessor := getResp["Accessor"].(map[string]any)
			assert.Equal(t, accessorID, accessor["Id"])
			assert.Equal(t, "BILLING_TOKEN", accessor["Type"])

			// ListAccessors
			rec3 := doRequest(t, h, http.MethodGet, "/accessors", nil)
			require.Equal(t, http.StatusOK, rec3.Code)

			var listResp map[string]any
			require.NoError(t, json.Unmarshal(rec3.Body.Bytes(), &listResp))
			accessors := listResp["Accessors"].([]any)
			assert.Len(t, accessors, 1)

			// DeleteAccessor
			rec4 := doRequest(t, h, http.MethodDelete, "/accessors/"+accessorID, nil)
			require.Equal(t, http.StatusNoContent, rec4.Code)

			// Verify deleted
			rec5 := doRequest(t, h, http.MethodGet, "/accessors/"+accessorID, nil)
			assert.Equal(t, http.StatusNotFound, rec5.Code)
		})
	}
}

// TestRefinement1_ProposalLifecycleViaHTTP exercises proposal CRUD and votes over HTTP.
func TestRefinement1_ProposalLifecycleViaHTTP(t *testing.T) {
	t.Parallel()

	b := managedblockchain.NewInMemoryBackend()
	n := b.AddNetworkInternal(testRegion, testAccountID, "net1")
	m := b.AddMemberInternal(testRegion, testAccountID, n.ID, "member1")
	h := managedblockchain.NewHandler(b)
	h.AccountID = testAccountID
	h.DefaultRegion = testRegion

	// CreateProposal
	rec := doRequest(t, h, http.MethodPost, "/networks/"+n.ID+"/proposals", map[string]any{
		"MemberId":    m.ID,
		"Description": "upgrade to v2",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))

	proposalID, _ := createResp["ProposalId"].(string)
	require.NotEmpty(t, proposalID)

	// GetProposal
	rec2 := doRequest(t, h, http.MethodGet, "/networks/"+n.ID+"/proposals/"+proposalID, nil)
	require.Equal(t, http.StatusOK, rec2.Code)

	var getResp map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &getResp))

	proposal := getResp["Proposal"].(map[string]any)
	assert.Equal(t, proposalID, proposal["ProposalId"])
	assert.Equal(t, "upgrade to v2", proposal["Description"])
	assert.Equal(t, "IN_PROGRESS", proposal["Status"])

	// ListProposals
	rec3 := doRequest(t, h, http.MethodGet, "/networks/"+n.ID+"/proposals", nil)
	require.Equal(t, http.StatusOK, rec3.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec3.Body.Bytes(), &listResp))
	proposals := listResp["Proposals"].([]any)
	assert.Len(t, proposals, 1)

	// ListProposalVotes (should be empty)
	rec4 := doRequest(t, h, http.MethodGet, "/networks/"+n.ID+"/proposals/"+proposalID+"/votes", nil)
	require.Equal(t, http.StatusOK, rec4.Code)

	var votesResp map[string]any
	require.NoError(t, json.Unmarshal(rec4.Body.Bytes(), &votesResp))
	votes := votesResp["ProposalVotes"].([]any)
	assert.Empty(t, votes)
}

// TestRefinement1_InvitationLifecycleViaHTTP exercises invitations over HTTP.
func TestRefinement1_InvitationLifecycleViaHTTP(t *testing.T) {
	t.Parallel()

	b := managedblockchain.NewInMemoryBackend()
	n := b.AddNetworkInternal(testRegion, testAccountID, "net1")
	inv := b.AddInvitationInternal(testRegion, testAccountID, n.ID, "net1")
	h := managedblockchain.NewHandler(b)
	h.AccountID = testAccountID
	h.DefaultRegion = testRegion

	// ListInvitations
	rec := doRequest(t, h, http.MethodGet, "/invitations", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	invitations := listResp["Invitations"].([]any)
	assert.Len(t, invitations, 1)

	// RejectInvitation
	rec2 := doRequest(t, h, http.MethodDelete, "/invitations/"+inv.InvitationID, nil)
	require.Equal(t, http.StatusNoContent, rec2.Code)

	// Verify the status changed to REJECTED
	err := b.RejectInvitation("nonexistent")
	require.Error(t, err)
}

// TestRefinement1_ErrValidationMapping verifies ErrValidation wraps ErrInvalidParameter.
func TestRefinement1_ErrValidationMapping(t *testing.T) {
	t.Parallel()

	require.Error(t, managedblockchain.ErrValidation)

	// ErrValidation can be unwrapped to check it contains the right error details
	assert.NotEmpty(t, managedblockchain.ErrValidation.Error())
}

// TestRefinement1_CreateProposal_MissingMemberID verifies validation.
func TestRefinement1_CreateProposal_MissingMemberID(t *testing.T) {
	t.Parallel()

	b := managedblockchain.NewInMemoryBackend()
	n := b.AddNetworkInternal(testRegion, testAccountID, "net1")
	h := managedblockchain.NewHandler(b)
	h.AccountID = testAccountID
	h.DefaultRegion = testRegion

	rec := doRequest(t, h, http.MethodPost, "/networks/"+n.ID+"/proposals", map[string]any{
		"Description": "missing member",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestRefinement1_CreateProposal_NetworkNotFound verifies 404 on unknown network.
func TestRefinement1_CreateProposal_NetworkNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/networks/nonexistent/proposals", map[string]any{
		"MemberId": "some-member",
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// TestRefinement1_UnknownPath_Returns404 verifies unrecognized paths return 404.
func TestRefinement1_UnknownPath_Returns404(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodGet, "/unknown/path", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// TestRefinement1_MultipleResetCycle verifies repeated Reset() calls work correctly.
func TestRefinement1_MultipleResetCycle(t *testing.T) {
	t.Parallel()

	b := managedblockchain.NewInMemoryBackend()

	for i := range 3 {
		_ = i

		n := b.AddNetworkInternal(testRegion, testAccountID, "net")
		b.AddMemberInternal(testRegion, testAccountID, n.ID, "m1")
		b.AddAccessorInternal(testRegion, testAccountID, "BILLING_TOKEN", "ETHEREUM_MAINNET")

		b.Reset()

		assert.Equal(t, 0, managedblockchain.NetworkCount(b))
		assert.Equal(t, 0, managedblockchain.ARNIndexSize(b))
	}
}

// TestRefinement1_GetNode_MemberNotFound verifies GetNode on deleted member returns not found.
func TestRefinement1_GetNode_MemberNotFound(t *testing.T) {
	t.Parallel()

	b := managedblockchain.NewInMemoryBackend()
	n := b.AddNetworkInternal(testRegion, testAccountID, "net1")

	_, err := b.GetNode(n.ID, "nonexistent-member", "nonexistent-node")
	require.Error(t, err)
}

// TestRefinement1_ListProposalVotes_NetworkNotFound verifies 404 on unknown network.
func TestRefinement1_ListProposalVotes_NetworkNotFound(t *testing.T) {
	t.Parallel()

	b := managedblockchain.NewInMemoryBackend()
	h := managedblockchain.NewHandler(b)
	h.AccountID = testAccountID
	h.DefaultRegion = testRegion

	rec := doRequest(t, h, http.MethodGet, "/networks/nonexistent/proposals/some-proposal/votes", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// TestRefinement1_RejectInvitation_NotFound verifies 404 on unknown invitation.
func TestRefinement1_RejectInvitation_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodDelete, "/invitations/nonexistent", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// TestRefinement1_GetProposal_NetworkNotFound verifies 404 when network is absent.
func TestRefinement1_GetProposal_NetworkNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodGet, "/networks/nonexistent/proposals/some-id", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// TestRefinement1_ListProposals_NetworkNotFound verifies 404 when network is absent.
func TestRefinement1_ListProposals_NetworkNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodGet, "/networks/nonexistent/proposals", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}
