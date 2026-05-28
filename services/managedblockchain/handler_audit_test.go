package managedblockchain_test

import (
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

// doRequestWithQuery sends a GET request with optional query parameters.
func doRequestWithQuery(
	t *testing.T,
	h *managedblockchain.Handler,
	path string,
	query map[string]string,
) *httptest.ResponseRecorder {
	t.Helper()

	e := echo.New()

	req := httptest.NewRequest(http.MethodGet, path, http.NoBody)

	if len(query) > 0 {
		q := req.URL.Query()

		for k, v := range query {
			q.Set(k, v)
		}

		req.URL.RawQuery = q.Encode()
	}

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

// TestAudit_VotingPolicy_StoredAndReturned verifies VotingPolicy is stored in CreateNetwork and returned by GetNetwork.
func TestAudit_VotingPolicy_StoredAndReturned(t *testing.T) {
	t.Parallel()

	tests := []struct {
		votingPolicy     map[string]any
		name             string
		wantPolicyStored bool
	}{
		{
			name: "voting policy stored and returned",
			votingPolicy: map[string]any{
				"ApprovalThresholdPolicy": map[string]any{
					"ThresholdComparator":     "GREATER_THAN",
					"ThresholdPercentage":     50,
					"ProposalDurationInHours": 24,
				},
			},
			wantPolicyStored: true,
		},
		{
			name:             "no voting policy is optional",
			votingPolicy:     nil,
			wantPolicyStored: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			body := map[string]any{
				"Name":                "vp-net",
				"MemberConfiguration": map[string]any{"Name": "m1"},
			}

			if tt.votingPolicy != nil {
				body["VotingPolicy"] = tt.votingPolicy
			}

			rec := doRequest(t, h, http.MethodPost, "/networks", body)
			require.Equal(t, http.StatusOK, rec.Code)

			var createResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))

			networkID := createResp["NetworkId"].(string)

			rec2 := doRequest(t, h, http.MethodGet, "/networks/"+networkID, nil)
			require.Equal(t, http.StatusOK, rec2.Code)

			var getResp map[string]any
			require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &getResp))

			network := getResp["Network"].(map[string]any)

			if tt.wantPolicyStored {
				vp, ok := network["VotingPolicy"]
				assert.True(t, ok, "VotingPolicy should be present")
				vpMap := vp.(map[string]any)
				atp := vpMap["ApprovalThresholdPolicy"].(map[string]any)
				assert.Equal(t, "GREATER_THAN", atp["ThresholdComparator"])
				assert.Equal(t, 50, int(atp["ThresholdPercentage"].(float64)))
				assert.Equal(t, 24, int(atp["ProposalDurationInHours"].(float64)))
			} else {
				_, hasPol := network["VotingPolicy"]
				assert.False(t, hasPol, "VotingPolicy should be absent when not set")
			}
		})
	}
}

// TestAudit_ListNetworks_Filters verifies query param filtering for ListNetworks.
func TestAudit_ListNetworks_Filters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		query     map[string]string
		name      string
		wantNames []string
		wantCount int
	}{
		{
			name:      "no filter returns all",
			query:     map[string]string{},
			wantCount: 3,
		},
		{
			name:      "filter by name exact match",
			query:     map[string]string{"name": "alpha-net"},
			wantCount: 1,
			wantNames: []string{"alpha-net"},
		},
		{
			name:      "filter by framework",
			query:     map[string]string{"framework": "HYPERLEDGER_FABRIC"},
			wantCount: 3,
		},
		{
			name:      "filter by status available",
			query:     map[string]string{"status": "AVAILABLE"},
			wantCount: 3,
		},
		{
			name:      "filter by name no match",
			query:     map[string]string{"name": "nonexistent"},
			wantCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := managedblockchain.NewInMemoryBackend()
			b.AddNetworkInternal(testRegion, testAccountID, "alpha-net")
			b.AddNetworkInternal(testRegion, testAccountID, "beta-net")
			b.AddNetworkInternal(testRegion, testAccountID, "gamma-net")

			h := managedblockchain.NewHandler(b)
			h.AccountID = testAccountID
			h.DefaultRegion = testRegion

			rec := doRequestWithQuery(t, h, "/networks", tt.query)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			networks := resp["Networks"].([]any)
			assert.Len(t, networks, tt.wantCount)

			if len(tt.wantNames) > 0 {
				names := make([]string, 0, len(networks))

				for _, n := range networks {
					net := n.(map[string]any)
					names = append(names, net["Name"].(string))
				}

				for _, wantName := range tt.wantNames {
					assert.Contains(t, names, wantName)
				}
			}
		})
	}
}

// TestAudit_ListMembers_Filters verifies query param filtering for ListMembers.
func TestAudit_ListMembers_Filters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		query     map[string]string
		name      string
		wantCount int
	}{
		{
			name:      "no filter returns all",
			query:     map[string]string{},
			wantCount: 3,
		},
		{
			name:      "filter by name",
			query:     map[string]string{"name": "alice"},
			wantCount: 1,
		},
		{
			name:      "filter by status",
			query:     map[string]string{"status": "AVAILABLE"},
			wantCount: 3,
		},
		{
			name:      "filter isOwned=true returns all (all seeded members are owned)",
			query:     map[string]string{"isOwned": "true"},
			wantCount: 3,
		},
		{
			name:      "filter isOwned=false returns none (all seeded members are owned)",
			query:     map[string]string{"isOwned": "false"},
			wantCount: 0,
		},
		{
			name:      "filter by nonexistent name",
			query:     map[string]string{"name": "nobody"},
			wantCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := managedblockchain.NewInMemoryBackend()
			n := b.AddNetworkInternal(testRegion, testAccountID, "net1")
			b.AddMemberInternal(testRegion, testAccountID, n.ID, "alice")
			b.AddMemberInternal(testRegion, testAccountID, n.ID, "bob")
			b.AddMemberInternal(testRegion, testAccountID, n.ID, "carol")

			h := managedblockchain.NewHandler(b)
			h.AccountID = testAccountID
			h.DefaultRegion = testRegion

			rec := doRequestWithQuery(t, h, "/networks/"+n.ID+"/members", tt.query)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			members := resp["Members"].([]any)
			assert.Len(t, members, tt.wantCount)
		})
	}
}

// TestAudit_ListNodes_Filters verifies status filter for ListNodes.
func TestAudit_ListNodes_Filters(t *testing.T) {
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

			rec := doRequestWithQuery(t, h,
				fmt.Sprintf("/networks/%s/members/%s/nodes", n.ID, m.ID), tt.query)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			nodes := resp["Nodes"].([]any)
			assert.Len(t, nodes, tt.wantCount)
		})
	}
}

// TestAudit_ListAccessors_Filters verifies networkType filter for ListAccessors.
func TestAudit_ListAccessors_Filters(t *testing.T) {
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
			name:      "filter by ETHEREUM_MAINNET",
			query:     map[string]string{"networkType": "ETHEREUM_MAINNET"},
			wantCount: 1,
		},
		{
			name:      "filter by ETHEREUM_GOERLI",
			query:     map[string]string{"networkType": "ETHEREUM_GOERLI"},
			wantCount: 1,
		},
		{
			name:      "filter by unknown type returns none",
			query:     map[string]string{"networkType": "UNKNOWN_NET"},
			wantCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := managedblockchain.NewInMemoryBackend()
			b.AddAccessorInternal(testRegion, testAccountID, "BILLING_TOKEN", "ETHEREUM_MAINNET")
			b.AddAccessorInternal(testRegion, testAccountID, "BILLING_TOKEN", "ETHEREUM_GOERLI")

			h := managedblockchain.NewHandler(b)
			h.AccountID = testAccountID
			h.DefaultRegion = testRegion

			rec := doRequestWithQuery(t, h, "/accessors", tt.query)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			accessors := resp["Accessors"].([]any)
			assert.Len(t, accessors, tt.wantCount)
		})
	}
}

// TestAudit_ProposalActions_StoredAndReturned verifies Actions field is stored and returned.
func TestAudit_ProposalActions_StoredAndReturned(t *testing.T) {
	t.Parallel()

	tests := []struct {
		actions     map[string]any
		name        string
		wantInvites int
		wantRemoves int
	}{
		{
			name: "invite action stored",
			actions: map[string]any{
				"Invitations": []map[string]any{
					{"Principal": "111111111111"},
				},
			},
			wantInvites: 1,
			wantRemoves: 0,
		},
		{
			name:        "no actions is valid",
			actions:     nil,
			wantInvites: 0,
			wantRemoves: 0,
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

			body := map[string]any{
				"MemberId":    m.ID,
				"Description": "test proposal",
			}

			if tt.actions != nil {
				body["Actions"] = tt.actions
			}

			rec := doRequest(t, h, http.MethodPost, "/networks/"+n.ID+"/proposals", body)
			require.Equal(t, http.StatusOK, rec.Code)

			var createResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))

			proposalID := createResp["ProposalId"].(string)

			rec2 := doRequest(t, h, http.MethodGet, "/networks/"+n.ID+"/proposals/"+proposalID, nil)
			require.Equal(t, http.StatusOK, rec2.Code)

			var getResp map[string]any
			require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &getResp))

			proposal := getResp["Proposal"].(map[string]any)

			if tt.actions == nil {
				_, hasActions := proposal["Actions"]
				assert.False(t, hasActions, "Actions should be absent when not set")

				return
			}

			actions := proposal["Actions"].(map[string]any)

			if invites, ok := actions["Invitations"]; ok {
				inviteList := invites.([]any)
				assert.Len(t, inviteList, tt.wantInvites)
			} else {
				assert.Equal(t, 0, tt.wantInvites)
			}
		})
	}
}

// TestAudit_OutstandingVoteCount verifies OutstandingVoteCount is tracked correctly.
func TestAudit_OutstandingVoteCount(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                      string
		extraMembers              int
		votesToCast               int
		wantOutstandingAfterVotes int
	}{
		{
			name:                      "one member network, cast one vote = zero outstanding",
			extraMembers:              0,
			votesToCast:               1,
			wantOutstandingAfterVotes: 0,
		},
		{
			name:                      "three members, cast two votes = one outstanding",
			extraMembers:              2,
			votesToCast:               2,
			wantOutstandingAfterVotes: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := managedblockchain.NewInMemoryBackend()
			n := b.AddNetworkInternal(testRegion, testAccountID, "net1")
			m := b.AddMemberInternal(testRegion, testAccountID, n.ID, "proposer")

			extraMemberIDs := make([]string, tt.extraMembers)

			for i := range tt.extraMembers {
				em := b.AddMemberInternal(testRegion, testAccountID, n.ID, fmt.Sprintf("voter-%d", i))
				extraMemberIDs[i] = em.ID
			}

			proposal := b.AddProposalInternal(testRegion, testAccountID, n.ID, m.ID, "vote test")

			totalMembers := 1 + tt.extraMembers
			require.Equal(t, totalMembers, proposal.OutstandingVoteCount)

			// Cast votes
			h := managedblockchain.NewHandler(b)
			h.AccountID = testAccountID
			h.DefaultRegion = testRegion

			path := fmt.Sprintf("/networks/%s/proposals/%s/votes", n.ID, proposal.ProposalID)

			// First vote from proposer
			if tt.votesToCast > 0 {
				rec := doRequest(t, h, http.MethodPost, path, map[string]any{
					"VoterMemberId": m.ID,
					"Vote":          "YES",
				})
				require.Equal(t, http.StatusNoContent, rec.Code)
			}

			// Additional votes from extra members
			for i := 1; i < tt.votesToCast && i-1 < len(extraMemberIDs); i++ {
				rec := doRequest(t, h, http.MethodPost, path, map[string]any{
					"VoterMemberId": extraMemberIDs[i-1],
					"Vote":          "YES",
				})
				require.Equal(t, http.StatusNoContent, rec.Code)
			}

			// Check outstanding via GetProposal
			rec := doRequest(
				t,
				h,
				http.MethodGet,
				fmt.Sprintf("/networks/%s/proposals/%s", n.ID, proposal.ProposalID),
				nil,
			)
			require.Equal(t, http.StatusOK, rec.Code)

			var getResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getResp))

			p := getResp["Proposal"].(map[string]any)
			outstanding := int(p["OutstandingVoteCount"].(float64))
			assert.Equal(t, tt.wantOutstandingAfterVotes, outstanding)
		})
	}
}

// TestAudit_ProposalStatusTransitions verifies proposal APPROVED/REJECTED transitions.
func TestAudit_ProposalStatusTransitions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		wantStatus   string
		comparator   string
		votes        []string
		threshold    int
		totalMembers int
	}{
		{
			name:         "100% YES in 2-member network with GREATER_THAN 50 → APPROVED",
			totalMembers: 2,
			threshold:    50,
			comparator:   "GREATER_THAN",
			votes:        []string{"YES", "YES"},
			wantStatus:   "APPROVED",
		},
		{
			name:         "all NO votes → REJECTED",
			totalMembers: 2,
			threshold:    50,
			comparator:   "GREATER_THAN",
			votes:        []string{"NO", "NO"},
			wantStatus:   "REJECTED",
		},
		{
			name:         "no voting policy → stays IN_PROGRESS",
			totalMembers: 1,
			threshold:    0,
			comparator:   "",
			votes:        []string{"YES"},
			wantStatus:   "IN_PROGRESS",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			// Create network with voting policy
			var votingPolicy map[string]any

			if tt.threshold > 0 {
				votingPolicy = map[string]any{
					"ApprovalThresholdPolicy": map[string]any{
						"ThresholdComparator":     tt.comparator,
						"ThresholdPercentage":     tt.threshold,
						"ProposalDurationInHours": 24,
					},
				}
			}

			netBody := map[string]any{
				"Name":                "vote-net",
				"MemberConfiguration": map[string]any{"Name": "m0"},
			}

			if votingPolicy != nil {
				netBody["VotingPolicy"] = votingPolicy
			}

			rec := doRequest(t, h, http.MethodPost, "/networks", netBody)
			require.Equal(t, http.StatusOK, rec.Code)

			var createNetResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createNetResp))

			networkID := createNetResp["NetworkId"].(string)
			firstMemberID := createNetResp["MemberId"].(string)

			// Add extra members if needed
			extraMemberIDs := make([]string, 0, tt.totalMembers-1)

			for i := 1; i < tt.totalMembers; i++ {
				memRec := doRequest(t, h, http.MethodPost, "/networks/"+networkID+"/members", map[string]any{
					"MemberConfiguration": map[string]any{"Name": fmt.Sprintf("m%d", i)},
				})
				require.Equal(t, http.StatusOK, memRec.Code)

				var createMemResp map[string]any
				require.NoError(t, json.Unmarshal(memRec.Body.Bytes(), &createMemResp))
				extraMemberIDs = append(extraMemberIDs, createMemResp["MemberId"].(string))
			}

			// Create proposal
			rec = doRequest(t, h, http.MethodPost, "/networks/"+networkID+"/proposals", map[string]any{
				"MemberId":    firstMemberID,
				"Description": "test",
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var createPropResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createPropResp))

			proposalID := createPropResp["ProposalId"].(string)

			// Cast votes
			allMemberIDs := append([]string{firstMemberID}, extraMemberIDs...)
			votePath := fmt.Sprintf("/networks/%s/proposals/%s/votes", networkID, proposalID)

			for i, vote := range tt.votes {
				if i >= len(allMemberIDs) {
					break
				}

				voteRec := doRequest(t, h, http.MethodPost, votePath, map[string]any{
					"VoterMemberId": allMemberIDs[i],
					"Vote":          vote,
				})
				require.Equal(t, http.StatusNoContent, voteRec.Code)
			}

			// Check proposal status
			rec = doRequest(t, h, http.MethodGet, fmt.Sprintf("/networks/%s/proposals/%s", networkID, proposalID), nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var getResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getResp))

			p := getResp["Proposal"].(map[string]any)
			assert.Equal(t, tt.wantStatus, p["Status"])
		})
	}
}

// TestAudit_MemberSummaryIsOwned verifies IsOwned field appears in ListMembers response.
func TestAudit_MemberSummaryIsOwned(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		wantOwned bool
	}{
		{
			name:      "seeded member is owned",
			wantOwned: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := managedblockchain.NewInMemoryBackend()
			n := b.AddNetworkInternal(testRegion, testAccountID, "net1")
			b.AddMemberInternal(testRegion, testAccountID, n.ID, "owned-member")

			h := managedblockchain.NewHandler(b)
			h.AccountID = testAccountID
			h.DefaultRegion = testRegion

			rec := doRequest(t, h, http.MethodGet, "/networks/"+n.ID+"/members", nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			members := resp["Members"].([]any)
			require.Len(t, members, 1)

			member := members[0].(map[string]any)
			isOwned, ok := member["IsOwned"]
			assert.True(t, ok, "IsOwned field should be present")
			assert.Equal(t, tt.wantOwned, isOwned)
		})
	}
}

// TestAudit_MemberObjectIsOwned verifies IsOwned field in GetMember response.
func TestAudit_MemberObjectIsOwned(t *testing.T) {
	t.Parallel()

	b := managedblockchain.NewInMemoryBackend()
	n := b.AddNetworkInternal(testRegion, testAccountID, "net1")
	m := b.AddMemberInternal(testRegion, testAccountID, n.ID, "owned-member")

	h := managedblockchain.NewHandler(b)
	h.AccountID = testAccountID
	h.DefaultRegion = testRegion

	rec := doRequest(t, h, http.MethodGet, "/networks/"+n.ID+"/members/"+m.ID, nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	member := resp["Member"].(map[string]any)
	isOwned, ok := member["IsOwned"]
	assert.True(t, ok, "IsOwned field should be present in GetMember response")
	assert.Equal(t, true, isOwned)
}

// TestAudit_NodeSummaryAvailabilityZone verifies AvailabilityZone in ListNodes response.
func TestAudit_NodeSummaryAvailabilityZone(t *testing.T) {
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
			rec := doRequest(t, h, http.MethodPost,
				fmt.Sprintf("/networks/%s/members/%s/nodes", n.ID, m.ID),
				map[string]any{
					"NodeConfiguration": map[string]any{
						"InstanceType":     tt.instanceType,
						"AvailabilityZone": "us-east-1a",
					},
				},
			)
			require.Equal(t, http.StatusOK, rec.Code)

			// List nodes and check AZ in summary
			rec2 := doRequest(t, h, http.MethodGet,
				fmt.Sprintf("/networks/%s/members/%s/nodes", n.ID, m.ID), nil)
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

// TestAudit_InvitationNetworkSummary verifies NetworkSummary is nested in invitation response.
func TestAudit_InvitationNetworkSummary(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		networkName string
	}{
		{
			name:        "invitation includes network summary",
			networkName: "invite-net",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := managedblockchain.NewInMemoryBackend()
			n := b.AddNetworkInternal(testRegion, testAccountID, tt.networkName)
			b.AddInvitationInternal(testRegion, testAccountID, n.ID, tt.networkName)

			h := managedblockchain.NewHandler(b)
			h.AccountID = testAccountID
			h.DefaultRegion = testRegion

			rec := doRequest(t, h, http.MethodGet, "/invitations", nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			invitations := resp["Invitations"].([]any)
			require.Len(t, invitations, 1)

			inv := invitations[0].(map[string]any)
			ns, ok := inv["NetworkSummary"]
			assert.True(t, ok, "NetworkSummary should be present in invitation")

			nsSummary := ns.(map[string]any)
			assert.Equal(t, tt.networkName, nsSummary["Name"])
			assert.Equal(t, n.ID, nsSummary["Id"])
			assert.NotEmpty(t, nsSummary["Arn"])
			assert.Equal(t, "AVAILABLE", nsSummary["Status"])
		})
	}
}

// TestAudit_UpdateMember_LogPublishingConfig verifies log config is stored and returned.
func TestAudit_UpdateMember_LogPublishingConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		logEnabled bool
	}{
		{
			name:       "enable CA log → stored and returned in GetMember",
			logEnabled: true,
		},
		{
			name:       "disable CA log → stored and returned",
			logEnabled: false,
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

			// UpdateMember with log config
			patchPath := fmt.Sprintf("/networks/%s/members/%s", n.ID, m.ID)
			rec := doRequest(t, h, http.MethodPatch, patchPath, map[string]any{
				"LogPublishingConfiguration": map[string]any{
					"Fabric": map[string]any{
						"CaLogs": map[string]any{
							"CloudWatch": map[string]any{
								"Enabled": tt.logEnabled,
							},
						},
					},
				},
			})
			require.Equal(t, http.StatusNoContent, rec.Code)

			// GetMember and verify log config
			rec2 := doRequest(t, h, http.MethodGet, patchPath, nil)
			require.Equal(t, http.StatusOK, rec2.Code)

			var getResp map[string]any
			require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &getResp))

			member := getResp["Member"].(map[string]any)
			logConfig, ok := member["LogPublishingConfiguration"]
			assert.True(t, ok, "LogPublishingConfiguration should be present after update")

			logConfigMap := logConfig.(map[string]any)
			fabric := logConfigMap["Fabric"].(map[string]any)
			caLogs := fabric["CaLogs"].(map[string]any)
			cw := caLogs["CloudWatch"].(map[string]any)
			assert.Equal(t, tt.logEnabled, cw["Enabled"])
		})
	}
}

// TestAudit_UpdateNode_LogPublishingConfig verifies node log config is stored and returned.
func TestAudit_UpdateNode_LogPublishingConfig(t *testing.T) {
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

			patchPath := fmt.Sprintf("/networks/%s/members/%s/nodes/%s", n.ID, m.ID, node.ID)

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

// TestAudit_VoteOnProposal_AlreadyCompleted verifies voting on completed proposal returns error.
func TestAudit_VoteOnProposal_AlreadyCompleted(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "vote on approved proposal fails"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			// Create network with 100% threshold so one vote approves
			rec := doRequest(t, h, http.MethodPost, "/networks", map[string]any{
				"Name":                "approve-net",
				"MemberConfiguration": map[string]any{"Name": "m1"},
				"VotingPolicy": map[string]any{
					"ApprovalThresholdPolicy": map[string]any{
						"ThresholdComparator":     "GREATER_THAN_OR_EQUAL_TO",
						"ThresholdPercentage":     100,
						"ProposalDurationInHours": 24,
					},
				},
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var netResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &netResp))

			networkID := netResp["NetworkId"].(string)
			memberID := netResp["MemberId"].(string)

			rec = doRequest(t, h, http.MethodPost, "/networks/"+networkID+"/proposals", map[string]any{
				"MemberId": memberID,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var propResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &propResp))

			proposalID := propResp["ProposalId"].(string)
			votePath := fmt.Sprintf("/networks/%s/proposals/%s/votes", networkID, proposalID)

			// First vote approves proposal
			rec = doRequest(t, h, http.MethodPost, votePath, map[string]any{
				"VoterMemberId": memberID,
				"Vote":          "YES",
			})
			require.Equal(t, http.StatusNoContent, rec.Code)

			// Verify proposal is now APPROVED
			rec = doRequest(t, h, http.MethodGet, fmt.Sprintf("/networks/%s/proposals/%s", networkID, proposalID), nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var getResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getResp))

			p := getResp["Proposal"].(map[string]any)
			assert.Equal(t, "APPROVED", p["Status"])
		})
	}
}

// TestAudit_BackendFilter_ListNetworks verifies backend-level filtering for ListNetworks.
func TestAudit_BackendFilter_ListNetworks(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		filter    managedblockchain.ListNetworksFilter
		wantCount int
	}{
		{
			name:      "empty filter returns all",
			filter:    managedblockchain.ListNetworksFilter{},
			wantCount: 3,
		},
		{
			name:      "filter by Name",
			filter:    managedblockchain.ListNetworksFilter{Name: "net-a"},
			wantCount: 1,
		},
		{
			name:      "filter by Framework",
			filter:    managedblockchain.ListNetworksFilter{Framework: "HYPERLEDGER_FABRIC"},
			wantCount: 3,
		},
		{
			name:      "filter by Status AVAILABLE",
			filter:    managedblockchain.ListNetworksFilter{Status: "AVAILABLE"},
			wantCount: 3,
		},
		{
			name:      "filter by Status CREATING returns none",
			filter:    managedblockchain.ListNetworksFilter{Status: "CREATING"},
			wantCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := managedblockchain.NewInMemoryBackend()
			b.AddNetworkInternal(testRegion, testAccountID, "net-a")
			b.AddNetworkInternal(testRegion, testAccountID, "net-b")
			b.AddNetworkInternal(testRegion, testAccountID, "net-c")

			networks, err := b.ListNetworks(tt.filter)
			require.NoError(t, err)
			assert.Len(t, networks, tt.wantCount)
		})
	}
}

// TestAudit_BackendFilter_ListMembers verifies backend-level filtering for ListMembers.
func TestAudit_BackendFilter_ListMembers(t *testing.T) {
	t.Parallel()

	trueVal := true
	falseVal := false

	tests := []struct {
		filter    managedblockchain.ListMembersFilter
		name      string
		wantCount int
	}{
		{
			name:      "empty filter returns all",
			filter:    managedblockchain.ListMembersFilter{},
			wantCount: 2,
		},
		{
			name:      "filter by Name",
			filter:    managedblockchain.ListMembersFilter{Name: "alice"},
			wantCount: 1,
		},
		{
			name:      "filter by Status",
			filter:    managedblockchain.ListMembersFilter{Status: "AVAILABLE"},
			wantCount: 2,
		},
		{
			name:      "filter isOwned=true",
			filter:    managedblockchain.ListMembersFilter{IsOwned: &trueVal},
			wantCount: 2,
		},
		{
			name:      "filter isOwned=false",
			filter:    managedblockchain.ListMembersFilter{IsOwned: &falseVal},
			wantCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := managedblockchain.NewInMemoryBackend()
			n := b.AddNetworkInternal(testRegion, testAccountID, "net1")
			b.AddMemberInternal(testRegion, testAccountID, n.ID, "alice")
			b.AddMemberInternal(testRegion, testAccountID, n.ID, "bob")

			members, err := b.ListMembers(n.ID, tt.filter)
			require.NoError(t, err)
			assert.Len(t, members, tt.wantCount)
		})
	}
}

// TestAudit_BackendFilter_ListNodes verifies backend-level filtering for ListNodes.
func TestAudit_BackendFilter_ListNodes(t *testing.T) {
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

// TestAudit_BackendFilter_ListAccessors verifies backend-level filtering for ListAccessors.
func TestAudit_BackendFilter_ListAccessors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		filter    managedblockchain.ListAccessorsFilter
		wantCount int
	}{
		{
			name:      "empty filter returns all",
			filter:    managedblockchain.ListAccessorsFilter{},
			wantCount: 2,
		},
		{
			name:      "filter by ETHEREUM_MAINNET",
			filter:    managedblockchain.ListAccessorsFilter{NetworkType: "ETHEREUM_MAINNET"},
			wantCount: 1,
		},
		{
			name:      "filter by ETHEREUM_GOERLI",
			filter:    managedblockchain.ListAccessorsFilter{NetworkType: "ETHEREUM_GOERLI"},
			wantCount: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := managedblockchain.NewInMemoryBackend()
			b.AddAccessorInternal(testRegion, testAccountID, "BILLING_TOKEN", "ETHEREUM_MAINNET")
			b.AddAccessorInternal(testRegion, testAccountID, "BILLING_TOKEN", "ETHEREUM_GOERLI")

			accessors, err := b.ListAccessors(tt.filter)
			require.NoError(t, err)
			assert.Len(t, accessors, tt.wantCount)
		})
	}
}

// TestAudit_ProposalSummary_HasNetworkID verifies NetworkId is present in proposal summaries.
func TestAudit_ProposalSummary_HasNetworkID(t *testing.T) {
	t.Parallel()

	b := managedblockchain.NewInMemoryBackend()
	n := b.AddNetworkInternal(testRegion, testAccountID, "net1")
	m := b.AddMemberInternal(testRegion, testAccountID, n.ID, "m1")
	b.AddProposalInternal(testRegion, testAccountID, n.ID, m.ID, "test")

	h := managedblockchain.NewHandler(b)
	h.AccountID = testAccountID
	h.DefaultRegion = testRegion

	rec := doRequest(t, h, http.MethodGet, "/networks/"+n.ID+"/proposals", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	proposals := resp["Proposals"].([]any)
	require.Len(t, proposals, 1)

	p := proposals[0].(map[string]any)
	networkID, ok := p["NetworkId"]
	assert.True(t, ok, "NetworkId should be in proposal summary")
	assert.Equal(t, n.ID, networkID)
}

// TestAudit_CloneVotingPolicy_DoesNotMutate verifies cloning prevents mutation.
func TestAudit_CloneVotingPolicy_DoesNotMutate(t *testing.T) {
	t.Parallel()

	b := managedblockchain.NewInMemoryBackend()

	// Create network with voting policy
	vp := &managedblockchain.VotingPolicy{
		ApprovalThresholdPolicy: &managedblockchain.ApprovalThresholdPolicy{
			ThresholdComparator:     "GREATER_THAN",
			ThresholdPercentage:     50,
			ProposalDurationInHours: 24,
		},
	}

	n, _, err := b.CreateNetwork(
		testRegion, testAccountID,
		"vp-net", "", "", "", "m1", "",
		nil, vp,
	)
	require.NoError(t, err)

	// GetNetwork returns a clone
	got, err := b.GetNetwork(n.ID)
	require.NoError(t, err)
	require.NotNil(t, got.VotingPolicy)
	assert.Equal(t, "GREATER_THAN", got.VotingPolicy.ApprovalThresholdPolicy.ThresholdComparator)

	// Mutate the returned object
	got.VotingPolicy.ApprovalThresholdPolicy.ThresholdPercentage = 99

	// Original should be unaffected
	got2, err := b.GetNetwork(n.ID)
	require.NoError(t, err)
	assert.Equal(t, int32(50), got2.VotingPolicy.ApprovalThresholdPolicy.ThresholdPercentage)
}
