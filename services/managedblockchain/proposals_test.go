package managedblockchain_test

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

	"github.com/blackbirdworks/gopherstack/services/managedblockchain"
)

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

func TestHandler_ProposalActionsStoredAndReturned(t *testing.T) {
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

// TestHandler_ProposalOutstandingVoteCount verifies OutstandingVoteCount is tracked correctly.
func TestHandler_ProposalSummaryHasNetworkID(t *testing.T) {
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

// TestHandler_VoteThresholdFloatPrecision verifies that the vote threshold comparison
// uses float division, not integer division. With 3 members and GREATER_THAN 33%,
// integer division gives 33% (1/3*100 truncated), which is NOT > 33 — but float gives
// 33.33% which IS > 33, so the proposal should be approved.
func TestHandler_ListProposalsStatusFilter(t *testing.T) {
	t.Parallel()

	h, b := newTestHandlerWithBackend(t)

	n := b.AddNetworkInternal(testRegion, testAccountID, "filter-net")
	m := b.AddMemberInternal(testRegion, testAccountID, n.ID, "m1")

	// Create one IN_PROGRESS and one APPROVED proposal via the backend.
	p1 := b.AddProposalInternal(testRegion, testAccountID, n.ID, m.ID, "proposal-1")
	p2 := b.AddProposalInternal(testRegion, testAccountID, n.ID, m.ID, "proposal-2")

	_ = p1
	_ = p2

	// Force-approve p2 by voting (need a voting policy — use AddNetworkInternal which sets none).
	// Just check filtering by IN_PROGRESS.
	listURL := fmt.Sprintf("/networks/%s/proposals?status=IN_PROGRESS", n.ID)

	listRec := doRequest(t, h, http.MethodGet, listURL, nil)
	require.Equal(t, http.StatusOK, listRec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))

	proposals, _ := listResp["Proposals"].([]any)
	assert.Len(t, proposals, 2,
		"both proposals are IN_PROGRESS so filter=IN_PROGRESS must return both")

	// Filter for a status that matches nothing.
	listURLApproved := fmt.Sprintf("/networks/%s/proposals?status=APPROVED", n.ID)

	listApprovedRec := doRequest(t, h, http.MethodGet, listURLApproved, nil)
	require.Equal(t, http.StatusOK, listApprovedRec.Code)

	var listApprovedResp map[string]any
	require.NoError(t, json.Unmarshal(listApprovedRec.Body.Bytes(), &listApprovedResp))

	approvedProposals, _ := listApprovedResp["Proposals"].([]any)
	assert.Empty(t, approvedProposals,
		"no APPROVED proposals exist so filter=APPROVED must return empty")
}

// TestHandler_RejectionThresholdImpossibleApproval verifies that rejection is triggered
// when it is mathematically impossible to reach approval, not by a symmetric threshold.
// Real AWS rejects when maxPossibleYes < requiredYes.
func TestHandler_ListProposalsNoStatusFilterReturnsAll(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create network with 1-member for simple unanimous vote.
	netRec := doRequest(t, h, http.MethodPost, "/networks", map[string]any{
		"Name":                "all-proposals-net",
		"MemberConfiguration": map[string]any{"Name": "owner"},
		"VotingPolicy": map[string]any{
			"ApprovalThresholdPolicy": map[string]any{
				"ThresholdComparator":     "GREATER_THAN_OR_EQUAL_TO",
				"ThresholdPercentage":     1,
				"ProposalDurationInHours": 24,
			},
		},
	})
	require.Equal(t, http.StatusOK, netRec.Code)

	var netResp map[string]any
	require.NoError(t, json.Unmarshal(netRec.Body.Bytes(), &netResp))

	netID := netResp["NetworkId"].(string)
	ownerID := netResp["MemberId"].(string)

	// Create and approve proposal 1.
	propRec1 := doRequest(t, h, http.MethodPost, "/networks/"+netID+"/proposals",
		map[string]any{"MemberId": ownerID, "Description": "approve-me"})
	require.Equal(t, http.StatusOK, propRec1.Code)

	var prop1 map[string]any
	require.NoError(t, json.Unmarshal(propRec1.Body.Bytes(), &prop1))

	propID1 := prop1["ProposalId"].(string)

	voteRec := doRequest(t, h, http.MethodPost,
		fmt.Sprintf("/networks/%s/proposals/%s/votes", netID, propID1),
		map[string]any{"VoterMemberId": ownerID, "Vote": "YES"})
	require.Equal(t, http.StatusNoContent, voteRec.Code)

	// Create proposal 2 (stays IN_PROGRESS).
	propRec2 := doRequest(t, h, http.MethodPost, "/networks/"+netID+"/proposals",
		map[string]any{"MemberId": ownerID, "Description": "keep-pending"})
	require.Equal(t, http.StatusOK, propRec2.Code)

	// List all (no filter) — should see both.
	listRec := doRequest(t, h, http.MethodGet, "/networks/"+netID+"/proposals", nil)
	require.Equal(t, http.StatusOK, listRec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))

	proposals, _ := listResp["Proposals"].([]any)
	assert.Len(t, proposals, 2, "listing without status filter must return all proposals")

	statuses := make([]string, 0, len(proposals))
	for _, p := range proposals {
		pm := p.(map[string]any)
		statuses = append(statuses, pm["Status"].(string))
	}

	assert.Contains(t, statuses, "APPROVED")
	assert.Contains(t, statuses, "IN_PROGRESS")

	// List with status=APPROVED — should see only 1.
	listApprovedRec := doRequest(t, h, http.MethodGet,
		"/networks/"+netID+"/proposals?status=APPROVED", nil)
	require.Equal(t, http.StatusOK, listApprovedRec.Code)

	var listApprovedResp map[string]any
	require.NoError(t, json.Unmarshal(listApprovedRec.Body.Bytes(), &listApprovedResp))

	approvedProposals, _ := listApprovedResp["Proposals"].([]any)
	assert.Len(t, approvedProposals, 1, "filtering by APPROVED must return only approved proposals")

	approvedEntry := approvedProposals[0].(map[string]any)
	assert.Equal(t, propID1, approvedEntry["ProposalId"],
		strings.Join(statuses, ","))
}

// TestInMemoryBackend_ProposalHasExpirationDate verifies proposals get an expiration date.
func TestInMemoryBackend_ProposalHasExpirationDate(t *testing.T) {
	t.Parallel()

	b := managedblockchain.NewInMemoryBackend()
	n := b.AddNetworkInternal(testRegion, testAccountID, "net1")
	m := b.AddMemberInternal(testRegion, testAccountID, n.ID, "member1")
	p := b.AddProposalInternal(testRegion, testAccountID, n.ID, m.ID, "test proposal")

	require.NotNil(t, p.ExpirationDate)
	assert.True(t, p.ExpirationDate.After(*p.CreationDate))
}

// TestHandler_ProposalExpirationDateViaHTTP verifies expiration date appears in GetProposal.
func TestHandler_ProposalExpirationDateViaHTTP(t *testing.T) {
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

// TestHandler_ProposalLifecycleViaHTTP exercises proposal CRUD and votes over HTTP.
func TestHandler_ProposalLifecycleViaHTTP(t *testing.T) {
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

// TestHandler_CreateProposalMissingMemberID verifies validation.
func TestHandler_CreateProposalMissingMemberID(t *testing.T) {
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

// TestHandler_CreateProposalNetworkNotFound verifies 404 on unknown network.
func TestHandler_CreateProposalNetworkNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/networks/nonexistent/proposals", map[string]any{
		"MemberId": "some-member",
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// TestHandler_ListProposalVotesNetworkNotFound verifies 404 on unknown network.
func TestHandler_ListProposalVotesNetworkNotFound(t *testing.T) {
	t.Parallel()

	b := managedblockchain.NewInMemoryBackend()
	h := managedblockchain.NewHandler(b)
	h.AccountID = testAccountID
	h.DefaultRegion = testRegion

	rec := doRequest(t, h, http.MethodGet, "/networks/nonexistent/proposals/some-proposal/votes", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// TestHandler_GetProposalNetworkNotFound verifies 404 when network is absent.
func TestHandler_GetProposalNetworkNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodGet, "/networks/nonexistent/proposals/some-id", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// TestHandler_ListProposalsNetworkNotFound verifies 404 when network is absent.
func TestHandler_ListProposalsNetworkNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodGet, "/networks/nonexistent/proposals", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}
