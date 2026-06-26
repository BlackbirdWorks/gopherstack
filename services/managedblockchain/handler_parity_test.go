package managedblockchain_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/managedblockchain"
)

// newTestHandlerWithBackend creates a handler and returns both handler and backend for
// direct backend seeding in parity tests.
func newTestHandlerWithBackend(t *testing.T) (*managedblockchain.Handler, *managedblockchain.InMemoryBackend) {
	t.Helper()

	b := managedblockchain.NewInMemoryBackend()
	h := managedblockchain.NewHandler(b)
	h.AccountID = testAccountID
	h.DefaultRegion = testRegion

	return h, b
}

// TestParity_ErrorResponse_HasCodeField verifies that error responses include a Code
// field in addition to Message. Real AWS Managed Blockchain returns both fields.
func TestParity_ErrorResponse_HasCodeField(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		method   string
		path     string
		wantCode string
		wantHTTP int
	}{
		{
			name:     "get missing network returns ResourceNotFoundException code",
			method:   http.MethodGet,
			path:     "/networks/no-such-network-id",
			wantHTTP: http.StatusNotFound,
			wantCode: "ResourceNotFoundException",
		},
		{
			name:   "create member with bad body returns InvalidRequestException code",
			method: http.MethodPost,
			path:   "/networks/no-net/members",
			body:   map[string]any{"MemberConfiguration": map[string]any{}},
			// MemberConfiguration.Name is empty → invalid
			wantHTTP: http.StatusBadRequest,
			wantCode: "InvalidRequestException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, tt.method, tt.path, tt.body)
			assert.Equal(t, tt.wantHTTP, rec.Code)

			var resp map[string]string
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Equal(t, tt.wantCode, resp["Code"],
				"error response must include Code field; body: %s", rec.Body.String())
			assert.NotEmpty(t, resp["message"],
				"error response must include message field")
		})
	}
}

// TestParity_VoteThreshold_FloatPrecision verifies that the vote threshold comparison
// uses float division, not integer division. With 3 members and GREATER_THAN 33%,
// integer division gives 33% (1/3*100 truncated), which is NOT > 33 — but float gives
// 33.33% which IS > 33, so the proposal should be approved.
func TestParity_VoteThreshold_FloatPrecision(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// 3 members, GREATER_THAN 33%: 1/3 YES = 33.33% > 33 with float (but 33 > 33 = false with integer).
	netRec := doRequest(t, h, http.MethodPost, "/networks", map[string]any{
		"Name":                "float-precision-net",
		"MemberConfiguration": map[string]any{"Name": "owner"},
		"VotingPolicy": map[string]any{
			"ApprovalThresholdPolicy": map[string]any{
				"ThresholdComparator":     "GREATER_THAN",
				"ThresholdPercentage":     33,
				"ProposalDurationInHours": 24,
			},
		},
	})
	require.Equal(t, http.StatusOK, netRec.Code)

	var netResp map[string]any
	require.NoError(t, json.Unmarshal(netRec.Body.Bytes(), &netResp))

	netID := netResp["NetworkId"].(string)
	ownerMemberID := netResp["MemberId"].(string)

	addMem := func(name string) {
		rec := doRequest(t, h, http.MethodPost, "/networks/"+netID+"/members",
			map[string]any{"MemberConfiguration": map[string]any{"Name": name}})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	// Add 2 more members for 3 total.
	addMem("m2")
	addMem("m3")

	// Create proposal.
	propRec := doRequest(t, h, http.MethodPost, "/networks/"+netID+"/proposals",
		map[string]any{"MemberId": ownerMemberID, "Description": "float precision test"})
	require.Equal(t, http.StatusOK, propRec.Code)

	var propResp map[string]any
	require.NoError(t, json.Unmarshal(propRec.Body.Bytes(), &propResp))

	propID := propResp["ProposalId"].(string)
	votePath := fmt.Sprintf("/networks/%s/proposals/%s/votes", netID, propID)

	// Cast 1 YES vote out of 3 (33.33% > 33 with float; 33 > 33 = false with integer).
	rec := doRequest(t, h, http.MethodPost, votePath,
		map[string]any{"VoterMemberId": ownerMemberID, "Vote": "YES"})
	require.Equal(t, http.StatusNoContent, rec.Code)

	getRec := doRequest(t, h, http.MethodGet,
		fmt.Sprintf("/networks/%s/proposals/%s", netID, propID), nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	var getResp map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getResp))

	p := getResp["Proposal"].(map[string]any)
	assert.Equal(t, "APPROVED", p["Status"],
		"1/3 YES votes (33.33%%) must satisfy GREATER_THAN 33%% with float division; "+
			"integer division (33 > 33 = false) would leave proposal IN_PROGRESS")
}

// TestParity_ApprovedProposal_ExecutesInvitationActions verifies that when a proposal
// with Invitation actions is approved, the invitations are automatically created. Real
// AWS executes proposal actions immediately upon approval.
func TestParity_ApprovedProposal_ExecutesInvitationActions(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create a network (1 member = only 1 vote needed for unanimous approval).
	netRec := doRequest(t, h, http.MethodPost, "/networks", map[string]any{
		"Name":                "actions-net",
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
	ownerMemberID := netResp["MemberId"].(string)

	// Verify no invitations exist initially.
	listBefore := doRequest(t, h, http.MethodGet, "/invitations", nil)
	require.Equal(t, http.StatusOK, listBefore.Code)

	var listBeforeResp map[string]any
	require.NoError(t, json.Unmarshal(listBefore.Body.Bytes(), &listBeforeResp))
	assert.Empty(t, listBeforeResp["Invitations"],
		"no invitations should exist before proposal approval")

	// Create proposal with an Invitation action.
	propRec := doRequest(t, h, http.MethodPost, "/networks/"+netID+"/proposals", map[string]any{
		"MemberId":    ownerMemberID,
		"Description": "invite new member",
		"Actions": map[string]any{
			"Invitations": []map[string]any{
				{"Principal": "987654321098"},
			},
		},
	})
	require.Equal(t, http.StatusOK, propRec.Code)

	var propResp map[string]any
	require.NoError(t, json.Unmarshal(propRec.Body.Bytes(), &propResp))

	propID := propResp["ProposalId"].(string)

	// Vote YES to approve.
	voteRec := doRequest(t, h, http.MethodPost,
		fmt.Sprintf("/networks/%s/proposals/%s/votes", netID, propID),
		map[string]any{"VoterMemberId": ownerMemberID, "Vote": "YES"})
	require.Equal(t, http.StatusNoContent, voteRec.Code)

	// Verify invitation was created by the approval.
	listAfter := doRequest(t, h, http.MethodGet, "/invitations", nil)
	require.Equal(t, http.StatusOK, listAfter.Code)

	var listAfterResp map[string]any
	require.NoError(t, json.Unmarshal(listAfter.Body.Bytes(), &listAfterResp))

	invitations, _ := listAfterResp["Invitations"].([]any)
	assert.Len(t, invitations, 1,
		"approved proposal with Invitation action must create one invitation")
}

// TestParity_ListProposals_StatusFilter verifies that the status query parameter
// filters proposals. Real AWS supports ?status=IN_PROGRESS|APPROVED|REJECTED etc.
func TestParity_ListProposals_StatusFilter(t *testing.T) {
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

// TestParity_RejectionThreshold_ImpossibleApproval verifies that rejection is triggered
// when it is mathematically impossible to reach approval, not by a symmetric threshold.
// Real AWS rejects when maxPossibleYes < requiredYes.
func TestParity_RejectionThreshold_ImpossibleApproval(t *testing.T) {
	t.Parallel()

	// 4 members, GREATER_THAN 50%: need >50% YES = 3 votes minimum.
	// After 2 NO votes: maxPossibleYes = 4 - 2 = 2 < 3 → REJECTED.
	// Old wrong logic: rejectionThreshold = 100 - 50 = 50%, needed >50% NO = 3 NO votes.
	h := newTestHandler(t)

	netRec := doRequest(t, h, http.MethodPost, "/networks", map[string]any{
		"Name":                "reject-net",
		"MemberConfiguration": map[string]any{"Name": "m0"},
		"VotingPolicy": map[string]any{
			"ApprovalThresholdPolicy": map[string]any{
				"ThresholdComparator":     "GREATER_THAN",
				"ThresholdPercentage":     50,
				"ProposalDurationInHours": 24,
			},
		},
	})
	require.Equal(t, http.StatusOK, netRec.Code)

	var netResp map[string]any
	require.NoError(t, json.Unmarshal(netRec.Body.Bytes(), &netResp))

	netID := netResp["NetworkId"].(string)
	m0ID := netResp["MemberId"].(string)

	addMem := func(name string) string {
		rec := doRequest(t, h, http.MethodPost, "/networks/"+netID+"/members",
			map[string]any{"MemberConfiguration": map[string]any{"Name": name}})
		require.Equal(t, http.StatusOK, rec.Code)

		var r map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &r))

		return r["MemberId"].(string)
	}

	m1ID := addMem("m1")
	m2ID := addMem("m2")
	m3ID := addMem("m3")

	propRec := doRequest(t, h, http.MethodPost, "/networks/"+netID+"/proposals",
		map[string]any{"MemberId": m0ID, "Description": "rejection threshold test"})
	require.Equal(t, http.StatusOK, propRec.Code)

	var propResp map[string]any
	require.NoError(t, json.Unmarshal(propRec.Body.Bytes(), &propResp))

	propID := propResp["ProposalId"].(string)
	votePath := fmt.Sprintf("/networks/%s/proposals/%s/votes", netID, propID)

	// Cast 2 NO votes (m0 and m1).
	for _, mID := range []string{m0ID, m1ID} {
		rec := doRequest(t, h, http.MethodPost, votePath,
			map[string]any{"VoterMemberId": mID, "Vote": "NO"})
		require.Equal(t, http.StatusNoContent, rec.Code)
	}

	_ = m2ID
	_ = m3ID

	// Proposal must be REJECTED now (maxPossibleYes = 4-2 = 2 < requiredYes = 3).
	getRec := doRequest(t, h, http.MethodGet,
		fmt.Sprintf("/networks/%s/proposals/%s", netID, propID), nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	var getResp map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getResp))

	p := getResp["Proposal"].(map[string]any)
	assert.Equal(t, "REJECTED", p["Status"],
		"2 NO votes in a 4-member GREATER_THAN 50%% network must trigger rejection "+
			"(maxPossibleYes=2 < requiredYes=3)")
}

// TestParity_ErrorResponse_MessageNotEmpty verifies all error paths return non-empty
// messages alongside error codes.
func TestParity_ErrorResponse_MessageNotEmpty(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	errorCases := []struct {
		body   any
		name   string
		method string
		path   string
	}{
		{
			name:   "get missing network",
			method: http.MethodGet,
			path:   "/networks/no-such",
		},
		{
			name:   "get member on missing network",
			method: http.MethodGet,
			path:   "/networks/no-net/members/no-mem",
		},
		{
			name:   "get missing proposal",
			method: http.MethodGet,
			path:   "/networks/no-net/proposals/no-prop",
		},
	}

	for _, tc := range errorCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			rec := doRequest(t, h, tc.method, tc.path, tc.body)
			assert.NotEqual(t, http.StatusOK, rec.Code)

			var resp map[string]string
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.NotEmpty(t, resp["message"], "error response must include non-empty message")
			assert.NotEmpty(t, resp["Code"], "error response must include non-empty Code")
		})
	}
}

// TestParity_ListProposals_NoStatusFilter_ReturnsAll verifies that omitting the status
// filter returns all proposals regardless of status.
func TestParity_ListProposals_NoStatusFilter_ReturnsAll(t *testing.T) {
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
