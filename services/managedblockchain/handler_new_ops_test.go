package managedblockchain_test

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/managedblockchain"
)

func TestHandler_UpdateMember(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		networkID  string
		memberID   string
		wantStatus int
	}{
		{
			name:       "happy path returns 204",
			wantStatus: http.StatusNoContent,
		},
		{
			name:       "network not found returns 404",
			networkID:  "nonexistent-network",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "member not found returns 404",
			memberID:   "nonexistent-member",
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

			networkID := net.ID
			memberID := mem.ID

			if tt.networkID != "" {
				networkID = tt.networkID
			}

			if tt.memberID != "" {
				memberID = tt.memberID
			}

			path := fmt.Sprintf("/networks/%s/members/%s", networkID, memberID)
			rec := doRequest(t, h, http.MethodPatch, path, map[string]any{})
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

func TestHandler_VoteOnProposal(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantStatus int
	}{
		{
			name:       "happy path YES vote returns 204",
			body:       map[string]any{"VoterMemberId": "placeholder", "Vote": "YES"},
			wantStatus: http.StatusNoContent,
		},
		{
			name:       "happy path NO vote returns 204",
			body:       map[string]any{"VoterMemberId": "placeholder", "Vote": "NO"},
			wantStatus: http.StatusNoContent,
		},
		{
			name:       "missing VoterMemberId returns 400",
			body:       map[string]any{"Vote": "YES"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "duplicate vote returns 400",
			body:       map[string]any{"VoterMemberId": "placeholder", "Vote": "YES", "duplicate": true},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "invalid vote value returns 400",
			body:       map[string]any{"VoterMemberId": "placeholder", "Vote": "MAYBE"},
			wantStatus: http.StatusBadRequest,
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
			proposal := b.AddProposalInternal(testRegion, testAccountID, net.ID, mem.ID, "test proposal")

			path := fmt.Sprintf("/networks/%s/proposals/%s/votes", net.ID, proposal.ProposalID)

			body := tt.body
			isDuplicate := false

			if v, ok := body["duplicate"]; ok && v == true {
				isDuplicate = true
				// Remove the marker from the actual body.
				body = map[string]any{
					"VoterMemberId": mem.ID,
					"Vote":          "YES",
				}
			}

			// Replace placeholder with real member ID.
			if vid, ok := body["VoterMemberId"]; ok && vid == "placeholder" {
				body = map[string]any{
					"VoterMemberId": mem.ID,
					"Vote":          body["Vote"],
				}
			}

			if isDuplicate {
				// Cast the first vote to set up the duplicate scenario.
				firstRec := doRequest(t, h, http.MethodPost, path, body)
				require.Equal(t, http.StatusNoContent, firstRec.Code)
			}

			rec := doRequest(t, h, http.MethodPost, path, body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}
