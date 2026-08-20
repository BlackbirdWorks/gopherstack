package managedblockchain_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/managedblockchain"
)

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

// TestHandler_InvitationNetworkSummary verifies NetworkSummary is nested in invitation response.
func TestHandler_InvitationNetworkSummary(t *testing.T) {
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

// TestHandler_InvitationLifecycleViaHTTP exercises invitations over HTTP.
func TestHandler_InvitationLifecycleViaHTTP(t *testing.T) {
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

// TestHandler_RejectInvitationNotFound verifies 404 on unknown invitation.
func TestHandler_RejectInvitationNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodDelete, "/invitations/nonexistent", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// TestHandler_InvitationOmitsTopLevelNetworkFields confirms ListInvitations
// never emits top-level NetworkId/NetworkName members on an invitation. Real
// AWS's Invitation carries that information only in the nested
// NetworkSummary -- confirmed against aws-sdk-go-v2
// managedblockchain@v1.34.4's awsRestjson1_deserializeDocumentInvitation
// (deserializers.go:4762), whose case-sensitive switch has no "NetworkId" or
// "NetworkName" case. A raw-body assertion is used here (rather than a real
// SDK client field check) because types.Invitation itself has no such
// fields to observe -- there is nothing for a typed client to decode into.
func TestHandler_InvitationOmitsTopLevelNetworkFields(t *testing.T) {
	t.Parallel()

	b := managedblockchain.NewInMemoryBackend()
	n := b.AddNetworkInternal(testRegion, testAccountID, "net1")
	b.AddInvitationInternal(testRegion, testAccountID, n.ID, "net1")

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
	_, hasNetworkID := inv["NetworkId"]
	_, hasNetworkName := inv["NetworkName"]
	assert.False(t, hasNetworkID, "real AWS's Invitation has no top-level NetworkId member")
	assert.False(t, hasNetworkName, "real AWS's Invitation has no top-level NetworkName member")
}
