package managedblockchain_test

import (
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/managedblockchain"
)

// sdkRouteCases is the authoritative method+path for every real Managed
// Blockchain operation, extracted from managedblockchain@v1.34.4
// serializers.go: each entry's "request.Method" and the string passed to
// httpbinding.SplitURI in that op's
// awsRestjson1_serializeOp<Op>.HandleSerialize. PLACEHOLDER stands in for
// any {NetworkId}/{MemberId}/{NodeId}/{ProposalId}/{AccessorId}/
// {InvitationId}/{ResourceArn} URI label -- parsePath and its per-family
// helpers (handler.go) never validate identifier shape, so the literal
// value doesn't matter here, only path depth and static segments. 27 real
// ops here, matching Managed Blockchain's real op count exactly (also
// matches GetSupportedOperations's own 27 entries one-for-one).
//
// A systematic check for a shared method+path across all 27 ops found zero
// collisions -- even ListProposalVotes/VoteOnProposal sharing
// "/networks/{id}/proposals/{id}/votes" are disambiguated by method
// (GET/POST), which parseProposalIDPath already switches on -- so no
// *required dynamic* (non-template) member -- the s3/glacier vacuity-trap
// class -- was needed to disambiguate any route in this table.
//
// Regenerate by grepping serializers.go for every
// "func (m *awsRestjson1_serializeOp<Op>) HandleSerialize" and pulling
// "request.Method" and the httpbinding.SplitURI(...) argument from its body.
func sdkRouteCases() []struct{ op, method, path string } {
	return []struct{ op, method, path string }{
		{"CreateAccessor", "POST", "/accessors"},
		{"CreateMember", "POST", "/networks/PLACEHOLDER/members"},
		{"CreateNetwork", "POST", "/networks"},
		{"CreateNode", "POST", "/networks/PLACEHOLDER/nodes"},
		{"CreateProposal", "POST", "/networks/PLACEHOLDER/proposals"},
		{"DeleteAccessor", "DELETE", "/accessors/PLACEHOLDER"},
		{"DeleteMember", "DELETE", "/networks/PLACEHOLDER/members/PLACEHOLDER"},
		{"DeleteNode", "DELETE", "/networks/PLACEHOLDER/nodes/PLACEHOLDER"},
		{"GetAccessor", "GET", "/accessors/PLACEHOLDER"},
		{"GetMember", "GET", "/networks/PLACEHOLDER/members/PLACEHOLDER"},
		{"GetNetwork", "GET", "/networks/PLACEHOLDER"},
		{"GetNode", "GET", "/networks/PLACEHOLDER/nodes/PLACEHOLDER"},
		{"GetProposal", "GET", "/networks/PLACEHOLDER/proposals/PLACEHOLDER"},
		{"ListAccessors", "GET", "/accessors"},
		{"ListInvitations", "GET", "/invitations"},
		{"ListMembers", "GET", "/networks/PLACEHOLDER/members"},
		{"ListNetworks", "GET", "/networks"},
		{"ListNodes", "GET", "/networks/PLACEHOLDER/nodes"},
		{"ListProposals", "GET", "/networks/PLACEHOLDER/proposals"},
		{"ListProposalVotes", "GET", "/networks/PLACEHOLDER/proposals/PLACEHOLDER/votes"},
		{"ListTagsForResource", "GET", "/tags/PLACEHOLDER"},
		{"RejectInvitation", "DELETE", "/invitations/PLACEHOLDER"},
		{"TagResource", "POST", "/tags/PLACEHOLDER"},
		{"UntagResource", "DELETE", "/tags/PLACEHOLDER"},
		{"UpdateMember", "PATCH", "/networks/PLACEHOLDER/members/PLACEHOLDER"},
		{"UpdateNode", "PATCH", "/networks/PLACEHOLDER/nodes/PLACEHOLDER"},
		{"VoteOnProposal", "POST", "/networks/PLACEHOLDER/proposals/PLACEHOLDER/votes"},
	}
}

// TestExtractOperation_SDKRouteTable drives every real Managed Blockchain
// op's authoritative method+path (see sdkRouteCases) through
// ExtractOperation and asserts parsePath (handler.go) resolves it to the
// right op, all 27 ops against Managed Blockchain's real op count. It then
// drives the same request through the real Handler() and asserts the
// response's decoded "message" field is not the exact literal "resource not
// found" that Handler() emits via writeError(c, http.StatusNotFound,
// "ResourceNotFoundException", "resource not found") when parsePath returns
// an empty op.
//
// A bare substring check on "resource not found" is NOT safe for this
// service -- ErrResourceNotFound in errors.go is
// awserr.New("ResourceNotFoundException: resource not found",
// awserr.ErrNotFound), and writeBackendError passes err.Error() straight
// through as the message, so a request that legitimately 404s via
// ErrResourceNotFound (or any of its ErrNetworkNotFound/ErrMemberNotFound/
// etc. siblings, all "ResourceNotFoundException: <x> not found") would
// *contain* the miss sentinel's exact text as a substring -- exactly the
// amplify/xray collision trap called out for this campaign. Resolved by
// decoding the JSON body and comparing the "message" field for exact
// equality, since the real domain errors' message field always carries the
// "ResourceNotFoundException: " prefix and the miss sentinel never does.
// dispatch()'s own "unknown operation" branch is a second miss text, but it
// is unreachable from any HTTP request -- parsePath only ever returns a
// known op constant or "", and the "" case is caught by Handler() before
// dispatch() is called.
func TestExtractOperation_SDKRouteTable(t *testing.T) {
	t.Parallel()

	for _, tc := range sdkRouteCases() {
		t.Run(strings.ToLower(tc.op), func(t *testing.T) {
			t.Parallel()

			backend := managedblockchain.NewInMemoryBackend()
			h := managedblockchain.NewHandler(backend)

			e := echo.New()
			req := httptest.NewRequest(tc.method, tc.path, nil)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			got := h.ExtractOperation(c)
			require.Equal(t, tc.op, got, "method=%s path=%s", tc.method, tc.path)

			require.NoError(t, h.Handler()(c))

			var resp struct {
				Message string `json:"message"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.NotEqual(t, "resource not found", resp.Message,
				"method=%s path=%s op=%s: dispatched to the unmatched-route default", tc.method, tc.path, tc.op)
		})
	}
}
