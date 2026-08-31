package managedblockchain_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/managedblockchain"
)

// TestHandler_CreateOps_MissingClientRequestToken verifies that
// CreateNetwork/CreateMember/CreateNode/CreateProposal/CreateAccessor reject
// a request with no ClientRequestToken. The real aws-sdk-go-v2 client-side
// validator (validators.go, all 5 ops, v1.34.4) marks it required
// ("This member is required") and never sends a request without it -- an SDK
// client always has one, auto-filled by the idempotency-token middleware
// when unset. A raw HTTP caller bypassing that middleware, though, can send
// an empty/missing token; real AWS rejects it (InvalidRequestException), so
// gopherstack must too.
func TestHandler_CreateOps_MissingClientRequestToken(t *testing.T) {
	t.Parallel()

	tests := []struct {
		makeReq func(t *testing.T, h *managedblockchain.Handler, b *managedblockchain.InMemoryBackend) (
			method, path string, body map[string]any,
		)
		name string
	}{
		{
			name: "createnetwork",
			makeReq: func(
				_ *testing.T, _ *managedblockchain.Handler, _ *managedblockchain.InMemoryBackend,
			) (string, string, map[string]any) {
				return http.MethodPost, "/networks", map[string]any{
					"Name":                "no-token-net",
					"MemberConfiguration": testMemberConfiguration("m1"),
				}
			},
		},
		{
			name: "createmember",
			makeReq: func(
				t *testing.T, h *managedblockchain.Handler, b *managedblockchain.InMemoryBackend,
			) (string, string, map[string]any) {
				t.Helper()

				netID, _ := createTestNetwork(t, h)
				invID := createTestInvitation(t, b, netID, "no-token-net")

				return http.MethodPost, "/networks/" + netID + "/members", map[string]any{
					"InvitationId":        invID,
					"MemberConfiguration": testMemberConfiguration("m2"),
				}
			},
		},
		{
			name: "createnode",
			makeReq: func(
				t *testing.T, h *managedblockchain.Handler, _ *managedblockchain.InMemoryBackend,
			) (string, string, map[string]any) {
				t.Helper()

				netID, memID := createTestNetwork(t, h)

				return http.MethodPost, "/networks/" + netID + "/nodes", map[string]any{
					"MemberId": memID,
					"NodeConfiguration": map[string]any{
						"InstanceType":     "bc.t3.small",
						"AvailabilityZone": "us-east-1a",
					},
				}
			},
		},
		{
			name: "createproposal",
			makeReq: func(
				t *testing.T, h *managedblockchain.Handler, _ *managedblockchain.InMemoryBackend,
			) (string, string, map[string]any) {
				t.Helper()

				netID, memID := createTestNetwork(t, h)

				return http.MethodPost, "/networks/" + netID + "/proposals", map[string]any{
					"MemberId":    memID,
					"Description": "no token",
				}
			},
		},
		{
			name: "createaccessor",
			makeReq: func(
				_ *testing.T, _ *managedblockchain.Handler, _ *managedblockchain.InMemoryBackend,
			) (string, string, map[string]any) {
				return http.MethodPost, "/accessors", map[string]any{
					"AccessorType": "BILLING_TOKEN",
					"NetworkType":  "ETHEREUM_MAINNET",
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandlerWithBackend(t)
			method, path, body := tt.makeReq(t, h, b)

			rec := doRequest(t, h, method, path, body)
			require.Equal(t, http.StatusBadRequest, rec.Code, "response body: %s", rec.Body.String())
			assert.Contains(t, rec.Body.String(), "ClientRequestToken")
		})
	}
}
