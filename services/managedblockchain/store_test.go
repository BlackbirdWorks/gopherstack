package managedblockchain_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/managedblockchain"
)

const (
	testRegion    = "us-east-1"
	testAccountID = "000000000000"
)

// newBackend returns a fresh InMemoryBackend. Shared across the
// managedblockchain_test package's family test files.
func newBackend() *managedblockchain.InMemoryBackend {
	return managedblockchain.NewInMemoryBackend()
}

// newTestHandler returns a fresh Handler backed by a fresh InMemoryBackend.
// Shared across the managedblockchain_test package's family test files.
func newTestHandler(t *testing.T) *managedblockchain.Handler {
	t.Helper()

	b := managedblockchain.NewInMemoryBackend()
	h := managedblockchain.NewHandler(b)
	h.AccountID = testAccountID
	h.DefaultRegion = testRegion

	return h
}

// newTestHandlerWithBackend creates a handler and returns both handler and backend for
// direct backend seeding in tests. Shared across the managedblockchain_test package's
// family test files.
func newTestHandlerWithBackend(t *testing.T) (*managedblockchain.Handler, *managedblockchain.InMemoryBackend) {
	t.Helper()

	b := managedblockchain.NewInMemoryBackend()
	h := managedblockchain.NewHandler(b)
	h.AccountID = testAccountID
	h.DefaultRegion = testRegion

	return h, b
}

// doRequest performs a request against the managedblockchain handler via echo.
// Shared across the managedblockchain_test package's family test files.
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

// testMemberConfiguration returns a MemberConfiguration request body with the
// FrameworkConfiguration.Fabric.AdminUsername/AdminPassword the real API's
// client-side validator (and gopherstack's own server-side mirror of it,
// see validateMemberConfigurationRequest in handler_networks.go) requires.
// Shared across the managedblockchain_test package's family test files so
// every CreateNetwork/CreateMember request body stays wire-shape valid.
func testMemberConfiguration(name string) map[string]any {
	return map[string]any{
		"Name": name,
		"FrameworkConfiguration": map[string]any{
			"Fabric": map[string]any{
				"AdminUsername": "admin",
				"AdminPassword": "Passw0rd!",
			},
		},
	}
}

// createTestInvitation seeds a PENDING invitation for networkID and returns its
// InvitationId, satisfying CreateMember's real-API-required InvitationId field
// (aws-sdk-go-v2 managedblockchain validators.go:805-806, v1.34.4). Shared across the
// managedblockchain_test package's family test files.
func createTestInvitation(t *testing.T, b *managedblockchain.InMemoryBackend, networkID, networkName string) string {
	t.Helper()

	return b.AddInvitationInternal(testRegion, testAccountID, networkID, networkName).InvitationID
}

// createTestNetwork creates a network with one member and returns their IDs.
// Shared across the managedblockchain_test package's family test files.
func createTestNetwork(t *testing.T, h *managedblockchain.Handler) (string, string) {
	t.Helper()

	rec := doRequest(t, h, http.MethodPost, "/networks", map[string]any{
		"Name":                "test-net",
		"MemberConfiguration": testMemberConfiguration("member-1"),
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		NetworkID string `json:"NetworkId"`
		MemberID  string `json:"MemberId"`
	}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))

	return out.NetworkID, out.MemberID
}

// TestInMemoryBackend_Reset verifies that Backend.Reset clears all data.
func TestInMemoryBackend_Reset(t *testing.T) {
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

// TestHandler_ResetDelegatesToBackend verifies that Handler.Reset delegates to backend.
func TestHandler_ResetDelegatesToBackend(t *testing.T) {
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

// TestProvider_InitNilContext verifies that Provider.Init rejects a nil context.
func TestProvider_InitNilContext(t *testing.T) {
	t.Parallel()

	p := &managedblockchain.Provider{}
	_, err := p.Init(nil)

	require.ErrorIs(t, err, managedblockchain.ErrNilAppContext)
}

// TestHandler_GetSupportedOperationsSorted verifies operations are sorted.
func TestHandler_GetSupportedOperationsSorted(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ops := h.GetSupportedOperations()

	require.NotEmpty(t, ops)
	assert.Equal(t, 27, managedblockchain.HandlerOpsLen(h))

	for i := 1; i < len(ops); i++ {
		assert.LessOrEqual(t, ops[i-1], ops[i], "ops must be sorted: %s >= %s", ops[i-1], ops[i])
	}
}

// TestInMemoryBackend_SeedHelpers verifies all AddXInternal helpers work.
func TestInMemoryBackend_SeedHelpers(t *testing.T) {
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

// TestInMemoryBackend_ExportCountHelpers verifies count helpers.
func TestInMemoryBackend_ExportCountHelpers(t *testing.T) {
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
	// ARN index: network + member + node + accessor + proposal = 5
	// (proposal included since CreateProposal accepts Tags on the real API,
	// gopherstack-2mwl).
	assert.Equal(t, 5, managedblockchain.ARNIndexSize(b))
}

// TestErrValidationMapping verifies ErrValidation wraps ErrInvalidParameter.
func TestErrValidationMapping(t *testing.T) {
	t.Parallel()

	require.Error(t, managedblockchain.ErrValidation)

	// ErrValidation can be unwrapped to check it contains the right error details
	assert.NotEmpty(t, managedblockchain.ErrValidation.Error())
}

// TestInMemoryBackend_MultipleResetCycle verifies repeated Reset() calls work correctly.
func TestInMemoryBackend_MultipleResetCycle(t *testing.T) {
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
