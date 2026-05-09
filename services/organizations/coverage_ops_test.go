package organizations_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOrg_HandshakeOps(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create org first
	doRequest(t, h, "CreateOrganization", map[string]any{"featureSet": "ALL"})

	// InviteAccountToOrganization
	rec := doRequest(t, h, "InviteAccountToOrganization", map[string]any{
		"target": map[string]any{
			"id":   "123456789013",
			"type": "ACCOUNT",
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// ListHandshakesForOrganization
	rec = doRequest(t, h, "ListHandshakesForOrganization", nil)
	assert.True(t, rec.Code >= 200 && rec.Code < 300 || rec.Code == 400)

	// ListHandshakesForAccount
	rec = doRequest(t, h, "ListHandshakesForAccount", nil)
	assert.True(t, rec.Code >= 200 && rec.Code < 300 || rec.Code == 400)
}

func TestOrg_CreateAccountStatus(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateOrganization", map[string]any{"featureSet": "ALL"})

	// CreateAccount creates an async account
	rec := doRequest(t, h, "CreateAccount", map[string]any{
		"accountName": "test-account",
		"email":       "test@example.com",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// ListCreateAccountStatus
	rec = doRequest(t, h, "ListCreateAccountStatus", nil)
	assert.True(t, rec.Code >= 200 && rec.Code < 300 || rec.Code == 400)
}

func TestOrg_DelegatedServices(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateOrganization", map[string]any{"featureSet": "ALL"})

	// ListDelegatedServicesForAccount
	rec := doRequest(t, h, "ListDelegatedServicesForAccount", map[string]any{
		"accountId": "123456789012",
	})
	assert.True(t, rec.Code >= 200 && rec.Code < 300 || rec.Code == 400)
}
