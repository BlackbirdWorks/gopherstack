package workmail_test

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/workmail"
)

// ---- Personal Access Tokens ----

func TestPersonalAccessTokenLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		scopes []string
	}{
		{name: "read-only scopes", scopes: []string{"ReadMail", "ReadCalendar"}},
		{name: "full scopes", scopes: []string{"ReadMail", "WriteMail", "ReadCalendar", "WriteCalendar"}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t) //nolint:ineffassign,staticcheck,wastedassign // existing issue.
			backend := workmail.NewInMemoryBackend("000000000000", "us-east-1")
			h = workmail.NewHandler(backend)
			orgID := createTestOrg(t, h, "pat-org")
			userID := createTestUser(t, h, orgID, "patuser", "PAT User")

			// Create a PAT via backend directly (no CreatePersonalAccessToken op in the 36)
			tok, err := backend.CreatePersonalAccessToken(orgID, userID, "my-token", tc.scopes)
			require.NoError(t, err)

			// GetPersonalAccessTokenMetadata
			rec := doOp(t, h, "GetPersonalAccessTokenMetadata", fmt.Sprintf(
				`{"OrganizationId":%q,"PersonalAccessTokenId":%q}`, orgID, tok.TokenID,
			))
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			m := decodeJSON(t, rec)
			assert.Equal(t, tok.TokenID, m["PersonalAccessTokenId"])
			assert.Equal(t, userID, m["UserId"])
			assert.Equal(t, "my-token", m["Name"])

			// ListPersonalAccessTokens all
			rec = doOp(t, h, "ListPersonalAccessTokens", fmt.Sprintf(`{"OrganizationId":%q}`, orgID))
			require.Equal(t, http.StatusOK, rec.Code)
			m = decodeJSON(t, rec)
			summaries := m["PersonalAccessTokenSummaries"].([]any)
			assert.Len(t, summaries, 1)

			// ListPersonalAccessTokens filtered by user
			rec = doOp(t, h, "ListPersonalAccessTokens", fmt.Sprintf(
				`{"OrganizationId":%q,"UserId":%q}`, orgID, userID,
			))
			m = decodeJSON(t, rec)
			summaries = m["PersonalAccessTokenSummaries"].([]any)
			assert.Len(t, summaries, 1)

			// Delete
			rec = doOp(t, h, "DeletePersonalAccessToken", fmt.Sprintf(
				`{"OrganizationId":%q,"PersonalAccessTokenId":%q}`, orgID, tok.TokenID,
			))
			require.Equal(t, http.StatusOK, rec.Code)

			// Get after delete → not found
			rec = doOp(t, h, "GetPersonalAccessTokenMetadata", fmt.Sprintf(
				`{"OrganizationId":%q,"PersonalAccessTokenId":%q}`, orgID, tok.TokenID,
			))
			require.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}
}
