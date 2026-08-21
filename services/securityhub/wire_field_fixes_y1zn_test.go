package securityhub_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestListAggregatorsV2_AggregatorsV2Key_RealClient covers gopherstack-y1zn.
// handleListAggregatorsV2 emitted "Aggregators"; ListAggregatorsV2Output
// (securityhub@v1.75.4 deserializers.go's
// awsRestjson1_deserializeOpDocumentListAggregatorsV2Output) declares only
// AggregatorsV2/NextToken.
func TestListAggregatorsV2_AggregatorsV2Key_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodGet, "/aggregatorv2/list", nil)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	body := rec.Body.String()
	assert.NotContains(t, body, `"Aggregators"`,
		"ListAggregatorsV2Output has no Aggregators member")
	assert.Contains(t, body, `"AggregatorsV2"`,
		"ListAggregatorsV2Output's real member is AggregatorsV2")
}

// TestDeclineDeleteInvitations_NoProcessedAccountsKey_RealClient covers
// gopherstack-y1zn. handleDeclineInvitations/handleDeleteInvitations each
// emitted an extra "ProcessedAccounts" key; DeclineInvitationsOutput and
// DeleteInvitationsOutput (securityhub@v1.75.4 deserializers.go) declare only
// UnprocessedAccounts -- success is implied by an account's absence from
// that list, not by a separate processed-accounts echo.
func TestDeclineDeleteInvitations_NoProcessedAccountsKey_RealClient(t *testing.T) {
	t.Parallel()

	t.Run("decline", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		rec := doRequest(t, h, http.MethodPost, "/invitations/decline", map[string]any{
			"AccountIds": []any{"111111111111"},
		})
		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

		body := rec.Body.String()
		assert.NotContains(t, body, `"ProcessedAccounts"`,
			"DeclineInvitationsOutput has no ProcessedAccounts member")
		assert.Contains(t, body, `"UnprocessedAccounts"`,
			"DeclineInvitationsOutput's real member is UnprocessedAccounts")
	})

	t.Run("delete", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		rec := doRequest(t, h, http.MethodPost, "/invitations/delete", map[string]any{
			"AccountIds": []any{"111111111111"},
		})
		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

		body := rec.Body.String()
		assert.NotContains(t, body, `"ProcessedAccounts"`,
			"DeleteInvitationsOutput has no ProcessedAccounts member")
		assert.Contains(t, body, `"UnprocessedAccounts"`,
			"DeleteInvitationsOutput's real member is UnprocessedAccounts")
	})
}
