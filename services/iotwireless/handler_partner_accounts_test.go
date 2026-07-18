package iotwireless_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_AssociateAwsAccountWithPartnerAccount(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		partnerAccountID string
		wantStatus       int
	}{
		{
			name:             "associate_partner_account",
			partnerAccountID: "partner-123",
			wantStatus:       http.StatusOK,
		},
		{
			name:             "idempotent_reassociation",
			partnerAccountID: "partner-456",
			wantStatus:       http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandlerHTTP()
			// Real AWS binds this op to POST /partner-accounts (no path
			// parameter): the partner account ID is Sidewalk.AmazonId in the
			// body, and Tags is a []Tag{Key,Value} list.
			body := `{"Sidewalk":{"AmazonId":"` + tt.partnerAccountID + `"},"Tags":[{"Key":"env","Value":"prod"}]}`
			rec := doIoTWRequest(t, h, http.MethodPost, "/partner-accounts", body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.NotEmpty(t, resp["Arn"])
			sidewalk, ok := resp["Sidewalk"].(map[string]any)
			require.True(t, ok)
			assert.Equal(t, tt.partnerAccountID, sidewalk["AmazonId"])
		})
	}
}

func TestHandler_PartnerAccounts(t *testing.T) {
	t.Parallel()

	h := newTestHandlerHTTP()

	// Associate partner account. Real AWS binds this op to the bare
	// collection path (POST /partner-accounts): the partner account ID
	// travels as Sidewalk.AmazonId in the body, never as a path parameter.
	rec := doIoTWRequest(t, h, http.MethodPost, "/partner-accounts", `{"Sidewalk":{"AmazonId":"partner-123"}}`)
	assert.Equal(t, http.StatusOK, rec.Code)

	var assocResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &assocResp))
	assert.NotEmpty(t, assocResp["Arn"])

	// Get partner account
	rec = doIoTWRequest(t, h, http.MethodGet, "/partner-accounts/partner-123", "")
	assert.Equal(t, http.StatusOK, rec.Code)

	var getResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getResp))
	assert.Equal(t, true, getResp["AccountLinked"])
	sidewalk, ok := getResp["Sidewalk"].(map[string]any)
	require.True(t, ok, "AccountLinked accounts must include Sidewalk info")
	assert.Equal(t, "partner-123", sidewalk["AmazonId"])
	assert.NotEmpty(t, sidewalk["Arn"])

	// Get an account that was never associated: AccountLinked must be false,
	// not a fabricated success.
	rec = doIoTWRequest(t, h, http.MethodGet, "/partner-accounts/never-linked", "")
	assert.Equal(t, http.StatusOK, rec.Code)

	var unlinkedResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &unlinkedResp))
	assert.Equal(t, false, unlinkedResp["AccountLinked"])

	// List partner accounts
	rec = doIoTWRequest(t, h, http.MethodGet, "/partner-accounts", "")
	assert.Equal(t, http.StatusOK, rec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	sidewalkList, ok := listResp["Sidewalk"].([]any)
	require.True(t, ok)
	require.Len(
		t,
		sidewalkList,
		1,
		"list must reflect the associated account, not a hardcoded empty list",
	)
	assert.Equal(t, "partner-123", sidewalkList[0].(map[string]any)["AmazonId"])

	// Update partner account for a linked account succeeds.
	rec = doIoTWRequest(t, h, http.MethodPatch, "/partner-accounts/partner-123", `{}`)
	assert.Equal(t, http.StatusNoContent, rec.Code)

	// Update partner account for an account that was never linked must be a
	// real 404, not a fabricated success.
	rec = doIoTWRequest(t, h, http.MethodPatch, "/partner-accounts/never-linked", `{}`)
	assert.Equal(t, http.StatusNotFound, rec.Code)

	// Disassociate partner account
	rec = doIoTWRequest(t, h, http.MethodDelete, "/partner-accounts/partner-123", "")
	assert.Equal(t, http.StatusNoContent, rec.Code)
}
