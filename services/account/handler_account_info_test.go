package account_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/account"
)

func TestHandler_GetAccountInformation(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "/getAccountInformation", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	respBody := rec.Body.Bytes()

	var out account.Info
	require.NoError(t, json.Unmarshal(respBody, &out))
	assert.NotEmpty(t, out.AccountID)
	assert.NotEmpty(t, out.AccountCreatedDate)
	assert.Equal(t, account.StateActive, out.AccountState)

	// Response is a flat object, not wrapped like most other operations.
	var raw map[string]any
	require.NoError(t, json.Unmarshal(respBody, &raw))
	assert.NotContains(t, raw, "Account")
}

func TestHandler_PutAccountName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		accountName string
		wantStatus  int
	}{
		{name: "valid_name", accountName: "My Corp", wantStatus: http.StatusOK},
		{name: "empty_name", accountName: "", wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			body := map[string]any{}
			if tt.accountName != "" {
				body["AccountName"] = tt.accountName
			}

			rec := doRequest(t, h, "/putAccountName", body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				infoRec := doRequest(t, h, "/getAccountInformation", map[string]any{})
				require.Equal(t, http.StatusOK, infoRec.Code)

				var out account.Info
				require.NoError(t, json.NewDecoder(infoRec.Body).Decode(&out))
				assert.Equal(t, tt.accountName, out.AccountName)
			}
		})
	}
}

func TestHandler_PrimaryEmail_GetDefault(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "/getPrimaryEmail", map[string]any{"AccountId": "000000000000"})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
	assert.NotEmpty(t, out["PrimaryEmail"])
}

func TestHandler_PrimaryEmail_GetMissingAccountID(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "/getPrimaryEmail", map[string]any{})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_PrimaryEmail_UpdateFlow(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		newEmail string
	}{
		{name: "update_email", newEmail: "new@example.com"},
		{name: "update_email_again", newEmail: "another@example.org"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			startRec := doRequest(t, h, "/startPrimaryEmailUpdate", map[string]any{
				"AccountId":    "000000000000",
				"PrimaryEmail": tc.newEmail,
			})
			require.Equal(t, http.StatusOK, startRec.Code)

			var startOut map[string]any
			require.NoError(t, json.NewDecoder(startRec.Body).Decode(&startOut))
			assert.Equal(t, "PENDING", startOut["Status"])

			acceptRec := doRequest(t, h, "/acceptPrimaryEmailUpdate", map[string]any{
				"AccountId":    "000000000000",
				"Otp":          "123456",
				"PrimaryEmail": tc.newEmail,
			})
			require.Equal(t, http.StatusOK, acceptRec.Code)

			var acceptOut map[string]any
			require.NoError(t, json.NewDecoder(acceptRec.Body).Decode(&acceptOut))
			assert.Equal(t, "ACCEPTED", acceptOut["Status"])

			getRec := doRequest(t, h, "/getPrimaryEmail", map[string]any{"AccountId": "000000000000"})
			require.Equal(t, http.StatusOK, getRec.Code)

			var getOut map[string]any
			require.NoError(t, json.NewDecoder(getRec.Body).Decode(&getOut))
			assert.Equal(t, tc.newEmail, getOut["PrimaryEmail"])
		})
	}
}

func TestHandler_PrimaryEmail_AcceptInvalidOTP(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		otp        string
		wantStatus int
	}{
		{name: "wrong_otp", otp: "000000", wantStatus: http.StatusBadRequest},
		{name: "no_pending", otp: "", wantStatus: http.StatusNotFound},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tc.otp != "" {
				doRequest(t, h, "/startPrimaryEmailUpdate", map[string]any{
					"AccountId": "000000000000", "PrimaryEmail": "x@example.com",
				})

				rec := doRequest(t, h, "/acceptPrimaryEmailUpdate", map[string]any{
					"AccountId": "000000000000", "Otp": tc.otp, "PrimaryEmail": "x@example.com",
				})
				assert.Equal(t, tc.wantStatus, rec.Code)
			} else {
				rec := doRequest(t, h, "/acceptPrimaryEmailUpdate", map[string]any{
					"AccountId": "000000000000", "Otp": "999999", "PrimaryEmail": "y@example.com",
				})
				assert.Equal(t, tc.wantStatus, rec.Code)
			}
		})
	}
}

func TestHandler_PrimaryEmail_StartMissingFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body map[string]any
		name string
	}{
		{name: "missing_account_id", body: map[string]any{"PrimaryEmail": "a@example.com"}},
		{name: "missing_primary_email", body: map[string]any{"AccountId": "000000000000"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "/startPrimaryEmailUpdate", tt.body)
			assert.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}
}

func TestHandler_AcceptPrimaryEmailUpdate_MissingFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body map[string]any
		name string
	}{
		{name: "missing_account_id", body: map[string]any{"Otp": "123456", "PrimaryEmail": "a@example.com"}},
		{name: "missing_otp", body: map[string]any{"AccountId": "000000000000", "PrimaryEmail": "a@example.com"}},
		{name: "missing_primary_email", body: map[string]any{"AccountId": "000000000000", "Otp": "123456"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "/acceptPrimaryEmailUpdate", tt.body)
			assert.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}
}
