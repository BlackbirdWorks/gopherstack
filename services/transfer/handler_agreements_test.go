package transfer_test

import (
	"encoding/json"
	"maps"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/transfer"
)

func TestHandler_DescribeAgreementIncludesArnAndTags(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Need two profiles and a server for an agreement.
	localRec := doTransferRequest(t, h, "CreateProfile", map[string]any{
		"As2Id": "local-as2", "ProfileType": "LOCAL",
	})
	require.Equal(t, http.StatusOK, localRec.Code)
	var localResp map[string]any
	require.NoError(t, json.Unmarshal(localRec.Body.Bytes(), &localResp))
	localProfileID := localResp["ProfileId"].(string)

	partnerRec := doTransferRequest(t, h, "CreateProfile", map[string]any{
		"As2Id": "partner-as2", "ProfileType": "PARTNER",
	})
	require.Equal(t, http.StatusOK, partnerRec.Code)
	var partnerResp map[string]any
	require.NoError(t, json.Unmarshal(partnerRec.Body.Bytes(), &partnerResp))
	partnerProfileID := partnerResp["ProfileId"].(string)

	serverRec := doTransferRequest(t, h, "CreateServer", map[string]any{})
	require.Equal(t, http.StatusOK, serverRec.Code)
	var serverResp map[string]any
	require.NoError(t, json.Unmarshal(serverRec.Body.Bytes(), &serverResp))
	serverID := serverResp["ServerId"].(string)

	agRec := doTransferRequest(t, h, "CreateAgreement", map[string]any{
		"ServerId":         serverID,
		"LocalProfileId":   localProfileID,
		"PartnerProfileId": partnerProfileID,
		"BaseDirectory":    "/transfers",
		"AccessRole":       "arn:aws:iam::000000000000:role/transfer-role",
		"Tags":             []map[string]string{{"Key": "team", "Value": "ops"}},
	})
	require.Equal(t, http.StatusOK, agRec.Code)
	var agResp map[string]any
	require.NoError(t, json.Unmarshal(agRec.Body.Bytes(), &agResp))
	agreementID := agResp["AgreementId"].(string)

	rec := doTransferRequest(t, h, "DescribeAgreement", map[string]any{
		"ServerId":    serverID,
		"AgreementId": agreementID,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	ag := resp["Agreement"].(map[string]any)

	arn, hasArn := ag["Arn"].(string)
	assert.True(t, hasArn, "Arn must be present in DescribeAgreement response")
	assert.Contains(t, arn, agreementID, "Arn must contain AgreementId")
	assert.Contains(t, arn, "arn:aws:transfer:", "Arn must start with arn:aws:transfer:")

	tags, hasTags := ag["Tags"].([]any)
	assert.True(t, hasTags, "Tags must be present in DescribeAgreement response")
	assert.Len(t, tags, 1)
}

func TestHandler_CreateAgreement(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		wantCode int
	}{
		{
			name: "success",
			body: map[string]any{
				"LocalProfileId":   "p-local",
				"PartnerProfileId": "p-partner",
				"BaseDirectory":    "/home/as2",
				"AccessRole":       "arn:aws:iam::123456789012:role/as2-role",
				"Description":      "test agreement",
			},
			wantCode: http.StatusOK,
		},
		{
			name:     "minimal fields",
			body:     map[string]any{},
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			s, err := h.Backend.CreateServer(nil, nil)
			require.NoError(t, err)

			body := make(map[string]any, len(tt.body)+1)
			maps.Copy(body, tt.body)
			body["ServerId"] = s.ServerID

			rec := doTransferRequest(t, h, "CreateAgreement", body)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.NotEmpty(t, resp["AgreementId"])
			}
		})
	}
}

func TestHandler_CreateAgreement_MissingServerID(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doTransferRequest(t, h, "CreateAgreement", map[string]any{})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_CreateAgreement_ServerNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doTransferRequest(t, h, "CreateAgreement", map[string]any{
		"ServerId": "s-doesnotexist",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_DeleteAgreement(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(*transfer.Handler) (serverID, agreementID string)
		name     string
		wantCode int
	}{
		{
			name: "success",
			setup: func(h *transfer.Handler) (string, string) {
				s, _ := h.Backend.CreateServer(nil, nil)
				ag, _ := h.Backend.CreateAgreement(s.ServerID, "desc", "p-local", "p-partner", "/base", "arn:role", nil)

				return s.ServerID, ag.AgreementID
			},
			wantCode: http.StatusOK,
		},
		{
			name: "agreement not found",
			setup: func(h *transfer.Handler) (string, string) {
				s, _ := h.Backend.CreateServer(nil, nil)

				return s.ServerID, "a-doesnotexist"
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "server not found",
			setup: func(_ *transfer.Handler) (string, string) {
				return "s-doesnotexist", "a-doesnotexist"
			},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			serverID, agreementID := tt.setup(h)

			rec := doTransferRequest(t, h, "DeleteAgreement", map[string]any{
				"ServerId":    serverID,
				"AgreementId": agreementID,
			})
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestHandler_DeleteAgreement_MissingFields(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	s, err := h.Backend.CreateServer(nil, nil)
	require.NoError(t, err)

	tests := []struct {
		body     map[string]any
		name     string
		wantCode int
	}{
		{
			name:     "missing server id",
			body:     map[string]any{"AgreementId": "a-123"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "missing agreement id",
			body:     map[string]any{"ServerId": s.ServerID},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doTransferRequest(t, h, "DeleteAgreement", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestHandler_DescribeAgreement(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	s, err := h.Backend.CreateServer(nil, nil)
	require.NoError(t, err)

	prof, err := h.Backend.CreateProfile("LOCAL", "TESTPARTNER", nil)
	require.NoError(t, err)

	createRec := doTransferRequest(t, h, "CreateAgreement", map[string]any{
		"ServerId":         s.ServerID,
		"LocalProfileId":   prof.ProfileID,
		"PartnerProfileId": prof.ProfileID,
		"AccessRole":       "arn:aws:iam::123456789012:role/TransferRole",
		"BaseDirectory":    "/agreements",
		"Description":      "test agreement",
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	agreementID := createResp["AgreementId"].(string)

	rec := doTransferRequest(t, h, "DescribeAgreement", map[string]any{
		"ServerId":    s.ServerID,
		"AgreementId": agreementID,
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_ListAgreements(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	s, err := h.Backend.CreateServer(nil, nil)
	require.NoError(t, err)

	rec := doTransferRequest(t, h, "ListAgreements", map[string]any{
		"ServerId": s.ServerID,
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_UpdateAgreement(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	s, err := h.Backend.CreateServer(nil, nil)
	require.NoError(t, err)

	prof, err := h.Backend.CreateProfile("LOCAL", "TESTPARTNER", nil)
	require.NoError(t, err)

	createRec := doTransferRequest(t, h, "CreateAgreement", map[string]any{
		"ServerId":         s.ServerID,
		"LocalProfileId":   prof.ProfileID,
		"PartnerProfileId": prof.ProfileID,
		"AccessRole":       "arn:aws:iam::123456789012:role/TransferRole",
		"BaseDirectory":    "/agreements",
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	agreementID := createResp["AgreementId"].(string)

	rec := doTransferRequest(t, h, "UpdateAgreement", map[string]any{
		"ServerId":    s.ServerID,
		"AgreementId": agreementID,
		"Description": "updated description",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// Test 11: ListAgreements response includes Arn + LocalProfileId + PartnerProfileId.
func TestHandler_ListAgreementsEnrichedFields(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	s, err := h.Backend.CreateServer(nil, nil)
	require.NoError(t, err)

	_, err = h.Backend.CreateAgreementFull(
		s.ServerID,
		"test agreement",
		"local-profile-1",
		"partner-profile-1",
		"/base",
		"arn:aws:iam::000000000000:role/transfer",
		"ACTIVE",
		nil,
	)
	require.NoError(t, err)

	listRec := doTransferRequest(t, h, "ListAgreements", map[string]any{
		"ServerId": s.ServerID,
	})
	require.Equal(t, http.StatusOK, listRec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
	agreements := listResp["Agreements"].([]any)
	require.Len(t, agreements, 1)

	ag := agreements[0].(map[string]any)
	assert.NotEmpty(t, ag["Arn"])
	assert.Equal(t, "local-profile-1", ag["LocalProfileId"])
	assert.Equal(t, "partner-profile-1", ag["PartnerProfileId"])
}

// Test 12: Agreement with invalid Status on Create returns 400.
func TestHandler_CreateAgreementInvalidStatus(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	s, err := h.Backend.CreateServer(nil, nil)
	require.NoError(t, err)

	rec := doTransferRequest(t, h, "CreateAgreement", map[string]any{
		"ServerId":         s.ServerID,
		"LocalProfileId":   "lp-1",
		"PartnerProfileId": "pp-1",
		"BaseDirectory":    "/",
		"AccessRole":       "arn:aws:iam::000000000000:role/transfer",
		"Status":           "PENDING", // invalid
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// Test 27: Agreement created with INACTIVE status is stored correctly.
func TestHandler_CreateAgreementInactiveStatus(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	s, err := h.Backend.CreateServer(nil, nil)
	require.NoError(t, err)

	rec := doTransferRequest(t, h, "CreateAgreement", map[string]any{
		"ServerId":         s.ServerID,
		"LocalProfileId":   "lp-1",
		"PartnerProfileId": "pp-1",
		"BaseDirectory":    "/",
		"AccessRole":       "arn:aws:iam::000000000000:role/transfer",
		"Status":           "INACTIVE",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	agID := createResp["AgreementId"].(string)

	descRec := doTransferRequest(t, h, "DescribeAgreement", map[string]any{
		"ServerId":    s.ServerID,
		"AgreementId": agID,
	})
	require.Equal(t, http.StatusOK, descRec.Code)

	var descResp map[string]any
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descResp))
	ag := descResp["Agreement"].(map[string]any)
	assert.Equal(t, "INACTIVE", ag["Status"])
}
