package cognitoidp_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func validTermsBody(poolID, clientID string) map[string]any {
	return map[string]any{
		"UserPoolId":  poolID,
		"ClientId":    clientID,
		"TermsName":   "terms-of-use",
		"Enforcement": "NONE",
		"TermsSource": "LINK",
		"Links":       map[string]string{"cognito:default": "https://terms.example.com"},
	}
}

func TestCreateTerms_Validation(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolID, clientID := setupHandlerPoolAndClient(t, h, "terms-validation-pool")

	tests := []struct {
		mutate func(map[string]any)
		name   string
	}{
		{name: "missing clientId", mutate: func(b map[string]any) { delete(b, "ClientId") }},
		{name: "missing termsName", mutate: func(b map[string]any) { delete(b, "TermsName") }},
		{name: "missing enforcement", mutate: func(b map[string]any) { delete(b, "Enforcement") }},
		{name: "missing termsSource", mutate: func(b map[string]any) { delete(b, "TermsSource") }},
		{name: "bad enforcement", mutate: func(b map[string]any) { b["Enforcement"] = "BOGUS" }},
		{name: "bad termsSource", mutate: func(b map[string]any) { b["TermsSource"] = "BOGUS" }},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			body := validTermsBody(poolID, clientID)
			tc.mutate(body)

			rec := doCognitoRequest(t, h, "CreateTerms", body)
			assert.Equal(t, http.StatusBadRequest, rec.Code)

			var errResp struct {
				Type string `json:"__type,omitempty"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
			assert.Equal(t, "InvalidParameterException", errResp.Type)
		})
	}
}

func TestCreateTerms_UnknownClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolID, _ := setupHandlerPoolAndClient(t, h, "terms-unknown-client-pool")

	rec := doCognitoRequest(t, h, "CreateTerms", validTermsBody(poolID, "bogus-client-id"))
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var errResp struct {
		Type string `json:"__type,omitempty"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "ResourceNotFoundException", errResp.Type)
}

func TestCreateTerms_Duplicate(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolID, clientID := setupHandlerPoolAndClient(t, h, "terms-dup-pool")

	rec := doCognitoRequest(t, h, "CreateTerms", validTermsBody(poolID, clientID))
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doCognitoRequest(t, h, "CreateTerms", validTermsBody(poolID, clientID))
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var errResp struct {
		Type string `json:"__type,omitempty"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "TermsExistsException", errResp.Type)
}

func TestTerms_CRUD(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolID, clientID := setupHandlerPoolAndClient(t, h, "terms-crud-pool")

	rec := doCognitoRequest(t, h, "ListTerms", map[string]any{"UserPoolId": poolID})
	require.Equal(t, http.StatusOK, rec.Code)

	var listResp struct {
		Terms []struct {
			TermsID     string `json:"TermsId,omitempty"`
			TermsName   string `json:"TermsName,omitempty"`
			Enforcement string `json:"Enforcement,omitempty"`
			ClientID    string `json:"ClientId,omitempty"`
		} `json:"Terms"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	assert.Empty(t, listResp.Terms)

	rec = doCognitoRequest(t, h, "CreateTerms", validTermsBody(poolID, clientID))
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp struct {
		Terms struct {
			Links            map[string]string `json:"Links,omitempty"`
			TermsID          string            `json:"TermsId,omitempty"`
			ClientID         string            `json:"ClientId,omitempty"`
			UserPoolID       string            `json:"UserPoolId,omitempty"`
			TermsName        string            `json:"TermsName,omitempty"`
			Enforcement      string            `json:"Enforcement,omitempty"`
			TermsSource      string            `json:"TermsSource,omitempty"`
			CreationDate     float64           `json:"CreationDate,omitempty"`
			LastModifiedDate float64           `json:"LastModifiedDate,omitempty"`
		} `json:"Terms"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	require.NotEmpty(t, createResp.Terms.TermsID)
	assert.Equal(t, clientID, createResp.Terms.ClientID)
	assert.Equal(t, poolID, createResp.Terms.UserPoolID)
	assert.Equal(t, "terms-of-use", createResp.Terms.TermsName)
	assert.Equal(t, "NONE", createResp.Terms.Enforcement)
	assert.Equal(t, "LINK", createResp.Terms.TermsSource)
	assert.Equal(t, "https://terms.example.com", createResp.Terms.Links["cognito:default"])
	assert.Positive(t, createResp.Terms.CreationDate)
	termsID := createResp.Terms.TermsID

	rec = doCognitoRequest(t, h, "DescribeTerms", map[string]any{"UserPoolId": poolID, "TermsId": termsID})
	require.Equal(t, http.StatusOK, rec.Code)

	// List returns TermsDescriptionType, which has no ClientId field on the real wire.
	rec = doCognitoRequest(t, h, "ListTerms", map[string]any{"UserPoolId": poolID})
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	require.Len(t, listResp.Terms, 1)
	assert.Equal(t, termsID, listResp.Terms[0].TermsID)
	assert.Empty(t, listResp.Terms[0].ClientID, "TermsDescriptionType has no ClientId field")

	rec = doCognitoRequest(t, h, "UpdateTerms", map[string]any{
		"UserPoolId": poolID,
		"TermsId":    termsID,
		"TermsName":  "terms-of-use-v2",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	assert.Equal(t, "terms-of-use-v2", createResp.Terms.TermsName)
	assert.Equal(t, "NONE", createResp.Terms.Enforcement, "unset fields on update are left unchanged")

	rec = doCognitoRequest(t, h, "DeleteTerms", map[string]any{"UserPoolId": poolID, "TermsId": termsID})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doCognitoRequest(t, h, "ListTerms", map[string]any{"UserPoolId": poolID})
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	assert.Empty(t, listResp.Terms)
}

func TestTerms_NotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body   func(poolID string) map[string]any
		name   string
		action string
	}{
		{
			name:   "describe unknown termsId",
			action: "DescribeTerms",
			body:   func(poolID string) map[string]any { return map[string]any{"UserPoolId": poolID, "TermsId": "bogus"} },
		},
		{
			name:   "update unknown termsId",
			action: "UpdateTerms",
			body:   func(poolID string) map[string]any { return map[string]any{"UserPoolId": poolID, "TermsId": "bogus"} },
		},
		{
			name:   "delete unknown termsId",
			action: "DeleteTerms",
			body:   func(poolID string) map[string]any { return map[string]any{"UserPoolId": poolID, "TermsId": "bogus"} },
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			poolID, _ := setupHandlerPoolAndClient(t, h, "terms-notfound-"+tc.name)

			rec := doCognitoRequest(t, h, tc.action, tc.body(poolID))
			assert.Equal(t, http.StatusBadRequest, rec.Code)

			var errResp struct {
				Type string `json:"__type,omitempty"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
			assert.Equal(t, "ResourceNotFoundException", errResp.Type)
		})
	}
}

func TestTerms_InvalidPool(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doCognitoRequest(t, h, "CreateTerms", validTermsBody("bad-pool", "bad-client"))
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}
