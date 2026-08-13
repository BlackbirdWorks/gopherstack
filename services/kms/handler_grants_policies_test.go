package kms_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kms"
)

func TestHandler_CreateGrant_WithRetiringPrincipal_ViaHTTP(t *testing.T) {
	t.Parallel()
	h := b2newHandler(t)
	b := h.Backend.(*kms.InMemoryBackend)

	out, _ := b.CreateKey(context.Background(), &kms.CreateKeyInput{})
	keyID := out.KeyMetadata.KeyID

	body := fmt.Sprintf(
		`{"KeyId":"%s","GranteePrincipal":"arn:aws:iam::123456789012:role/grantee",`+
			`"RetiringPrincipal":"arn:aws:iam::123456789012:role/retiree","Operations":["Encrypt"]}`,
		keyID,
	)
	rec := b2postKMSOp(t, h, "CreateGrant", body)
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		GrantID    string `json:"GrantId"`
		GrantToken string `json:"GrantToken"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.NotEmpty(t, resp.GrantID)
	assert.NotEmpty(t, resp.GrantToken)
}

// TestKMSGrantOperations verifies CreateGrant, ListGrants, RevokeGrant, and RetireGrant.
func TestKMSGrantOperations(t *testing.T) {
	t.Parallel()

	backend := kms.NewInMemoryBackend()
	h := kms.NewHandler(backend)

	keyOut, err := backend.CreateKey(context.Background(), &kms.CreateKeyInput{Description: "grant-test"})
	require.NoError(t, err)
	keyID := keyOut.KeyMetadata.KeyID

	// CreateGrant
	createBody := `{"KeyId":"` + keyID + `","GranteePrincipal":"arn:aws:iam::000000000000:role/my-role",` +
		`"Operations":["Decrypt","Encrypt"]}`
	rec := doKMSHTTPRequest(t, h, "CreateGrant", createBody)
	assert.Equal(t, http.StatusOK, rec.Code)
	var createOut kms.CreateGrantOutput
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createOut))
	assert.NotEmpty(t, createOut.GrantID)
	assert.NotEmpty(t, createOut.GrantToken)

	// ListGrants
	listBody := `{"KeyId":"` + keyID + `"}`
	rec = doKMSHTTPRequest(t, h, "ListGrants", listBody)
	assert.Equal(t, http.StatusOK, rec.Code)
	var listOut kms.ListGrantsOutput
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listOut))
	require.Len(t, listOut.Grants, 1)
	assert.Equal(t, createOut.GrantID, listOut.Grants[0].GrantID)

	// RevokeGrant
	revokeBody := `{"KeyId":"` + keyID + `","GrantId":"` + createOut.GrantID + `"}`
	rec = doKMSHTTPRequest(t, h, "RevokeGrant", revokeBody)
	assert.Equal(t, http.StatusOK, rec.Code)

	// ListGrants after revoke — should be empty
	rec = doKMSHTTPRequest(t, h, "ListGrants", listBody)
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listOut))
	assert.Empty(t, listOut.Grants)
}

// TestKMSKeyPolicy verifies PutKeyPolicy and GetKeyPolicy.
func TestKMSKeyPolicy(t *testing.T) {
	t.Parallel()

	backend := kms.NewInMemoryBackend()
	h := kms.NewHandler(backend)

	keyOut, err := backend.CreateKey(context.Background(), &kms.CreateKeyInput{Description: "policy-test"})
	require.NoError(t, err)
	keyID := keyOut.KeyMetadata.KeyID

	policy := `{"Version":"2012-10-17","Statement":[]}`
	putBody, _ := json.Marshal(map[string]string{
		"KeyId":      keyID,
		"PolicyName": "default",
		"Policy":     policy,
	})
	rec := doKMSHTTPRequest(t, h, "PutKeyPolicy", string(putBody))
	assert.Equal(t, http.StatusOK, rec.Code)

	getBody, _ := json.Marshal(map[string]string{"KeyId": keyID, "PolicyName": "default"})
	rec = doKMSHTTPRequest(t, h, "GetKeyPolicy", string(getBody))
	assert.Equal(t, http.StatusOK, rec.Code)
	var getOut kms.GetKeyPolicyOutput
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getOut))
	assert.Equal(t, policy, getOut.Policy)
}

// TestKMSRetireGrant verifies RetireGrant and ListRetirableGrants operations.
func TestKMSRetireGrant(t *testing.T) {
	t.Parallel()

	backend := kms.NewInMemoryBackend()
	h := kms.NewHandler(backend)

	// Create a key and grant
	keyOut, err := backend.CreateKey(context.Background(), &kms.CreateKeyInput{Description: "retire-grant-test"})
	require.NoError(t, err)
	keyID := keyOut.KeyMetadata.KeyID

	createBody := `{"KeyId":"` + keyID + `","GranteePrincipal":"arn:aws:iam::000000000000:role/my-role",` +
		`"Operations":["Decrypt"],"RetiringPrincipal":"arn:aws:iam::000000000000:role/retire-role"}`
	rec := doKMSHTTPRequest(t, h, "CreateGrant", createBody)
	assert.Equal(t, http.StatusOK, rec.Code)

	var createOut kms.CreateGrantOutput
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createOut))
	grantToken := createOut.GrantToken

	// ListRetirableGrants for the retiring principal
	listBody, _ := json.Marshal(map[string]string{
		"RetiringPrincipal": "arn:aws:iam::000000000000:role/retire-role",
	})
	rec = doKMSHTTPRequest(t, h, "ListRetirableGrants", string(listBody))
	assert.Equal(t, http.StatusOK, rec.Code)

	var listOut kms.ListGrantsOutput
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listOut))
	require.Len(t, listOut.Grants, 1)
	assert.Equal(t, createOut.GrantID, listOut.Grants[0].GrantID)

	// RetireGrant using grant token
	retireBody, _ := json.Marshal(map[string]string{"GrantToken": grantToken})
	rec = doKMSHTTPRequest(t, h, "RetireGrant", string(retireBody))
	assert.Equal(t, http.StatusOK, rec.Code)

	// ListGrants should now be empty
	listGrantsBody := `{"KeyId":"` + keyID + `"}`
	rec = doKMSHTTPRequest(t, h, "ListGrants", listGrantsBody)
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listOut))
	assert.Empty(t, listOut.Grants)
}

// TestListGrants_RawBody_NoGrantTokenLeak asserts the raw JSON body of a
// ListGrants/ListRetirableGrants response never carries GrantToken or
// TokenIssuedAt. A grant token is a bearer credential real AWS returns
// exactly once, from CreateGrant; a typed client-side decode wouldn't catch
// the leak since json.Unmarshal silently drops unrecognised fields.
func TestListGrants_RawBody_NoGrantTokenLeak(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body   func(keyID string) string
		name   string
		action string
	}{
		{
			name:   "list_grants",
			action: "ListGrants",
			body:   func(keyID string) string { return `{"KeyId":"` + keyID + `"}` },
		},
		{
			name:   "list_retirable_grants",
			action: "ListRetirableGrants",
			body: func(string) string {
				return `{"RetiringPrincipal":"arn:aws:iam::000000000000:role/retire-role"}`
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := b2newHandler(t)
			b := h.Backend.(*kms.InMemoryBackend)

			out, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{})
			require.NoError(t, err)
			keyID := out.KeyMetadata.KeyID

			createBody := `{"KeyId":"` + keyID + `","GranteePrincipal":"arn:aws:iam::000000000000:role/grantee",` +
				`"RetiringPrincipal":"arn:aws:iam::000000000000:role/retire-role","Operations":["Encrypt"]}`
			rec := doKMSHTTPRequest(t, h, "CreateGrant", createBody)
			require.Equal(t, http.StatusOK, rec.Code)

			rec = doKMSHTTPRequest(t, h, tt.action, tt.body(keyID))
			require.Equal(t, http.StatusOK, rec.Code)

			var raw struct {
				Grants []map[string]json.RawMessage `json:"Grants"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &raw))
			require.Len(t, raw.Grants, 1)

			_, hasToken := raw.Grants[0]["GrantToken"]
			assert.False(t, hasToken, "ListGrants must never echo GrantToken")

			_, hasIssuedAt := raw.Grants[0]["TokenIssuedAt"]
			assert.False(t, hasIssuedAt, "TokenIssuedAt is internal bookkeeping, not part of real AWS's GrantListEntry")
		})
	}
}
