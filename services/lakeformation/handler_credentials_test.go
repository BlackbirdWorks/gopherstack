package lakeformation_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/lakeformation"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_AssumeDecoratedRoleWithSAML(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		wantFields []string
		wantStatus int
	}{
		{
			name: "success",
			body: `{"PrincipalArn":"arn:aws:iam::123:saml-provider/MyProvider",` +
				`"RoleArn":"arn:aws:iam::123:role/MyRole",` +
				`"SAMLAssertion":"c2FtbGFzc2VydGlvbg=="}`,
			wantStatus: http.StatusOK,
			wantFields: []string{"AccessKeyId", "SecretAccessKey", "SessionToken", "Expiration"},
		},
		{
			name:       "missing_principal_arn",
			body:       `{"RoleArn":"arn:aws:iam::123:role/R","SAMLAssertion":"abc"}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_role_arn",
			body:       `{"PrincipalArn":"arn:aws:iam::123:saml-provider/P","SAMLAssertion":"abc"}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_saml_assertion",
			body:       `{"PrincipalArn":"arn:aws:iam::123:saml-provider/P","RoleArn":"arn:aws:iam::123:role/R"}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "invalid_json",
			body:       `not-json`,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			rec := doLFRequest(t, h, "/AssumeDecoratedRoleWithSAML", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				for _, field := range tt.wantFields {
					assert.Contains(t, resp, field)
					assert.NotEmpty(t, resp[field])
				}
			}
		})
	}
}

func TestAssumeDecoratedRoleWithSAML_RoundTrip(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	rec := postJSON(t, h, "/AssumeDecoratedRoleWithSAML", map[string]any{
		"PrincipalArn":  "arn:aws:iam::123:saml-provider/MyProvider",
		"RoleArn":       "arn:aws:iam::123456789012:role/MyRole",
		"SAMLAssertion": "base64encodedsamlassertion",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.NotEmpty(t, out["AccessKeyId"])
	assert.NotEmpty(t, out["SecretAccessKey"])
	assert.NotEmpty(t, out["SessionToken"])
	assert.NotEmpty(t, out["Expiration"])
}

func TestGetDataLakePrincipal(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	rec := postJSON(t, h, "/GetDataLakePrincipal", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, jsonDecode(rec.Body, &out))
	assert.NotEmpty(t, out["Identity"])
}

// --- Validation improvement tests ---

func TestGetTemporaryDataLocationCredentials_Success(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	// The real GetTemporaryDataLocationCredentialsInput has no ResourceArn
	// or Permissions members at all -- it takes DataLocations (plural) and
	// CredentialsScope (confirmed against
	// api_op_GetTemporaryDataLocationCredentials.go /
	// serializers.go@aws-sdk-go-v2/service/lakeformation@v1.50.4). A prior
	// version of this test sent ResourceArn/Permissions and only passed
	// because the handler agreed with the same wrong shape.
	rec := postJSON(t, h, "/GetTemporaryDataLocationCredentials", map[string]any{
		"DataLocations":    []string{"s3://my-bucket/path"},
		"CredentialsScope": "READWRITE",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, jsonDecode(rec.Body, &out))
	require.NotNil(t, out["Credentials"])
	// Real AWS nests Expiration inside Credentials for
	// GetTemporaryDataLocationCredentials (unlike the Glue partition/table
	// credential ops, which return it at the top level).
	creds := out["Credentials"].(map[string]any)
	assert.NotEmpty(t, creds["Expiration"])
	assert.Equal(t, "READWRITE", out["CredentialsScope"])
	assert.Equal(t, []any{"s3://my-bucket/path"}, out["AccessibleDataLocations"])
}

func TestGetTemporaryDataLocationCredentials_MissingDataLocations(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	rec := postJSON(t, h, "/GetTemporaryDataLocationCredentials", map[string]any{
		"DataLocations": []string{},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestGetTemporaryGlueTableCredentials_MissingTableArn(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	rec := postJSON(t, h, "/GetTemporaryGlueTableCredentials", map[string]any{
		"TableArn": "",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// --- ExtendTransaction ---

func TestAssumeDecoratedRoleWithSAML_RandomCreds(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	body := map[string]any{
		"PrincipalArn":  "arn:aws:iam::123:saml-provider/IdP",
		"RoleArn":       "arn:aws:iam::123:role/MyRole",
		"SAMLAssertion": "dGVzdC1zYW1sLWFzc2VydGlvbg==",
	}

	rec1 := postJSON(t, h, "/AssumeDecoratedRoleWithSAML", body)
	require.Equal(t, http.StatusOK, rec1.Code)

	rec2 := postJSON(t, h, "/AssumeDecoratedRoleWithSAML", body)
	require.Equal(t, http.StatusOK, rec2.Code)

	var out1, out2 map[string]any
	require.NoError(t, jsonDecode(rec1.Body, &out1))
	require.NoError(t, jsonDecode(rec2.Body, &out2))

	assert.NotEmpty(t, out1["AccessKeyId"])
	assert.NotEmpty(t, out1["SessionToken"])

	// Credentials must differ between calls
	assert.NotEqual(t, out1["AccessKeyId"], out2["AccessKeyId"], "AccessKeyId should be unique per call")
	assert.NotEqual(t, out1["SessionToken"], out2["SessionToken"], "SessionToken should be unique per call")
}

// --- #9: GetTemporaryCredentials unique per request ---

func TestGetTemporaryGlueTableCredentials_UniquePerCall(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	body := map[string]any{
		"TableArn": "arn:aws:glue:us-east-1:123:table/db/tbl",
	}

	sessions := make([]string, 0, 3)

	for range 3 {
		rec := postJSON(t, h, "/GetTemporaryGlueTableCredentials", body)
		require.Equal(t, http.StatusOK, rec.Code)

		var out map[string]any
		require.NoError(t, jsonDecode(rec.Body, &out))
		// GetTemporaryGlueTableCredentialsOutput returns credential fields
		// flat (no nested "Credentials" object), unlike
		// GetTemporaryDataLocationCredentials -- see the real
		// aws-sdk-go-v2 GetTemporaryGlueTableCredentialsOutput shape.
		sessions = append(sessions, out["SessionToken"].(string))
	}

	// All session tokens must be distinct
	seen := make(map[string]bool)
	for _, s := range sessions {
		assert.False(t, seen[s], "duplicate SessionToken: %s", s)
		seen[s] = true
	}
}
