package directoryservice_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateTrust_Validation(t *testing.T) {
	t.Parallel()

	baseBody := func() map[string]any {
		return map[string]any{
			"DirectoryId":      "placeholder",
			"RemoteDomainName": "partner.example.com",
			"TrustPassword":    "TrustPw1!",
			"TrustDirection":   "Two-Way",
		}
	}

	tests := []struct {
		body     map[string]any
		name     string
		wantType string
		wantCode int
	}{
		{
			name: "missing TrustDirection returns InvalidParameterException",
			body: func() map[string]any {
				b := baseBody()
				delete(b, "TrustDirection")

				return b
			}(),
			wantCode: http.StatusBadRequest,
			wantType: "InvalidParameterException",
		},
		{
			name: "invalid TrustDirection returns InvalidParameterException",
			body: func() map[string]any {
				b := baseBody()
				b["TrustDirection"] = "Sideways"

				return b
			}(),
			wantCode: http.StatusBadRequest,
			wantType: "InvalidParameterException",
		},
		{
			name: "invalid TrustType returns InvalidParameterException",
			body: func() map[string]any {
				b := baseBody()
				b["TrustType"] = "Galaxy"

				return b
			}(),
			wantCode: http.StatusBadRequest,
			wantType: "InvalidParameterException",
		},
		{
			name: "invalid SelectiveAuth returns InvalidParameterException",
			body: func() map[string]any {
				b := baseBody()
				b["SelectiveAuth"] = "Maybe"

				return b
			}(),
			wantCode: http.StatusBadRequest,
			wantType: "InvalidParameterException",
		},
		{
			name: "One-Way: Outgoing direction succeeds",
			body: func() map[string]any {
				b := baseBody()
				b["TrustDirection"] = "One-Way: Outgoing"

				return b
			}(),
			wantCode: http.StatusOK,
		},
		{
			name: "External trust type with SelectiveAuth succeeds",
			body: func() map[string]any {
				b := baseBody()
				b["TrustType"] = "External"
				b["SelectiveAuth"] = "Enabled"

				return b
			}(),
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			dirID := mustCreateSimpleAD(t, h, "corp.example.com")
			tt.body["DirectoryId"] = dirID

			rec := doRequest(t, h, "CreateTrust", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
			if tt.wantType != "" {
				body := respBody(t, rec)
				assert.Equal(t, tt.wantType, body["__type"])
			}
		})
	}
}

func TestDescribeTrusts_StateFields(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	dirID := mustCreateSimpleAD(t, h, "corp.example.com")

	rec := doRequest(t, h, "CreateTrust", map[string]any{
		"DirectoryId":      dirID,
		"RemoteDomainName": "partner.example.com",
		"TrustPassword":    "TrustPw1!",
		"TrustDirection":   "Two-Way",
		"SelectiveAuth":    "Enabled",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	descRec := doRequest(t, h, "DescribeTrusts", map[string]any{"DirectoryId": dirID})
	require.Equal(t, http.StatusOK, descRec.Code)
	body := respBody(t, descRec)
	trusts, _ := body["Trusts"].([]any)
	require.Len(t, trusts, 1)
	trust := trusts[0].(map[string]any)

	assert.Equal(t, "Enabled", trust["SelectiveAuth"])
	assert.NotEmpty(t, trust["StateLastUpdatedDateTime"], "StateLastUpdatedDateTime must be populated on the wire")
	assert.Contains(t, trust, "StateLastUpdatedDateTime")
}

func TestTrusts(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "create describe update verify delete cycle"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			dirID := mustCreateSimpleAD(t, h, "corp.example.com")

			// Create trust
			rec1 := doRequest(t, h, "CreateTrust", map[string]any{
				"DirectoryId":      dirID,
				"RemoteDomainName": "partner.example.com",
				"TrustPassword":    "TrustPw1!",
				"TrustDirection":   "Two-Way",
			})
			assert.Equal(t, http.StatusOK, rec1.Code)
			var r1 map[string]any
			require.NoError(t, json.Unmarshal(rec1.Body.Bytes(), &r1))
			trustID, _ := r1["TrustId"].(string)
			assert.NotEmpty(t, trustID)

			// Describe
			rec2 := doRequest(t, h, "DescribeTrusts", map[string]any{"DirectoryId": dirID})
			assert.Equal(t, http.StatusOK, rec2.Code)
			var r2 map[string]any
			require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &r2))
			trusts, _ := r2["Trusts"].([]any)
			require.Len(t, trusts, 1)
			trust := trusts[0].(map[string]any)
			assert.Equal(t, "partner.example.com", trust["RemoteDomainName"])

			// Update
			rec3 := doRequest(t, h, "UpdateTrust", map[string]any{
				"TrustId":       trustID,
				"SelectiveAuth": "Enabled",
			})
			assert.Equal(t, http.StatusOK, rec3.Code)
			var r3 map[string]any
			require.NoError(t, json.Unmarshal(rec3.Body.Bytes(), &r3))
			assert.Equal(t, trustID, r3["TrustId"])

			// Verify
			rec4 := doRequest(t, h, "VerifyTrust", map[string]any{"TrustId": trustID})
			assert.Equal(t, http.StatusOK, rec4.Code)
			var r4 map[string]any
			require.NoError(t, json.Unmarshal(rec4.Body.Bytes(), &r4))
			assert.Equal(t, trustID, r4["TrustId"])

			// Delete
			rec5 := doRequest(t, h, "DeleteTrust", map[string]any{"TrustId": trustID})
			assert.Equal(t, http.StatusOK, rec5.Code)
			var r5 map[string]any
			require.NoError(t, json.Unmarshal(rec5.Body.Bytes(), &r5))
			assert.Equal(t, trustID, r5["TrustId"])

			// Describe after delete
			rec6 := doRequest(t, h, "DescribeTrusts", map[string]any{"DirectoryId": dirID})
			assert.Equal(t, http.StatusOK, rec6.Code)
			var r6 map[string]any
			require.NoError(t, json.Unmarshal(rec6.Body.Bytes(), &r6))
			trusts2, _ := r6["Trusts"].([]any)
			assert.Empty(t, trusts2)

			_ = tc
		})
	}
}
