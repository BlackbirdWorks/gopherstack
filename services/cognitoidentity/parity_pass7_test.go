package cognitoidentity_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestParity_CreateIdentityPool_OIDCProviderARNs verifies that
// OpenIDConnectProviderARNs are stored and returned in CreateIdentityPool /
// DescribeIdentityPool responses.
func TestParity_CreateIdentityPool_OIDCProviderARNs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		arns     []string
		wantARNs []string
	}{
		{
			name:     "single_oidc_arn",
			arns:     []string{"arn:aws:iam::000000000000:oidc-provider/accounts.google.com"},
			wantARNs: []string{"arn:aws:iam::000000000000:oidc-provider/accounts.google.com"},
		},
		{
			name: "multiple_oidc_arns",
			arns: []string{
				"arn:aws:iam::000000000000:oidc-provider/accounts.google.com",
				"arn:aws:iam::000000000000:oidc-provider/appleid.apple.com",
			},
			wantARNs: []string{
				"arn:aws:iam::000000000000:oidc-provider/accounts.google.com",
				"arn:aws:iam::000000000000:oidc-provider/appleid.apple.com",
			},
		},
		{
			name:     "no_oidc_arns",
			arns:     nil,
			wantARNs: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			rec := doCognitoIdentityRequest(t, h, "CreateIdentityPool", map[string]any{
				"IdentityPoolName":               "oidc-pool-" + tt.name,
				"AllowUnauthenticatedIdentities": false,
				"OpenIDConnectProviderARNs":      tt.arns,
			})
			require.Equal(t, http.StatusOK, rec.Code, "create: %s", rec.Body.String())

			var created map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &created))

			// Verify ARNs in CreateIdentityPool response.
			raw, _ := json.Marshal(created["OpenIdConnectProviderARNs"])
			var gotARNs []string
			_ = json.Unmarshal(raw, &gotARNs)
			assert.Equal(t, tt.wantARNs, gotARNs, "create response OpenIdConnectProviderARNs mismatch")

			poolID, _ := created["IdentityPoolId"].(string)
			require.NotEmpty(t, poolID)

			// Verify ARNs survive DescribeIdentityPool.
			descRec := doCognitoIdentityRequest(t, h, "DescribeIdentityPool", map[string]any{
				"IdentityPoolId": poolID,
			})
			require.Equal(t, http.StatusOK, descRec.Code)

			var desc map[string]any
			require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &desc))

			raw, _ = json.Marshal(desc["OpenIdConnectProviderARNs"])
			var descARNs []string
			_ = json.Unmarshal(raw, &descARNs)
			assert.Equal(t, tt.wantARNs, descARNs, "describe response OpenIdConnectProviderARNs mismatch")
		})
	}
}

// TestParity_CreateIdentityPool_SAMLProviderARNs verifies that
// SamlProviderARNs are stored and returned correctly.
func TestParity_CreateIdentityPool_SAMLProviderARNs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		arns     []string
		wantARNs []string
	}{
		{
			name:     "single_saml_arn",
			arns:     []string{"arn:aws:iam::000000000000:saml-provider/MyCorpSAML"},
			wantARNs: []string{"arn:aws:iam::000000000000:saml-provider/MyCorpSAML"},
		},
		{
			name:     "no_saml_arns",
			arns:     nil,
			wantARNs: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			rec := doCognitoIdentityRequest(t, h, "CreateIdentityPool", map[string]any{
				"IdentityPoolName":               "saml-pool-" + tt.name,
				"AllowUnauthenticatedIdentities": false,
				"SamlProviderARNs":               tt.arns,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var created map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &created))

			raw, _ := json.Marshal(created["SamlProviderARNs"])
			var gotARNs []string
			_ = json.Unmarshal(raw, &gotARNs)
			assert.Equal(t, tt.wantARNs, gotARNs, "SamlProviderARNs mismatch in create response")
		})
	}
}

// TestParity_UpdateIdentityPool_OIDCAndSAMLARNs verifies that OIDC/SAML ARNs
// can be updated via UpdateIdentityPool.
func TestParity_UpdateIdentityPool_OIDCAndSAMLARNs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		createOIDC []string
		updateOIDC []string
		createSAML []string
		updateSAML []string
		wantOIDC   []string
		wantSAML   []string
	}{
		{
			name:       "oidc_added_on_update",
			createOIDC: nil,
			updateOIDC: []string{"arn:aws:iam::000000000000:oidc-provider/accounts.google.com"},
			createSAML: nil,
			updateSAML: nil,
			wantOIDC:   []string{"arn:aws:iam::000000000000:oidc-provider/accounts.google.com"},
			wantSAML:   nil,
		},
		{
			name:       "saml_added_on_update",
			createOIDC: nil,
			updateOIDC: nil,
			createSAML: nil,
			updateSAML: []string{"arn:aws:iam::000000000000:saml-provider/Corp"},
			wantOIDC:   nil,
			wantSAML:   []string{"arn:aws:iam::000000000000:saml-provider/Corp"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			createRec := doCognitoIdentityRequest(t, h, "CreateIdentityPool", map[string]any{
				"IdentityPoolName":               "upd-arn-pool-" + tt.name,
				"AllowUnauthenticatedIdentities": true,
				"OpenIDConnectProviderARNs":      tt.createOIDC,
				"SamlProviderARNs":               tt.createSAML,
			})
			require.Equal(t, http.StatusOK, createRec.Code)

			var created map[string]any
			require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &created))
			poolID, _ := created["IdentityPoolId"].(string)
			require.NotEmpty(t, poolID)

			updRec := doCognitoIdentityRequest(t, h, "UpdateIdentityPool", map[string]any{
				"IdentityPoolId":                 poolID,
				"IdentityPoolName":               "upd-arn-pool-" + tt.name,
				"AllowUnauthenticatedIdentities": true,
				"OpenIDConnectProviderARNs":      tt.updateOIDC,
				"SamlProviderARNs":               tt.updateSAML,
			})
			require.Equal(t, http.StatusOK, updRec.Code, "update: %s", updRec.Body.String())

			var updated map[string]any
			require.NoError(t, json.Unmarshal(updRec.Body.Bytes(), &updated))

			raw, _ := json.Marshal(updated["OpenIdConnectProviderARNs"])
			var gotOIDC []string
			_ = json.Unmarshal(raw, &gotOIDC)
			assert.Equal(t, tt.wantOIDC, gotOIDC, "OIDC ARNs after update")

			raw, _ = json.Marshal(updated["SamlProviderARNs"])
			var gotSAML []string
			_ = json.Unmarshal(raw, &gotSAML)
			assert.Equal(t, tt.wantSAML, gotSAML, "SAML ARNs after update")
		})
	}
}

// TestParity_ListIdentityPools_Pagination verifies ListIdentityPools pagination.
func TestParity_ListIdentityPools_Pagination(t *testing.T) {
	t.Parallel()

	tests := []struct { //nolint:govet // fieldalignment: readability over micro-optimization
		name       string
		pools      []string
		maxResults int
		wantPages  []int
	}{
		{
			name:       "two_pages",
			pools:      []string{"aaa-pool", "bbb-pool", "ccc-pool", "ddd-pool"},
			maxResults: 2,
			wantPages:  []int{2, 2},
		},
		{
			name:       "single_page_exact",
			pools:      []string{"aaa-pool", "bbb-pool"},
			maxResults: 2,
			wantPages:  []int{2},
		},
		{
			name:       "partial_second_page",
			pools:      []string{"aaa-pool", "bbb-pool", "ccc-pool"},
			maxResults: 2,
			wantPages:  []int{2, 1},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			for _, name := range tt.pools {
				doCognitoIdentityRequest(t, h, "CreateIdentityPool", map[string]any{
					"IdentityPoolName":               name,
					"AllowUnauthenticatedIdentities": true,
				})
			}

			var (
				token      string
				pageCounts []int
			)

			for {
				req := map[string]any{"MaxResults": tt.maxResults}
				if token != "" {
					req["NextToken"] = token
				}

				rec := doCognitoIdentityRequest(t, h, "ListIdentityPools", req)
				require.Equal(t, http.StatusOK, rec.Code)

				var out struct { //nolint:govet // fieldalignment: readability over micro-optimization
					IdentityPools []any  `json:"IdentityPools"`
					NextToken     string `json:"NextToken"`
				}
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

				pageCounts = append(pageCounts, len(out.IdentityPools))
				token = out.NextToken

				if token == "" {
					break
				}
			}

			assert.Equal(t, tt.wantPages, pageCounts)
		})
	}
}

// TestParity_ListIdentityPools_TokenPastEnd verifies that a nextToken past the
// last pool returns an empty page, not the first page.
func TestParity_ListIdentityPools_TokenPastEnd(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doCognitoIdentityRequest(t, h, "CreateIdentityPool", map[string]any{
		"IdentityPoolName":               "only-pool",
		"AllowUnauthenticatedIdentities": true,
	})

	rec := doCognitoIdentityRequest(t, h, "ListIdentityPools", map[string]any{
		"MaxResults": 10,
		"NextToken":  "zzz-past-end-token",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct { //nolint:govet // fieldalignment: readability over micro-optimization
		IdentityPools []any  `json:"IdentityPools"`
		NextToken     string `json:"NextToken"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	assert.Empty(t, out.IdentityPools, "token past last pool must return empty list, not first page")
	assert.Empty(t, out.NextToken)
}

// TestParity_GetCredentialsForIdentity_CustomRoleArn verifies that
// GetCredentialsForIdentity accepts a CustomRoleArn field without error.
func TestParity_GetCredentialsForIdentity_CustomRoleArn(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		customRoleArn string
	}{
		{
			name:          "with_custom_role",
			customRoleArn: "arn:aws:iam::000000000000:role/MyCustomRole",
		},
		{
			name:          "without_custom_role",
			customRoleArn: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			createRec := doCognitoIdentityRequest(t, h, "CreateIdentityPool", map[string]any{
				"IdentityPoolName":               "cust-role-pool-" + tt.name,
				"AllowUnauthenticatedIdentities": true,
			})
			require.Equal(t, http.StatusOK, createRec.Code)

			var created map[string]any
			require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &created))

			idRec := doCognitoIdentityRequest(t, h, "GetId", map[string]any{
				"AccountId":      "000000000000",
				"IdentityPoolId": created["IdentityPoolId"],
			})
			require.Equal(t, http.StatusOK, idRec.Code)

			var idOut map[string]any
			require.NoError(t, json.Unmarshal(idRec.Body.Bytes(), &idOut))

			req := map[string]any{"IdentityId": idOut["IdentityId"]}
			if tt.customRoleArn != "" {
				req["CustomRoleArn"] = tt.customRoleArn
			}

			rec := doCognitoIdentityRequest(t, h, "GetCredentialsForIdentity", req)
			assert.Equal(
				t,
				http.StatusOK,
				rec.Code,
				"GetCredentialsForIdentity with CustomRoleArn: %s",
				rec.Body.String(),
			)
		})
	}
}
