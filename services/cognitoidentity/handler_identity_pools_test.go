package cognitoidentity_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateIdentityPool_OIDCProviderARNs(t *testing.T) {
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

// TestCreateIdentityPool_SAMLProviderARNs verifies that
// SamlProviderARNs are stored and returned correctly.
func TestCreateIdentityPool_SAMLProviderARNs(t *testing.T) {
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

// TestUpdateIdentityPool_OIDCAndSAMLARNs verifies that OIDC/SAML ARNs
// can be updated via UpdateIdentityPool.
func TestUpdateIdentityPool_OIDCAndSAMLARNs(t *testing.T) {
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

// TestListIdentityPools_Pagination verifies ListIdentityPools pagination.
func TestListIdentityPools_Pagination(t *testing.T) {
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

// TestListIdentityPools_TokenPastEnd verifies that a nextToken past the
// last pool returns an empty page, not the first page.
func TestListIdentityPools_TokenPastEnd(t *testing.T) {
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

func TestHandler_CreateIdentityPool(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		poolName string
		wantCode int
	}{
		{
			name:     "success",
			poolName: "my-identity-pool",
			wantCode: http.StatusOK,
		},
		{
			name:     "empty_name",
			poolName: "",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doCognitoIdentityRequest(t, h, "CreateIdentityPool", map[string]any{
				"IdentityPoolName":               tt.poolName,
				"AllowUnauthenticatedIdentities": true,
			})

			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				assert.Equal(t, tt.poolName, out["IdentityPoolName"])
				assert.NotEmpty(t, out["IdentityPoolId"])
			}
		})
	}
}

func TestHandler_DeleteIdentityPool(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantCode int
		notFound bool
	}{
		{name: "success", wantCode: http.StatusOK},
		{name: "not_found", wantCode: http.StatusBadRequest, notFound: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			var poolID string

			if !tt.notFound {
				rec := doCognitoIdentityRequest(t, h, "CreateIdentityPool", map[string]any{
					"IdentityPoolName":               "del-pool",
					"AllowUnauthenticatedIdentities": true,
				})
				require.Equal(t, http.StatusOK, rec.Code)

				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				poolID = out["IdentityPoolId"].(string)
			} else {
				poolID = "us-east-1:nonexistent"
			}

			rec := doCognitoIdentityRequest(t, h, "DeleteIdentityPool", map[string]any{
				"IdentityPoolId": poolID,
			})

			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestHandler_DescribeIdentityPool(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doCognitoIdentityRequest(t, h, "CreateIdentityPool", map[string]any{
		"IdentityPoolName":               "desc-pool",
		"AllowUnauthenticatedIdentities": false,
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var created map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &created))

	rec := doCognitoIdentityRequest(t, h, "DescribeIdentityPool", map[string]any{
		"IdentityPoolId": created["IdentityPoolId"],
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Equal(t, "desc-pool", out["IdentityPoolName"])
}

func TestHandler_ListIdentityPools(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for _, name := range []string{"pool-1", "pool-2"} {
		rec := doCognitoIdentityRequest(t, h, "CreateIdentityPool", map[string]any{
			"IdentityPoolName":               name,
			"AllowUnauthenticatedIdentities": true,
		})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	rec := doCognitoIdentityRequest(t, h, "ListIdentityPools", map[string]any{
		"MaxResults": 10,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	pools, ok := out["IdentityPools"].([]any)
	require.True(t, ok)
	assert.Len(t, pools, 2)
}

func TestHandler_UpdateIdentityPool(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doCognitoIdentityRequest(t, h, "CreateIdentityPool", map[string]any{
		"IdentityPoolName":               "update-pool",
		"AllowUnauthenticatedIdentities": true,
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var created map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &created))

	rec := doCognitoIdentityRequest(t, h, "UpdateIdentityPool", map[string]any{
		"IdentityPoolId":                 created["IdentityPoolId"],
		"IdentityPoolName":               "update-pool",
		"AllowUnauthenticatedIdentities": false,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Equal(t, false, out["AllowUnauthenticatedIdentities"])
}

func TestHandler_DescribeIdentityPool_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doCognitoIdentityRequest(t, h, "DescribeIdentityPool", map[string]any{
		"IdentityPoolId": "us-east-1:nonexistent",
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_UpdateIdentityPool_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doCognitoIdentityRequest(t, h, "UpdateIdentityPool", map[string]any{
		"IdentityPoolId":                 "us-east-1:nonexistent",
		"IdentityPoolName":               "new-name",
		"AllowUnauthenticatedIdentities": false,
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_WithProviders(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doCognitoIdentityRequest(t, h, "CreateIdentityPool", map[string]any{
		"IdentityPoolName":               "provider-pool",
		"AllowUnauthenticatedIdentities": false,
		"CognitoIdentityProviders": []map[string]any{
			{
				"ProviderName":         "cognito-idp.us-east-1.amazonaws.com/us-east-1_xxx",
				"ClientId":             "client123",
				"ServerSideTokenCheck": true,
			},
		},
	})

	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Equal(t, "provider-pool", out["IdentityPoolName"])

	providers, ok := out["CognitoIdentityProviders"].([]any)
	require.True(t, ok)
	assert.Len(t, providers, 1)
}

func TestSortedListIdentityPools(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for _, name := range []string{"ccc-pool", "aaa-pool", "bbb-pool"} {
		rec := doCognitoIdentityRequest(t, h, "CreateIdentityPool", map[string]any{
			"IdentityPoolName":               name,
			"AllowUnauthenticatedIdentities": true,
		})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	rec := doCognitoIdentityRequest(t, h, "ListIdentityPools", map[string]any{
		"MaxResults": 0,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	pools, _ := out["IdentityPools"].([]any)
	require.Len(t, pools, 3)

	names := make([]string, len(pools))
	for i, p := range pools {
		pm, _ := p.(map[string]any)
		names[i] = pm["IdentityPoolName"].(string)
	}

	assert.Equal(t, []string{"aaa-pool", "bbb-pool", "ccc-pool"}, names)
}

func TestTagsInIdentityPoolOutput(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doCognitoIdentityRequest(t, h, "CreateIdentityPool", map[string]any{
		"IdentityPoolName":               "tags-output-pool",
		"AllowUnauthenticatedIdentities": true,
		"IdentityPoolTags": map[string]string{
			"env":  "test",
			"team": "backend",
		},
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var created map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &created))

	// Tags should be in the CreateIdentityPool response.
	tags, _ := created["IdentityPoolTags"].(map[string]any)
	assert.Equal(t, "test", tags["env"])
	assert.Equal(t, "backend", tags["team"])

	// Tags should be in DescribeIdentityPool response too.
	descRec := doCognitoIdentityRequest(t, h, "DescribeIdentityPool", map[string]any{
		"IdentityPoolId": created["IdentityPoolId"],
	})
	require.Equal(t, http.StatusOK, descRec.Code)

	var descOut map[string]any
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descOut))

	descTags, _ := descOut["IdentityPoolTags"].(map[string]any)
	assert.Equal(t, "test", descTags["env"])
}

func TestDeleteIdentityPool_CleansTagsAndPrincipalTags(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doCognitoIdentityRequest(t, h, "CreateIdentityPool", map[string]any{
		"IdentityPoolName":               "cascade-pool",
		"AllowUnauthenticatedIdentities": true,
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var created map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &created))
	poolID := created["IdentityPoolId"].(string)
	arn := "arn:aws:cognito-identity:us-east-1:000000000000:identitypool/" + poolID

	// Add principal tag mapping.
	setRec := doCognitoIdentityRequest(t, h, "SetPrincipalTagAttributeMap", map[string]any{
		"IdentityPoolId":       poolID,
		"IdentityProviderName": "cognito-idp.us-east-1.amazonaws.com/us-east-1_xxx",
		"UseDefaults":          false,
		"PrincipalTags":        map[string]string{"sub": "user_id"},
	})
	require.Equal(t, http.StatusOK, setRec.Code)
	assert.Equal(t, 1, h.Backend.PrincipalTagCount())

	// Tag the pool.
	tagRec := doCognitoIdentityRequest(t, h, "TagResource", map[string]any{
		"ResourceArn": arn,
		"Tags":        map[string]string{"env": "test"},
	})
	require.Equal(t, http.StatusOK, tagRec.Code)

	// Delete the pool.
	delRec := doCognitoIdentityRequest(t, h, "DeleteIdentityPool", map[string]any{
		"IdentityPoolId": poolID,
	})
	require.Equal(t, http.StatusOK, delRec.Code)

	// Principal tags for this pool should be cleaned up.
	assert.Equal(t, 0, h.Backend.PrincipalTagCount())
	assert.Equal(t, 0, h.Backend.PoolCount())
}

func TestHandler_DeveloperProviderName_InCreateAndDescribe(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doCognitoIdentityRequest(t, h, "CreateIdentityPool", map[string]any{
		"IdentityPoolName":               "dev-provider-pool",
		"AllowUnauthenticatedIdentities": false,
		"DeveloperProviderName":          "developer.myapp.com",
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var created map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &created))
	assert.Equal(t, "developer.myapp.com", created["DeveloperProviderName"])

	descRec := doCognitoIdentityRequest(t, h, "DescribeIdentityPool", map[string]any{
		"IdentityPoolId": created["IdentityPoolId"],
	})
	require.Equal(t, http.StatusOK, descRec.Code)

	var descOut map[string]any
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descOut))
	assert.Equal(t, "developer.myapp.com", descOut["DeveloperProviderName"])
}

func TestHandler_UpdateIdentityPool_WithTags(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doCognitoIdentityRequest(t, h, "CreateIdentityPool", map[string]any{
		"IdentityPoolName":               "tag-update-pool",
		"AllowUnauthenticatedIdentities": true,
		"IdentityPoolTags":               map[string]string{"env": "dev"},
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var created map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &created))
	poolID := created["IdentityPoolId"].(string)

	updateRec := doCognitoIdentityRequest(t, h, "UpdateIdentityPool", map[string]any{
		"IdentityPoolId":                 poolID,
		"IdentityPoolName":               "tag-update-pool",
		"AllowUnauthenticatedIdentities": true,
		"IdentityPoolTags":               map[string]string{"env": "prod", "team": "backend"},
	})
	require.Equal(t, http.StatusOK, updateRec.Code)

	var updated map[string]any
	require.NoError(t, json.Unmarshal(updateRec.Body.Bytes(), &updated))
	tags, _ := updated["IdentityPoolTags"].(map[string]any)
	assert.Equal(t, "prod", tags["env"])
	assert.Equal(t, "backend", tags["team"])
}

func TestHandler_DescribeIdentityPool_EmptyPoolId_Returns400(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doCognitoIdentityRequest(t, h, "DescribeIdentityPool", map[string]any{
		"IdentityPoolId": "",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_DeleteIdentityPool_EmptyPoolId_Returns400(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doCognitoIdentityRequest(t, h, "DeleteIdentityPool", map[string]any{
		"IdentityPoolId": "",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_UpdateIdentityPool_EmptyPoolId_Returns400(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doCognitoIdentityRequest(t, h, "UpdateIdentityPool", map[string]any{
		"IdentityPoolId":                 "",
		"IdentityPoolName":               "name",
		"AllowUnauthenticatedIdentities": true,
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_ListIdentityPools_NextToken(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for _, name := range []string{"pool-a", "pool-b", "pool-c"} {
		rec := doCognitoIdentityRequest(t, h, "CreateIdentityPool", map[string]any{
			"IdentityPoolName":               name,
			"AllowUnauthenticatedIdentities": false,
		})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	// Page 1.
	rec1 := doCognitoIdentityRequest(t, h, "ListIdentityPools", map[string]any{
		"MaxResults": 2,
	})
	require.Equal(t, http.StatusOK, rec1.Code)

	var out1 map[string]any
	require.NoError(t, json.NewDecoder(rec1.Body).Decode(&out1))
	assert.Len(t, out1["IdentityPools"], 2)
	nextToken, _ := out1["NextToken"].(string)
	assert.NotEmpty(t, nextToken)

	// Page 2 using the cursor.
	rec2 := doCognitoIdentityRequest(t, h, "ListIdentityPools", map[string]any{
		"MaxResults": 2,
		"NextToken":  nextToken,
	})
	require.Equal(t, http.StatusOK, rec2.Code)

	var out2 map[string]any
	require.NoError(t, json.NewDecoder(rec2.Body).Decode(&out2))
	assert.Len(t, out2["IdentityPools"], 1)
}
