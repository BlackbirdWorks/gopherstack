package cognitoidp_test

// batch2_audit_test.go covers the 5 AWS-accuracy gaps from batch-2 audit (go-vrzn):
//
// Gap 1: CreateIdentityProvider duplicate returns GroupExistsException instead of DuplicateProviderException.
// Gap 2: UpdateIdentityProvider merges ProviderDetails instead of replacing (AWS replaces).
// Gap 3: SetRiskConfiguration silently drops RiskExceptionConfiguration (never written or read).
// Gap 4: Group responses missing LastModifiedDate (CreateGroup, GetGroup, UpdateGroup, ListGroups).
// Gap 5: CreateUserPoolDomain for managed domains returns non-empty CloudFrontDomain (AWS returns empty).

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cognitoidp"
)

// ── Gap 1: CreateIdentityProvider duplicate → DuplicateProviderException ─────

func TestBatch2Audit_CreateIdentityProvider_Duplicate_ReturnsDuplicateProviderException(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "handler_level"},
		{name: "backend_level"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if tt.name == "handler_level" {
				h := newTestHandler(t)
				poolID, _ := setupHandlerPoolAndClient(t, h, "dup-idp-handler-pool")

				body := map[string]any{
					"UserPoolId":   poolID,
					"ProviderName": "Google",
					"ProviderType": "Google",
					"ProviderDetails": map[string]string{
						"client_id":     "g-id",
						"client_secret": "g-sec",
					},
				}
				rec := doCognitoRequest(t, h, "CreateIdentityProvider", body)
				require.Equal(t, http.StatusOK, rec.Code)

				// Second create with same name — must be DuplicateProviderException.
				rec = doCognitoRequest(t, h, "CreateIdentityProvider", body)
				require.Equal(t, http.StatusBadRequest, rec.Code)

				var errResp struct {
					Code string `json:"__type"`
				}
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
				assert.Equal(t, "DuplicateProviderException", errResp.Code)
			} else {
				b := newTestBackend()
				pool, err := b.CreateUserPool("dup-idp-backend-pool")
				require.NoError(t, err)

				_, err = b.CreateIdentityProviderFull(
					pool.ID, "Facebook", "Facebook",
					map[string]string{"client_id": "fb-id"}, nil, nil,
				)
				require.NoError(t, err)

				_, err = b.CreateIdentityProviderFull(
					pool.ID, "Facebook", "Facebook",
					map[string]string{"client_id": "fb-id2"}, nil, nil,
				)
				require.Error(t, err)
				assert.ErrorIs(t, err, cognitoidp.ErrDuplicateProvider)
			}
		})
	}
}

// ── Gap 2: UpdateIdentityProvider replaces ProviderDetails (not merge) ────────

func TestBatch2Audit_UpdateIdentityProvider_ReplacesProviderDetails(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "handler_level"},
		{name: "backend_level"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if tt.name == "handler_level" {
				h := newTestHandler(t)
				poolID, _ := setupHandlerPoolAndClient(t, h, "update-idp-handler-pool")

				rec := doCognitoRequest(t, h, "CreateIdentityProvider", map[string]any{
					"UserPoolId":   poolID,
					"ProviderName": "SAML",
					"ProviderType": "SAML",
					"ProviderDetails": map[string]string{
						"MetadataURL":           "https://old.example.com/metadata",
						"IDPSignout":            "true",
						"SLORedirectBindingURI": "https://old.example.com/slo",
					},
				})
				require.Equal(t, http.StatusOK, rec.Code)

				// Update with only MetadataURL — old keys must be gone.
				rec = doCognitoRequest(t, h, "UpdateIdentityProvider", map[string]any{
					"UserPoolId":   poolID,
					"ProviderName": "SAML",
					"ProviderDetails": map[string]string{
						"MetadataURL": "https://new.example.com/metadata",
					},
				})
				require.Equal(t, http.StatusOK, rec.Code)

				rec = doCognitoRequest(t, h, "DescribeIdentityProvider", map[string]any{
					"UserPoolId":   poolID,
					"ProviderName": "SAML",
				})
				require.Equal(t, http.StatusOK, rec.Code)

				var out struct {
					IdentityProvider *struct {
						ProviderDetails map[string]string `json:"ProviderDetails"`
					} `json:"IdentityProvider"`
				}
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				require.NotNil(t, out.IdentityProvider)

				assert.Equal(t, "https://new.example.com/metadata", out.IdentityProvider.ProviderDetails["MetadataURL"])
				// Old keys must be gone after replace (not still present from merge).
				assert.Empty(
					t,
					out.IdentityProvider.ProviderDetails["IDPSignout"],
					"old key IDPSignout must be absent after replace",
				)
				assert.Empty(
					t,
					out.IdentityProvider.ProviderDetails["SLORedirectBindingURI"],
					"old key must be absent after replace",
				)
			} else {
				b := newTestBackend()
				pool, err := b.CreateUserPool("update-idp-backend-pool")
				require.NoError(t, err)

				_, err = b.CreateIdentityProviderFull(
					pool.ID, "SAML", "SAML",
					map[string]string{"MetadataURL": "https://old.test/metadata", "IDPSignout": "true"},
					nil, nil,
				)
				require.NoError(t, err)

				updated, err := b.UpdateIdentityProviderFull(
					pool.ID, "SAML",
					map[string]string{"MetadataURL": "https://new.test/metadata"},
					nil, nil,
				)
				require.NoError(t, err)

				assert.Equal(t, "https://new.test/metadata", updated.ProviderDetails["MetadataURL"])
				_, hasOldKey := updated.ProviderDetails["IDPSignout"]
				assert.False(t, hasOldKey, "IDPSignout must be absent after replace")
			}
		})
	}
}

// ── Gap 3: SetRiskConfiguration persists RiskExceptionConfiguration ───────────

func TestBatch2Audit_SetRiskConfiguration_PersistsRiskExceptionConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "blocked_ip_ranges"},
		{name: "skipped_ip_ranges"},
		{name: "both_ranges"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			poolID, _ := setupHandlerPoolAndClient(t, h, "risk-exc-"+tt.name+"-pool")

			body := map[string]any{
				"UserPoolId": poolID,
			}

			switch tt.name {
			case "blocked_ip_ranges":
				body["RiskExceptionConfiguration"] = map[string]any{
					"BlockedIPRangeList": []string{"10.0.0.0/8", "192.168.0.0/16"},
				}
			case "skipped_ip_ranges":
				body["RiskExceptionConfiguration"] = map[string]any{
					"SkippedIPRangeList": []string{"172.16.0.0/12"},
				}
			case "both_ranges":
				body["RiskExceptionConfiguration"] = map[string]any{
					"BlockedIPRangeList": []string{"10.0.0.0/8"},
					"SkippedIPRangeList": []string{"172.16.0.0/12"},
				}
			}

			rec := doCognitoRequest(t, h, "SetRiskConfiguration", body)
			require.Equal(t, http.StatusOK, rec.Code)

			var setOut struct {
				RiskConfiguration *struct {
					RiskExceptionConfiguration *struct {
						BlockedIPRangeList []string `json:"BlockedIPRangeList"`
						SkippedIPRangeList []string `json:"SkippedIPRangeList"`
					} `json:"RiskExceptionConfiguration"`
				} `json:"RiskConfiguration"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &setOut))
			require.NotNil(t, setOut.RiskConfiguration)
			require.NotNil(
				t,
				setOut.RiskConfiguration.RiskExceptionConfiguration,
				"RiskExceptionConfiguration must be present in Set response",
			)

			switch tt.name {
			case "blocked_ip_ranges":
				assert.Contains(t, setOut.RiskConfiguration.RiskExceptionConfiguration.BlockedIPRangeList, "10.0.0.0/8")
			case "skipped_ip_ranges":
				assert.Contains(
					t,
					setOut.RiskConfiguration.RiskExceptionConfiguration.SkippedIPRangeList,
					"172.16.0.0/12",
				)
			case "both_ranges":
				assert.Contains(t, setOut.RiskConfiguration.RiskExceptionConfiguration.BlockedIPRangeList, "10.0.0.0/8")
				assert.Contains(
					t,
					setOut.RiskConfiguration.RiskExceptionConfiguration.SkippedIPRangeList,
					"172.16.0.0/12",
				)
			}

			// Verify Describe also returns the persisted config.
			rec = doCognitoRequest(t, h, "DescribeRiskConfiguration", map[string]any{
				"UserPoolId": poolID,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var descOut struct {
				RiskConfiguration *struct {
					RiskExceptionConfiguration *struct {
						BlockedIPRangeList []string `json:"BlockedIPRangeList"`
						SkippedIPRangeList []string `json:"SkippedIPRangeList"`
					} `json:"RiskExceptionConfiguration"`
				} `json:"RiskConfiguration"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descOut))
			require.NotNil(t, descOut.RiskConfiguration)
			require.NotNil(
				t,
				descOut.RiskConfiguration.RiskExceptionConfiguration,
				"RiskExceptionConfiguration must survive round-trip",
			)
		})
	}
}

// ── Gap 4: Group responses include LastModifiedDate ───────────────────────────

func TestBatch2Audit_Group_LastModifiedDate_Present(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		op   string
	}{
		{name: "create_group", op: "CreateGroup"},
		{name: "get_group", op: "GetGroup"},
		{name: "update_group", op: "UpdateGroup"},
		{name: "list_groups", op: "ListGroups"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			poolID, _ := setupHandlerPoolAndClient(t, h, "group-lmd-"+tt.name+"-pool")

			createRec := doCognitoRequest(t, h, "CreateGroup", map[string]any{
				"UserPoolId":  poolID,
				"GroupName":   "admins",
				"Description": "Admin group",
				"Precedence":  1,
			})
			require.Equal(t, http.StatusOK, createRec.Code)

			switch tt.op {
			case "CreateGroup":
				var out struct {
					Group *struct {
						LastModifiedDate float64 `json:"LastModifiedDate"`
					} `json:"Group"`
				}
				require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &out))
				require.NotNil(t, out.Group)
				assert.Greater(t, out.Group.LastModifiedDate, float64(0), "CreateGroup must include LastModifiedDate")

			case "GetGroup":
				rec := doCognitoRequest(t, h, "GetGroup", map[string]any{
					"UserPoolId": poolID,
					"GroupName":  "admins",
				})
				require.Equal(t, http.StatusOK, rec.Code)

				var out struct {
					Group *struct {
						LastModifiedDate float64 `json:"LastModifiedDate"`
					} `json:"Group"`
				}
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				require.NotNil(t, out.Group)
				assert.Greater(t, out.Group.LastModifiedDate, float64(0), "GetGroup must include LastModifiedDate")

			case "UpdateGroup":
				rec := doCognitoRequest(t, h, "UpdateGroup", map[string]any{
					"UserPoolId":  poolID,
					"GroupName":   "admins",
					"Description": "Updated admin group",
				})
				require.Equal(t, http.StatusOK, rec.Code)

				var out struct {
					Group *struct {
						LastModifiedDate float64 `json:"LastModifiedDate"`
					} `json:"Group"`
				}
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				require.NotNil(t, out.Group)
				assert.Greater(t, out.Group.LastModifiedDate, float64(0), "UpdateGroup must include LastModifiedDate")

			case "ListGroups":
				rec := doCognitoRequest(t, h, "ListGroups", map[string]any{
					"UserPoolId": poolID,
				})
				require.Equal(t, http.StatusOK, rec.Code)

				var out struct {
					Groups []struct {
						GroupName        string  `json:"GroupName"`
						LastModifiedDate float64 `json:"LastModifiedDate"`
					} `json:"Groups"`
				}
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				require.Len(t, out.Groups, 1)
				assert.Greater(
					t,
					out.Groups[0].LastModifiedDate,
					float64(0),
					"ListGroups must include LastModifiedDate",
				)
			}
		})
	}
}

// ── Gap 5: CreateUserPoolDomain managed domain returns empty CloudFrontDomain ─

func TestBatch2Audit_CreateUserPoolDomain_ManagedDomain_EmptyCloudFrontDomain(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		certArn string
		wantCF  bool
	}{
		{
			name:    "managed_domain_no_cert",
			certArn: "",
			wantCF:  false,
		},
		{
			name:    "custom_domain_with_cert",
			certArn: "arn:aws:acm:us-east-1:123456789012:certificate/abc123",
			wantCF:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			poolID, _ := setupHandlerPoolAndClient(t, h, "domain-audit-"+tt.name+"-pool")

			body := map[string]any{
				"UserPoolId": poolID,
				"Domain":     "audit-domain-" + tt.name,
			}
			if tt.certArn != "" {
				body["CustomDomainConfig"] = map[string]any{
					"CertificateArn": tt.certArn,
				}
			}

			rec := doCognitoRequest(t, h, "CreateUserPoolDomain", body)
			require.Equal(t, http.StatusOK, rec.Code)

			var out struct {
				CloudFrontDomain string `json:"CloudFrontDomain"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

			if tt.wantCF {
				assert.Contains(
					t,
					out.CloudFrontDomain,
					"cloudfront.net",
					"custom domain must return CloudFront domain",
				)
			} else {
				assert.Empty(
					t,
					out.CloudFrontDomain,
					"managed domain must return empty CloudFrontDomain in create response",
				)
			}
		})
	}
}
