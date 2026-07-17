package ssoadmin_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ssoadmin"
)

// TestTrustedTokenIssuerValidation verifies TTI type validation.
func TestTrustedTokenIssuerValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		issuerType string
		wantStatus int
	}{
		{
			name:       "OIDC_JWT accepted",
			issuerType: "OIDC_JWT",
			wantStatus: http.StatusOK,
		},
		{
			name:       "invalid type rejected",
			issuerType: "SAML",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			instanceArn := createInstance(t, h, "tti-validation-inst")
			rec := doRequest(t, h, "CreateTrustedTokenIssuer", map[string]any{
				"InstanceArn":            instanceArn,
				"Name":                   "MyIssuer",
				"TrustedTokenIssuerType": tt.issuerType,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestCreateTrustedTokenIssuer(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		issuerName     string
		issuerType     string
		wantStatus     int
		wantArn        bool
		useInvalidInst bool
	}{
		{
			name:       "create trusted token issuer",
			issuerName: "MyIssuer",
			issuerType: "OIDC_JWT",
			wantStatus: http.StatusOK,
			wantArn:    true,
		},
		{
			name:           "create trusted token issuer for nonexistent instance",
			issuerName:     "BadIssuer",
			issuerType:     "OIDC_JWT",
			wantStatus:     http.StatusNotFound,
			useInvalidInst: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()
			var instanceArn string
			if tt.useInvalidInst {
				instanceArn = "arn:aws:sso:::instance/ssoins-nonexistent"
			} else {
				instanceArn = createInstance(t, h, "tti-test-instance")
			}
			rec := doRequest(t, h, "CreateTrustedTokenIssuer", map[string]any{
				"InstanceArn":            instanceArn,
				"Name":                   tt.issuerName,
				"TrustedTokenIssuerType": tt.issuerType,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
			if tt.wantArn {
				resp := parseResponse(t, rec)
				arn, ok := resp["TrustedTokenIssuerArn"].(string)
				assert.True(t, ok)
				assert.NotEmpty(t, arn)
			}
		})
	}
}

func TestCreateTrustedTokenIssuerDuplicate(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	instanceArn := createInstance(t, h, "tti-dup-instance")

	rec := doRequest(t, h, "CreateTrustedTokenIssuer", map[string]any{
		"InstanceArn":            instanceArn,
		"Name":                   "DupIssuer",
		"TrustedTokenIssuerType": "OIDC_JWT",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec2 := doRequest(t, h, "CreateTrustedTokenIssuer", map[string]any{
		"InstanceArn":            instanceArn,
		"Name":                   "DupIssuer",
		"TrustedTokenIssuerType": "OIDC_JWT",
	})
	assert.Equal(t, http.StatusConflict, rec2.Code)
}

func TestTrustedTokenIssuerAdditionalOperations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, h *ssoadmin.Handler) map[string]any
		name       string
		op         string
		wantStatus int
	}{
		{
			name: "list trusted token issuers",
			op:   "ListTrustedTokenIssuers",
			setup: func(t *testing.T, h *ssoadmin.Handler) map[string]any {
				t.Helper()
				instanceArn := createInstance(t, h, "tti-list-instance")
				createRec := doRequest(t, h, "CreateTrustedTokenIssuer", map[string]any{
					"InstanceArn":            instanceArn,
					"Name":                   "IssuerList",
					"TrustedTokenIssuerType": "OIDC_JWT",
				})
				require.Equal(t, http.StatusOK, createRec.Code)

				return map[string]any{"InstanceArn": instanceArn}
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "describe trusted token issuer",
			op:   "DescribeTrustedTokenIssuer",
			setup: func(t *testing.T, h *ssoadmin.Handler) map[string]any {
				t.Helper()
				instanceArn := createInstance(t, h, "tti-describe-instance")
				createRec := doRequest(t, h, "CreateTrustedTokenIssuer", map[string]any{
					"InstanceArn":            instanceArn,
					"Name":                   "IssuerDescribe",
					"TrustedTokenIssuerType": "OIDC_JWT",
				})
				require.Equal(t, http.StatusOK, createRec.Code)
				issuerArn := parseResponse(t, createRec)["TrustedTokenIssuerArn"].(string)

				return map[string]any{"TrustedTokenIssuerArn": issuerArn}
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "update trusted token issuer",
			op:   "UpdateTrustedTokenIssuer",
			setup: func(t *testing.T, h *ssoadmin.Handler) map[string]any {
				t.Helper()
				instanceArn := createInstance(t, h, "tti-update-instance")
				createRec := doRequest(t, h, "CreateTrustedTokenIssuer", map[string]any{
					"InstanceArn":            instanceArn,
					"Name":                   "IssuerUpdate",
					"TrustedTokenIssuerType": "OIDC_JWT",
				})
				require.Equal(t, http.StatusOK, createRec.Code)
				issuerArn := parseResponse(t, createRec)["TrustedTokenIssuerArn"].(string)

				return map[string]any{"TrustedTokenIssuerArn": issuerArn, "Name": "IssuerUpdated"}
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "describe deleted trusted token issuer returns not found",
			op:   "DescribeTrustedTokenIssuer",
			setup: func(t *testing.T, h *ssoadmin.Handler) map[string]any {
				t.Helper()
				instanceArn := createInstance(t, h, "tti-delete-instance")
				createRec := doRequest(t, h, "CreateTrustedTokenIssuer", map[string]any{
					"InstanceArn":            instanceArn,
					"Name":                   "IssuerDelete",
					"TrustedTokenIssuerType": "OIDC_JWT",
				})
				require.Equal(t, http.StatusOK, createRec.Code)
				issuerArn := parseResponse(t, createRec)["TrustedTokenIssuerArn"].(string)

				deleteRec := doRequest(t, h, "DeleteTrustedTokenIssuer", map[string]any{
					"TrustedTokenIssuerArn": issuerArn,
				})
				require.Equal(t, http.StatusOK, deleteRec.Code)

				return map[string]any{"TrustedTokenIssuerArn": issuerArn}
			},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()
			body := tt.setup(t, h)
			opRec := doRequest(t, h, tt.op, body)
			assert.Equal(t, tt.wantStatus, opRec.Code)
		})
	}
}

// TestTrustedTokenIssuerCount verifies TrustedTokenIssuerCount export helper.
func TestTrustedTokenIssuerCount(t *testing.T) {
	t.Parallel()

	b := ssoadmin.NewInMemoryBackend("000000000000", "us-east-1")
	h := ssoadmin.NewHandler(b)
	assert.Equal(t, 0, ssoadmin.TrustedTokenIssuerCount(b))

	inst := b.AddInstanceInternal("tti-inst")
	rec := doRequest(t, h, "CreateTrustedTokenIssuer", map[string]any{
		"InstanceArn":            inst.InstanceArn,
		"Name":                   "Issuer1",
		"TrustedTokenIssuerType": "OIDC_JWT",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 1, ssoadmin.TrustedTokenIssuerCount(b))
}

// TestCreateTrustedTokenIssuerNameRequired verifies that creating a trusted token
// issuer requires a non-empty Name field.
func TestCreateTrustedTokenIssuerNameRequired(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	instanceArn := createInstance(t, h, "tti-name-req-inst")
	rec := doRequest(t, h, "CreateTrustedTokenIssuer", map[string]any{
		"InstanceArn":            instanceArn,
		"Name":                   "",
		"TrustedTokenIssuerType": "OIDC_JWT",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestCreateTrustedTokenIssuerWithTags verifies that CreateTrustedTokenIssuer accepts tags.
func TestCreateTrustedTokenIssuerWithTags(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	instanceArn := createInstance(t, h, "tti-tags-inst")

	rec := doRequest(t, h, "CreateTrustedTokenIssuer", map[string]any{
		"InstanceArn":            instanceArn,
		"Name":                   "MyIssuer",
		"TrustedTokenIssuerType": "OIDC_JWT",
		"Tags":                   []map[string]any{{"Key": "service", "Value": "auth"}},
		"TrustedTokenIssuerConfiguration": map[string]any{
			"OidcJwtConfiguration": map[string]any{
				"IssuerUrl":          "https://auth.example.com",
				"ClaimAttributePath": "sub",
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseResponse(t, rec)
	assert.NotEmpty(t, resp["TrustedTokenIssuerArn"])
}

// TestDescribeTrustedTokenIssuerConfiguration verifies TrustedTokenIssuerConfiguration is returned.
func TestDescribeTrustedTokenIssuerConfiguration(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	instanceArn := createInstance(t, h, "tti-cfg-inst")

	createRec := doRequest(t, h, "CreateTrustedTokenIssuer", map[string]any{
		"InstanceArn":            instanceArn,
		"Name":                   "CfgIssuer",
		"TrustedTokenIssuerType": "OIDC_JWT",
		"TrustedTokenIssuerConfiguration": map[string]any{
			"OidcJwtConfiguration": map[string]any{
				"IssuerUrl":           "https://auth.example.com",
				"ClaimAttributePath":  "sub",
				"JwksRetrievalOption": "OPEN_ID_DISCOVERY",
			},
		},
	})
	require.Equal(t, http.StatusOK, createRec.Code)
	createResp := parseResponse(t, createRec)
	ttiArn := createResp["TrustedTokenIssuerArn"].(string)

	rec := doRequest(t, h, "DescribeTrustedTokenIssuer", map[string]any{"TrustedTokenIssuerArn": ttiArn})
	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseResponse(t, rec)

	tti := resp["TrustedTokenIssuer"].(map[string]any)
	cfg, ok := tti["TrustedTokenIssuerConfiguration"].(map[string]any)
	require.True(t, ok, "TrustedTokenIssuerConfiguration should be in DescribeTrustedTokenIssuer")
	oidc, ok := cfg["OidcJwtConfiguration"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "https://auth.example.com", oidc["IssuerUrl"])
	assert.Equal(t, "sub", oidc["ClaimAttributePath"])
}

// TestDescribeTrustedTokenIssuerIncludesTags verifies Tags field in
// DescribeTrustedTokenIssuer response.
func TestDescribeTrustedTokenIssuerIncludesTags(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	instanceArn := createInstance(t, h, "r3-tti-tags-inst")

	rec := doRequest(t, h, "CreateTrustedTokenIssuer", map[string]any{
		"InstanceArn":            instanceArn,
		"Name":                   "r3-tti-tagged",
		"TrustedTokenIssuerType": "OIDC_JWT",
		"Tags":                   []map[string]any{{"Key": "tier", "Value": "prod"}},
		"TrustedTokenIssuerConfiguration": map[string]any{
			"OidcJwtConfiguration": map[string]any{
				"IssuerUrl":                  "https://issuer.example.com",
				"ClaimAttributePath":         "sub",
				"IdentityStoreAttributePath": "UserName",
				"JwksRetrievalOption":        "OPEN_ID_DISCOVERY",
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	ttiArn := parseResponse(t, rec)["TrustedTokenIssuerArn"].(string)

	descRec := doRequest(t, h, "DescribeTrustedTokenIssuer", map[string]any{
		"TrustedTokenIssuerArn": ttiArn,
	})
	require.Equal(t, http.StatusOK, descRec.Code)

	resp := parseResponse(t, descRec)
	ttiMap := resp["TrustedTokenIssuer"].(map[string]any)

	tags, hasTags := ttiMap["Tags"]
	assert.True(t, hasTags, "DescribeTrustedTokenIssuer should include Tags")
	tagSlice, ok := tags.([]any)
	assert.True(t, ok)
	assert.Len(t, tagSlice, 1)
	tagEntry := tagSlice[0].(map[string]any)
	assert.Equal(t, "tier", tagEntry["Key"])
	assert.Equal(t, "prod", tagEntry["Value"])
}

// TestUpdateTrustedTokenIssuerWithConfig verifies that configuration can be updated.
func TestUpdateTrustedTokenIssuerWithConfig(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	instanceArn := createInstance(t, h, "r3-tti-cfg-inst")

	createRec := doRequest(t, h, "CreateTrustedTokenIssuer", map[string]any{
		"InstanceArn":            instanceArn,
		"Name":                   "r3-tti-update",
		"TrustedTokenIssuerType": "OIDC_JWT",
		"TrustedTokenIssuerConfiguration": map[string]any{
			"OidcJwtConfiguration": map[string]any{
				"IssuerUrl":                  "https://old-issuer.example.com",
				"ClaimAttributePath":         "sub",
				"IdentityStoreAttributePath": "UserName",
				"JwksRetrievalOption":        "OPEN_ID_DISCOVERY",
			},
		},
	})
	require.Equal(t, http.StatusOK, createRec.Code)
	ttiArn := parseResponse(t, createRec)["TrustedTokenIssuerArn"].(string)

	// Update with new OIDC config.
	updateRec := doRequest(t, h, "UpdateTrustedTokenIssuer", map[string]any{
		"TrustedTokenIssuerArn": ttiArn,
		"TrustedTokenIssuerConfiguration": map[string]any{
			"OidcJwtConfiguration": map[string]any{
				"IssuerUrl":                  "https://new-issuer.example.com",
				"ClaimAttributePath":         "email",
				"IdentityStoreAttributePath": "Emails",
				"JwksRetrievalOption":        "OPEN_ID_DISCOVERY",
			},
		},
	})
	require.Equal(t, http.StatusOK, updateRec.Code)

	// Verify the config was updated.
	descRec := doRequest(t, h, "DescribeTrustedTokenIssuer", map[string]any{
		"TrustedTokenIssuerArn": ttiArn,
	})
	require.Equal(t, http.StatusOK, descRec.Code)

	resp := parseResponse(t, descRec)
	ttiMap := resp["TrustedTokenIssuer"].(map[string]any)
	cfg := ttiMap["TrustedTokenIssuerConfiguration"].(map[string]any)
	oidc := cfg["OidcJwtConfiguration"].(map[string]any)
	assert.Equal(t, "https://new-issuer.example.com", oidc["IssuerUrl"])
	assert.Equal(t, "email", oidc["ClaimAttributePath"])
}
