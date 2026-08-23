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
			wantStatus:     http.StatusBadRequest,
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
	assert.Equal(t, http.StatusBadRequest, rec2.Code)
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
			wantStatus: http.StatusBadRequest,
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

	cfg, ok := resp["TrustedTokenIssuerConfiguration"].(map[string]any)
	require.True(t, ok, "TrustedTokenIssuerConfiguration should be in DescribeTrustedTokenIssuer")
	oidc, ok := cfg["OidcJwtConfiguration"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "https://auth.example.com", oidc["IssuerUrl"])
	assert.Equal(t, "sub", oidc["ClaimAttributePath"])
}

// TestDescribeTrustedTokenIssuerWireShape locks in the real
// DescribeTrustedTokenIssuerOutput wire shape: flat top-level fields
// (Name, TrustedTokenIssuerArn, TrustedTokenIssuerConfiguration,
// TrustedTokenIssuerType) with NO nested "TrustedTokenIssuer" wrapper, no
// InstanceArn member, and no Tags member. gopherstack previously nested
// everything (including a fabricated InstanceArn and Tags) under an invented
// "TrustedTokenIssuer" key that doesn't exist on the real wire; a real
// aws-sdk-go-v2 client parsing that response would find every
// DescribeTrustedTokenIssuerOutput field nil/empty. Tags are fetched
// separately via ListTagsForResource, matching every other taggable ssoadmin
// resource.
func TestDescribeTrustedTokenIssuerWireShape(t *testing.T) {
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

	createResp := parseResponse(t, rec)
	ttiArn := createResp["TrustedTokenIssuerArn"].(string)
	assert.NotContains(t, createResp, "TrustedTokenIssuer",
		`CreateTrustedTokenIssuerOutput has no nested "TrustedTokenIssuer" member`)

	descRec := doRequest(t, h, "DescribeTrustedTokenIssuer", map[string]any{
		"TrustedTokenIssuerArn": ttiArn,
	})
	require.Equal(t, http.StatusOK, descRec.Code)

	resp := parseResponse(t, descRec)
	assert.NotContains(t, resp, "TrustedTokenIssuer",
		`DescribeTrustedTokenIssuerOutput has no nested "TrustedTokenIssuer" member`)
	assert.NotContains(t, resp, "InstanceArn", "DescribeTrustedTokenIssuerOutput has no InstanceArn member")
	assert.NotContains(t, resp, "Tags", "DescribeTrustedTokenIssuerOutput has no Tags member")
	assert.Equal(t, ttiArn, resp["TrustedTokenIssuerArn"])
	assert.Equal(t, "r3-tti-tagged", resp["Name"])
	assert.Equal(t, "OIDC_JWT", resp["TrustedTokenIssuerType"])
	require.Contains(t, resp, "TrustedTokenIssuerConfiguration")

	// Tags are only reachable via ListTagsForResource, matching real AWS.
	tagsRec := doRequest(t, h, "ListTagsForResource", map[string]any{
		"InstanceArn": instanceArn,
		"ResourceArn": ttiArn,
	})
	require.Equal(t, http.StatusOK, tagsRec.Code)
	tagsResp := parseResponse(t, tagsRec)
	tags, ok := tagsResp["Tags"].([]any)
	require.True(t, ok)
	require.Len(t, tags, 1)
	tagEntry := tags[0].(map[string]any)
	assert.Equal(t, "tier", tagEntry["Key"])
	assert.Equal(t, "prod", tagEntry["Value"])
}

// TestUpdateTrustedTokenIssuerWireShape locks in that
// UpdateTrustedTokenIssuerOutput is void (no members at all) --
// gopherstack previously echoed a full invented "TrustedTokenIssuer" object
// (including a fabricated InstanceArn field) that doesn't exist on the real
// wire.
func TestUpdateTrustedTokenIssuerWireShape(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	instanceArn := createInstance(t, h, "tti-update-wire-shape-inst")

	createRec := doRequest(t, h, "CreateTrustedTokenIssuer", map[string]any{
		"InstanceArn":            instanceArn,
		"Name":                   "UpdateWireShapeIssuer",
		"TrustedTokenIssuerType": "OIDC_JWT",
	})
	require.Equal(t, http.StatusOK, createRec.Code)
	ttiArn := parseResponse(t, createRec)["TrustedTokenIssuerArn"].(string)

	rec := doRequest(t, h, "UpdateTrustedTokenIssuer", map[string]any{
		"TrustedTokenIssuerArn": ttiArn,
		"Name":                  "RenamedIssuer",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseResponse(t, rec)
	assert.Empty(t, resp, "UpdateTrustedTokenIssuerOutput has no members")
}

// TestListTrustedTokenIssuers_NoInstanceArnMember locks in that
// types.TrustedTokenIssuerMetadata (the ListTrustedTokenIssuers item shape)
// has no InstanceArn member -- gopherstack previously invented one.
func TestListTrustedTokenIssuers_NoInstanceArnMember(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	instanceArn := createInstance(t, h, "tti-list-wire-shape-inst")

	createRec := doRequest(t, h, "CreateTrustedTokenIssuer", map[string]any{
		"InstanceArn":            instanceArn,
		"Name":                   "ListWireShapeIssuer",
		"TrustedTokenIssuerType": "OIDC_JWT",
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	rec := doRequest(t, h, "ListTrustedTokenIssuers", map[string]any{"InstanceArn": instanceArn})
	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseResponse(t, rec)
	issuers, ok := resp["TrustedTokenIssuers"].([]any)
	require.True(t, ok)
	require.NotEmpty(t, issuers)

	for _, raw := range issuers {
		item, itemOK := raw.(map[string]any)
		require.True(t, itemOK)
		assert.NotContains(t, item, "InstanceArn", "TrustedTokenIssuerMetadata has no InstanceArn member")
		assert.Contains(t, item, "TrustedTokenIssuerArn")
		assert.Contains(t, item, "Name")
	}
}

// TestUpdateTrustedTokenIssuerWithConfig verifies that configuration can be
// updated. Real UpdateTrustedTokenIssuerInput.TrustedTokenIssuerConfiguration
// (types.OidcJwtUpdateConfiguration) has no IssuerUrl member at all -- an
// issuer's IssuerUrl is immutable after creation -- so IssuerUrl must survive
// an Update untouched even when a request tries to send one.
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

	// Update with a new OIDC config. IssuerUrl isn't part of the real
	// update contract, so even sending one here must have no effect.
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
	cfg := resp["TrustedTokenIssuerConfiguration"].(map[string]any)
	oidc := cfg["OidcJwtConfiguration"].(map[string]any)
	assert.Equal(t, "https://old-issuer.example.com", oidc["IssuerUrl"], "IssuerUrl is immutable after creation")
	assert.Equal(t, "email", oidc["ClaimAttributePath"])
	assert.Equal(t, "Emails", oidc["IdentityStoreAttributePath"])
}

// TestUpdateTrustedTokenIssuer_FieldsSurviveIndependentUpdates guards
// gopherstack-c8ge: types.OidcJwtUpdateConfiguration's three fields are all
// independently optional. Updating JwksRetrievalOption alone in a later call
// must not wipe ClaimAttributePath/IdentityStoreAttributePath set by an
// earlier, unrelated call.
func TestUpdateTrustedTokenIssuer_FieldsSurviveIndependentUpdates(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	instanceArn := createInstance(t, h, "tti-c8ge-inst")

	createRec := doRequest(t, h, "CreateTrustedTokenIssuer", map[string]any{
		"InstanceArn":            instanceArn,
		"Name":                   "tti-c8ge",
		"TrustedTokenIssuerType": "OIDC_JWT",
		"TrustedTokenIssuerConfiguration": map[string]any{
			"OidcJwtConfiguration": map[string]any{
				"IssuerUrl":                  "https://issuer.example.com",
				"ClaimAttributePath":         "sub",
				"IdentityStoreAttributePath": "UserName",
				"JwksRetrievalOption":        "OPEN_ID_DISCOVERY",
			},
		},
	})
	require.Equal(t, http.StatusOK, createRec.Code)
	ttiArn := parseResponse(t, createRec)["TrustedTokenIssuerArn"].(string)

	// Update A: change ClaimAttributePath only.
	updateRec := doRequest(t, h, "UpdateTrustedTokenIssuer", map[string]any{
		"TrustedTokenIssuerArn": ttiArn,
		"TrustedTokenIssuerConfiguration": map[string]any{
			"OidcJwtConfiguration": map[string]any{
				"ClaimAttributePath": "email",
			},
		},
	})
	require.Equal(t, http.StatusOK, updateRec.Code)

	// Update B: change JwksRetrievalOption only, omitting the others.
	updateRec = doRequest(t, h, "UpdateTrustedTokenIssuer", map[string]any{
		"TrustedTokenIssuerArn": ttiArn,
		"TrustedTokenIssuerConfiguration": map[string]any{
			"OidcJwtConfiguration": map[string]any{
				"JwksRetrievalOption": "OPEN_ID_DISCOVERY",
			},
		},
	})
	require.Equal(t, http.StatusOK, updateRec.Code)

	descRec := doRequest(t, h, "DescribeTrustedTokenIssuer", map[string]any{
		"TrustedTokenIssuerArn": ttiArn,
	})
	require.Equal(t, http.StatusOK, descRec.Code)

	resp := parseResponse(t, descRec)
	cfg := resp["TrustedTokenIssuerConfiguration"].(map[string]any)
	oidc := cfg["OidcJwtConfiguration"].(map[string]any)
	assert.Equal(t, "https://issuer.example.com", oidc["IssuerUrl"], "IssuerUrl is immutable")
	assert.Equal(t, "email", oidc["ClaimAttributePath"], "A's own field must survive an Update that never mentioned it")
	assert.Equal(t, "UserName", oidc["IdentityStoreAttributePath"], "must survive both updates that never mentioned it")
	assert.Equal(t, "OPEN_ID_DISCOVERY", oidc["JwksRetrievalOption"], "B's own field must apply")
}
