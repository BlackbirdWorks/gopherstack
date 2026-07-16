package apigateway_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/apigateway"
)

// TestAPIGW_DomainNames covers CreateDomainName, GetDomainName, GetDomainNames,
// DeleteDomainName, CreateBasePathMapping, GetBasePathMapping, GetBasePathMappings,
// DeleteBasePathMapping.
func TestAPIGW_DomainNames(t *testing.T) {
	t.Parallel()

	h := newAPIGWHandler()

	// Create a rest API for base path mappings.
	rec := postWithHandler(t, h, nil, "CreateRestApi", `{"name":"dn-api"}`)
	require.True(t, rec.Code >= 200 && rec.Code < 300, "expected 2xx")

	var apiResp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&apiResp))
	apiID, _ := apiResp["id"].(string)
	require.NotEmpty(t, apiID)

	// CreateDomainName.
	rec = restRequest(t, h, http.MethodPost, "/domainnames",
		`{"domainName":"api.example.com","certificateArn":"arn:aws:acm:us-east-1:000000000000:certificate/abc"}`)
	require.True(t, rec.Code >= 200 && rec.Code < 300, "expected 2xx")

	// GetDomainName.
	rec = restRequest(t, h, http.MethodGet, "/domainnames/api.example.com", "")
	assert.True(t, rec.Code >= 200 && rec.Code < 300, "expected 2xx")

	// GetDomainNames.
	rec = restRequest(t, h, http.MethodGet, "/domainnames", "")
	assert.True(t, rec.Code >= 200 && rec.Code < 300, "expected 2xx")

	// CreateBasePathMapping.
	rec = restRequest(t, h, http.MethodPost, "/domainnames/api.example.com/basepathmappings",
		`{"basePath":"v1","restApiId":"`+apiID+`","stage":"prod"}`)
	assert.True(t, rec.Code >= 200 && rec.Code < 300, "expected 2xx")

	// GetBasePathMappings.
	rec = restRequest(t, h, http.MethodGet, "/domainnames/api.example.com/basepathmappings", "")
	assert.True(t, rec.Code >= 200 && rec.Code < 300, "expected 2xx")

	// GetBasePathMapping.
	rec = restRequest(t, h, http.MethodGet, "/domainnames/api.example.com/basepathmappings/v1", "")
	assert.True(t, rec.Code >= 200 && rec.Code < 300, "expected 2xx")

	// DeleteBasePathMapping.
	rec = restRequest(t, h, http.MethodDelete, "/domainnames/api.example.com/basepathmappings/v1", "")
	assert.True(t, rec.Code >= 200 && rec.Code < 300, "expected 2xx")

	// DeleteDomainName.
	rec = restRequest(t, h, http.MethodDelete, "/domainnames/api.example.com", "")
	assert.True(t, rec.Code >= 200 && rec.Code < 300, "expected 2xx")
}

func TestAPIGateway_DomainNameAccessAssociation_RESTLifecycle(t *testing.T) {
	t.Parallel()

	backend := apigateway.NewInMemoryBackend()
	h := apigateway.NewHandler(backend)

	createRec := restCall(t, h, http.MethodPost, "/domainnameaccessassociations", "application/json",
		`{"domainNameArn":"arn:aws:apigateway:us-east-1::/domainnames/example.com",`+
			`"accessAssociationSource":"vpce-1234","accessAssociationSourceType":"VPCE"}`)
	require.Equal(t, http.StatusCreated, createRec.Code)

	var created map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &created))
	arn, _ := created["domainNameAccessAssociationArn"].(string)
	require.NotEmpty(t, arn, "create must return a real association ARN")

	listRec := restCall(t, h, http.MethodGet, "/domainnameaccessassociations", "", "")
	require.Equal(t, http.StatusOK, listRec.Code)

	var list map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &list))
	items, _ := list["item"].([]any)
	require.Len(t, items, 1, "list must reflect the created association, not a hardcoded empty list")

	deleteRec := restCall(t, h, http.MethodDelete, "/domainnameaccessassociations/"+arn, "", "")
	require.Equal(t, http.StatusAccepted, deleteRec.Code)

	listAfterDeleteRec := restCall(t, h, http.MethodGet, "/domainnameaccessassociations", "", "")
	var listAfter map[string]any
	require.NoError(t, json.Unmarshal(listAfterDeleteRec.Body.Bytes(), &listAfter))
	itemsAfter, _ := listAfter["item"].([]any)
	assert.Empty(t, itemsAfter)

	deleteAgainRec := restCall(t, h, http.MethodDelete, "/domainnameaccessassociations/"+arn, "", "")
	assert.Equal(
		t,
		http.StatusNotFound,
		deleteAgainRec.Code,
		"deleting a removed association must 404, not silently succeed",
	)
}

func TestAPIGateway_RejectDomainNameAccessAssociation(t *testing.T) {
	t.Parallel()

	backend := apigateway.NewInMemoryBackend()
	h := apigateway.NewHandler(backend)

	assoc, err := backend.CreateDomainNameAccessAssociation(apigateway.CreateDomainNameAccessAssociationInput{
		DomainNameARN:               "arn:aws:apigateway:us-east-1::/domainnames/example.com",
		AccessAssociationSource:     "vpce-1234",
		AccessAssociationSourceType: "VPCE",
	})
	require.NoError(t, err)

	rec := restCall(t, h, http.MethodPost,
		"/rejectdomainnameaccessassociations?domainNameAccessAssociationArn="+
			assoc.DomainNameAccessAssociationARN+"&domainNameArn="+assoc.DomainNameARN,
		"", "")
	require.Equal(t, http.StatusAccepted, rec.Code)

	_, err = backend.GetDomainNameAccessAssociations("")
	require.NoError(t, err)

	list, err := backend.GetDomainNameAccessAssociations("")
	require.NoError(t, err)
	assert.Empty(t, list, "reject must remove the association")
}

func TestDomainName_RegionalAndDistributionFields(t *testing.T) {
	t.Parallel()

	b := apigateway.NewInMemoryBackend()
	dn, err := b.CreateDomainName(apigateway.CreateDomainNameInput{
		DomainName:             "api.example.com",
		RegionalCertificateARN: "arn:aws:acm:us-east-1:123456789012:certificate/abc",
		SecurityPolicy:         "TLS_1_2",
	})
	require.NoError(t, err)
	assert.Equal(t, "TLS_1_2", dn.SecurityPolicy)
	assert.NotEmpty(t, dn.RegionalDomainName)
	assert.NotEmpty(t, dn.RegionalHostedZoneID)
	assert.NotEmpty(t, dn.DistributionDomainName)
	assert.NotEmpty(t, dn.DistributionHostedZoneID)
	assert.Equal(t, "AVAILABLE", dn.DomainNameStatus)
	assert.NotNil(t, dn.EndpointConfiguration)
}

func TestDomainName_DefaultSecurityPolicy(t *testing.T) {
	t.Parallel()

	b := apigateway.NewInMemoryBackend()
	dn, err := b.CreateDomainName(apigateway.CreateDomainNameInput{
		DomainName:     "default.example.com",
		CertificateARN: "arn:aws:acm:us-east-1:123456789012:certificate/xyz",
	})
	require.NoError(t, err)
	assert.Equal(t, "TLS_1_2", dn.SecurityPolicy)
}

func TestDomainName_EndpointConfiguration_Edge(t *testing.T) {
	t.Parallel()

	b := apigateway.NewInMemoryBackend()
	dn, err := b.CreateDomainName(apigateway.CreateDomainNameInput{
		DomainName:     "edge.example.com",
		CertificateARN: "arn:aws:acm:us-east-1:123456789012:certificate/edge",
		EndpointConfiguration: &apigateway.EndpointConfiguration{
			Types: []string{"EDGE"},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, dn.EndpointConfiguration)
	assert.Contains(t, dn.EndpointConfiguration.Types, "EDGE")
	assert.NotEmpty(t, dn.DistributionDomainName)
}

func TestDomainName_UpdateSecurityPolicy(t *testing.T) {
	t.Parallel()

	b := apigateway.NewInMemoryBackend()
	_, _ = b.CreateDomainName(apigateway.CreateDomainNameInput{
		DomainName:     "update.example.com",
		CertificateARN: "arn:aws:acm:us-east-1:123456789012:certificate/upd",
		SecurityPolicy: "TLS_1_0",
	})

	updated, err := b.UpdateDomainName(apigateway.UpdateDomainNameInput{
		DomainName:     "update.example.com",
		SecurityPolicy: "TLS_1_2",
	})
	require.NoError(t, err)
	assert.Equal(t, "TLS_1_2", updated.SecurityPolicy)
}

func TestDomainName_UpdateRegionalCert(t *testing.T) {
	t.Parallel()

	b := apigateway.NewInMemoryBackend()
	_, _ = b.CreateDomainName(apigateway.CreateDomainNameInput{
		DomainName:             "regional.example.com",
		RegionalCertificateARN: "arn:aws:acm:us-east-1:123456789012:certificate/old",
	})

	updated, err := b.UpdateDomainName(apigateway.UpdateDomainNameInput{
		DomainName:             "regional.example.com",
		RegionalCertificateARN: "arn:aws:acm:us-east-1:123456789012:certificate/new",
	})
	require.NoError(t, err)
	assert.Equal(t, "arn:aws:acm:us-east-1:123456789012:certificate/new", updated.RegionalCertificateARN)
}

func TestHandlerDomainName_SecurityPolicy_RoundTrip(t *testing.T) {
	t.Parallel()

	h := newAPIGWHandler()

	rec := restRequest(
		t,
		h,
		http.MethodPost,
		"/domainnames",
		`{"domainName":"secure.example.com",`+
			`"certificateArn":"arn:aws:acm:us-east-1:000000000000:certificate/abc",`+
			`"securityPolicy":"TLS_1_2"}`,
	)
	require.True(t, rec.Code >= 200 && rec.Code < 300)

	var resp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	assert.Equal(t, "TLS_1_2", resp["securityPolicy"])
	assert.NotEmpty(t, resp["regionalDomainName"])
	assert.NotEmpty(t, resp["distributionDomainName"])
}

// TestUpdateDomainName tests UpdateDomainName.
func TestUpdateDomainName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		newCert  string
		wantCode int
		useValid bool
	}{
		{
			name:     "update_certificate",
			wantCode: http.StatusOK,
			useValid: true,
			newCert:  "arn:aws:acm:us-east-1:123:certificate/new",
		},
		{
			name:     "domain_not_found",
			wantCode: http.StatusNotFound,
			useValid: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, e := boostSetup()

			postWithHandler(t, handler, e, "CreateDomainName", `{"domainName":"api.example.com"}`)

			lookupDomain := "api.example.com"
			if !tt.useValid {
				lookupDomain = "notexist.example.com"
			}

			rec := postWithHandler(t, handler, e, "UpdateDomainName",
				fmt.Sprintf(`{"domainName":%q,"certificateArn":%q}`, lookupDomain, tt.newCert))
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestBackend_DomainName_NewFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check func(t *testing.T, dn *apigateway.DomainName)
		input apigateway.CreateDomainNameInput
		name  string
	}{
		{
			name: "default_security_policy",
			input: apigateway.CreateDomainNameInput{
				DomainName:     "test1.example.com",
				CertificateARN: "arn:aws:acm:us-east-1:123456789012:certificate/abc",
			},
			check: func(t *testing.T, dn *apigateway.DomainName) {
				t.Helper()
				assert.Equal(t, "TLS_1_2", dn.SecurityPolicy)
				assert.Equal(t, "AVAILABLE", dn.DomainNameStatus)
				assert.NotEmpty(t, dn.RegionalDomainName)
				assert.NotEmpty(t, dn.DistributionDomainName)
				assert.NotEmpty(t, dn.RegionalHostedZoneID)
				assert.NotEmpty(t, dn.DistributionHostedZoneID)
			},
		},
		{
			name: "custom_security_policy_tls10",
			input: apigateway.CreateDomainNameInput{
				DomainName:     "test2.example.com",
				CertificateARN: "arn:aws:acm:us-east-1:123456789012:certificate/tls10",
				SecurityPolicy: "TLS_1_0",
			},
			check: func(t *testing.T, dn *apigateway.DomainName) {
				t.Helper()
				assert.Equal(t, "TLS_1_0", dn.SecurityPolicy)
			},
		},
		{
			name: "regional_endpoint",
			input: apigateway.CreateDomainNameInput{
				DomainName:             "regional.example.com",
				RegionalCertificateARN: "arn:aws:acm:us-east-1:123456789012:certificate/regional",
				EndpointConfiguration: &apigateway.EndpointConfiguration{
					Types: []string{"REGIONAL"},
				},
			},
			check: func(t *testing.T, dn *apigateway.DomainName) {
				t.Helper()
				require.NotNil(t, dn.EndpointConfiguration)
				assert.Contains(t, dn.EndpointConfiguration.Types, "REGIONAL")
				assert.Equal(t, "arn:aws:acm:us-east-1:123456789012:certificate/regional", dn.RegionalCertificateARN)
			},
		},
		{
			name: "edge_endpoint",
			input: apigateway.CreateDomainNameInput{
				DomainName:     "edge.example.com",
				CertificateARN: "arn:aws:acm:us-east-1:123456789012:certificate/edge",
				EndpointConfiguration: &apigateway.EndpointConfiguration{
					Types: []string{"EDGE"},
				},
			},
			check: func(t *testing.T, dn *apigateway.DomainName) {
				t.Helper()
				require.NotNil(t, dn.EndpointConfiguration)
				assert.Contains(t, dn.EndpointConfiguration.Types, "EDGE")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := apigateway.NewInMemoryBackend()
			dn, err := b.CreateDomainName(tt.input)
			require.NoError(t, err)
			tt.check(t, dn)

			got, err := b.GetDomainName(tt.input.DomainName)
			require.NoError(t, err)
			tt.check(t, got)
		})
	}
}
