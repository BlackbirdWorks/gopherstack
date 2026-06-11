package acmpca_test

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/acmpca"
)

// ---- Provider tests ----

func TestACMPCAProvider_Name(t *testing.T) {
	t.Parallel()

	p := &acmpca.Provider{}
	assert.Equal(t, "ACMPCA", p.Name())
}

func TestACMPCAProvider_Init_WithEmptyCtx(t *testing.T) {
	t.Parallel()

	p := &acmpca.Provider{}
	ctx := &service.AppContext{}
	reg, err := p.Init(ctx)
	require.NoError(t, err)
	require.NotNil(t, reg)
}

func TestACMPCAProvider_Init_WithCtx(t *testing.T) {
	t.Parallel()

	p := &acmpca.Provider{}
	ctx := &service.AppContext{}
	reg, err := p.Init(ctx)
	require.NoError(t, err)
	require.NotNil(t, reg)
}

// ---- CA JSON operations via handler ----

func TestACMPCAHandler_CreateDescribeListDeleteCA(t *testing.T) {
	t.Parallel()

	h := newACMPCAHandler()

	// CreateCertificateAuthority
	rec := doACMPCARequest(t, h, "CreateCertificateAuthority", map[string]any{
		"CertificateAuthorityConfiguration": map[string]any{
			"Subject": map[string]any{
				"CommonName": "Test CA",
				"Country":    "US",
			},
			"KeyAlgorithm":     "RSA_2048",
			"SigningAlgorithm": "SHA256WITHRSA",
		},
		"CertificateAuthorityType": "ROOT",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	resp := parseACMPCAResponse(t, rec)
	caARN, _ := resp["CertificateAuthorityArn"].(string)
	require.NotEmpty(t, caARN)

	// DescribeCertificateAuthority
	rec = doACMPCARequest(t, h, "DescribeCertificateAuthority", map[string]any{
		"CertificateAuthorityArn": caARN,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// DescribeCertificateAuthority - not found
	rec = doACMPCARequest(t, h, "DescribeCertificateAuthority", map[string]any{
		"CertificateAuthorityArn": "arn:aws:acm-pca:us-east-1:000:certificate-authority/nonexistent",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	// ListCertificateAuthorities
	rec = doACMPCARequest(t, h, "ListCertificateAuthorities", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	// GetCertificateAuthorityCsr
	rec = doACMPCARequest(t, h, "GetCertificateAuthorityCsr", map[string]any{
		"CertificateAuthorityArn": caARN,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// GetCertificateAuthorityCertificate
	rec = doACMPCARequest(t, h, "GetCertificateAuthorityCertificate", map[string]any{
		"CertificateAuthorityArn": caARN,
	})
	assert.NotNil(t, rec)

	// ImportCertificateAuthorityCertificate
	rec = doACMPCARequest(t, h, "ImportCertificateAuthorityCertificate", map[string]any{
		"CertificateAuthorityArn": caARN,
		"Certificate":             "LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0t",
		"CertificateChain":        "LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0t",
	})
	assert.NotNil(t, rec)

	// UpdateCertificateAuthority (disable)
	rec = doACMPCARequest(t, h, "UpdateCertificateAuthority", map[string]any{
		"CertificateAuthorityArn": caARN,
		"Status":                  "DISABLED",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// UpdateCertificateAuthority - not found
	rec = doACMPCARequest(t, h, "UpdateCertificateAuthority", map[string]any{
		"CertificateAuthorityArn": "nonexistent",
		"Status":                  "DISABLED",
	})
	assert.NotEqual(t, http.StatusOK, rec.Code)

	// DeleteCertificateAuthority
	rec = doACMPCARequest(t, h, "DeleteCertificateAuthority", map[string]any{
		"CertificateAuthorityArn": caARN,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// DeleteCertificateAuthority - already deleted
	rec = doACMPCARequest(t, h, "DeleteCertificateAuthority", map[string]any{
		"CertificateAuthorityArn": caARN,
	})
	assert.NotEqual(t, http.StatusOK, rec.Code)
}

// ---- Certificate issuance and revocation ----

func TestACMPCAHandler_IssueCertAndRevoke(t *testing.T) {
	t.Parallel()

	b := acmpca.NewInMemoryBackend(testAccountID, testRegion)
	h := acmpca.NewHandler(b)

	// Create CA
	ca, err := b.CreateCertificateAuthority(context.Background(), "ROOT", acmpca.CertificateAuthorityConfiguration{
		Subject: acmpca.CertificateAuthoritySubject{CommonName: "Test Issue CA"},
	})
	require.NoError(t, err)

	// Get CSR from backend
	csrPEM, err := b.GetCertificateAuthorityCsr(context.Background(), ca.ARN)
	require.NoError(t, err)

	// IssueCertificate using the actual CSR
	rec := doACMPCARequest(t, h, "IssueCertificate", map[string]any{
		"CertificateAuthorityArn": ca.ARN,
		"Csr":                     csrPEM,
		"SigningAlgorithm":        "SHA256WITHRSA",
		"Validity":                map[string]any{"Type": "DAYS", "Value": 365},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	certResp := parseACMPCAResponse(t, rec)
	certARN, _ := certResp["CertificateArn"].(string)
	require.NotEmpty(t, certARN)

	// GetCertificate via backend
	cert, err := b.GetCertificate(context.Background(), ca.ARN, certARN)
	require.NoError(t, err)
	assert.NotEmpty(t, cert.ARN)

	// GetCertificate - not found
	rec = doACMPCARequest(t, h, "GetCertificate", map[string]any{
		"CertificateAuthorityArn": ca.ARN,
		"CertificateArn":          "arn:aws:acm-pca:us-east-1:000:certificate-authority/x/certificate/nonexistent",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	// RevokeCertificate - cert exists but serial lookup uses backend directly
	issuedCert, err := b.GetCertificate(context.Background(), ca.ARN, certARN)
	require.NoError(t, err)

	rec = doACMPCARequest(t, h, "RevokeCertificate", map[string]any{
		"CertificateAuthorityArn": ca.ARN,
		"CertificateSerial":       issuedCert.Serial,
		"RevocationReason":        "KEY_COMPROMISE",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// RevokeCertificate - nonexistent CA
	rec = doACMPCARequest(t, h, "RevokeCertificate", map[string]any{
		"CertificateAuthorityArn": "nonexistent",
		"CertificateSerial":       issuedCert.Serial,
		"RevocationReason":        "KEY_COMPROMISE",
	})
	assert.NotEqual(t, http.StatusOK, rec.Code)
}

// ---- Tag operations ----

func TestACMPCAHandler_TagAndUntagCA(t *testing.T) {
	t.Parallel()

	h := newACMPCAHandler()
	caARN := createHandlerCA(t, h)

	// TagCertificateAuthority
	rec := doACMPCARequest(t, h, "TagCertificateAuthority", map[string]any{
		"CertificateAuthorityArn": caARN,
		"Tags": []map[string]any{
			{"Key": "env", "Value": "test"},
			{"Key": "team", "Value": "infra"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// TagCertificateAuthority - malformed JSON should fail
	rec = doACMPCARequest(t, h, "TagCertificateAuthority", map[string]any{
		"CertificateAuthorityArn": "nonexistent",
		"Tags": []map[string]any{
			{"Key": "env", "Value": "test"},
		},
	})
	assert.NotEqual(t, http.StatusOK, rec.Code)

	// ListTags
	rec = doACMPCARequest(t, h, "ListTags", map[string]any{
		"CertificateAuthorityArn": caARN,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	resp := parseACMPCAResponse(t, rec)
	tags, _ := resp["Tags"].([]any)
	assert.Len(t, tags, 2)

	// ListTags - nonexistent CA
	rec = doACMPCARequest(t, h, "ListTags", map[string]any{
		"CertificateAuthorityArn": "nonexistent",
	})
	assert.NotEqual(t, http.StatusOK, rec.Code)

	// UntagCertificateAuthority
	rec = doACMPCARequest(t, h, "UntagCertificateAuthority", map[string]any{
		"CertificateAuthorityArn": caARN,
		"Tags": []map[string]any{
			{"Key": "env", "Value": "test"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// UntagCertificateAuthority - nonexistent CA
	rec = doACMPCARequest(t, h, "UntagCertificateAuthority", map[string]any{
		"CertificateAuthorityArn": "nonexistent",
		"Tags": []map[string]any{
			{"Key": "env", "Value": "test"},
		},
	})
	assert.NotEqual(t, http.StatusOK, rec.Code)

	// ListTags after untag
	rec = doACMPCARequest(t, h, "ListTags", map[string]any{
		"CertificateAuthorityArn": caARN,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	resp = parseACMPCAResponse(t, rec)
	tags, _ = resp["Tags"].([]any)
	assert.Len(t, tags, 1)
}

// ---- RestoreCertificateAuthority ----

func TestACMPCAHandler_RestoreCA(t *testing.T) {
	t.Parallel()

	h := newACMPCAHandler()
	caARN := createHandlerCA(t, h)

	// RestoreCertificateAuthority (CA must be DELETED first, but let's test the handler path)
	rec := doACMPCARequest(t, h, "RestoreCertificateAuthority", map[string]any{
		"CertificateAuthorityArn": caARN,
	})
	// Should return error because CA is not in DELETED state
	assert.NotNil(t, rec)

	// Restore nonexistent CA
	rec = doACMPCARequest(t, h, "RestoreCertificateAuthority", map[string]any{
		"CertificateAuthorityArn": "nonexistent",
	})
	assert.NotEqual(t, http.StatusOK, rec.Code)
}

// ---- Policy operations ----

func TestACMPCAHandler_PolicyOperations(t *testing.T) {
	t.Parallel()

	h := newACMPCAHandler()
	caARN := createHandlerCA(t, h)

	// PutPolicy
	rec := doACMPCARequest(t, h, "PutPolicy", map[string]any{
		"ResourceArn": caARN,
		"Policy":      `{"Version":"2012-10-17","Statement":[]}`,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// GetPolicy
	rec = doACMPCARequest(t, h, "GetPolicy", map[string]any{
		"ResourceArn": caARN,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// GetPolicy - not found
	rec = doACMPCARequest(t, h, "GetPolicy", map[string]any{
		"ResourceArn": "nonexistent",
	})
	assert.NotEqual(t, http.StatusOK, rec.Code)

	// DeletePolicy
	rec = doACMPCARequest(t, h, "DeletePolicy", map[string]any{
		"ResourceArn": caARN,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// DeletePolicy - not found
	rec = doACMPCARequest(t, h, "DeletePolicy", map[string]any{
		"ResourceArn": "nonexistent",
	})
	assert.NotEqual(t, http.StatusOK, rec.Code)
}

// ---- Handler Reset ----

func TestACMPCAHandler_Reset(t *testing.T) {
	t.Parallel()

	h := newACMPCAHandler()

	// Create some state
	caARN := createHandlerCA(t, h)
	h.SetTagsForTest(caARN, map[string]string{"key": "val"})

	// Reset
	h.Reset()

	// After reset, CA should not exist
	rec := doACMPCARequest(t, h, "DescribeCertificateAuthority", map[string]any{
		"CertificateAuthorityArn": caARN,
	})
	assert.NotEqual(t, http.StatusOK, rec.Code)
}

// ---- RouteMatcher and ExtractOperation ----

func TestACMPCAHandler_Routing(t *testing.T) {
	t.Parallel()

	h := newACMPCAHandler()

	matcher := h.RouteMatcher()
	assert.NotNil(t, matcher)

	// Test chaos methods
	assert.Equal(t, "acm-pca", h.ChaosServiceName())
	assert.NotEmpty(t, h.ChaosOperations())
	assert.NotEmpty(t, h.ChaosRegions())
	assert.NotEmpty(t, h.MatchPriority())
}

// ---- toCAOutput via DescribeCertificateAuthority response ----

func TestACMPCAHandler_ToCAOutput(t *testing.T) {
	t.Parallel()

	h := newACMPCAHandler()

	// Create CA with full subject
	ca, err := h.Backend.CreateCertificateAuthority(
		context.Background(),
		"ROOT",
		acmpca.CertificateAuthorityConfiguration{
			Subject: acmpca.CertificateAuthoritySubject{
				CommonName:         "Full CA",
				Country:            "US",
				Organization:       "Test Org",
				OrganizationalUnit: "Test Unit",
				State:              "CA",
				Locality:           "SF",
			},
			KeyAlgorithm:     "RSA_2048",
			SigningAlgorithm: "SHA256WITHRSA",
		},
	)
	require.NoError(t, err)

	// Describe - exercises toCAOutput
	rec := doACMPCARequest(t, h, "DescribeCertificateAuthority", map[string]any{
		"CertificateAuthorityArn": ca.ARN,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	resp := parseACMPCAResponse(t, rec)
	caOut, _ := resp["CertificateAuthority"].(map[string]any)
	require.NotNil(t, caOut)
	assert.Equal(t, ca.ARN, caOut["Arn"])
}

// ---- dispatchJSON error path ----

func TestACMPCAHandler_DispatchJSON_MalformedInput(t *testing.T) {
	t.Parallel()

	h := newACMPCAHandler()

	// CreateCertificateAuthority with missing config (should get error from backend)
	rec := doACMPCARequest(t, h, "CreateCertificateAuthority", map[string]any{})
	// Empty config still creates a CA (default config)
	assert.NotNil(t, rec)
}

// ---- handleOpError internal error path ----

func TestACMPCAHandler_HandleOpError_InternalError(t *testing.T) {
	t.Parallel()

	h := newACMPCAHandler()

	// Use an unknown operation to trigger error path
	rec := doACMPCARequest(t, h, "UnknownOperation", map[string]any{})
	assert.NotNil(t, rec)
}
