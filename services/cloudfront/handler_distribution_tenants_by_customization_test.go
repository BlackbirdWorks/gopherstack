package cloudfront_test

import (
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudfront"
)

// cfTenantPrefix is the CloudFront REST API path prefix used throughout this file.
const cfTenantPrefix = "/2020-05-31/"

// createTenantForCustomizationTest creates a distribution tenant and returns its ID.
func createTenantForCustomizationTest(t *testing.T, h *cloudfront.Handler, domain string) string {
	t.Helper()

	body := `<CreateDistributionTenantRequest><DistributionId>dist-cust-001</DistributionId>` +
		`<Domain>` + domain + `</Domain></CreateDistributionTenantRequest>`
	rec := doXML(t, h, http.MethodPost, cfTenantPrefix+"distribution-tenant", []byte(body))
	require.Equal(t, http.StatusCreated, rec.Code)

	return extractXMLID(t, rec.Body.String())
}

// TestListDistributionTenantsByCustomization_RealSDKRequestShape sends the request the way the
// real SDK actually serializes it: POST to the hyphenated
// "distribution-tenants-by-customization" path (cloudfront@v1.67.4 serializers.go
// awsRestxml_serializeOpListDistributionTenantsByCustomization) with WebACLArn/CertificateArn in
// the XML body (awsRestxml_serializeOpHttpBindingsListDistributionTenantsByCustomizationInput
// returns nil -- no HTTP-bound fields), not GET with a query parameter.
func TestListDistributionTenantsByCustomization_RealSDKRequestShape(t *testing.T) {
	t.Parallel()

	const webACLArn = "arn:aws:wafv2:us-east-1:123456789012:global/webacl/matched/aaa"
	const otherWebACLArn = "arn:aws:wafv2:us-east-1:123456789012:global/webacl/other/bbb"

	t.Run("web_acl_arn_filter_matches_only_associated_tenant", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)

		matchedID := createTenantForCustomizationTest(t, h, "matched.example.com")
		otherID := createTenantForCustomizationTest(t, h, "other.example.com")

		assocBody := `<WebACLAssociation><WebACLId>` + webACLArn + `</WebACLId></WebACLAssociation>`
		assocRec := doXML(
			t,
			h,
			http.MethodPut,
			cfTenantPrefix+"distribution-tenant/"+matchedID+"/associate-web-acl",
			[]byte(assocBody),
		)
		require.Equal(t, http.StatusOK, assocRec.Code)

		otherAssocBody := `<WebACLAssociation><WebACLId>` + otherWebACLArn + `</WebACLId></WebACLAssociation>`
		otherAssocRec := doXML(
			t,
			h,
			http.MethodPut,
			cfTenantPrefix+"distribution-tenant/"+otherID+"/associate-web-acl",
			[]byte(otherAssocBody),
		)
		require.Equal(t, http.StatusOK, otherAssocRec.Code)

		reqBody := `<ListDistributionTenantsByCustomizationRequest>` +
			`<WebACLArn>` + webACLArn + `</WebACLArn>` +
			`</ListDistributionTenantsByCustomizationRequest>`
		rec := doXML(t, h, http.MethodPost, cfTenantPrefix+"distribution-tenants-by-customization", []byte(reqBody))

		require.Equal(t, http.StatusOK, rec.Code)
		body := rec.Body.String()
		assert.Contains(t, body, matchedID)
		assert.NotContains(t, body, otherID)
	})

	t.Run("certificate_arn_filter_matches_only_that_tenant", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)

		matchedID := createTenantForCustomizationTest(t, h, "cert-matched.example.com")
		otherID := createTenantForCustomizationTest(t, h, "cert-other.example.com")

		certRec := doXML(
			t, h, http.MethodGet, cfTenantPrefix+"managed-certificate/"+matchedID, nil,
		)
		require.Equal(t, http.StatusOK, certRec.Code)
		certBody := certRec.Body.String()
		start := strings.Index(certBody, "<CertificateArn>") + len("<CertificateArn>")
		end := strings.Index(certBody, "</CertificateArn>")
		require.Greater(t, end, start)
		certArn := certBody[start:end]
		require.NotEmpty(t, certArn)

		reqBody := `<ListDistributionTenantsByCustomizationRequest>` +
			`<CertificateArn>` + certArn + `</CertificateArn>` +
			`</ListDistributionTenantsByCustomizationRequest>`
		rec := doXML(t, h, http.MethodPost, cfTenantPrefix+"distribution-tenants-by-customization", []byte(reqBody))

		require.Equal(t, http.StatusOK, rec.Code)
		body := rec.Body.String()
		assert.Contains(t, body, matchedID)
		assert.NotContains(t, body, otherID)
	})
}

// TestListDistributionTenantsByCustomization_Pagination verifies MaxItems/Marker read from the
// XML body page through all tenants without omission or duplication.
func TestListDistributionTenantsByCustomization_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	want := make(map[string]bool)
	for i := range 3 {
		id := createTenantForCustomizationTest(t, h, "page-"+string(rune('a'+i))+".example.com")
		want[id] = true
	}

	got := make(map[string]bool)
	marker := ""

	for range 10 {
		reqBody := `<ListDistributionTenantsByCustomizationRequest><MaxItems>1</MaxItems>`
		if marker != "" {
			reqBody += `<Marker>` + marker + `</Marker>`
		}
		reqBody += `</ListDistributionTenantsByCustomizationRequest>`

		rec := doXML(t, h, http.MethodPost, cfTenantPrefix+"distribution-tenants-by-customization", []byte(reqBody))
		require.Equal(t, http.StatusOK, rec.Code)

		body := rec.Body.String()
		for id := range want {
			if strings.Contains(body, id) {
				require.False(t, got[id], "tenant %s returned on more than one page", id)
				got[id] = true
			}
		}

		nmStart := strings.Index(body, "<NextMarker>")
		if nmStart < 0 {
			break
		}
		nmStart += len("<NextMarker>")
		nmEnd := strings.Index(body, "</NextMarker>")
		require.Greater(t, nmEnd, nmStart)
		marker = body[nmStart:nmEnd]
	}

	assert.Equal(t, want, got)
}
