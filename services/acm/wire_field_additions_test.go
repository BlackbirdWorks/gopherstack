package acm_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestACMHandler_ListCertificates_SummaryHasCreatedAtAndInUse locks in the
// fix for CertificateSummary previously omitting CreatedAt entirely (a field
// always present on the real AWS wire) and never surfacing InUse.
func TestACMHandler_ListCertificates_SummaryHasCreatedAtAndInUse(t *testing.T) {
	t.Parallel()

	h := newACMHandler()

	reqRec := postACMJSON(t, h, "RequestCertificate", `{"DomainName":"summaryfields.example.com"}`)
	require.Equal(t, http.StatusOK, reqRec.Code)

	listRec := postACMJSON(t, h, "ListCertificates", `{}`)
	require.Equal(t, http.StatusOK, listRec.Code)

	var out struct {
		CertificateSummaryList []struct {
			CreatedAt *int64 `json:"CreatedAt"`
			InUse     *bool  `json:"InUse"`
		} `json:"CertificateSummaryList"`
	}
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &out))
	require.NotEmpty(t, out.CertificateSummaryList)

	summary := out.CertificateSummaryList[0]
	require.NotNil(t, summary.CreatedAt, "CertificateSummary.CreatedAt must always be present on the wire")
	assert.Positive(t, *summary.CreatedAt)
	require.NotNil(t, summary.InUse)
	assert.False(t, *summary.InUse)
}

// TestACMHandler_ListCertificates_SummaryKeyUsages verifies that
// CertificateSummary.KeyUsages/ExtendedKeyUsages project the same key-usage
// data DescribeCertificate exposes, not just DescribeCertificate. Unlike
// CertificateDetail.KeyUsages ([]types.KeyUsage, a slice of {"Name": "..."}
// objects), real AWS's CertificateSummary.KeyUsages/ExtendedKeyUsages
// (returned by ListCertificates) are plain string arrays
// ([]types.KeyUsageName/[]types.ExtendedKeyUsageName) -- see
// aws-sdk-go-v2/service/acm/types.CertificateSummary. Every real SDK
// client's ListCertificates deserializer rejects the object-wrapped shape
// with "expected KeyUsageName to be of type string, got map[string]interface{}
// instead", caught by TestTerraform_ACM.
func TestACMHandler_ListCertificates_SummaryKeyUsages(t *testing.T) {
	t.Parallel()

	h := newACMHandler()

	reqRec := postACMJSON(t, h, "RequestCertificate", `{"DomainName":"summaryusage.example.com"}`)
	require.Equal(t, http.StatusOK, reqRec.Code)

	listRec := postACMJSON(t, h, "ListCertificates", `{}`)
	require.Equal(t, http.StatusOK, listRec.Code)

	var out struct {
		CertificateSummaryList []struct {
			KeyUsages         []string `json:"KeyUsages"`
			ExtendedKeyUsages []string `json:"ExtendedKeyUsages"`
		} `json:"CertificateSummaryList"`
	}
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &out))
	require.NotEmpty(t, out.CertificateSummaryList)

	summary := out.CertificateSummaryList[0]
	require.NotEmpty(t, summary.KeyUsages)
	assert.Equal(t, "DIGITAL_SIGNATURE", summary.KeyUsages[0])
	require.NotEmpty(t, summary.ExtendedKeyUsages)
	assert.Equal(t, "TLS_WEB_SERVER_AUTHENTICATION", summary.ExtendedKeyUsages[0])
}

// TestACMHandler_RenewCertificate_RenewalSummaryHasUpdatedAt locks in the fix
// for RenewalSummary previously omitting UpdatedAt, a required (always
// present) field on the real AWS wire.
func TestACMHandler_RenewCertificate_RenewalSummaryHasUpdatedAt(t *testing.T) {
	t.Parallel()

	h := newACMHandler()

	reqRec := postACMJSON(t, h, "RequestCertificate", `{"DomainName":"renewalupdated.example.com"}`)
	require.Equal(t, http.StatusOK, reqRec.Code)

	var reqOut struct {
		CertificateArn string `json:"CertificateArn"`
	}
	require.NoError(t, json.Unmarshal(reqRec.Body.Bytes(), &reqOut))

	renewBody, err := json.Marshal(map[string]string{"CertificateArn": reqOut.CertificateArn})
	require.NoError(t, err)

	renewRec := postACMJSON(t, h, "RenewCertificate", string(renewBody))
	require.Equal(t, http.StatusOK, renewRec.Code)

	descRec := postACMJSON(t, h, "DescribeCertificate", string(renewBody))
	require.Equal(t, http.StatusOK, descRec.Code)

	type renewalSummary struct {
		RenewalStatus string `json:"RenewalStatus"`
		UpdatedAt     int64  `json:"UpdatedAt"`
	}

	var descOut struct {
		Certificate struct {
			RenewalSummary renewalSummary `json:"RenewalSummary"`
		} `json:"Certificate"`
	}
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descOut))
	assert.NotEmpty(t, descOut.Certificate.RenewalSummary.RenewalStatus)
	assert.Positive(t, descOut.Certificate.RenewalSummary.UpdatedAt,
		"RenewalSummary.UpdatedAt must always be present on the wire")
}

// TestACMHandler_RequestCertificate_ExportOption_RoundTrips verifies that
// Options.Export supplied on RequestCertificate is stored and echoed back on
// DescribeCertificate.Options.Export.
func TestACMHandler_RequestCertificate_ExportOption_RoundTrips(t *testing.T) {
	t.Parallel()

	h := newACMHandler()

	body := `{"DomainName":"exportopt.example.com","Options":{"Export":"ENABLED"}}`
	reqRec := postACMJSON(t, h, "RequestCertificate", body)
	require.Equal(t, http.StatusOK, reqRec.Code)

	var reqOut struct {
		CertificateArn string `json:"CertificateArn"`
	}
	require.NoError(t, json.Unmarshal(reqRec.Body.Bytes(), &reqOut))

	descBody, err := json.Marshal(map[string]string{"CertificateArn": reqOut.CertificateArn})
	require.NoError(t, err)

	descRec := postACMJSON(t, h, "DescribeCertificate", string(descBody))
	require.Equal(t, http.StatusOK, descRec.Code)

	var descOut struct {
		Certificate struct {
			Options struct {
				Export string `json:"Export"`
			} `json:"Options"`
		} `json:"Certificate"`
	}
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descOut))
	assert.Equal(t, "ENABLED", descOut.Certificate.Options.Export)
}
