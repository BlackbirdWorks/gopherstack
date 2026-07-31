package dms_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestImportDeleteCertificate(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()

	importRec := doDMS(t, h, "ImportCertificate", map[string]any{
		"CertificateIdentifier": "my-cert",
		"CertificatePem":        "-----BEGIN CERTIFICATE-----",
	})
	require.Equal(t, http.StatusOK, importRec.Code)
	importedCert := parseJSON(t, importRec)["Certificate"].(map[string]any)
	certArn := importedCert["CertificateArn"].(string)
	// The real Certificate wire type carries CertificatePem; a real client
	// deserializing ImportCertificate's response must see back what it sent.
	assert.Equal(t, "-----BEGIN CERTIFICATE-----", importedCert["CertificatePem"])

	// Duplicate should fail.
	dupRec := doDMS(t, h, "ImportCertificate", map[string]any{
		"CertificateIdentifier": "my-cert",
		"CertificatePem":        "pem2",
	})
	assert.Equal(t, http.StatusConflict, dupRec.Code)

	// Missing identifier.
	missingRec := doDMS(t, h, "ImportCertificate", map[string]any{})
	assert.Equal(t, http.StatusBadRequest, missingRec.Code)

	// Describe.
	descRec := doDMS(t, h, "DescribeCertificates", map[string]any{})
	assert.Equal(t, http.StatusOK, descRec.Code)
	certs := parseJSON(t, descRec)["Certificates"].([]any)
	require.Len(t, certs, 1)
	assert.Equal(t, "-----BEGIN CERTIFICATE-----", certs[0].(map[string]any)["CertificatePem"])

	// Delete by ARN.
	delRec := doDMS(t, h, "DeleteCertificate", map[string]any{
		"CertificateArn": certArn,
	})
	assert.Equal(t, http.StatusOK, delRec.Code)

	// Not found.
	delRec2 := doDMS(t, h, "DeleteCertificate", map[string]any{
		"CertificateArn": certArn,
	})
	assert.Equal(t, http.StatusNotFound, delRec2.Code)
}
