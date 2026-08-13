package directoryservice_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestListCertificates_Pagination(t *testing.T) {
	t.Parallel()

	t.Run("paginate through certificates", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		dirID := mustCreateMicrosoftAD(t, h, "corp.example.com")

		for range 4 {
			doRequest(t, h, "RegisterCertificate", map[string]any{
				"DirectoryId":     dirID,
				"CertificateData": testCertPEM,
				"Type":            "ClientLDAPS",
			})
		}

		rec := doRequest(
			t,
			h,
			"ListCertificates",
			map[string]any{"DirectoryId": dirID, "PageSize": 2},
		)
		assert.Equal(t, http.StatusOK, rec.Code)
		body := respBody(t, rec)
		page1, _ := body["CertificatesInfo"].([]any)
		assert.Len(t, page1, 2)
		nextToken, _ := body["NextToken"].(string)
		assert.NotEmpty(t, nextToken)

		rec2 := doRequest(t, h, "ListCertificates", map[string]any{
			"DirectoryId": dirID, "PageSize": 2, "NextToken": nextToken,
		})
		assert.Equal(t, http.StatusOK, rec2.Code)
		body2 := respBody(t, rec2)
		page2, _ := body2["CertificatesInfo"].([]any)
		assert.Len(t, page2, 2)
		_, hasMore := body2["NextToken"]
		assert.False(t, hasMore)
	})
}

func TestCertificates(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "register list describe deregister cycle"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			dirID := mustCreateSimpleAD(t, h, "corp.example.com")

			// Register
			rec1 := doRequest(t, h, "RegisterCertificate", map[string]any{
				"DirectoryId":     dirID,
				"CertificateData": testCertPEM,
				"Type":            "ClientLDAPS",
			})
			assert.Equal(t, http.StatusOK, rec1.Code)
			var r1 map[string]any
			require.NoError(t, json.Unmarshal(rec1.Body.Bytes(), &r1))
			certID, _ := r1["CertificateId"].(string)
			assert.NotEmpty(t, certID)

			// List
			rec2 := doRequest(t, h, "ListCertificates", map[string]any{"DirectoryId": dirID})
			assert.Equal(t, http.StatusOK, rec2.Code)
			var r2 map[string]any
			require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &r2))
			certs, _ := r2["CertificatesInfo"].([]any)
			assert.Len(t, certs, 1)

			// Describe
			rec3 := doRequest(t, h, "DescribeCertificate", map[string]any{
				"DirectoryId":   dirID,
				"CertificateId": certID,
			})
			assert.Equal(t, http.StatusOK, rec3.Code)
			var r3 map[string]any
			require.NoError(t, json.Unmarshal(rec3.Body.Bytes(), &r3))
			cert, _ := r3["Certificate"].(map[string]any)
			assert.Equal(t, certID, cert["CertificateId"])
			assert.Equal(t, "test", cert["CommonName"], "CommonName must be parsed from the X.509 subject")

			// Deregister
			rec4 := doRequest(t, h, "DeregisterCertificate", map[string]any{
				"DirectoryId":   dirID,
				"CertificateId": certID,
			})
			assert.Equal(t, http.StatusOK, rec4.Code)

			// List after deregister
			rec5 := doRequest(t, h, "ListCertificates", map[string]any{"DirectoryId": dirID})
			assert.Equal(t, http.StatusOK, rec5.Code)
			var r5 map[string]any
			require.NoError(t, json.Unmarshal(rec5.Body.Bytes(), &r5))
			certs2, _ := r5["CertificatesInfo"].([]any)
			assert.Empty(t, certs2)

			_ = tc
		})
	}
}

func TestCAEnrollmentPolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "enable describe disable cycle"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			dirID := mustCreateSimpleAD(t, h, "corp.example.com")

			connectorArn := "arn:aws:pca-connector-ad:us-east-1:000000000000:connector/conn-1"

			// Enable
			rec1 := doRequest(t, h, "EnableCAEnrollmentPolicy", map[string]any{
				"DirectoryId":     dirID,
				"PcaConnectorArn": connectorArn,
			})
			assert.Equal(t, http.StatusOK, rec1.Code)

			// Describe
			rec2 := doRequest(t, h, "DescribeCAEnrollmentPolicy", map[string]any{"DirectoryId": dirID})
			assert.Equal(t, http.StatusOK, rec2.Code)
			var r2 map[string]any
			require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &r2))
			assert.Equal(t, "Success", r2["CaEnrollmentPolicyStatus"])
			assert.Equal(t, connectorArn, r2["PcaConnectorArn"])
			assert.Equal(t, dirID, r2["DirectoryId"])

			// Disable
			rec3 := doRequest(t, h, "DisableCAEnrollmentPolicy", map[string]any{"DirectoryId": dirID})
			assert.Equal(t, http.StatusOK, rec3.Code)

			// Describe after disable
			rec4 := doRequest(t, h, "DescribeCAEnrollmentPolicy", map[string]any{"DirectoryId": dirID})
			assert.Equal(t, http.StatusOK, rec4.Code)
			var r4 map[string]any
			require.NoError(t, json.Unmarshal(rec4.Body.Bytes(), &r4))
			assert.Equal(t, "Disabled", r4["CaEnrollmentPolicyStatus"])
			assert.Equal(t, connectorArn, r4["PcaConnectorArn"])

			_ = tc
		})
	}
}

// TestEnableCAEnrollmentPolicy_RequiresPcaConnectorArn proves PcaConnectorArn
// is a required member of EnableCAEnrollmentPolicyInput, not silently
// dropped (gopherstack-h910).
func TestEnableCAEnrollmentPolicy_RequiresPcaConnectorArn(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	dirID := mustCreateSimpleAD(t, h, "corp.example.com")

	rec := doRequest(t, h, "EnableCAEnrollmentPolicy", map[string]any{"DirectoryId": dirID})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "InvalidParameterException")
}

func TestRegisterCertificate_InvalidPEM(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		data string
	}{
		{name: "not PEM at all", data: "not-a-certificate"},
		{
			name: "PEM wrapper with garbage body",
			data: "-----BEGIN CERTIFICATE-----\nMIIA...\n-----END CERTIFICATE-----",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			dirID := mustCreateSimpleAD(t, h, "corp.example.com")

			rec := doRequest(t, h, "RegisterCertificate", map[string]any{
				"DirectoryId":     dirID,
				"CertificateData": tt.data,
				"Type":            "ClientLDAPS",
			})
			assert.Equal(t, http.StatusBadRequest, rec.Code)
			body := respBody(t, rec)
			assert.Equal(t, "InvalidCertificateException", body["__type"])
		})
	}
}
