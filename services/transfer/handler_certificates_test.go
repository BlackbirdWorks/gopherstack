package transfer_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/transfer"
)

// testCertPEM is a self-signed RSA 2048-bit certificate used by certificate parity tests.
// Generated with: openssl req -x509 -newkey rsa:2048 -keyout /dev/null -out - -days 3650 -nodes -subj "/CN=test".
const testCertPEM = `-----BEGIN CERTIFICATE-----
MIIC/zCCAeegAwIBAgIURQA1ea7ssWq2hJxY5habS2n67x8wDQYJKoZIhvcNAQEL
BQAwDzENMAsGA1UEAwwEdGVzdDAeFw0yNjA2MjYxNTU3MzFaFw0zNjA2MjMxNTU3
MzFaMA8xDTALBgNVBAMMBHRlc3QwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEK
AoIBAQCTZgjXmrJtpbR3QMaMPF/TAp5dTr0T92wCw0sU0TUrYEEOy+sNpSrHHL2G
bA3LCrces9M6SsKK9jlBUpd9THLccwDDZu9qadUgTYvufaMXrJRlaBVuDf7ek1Um
SogeUz+J8mhdQvW2lHblDf14H6IF4ZZhWEWcBDGHNPvjrUgyArjEVgrBAnnmBRUJ
j4Sd+ZU/56Xj9kMjXLcz/X+Xxx4enhQZaJ5RamyY2N05yMB5V9AdZhQNstttLHLa
hcnWnQN6hGY592k/QESSd3iF7SKSYi9ibJHYdmL8ER8sDCfrMGA6p5kcfBlNs03d
ZyloCovDxS8Ut67QPNRzoHVVlFuzAgMBAAGjUzBRMB0GA1UdDgQWBBRsmCv3SxDr
aYhA7NougOn/HZtnGDAfBgNVHSMEGDAWgBRsmCv3SxDraYhA7NougOn/HZtnGDAP
BgNVHRMBAf8EBTADAQH/MA0GCSqGSIb3DQEBCwUAA4IBAQBun1ThOPLQ5uokRaNg
L0lr39TK3vMZPD4FwUPbtLJ7DIiOhs2bs0VUIsawfeBW3Hy1BMuYPcNiVIn8YM9o
F+KosTDHt9mUN56dNQdqHWoXYXGyu47m0642K0hs7AZaqbHmlHdqdfnd3Ej7Dd18
5eWN4A/OsiWPZxCXN/UNOPQYY+iGo7Zzw5qhg4tmhzUJiA06IR1aXx6VvQpLy3Us
sc+cWqCMXDtucv4DJ4+cvp8dnMo78XSEpCV6qyJcWjUjkLmqYKpxiwDtslVz5ktd
CSPNOxS7HMW5q6nQ5NaTo2FivH0VfliOA3BspWypU02jPWghQkJTjRlzOeCK3PAu
t/Kr
-----END CERTIFICATE-----
`

// TestHandler_ImportCertificateTLSUsageAccepted verifies that Usage=TLS is accepted by
// ImportCertificate. Real AWS supports SIGNING, ENCRYPTION, and TLS as valid Usage values.
func TestHandler_ImportCertificateTLSUsageAccepted(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doTransferRequest(t, h, "ImportCertificate", map[string]any{
		"Usage": "TLS",
	})

	assert.Equal(t, http.StatusOK, rec.Code,
		"ImportCertificate with Usage=TLS must return 200; body: %s", rec.Body.String())
}

// TestHandler_ImportCertificateInvalidUsageRejected verifies that ImportCertificate returns 400
// for an unknown Usage value, matching real AWS behaviour.
func TestHandler_ImportCertificateInvalidUsageRejected(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doTransferRequest(t, h, "ImportCertificate", map[string]any{
		"Usage": "UNKNOWN_USAGE",
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code,
		"ImportCertificate with unknown Usage must return 400; body: %s", rec.Body.String())
}

// TestHandler_DescribeCertificateBodyReturnedWhenPresent verifies that DescribeCertificate
// returns the Certificate body when one was stored at import time. Real AWS returns the
// certificate PEM body in the Certificate field of the response.
func TestHandler_DescribeCertificateBodyReturnedWhenPresent(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	importRec := doTransferRequest(t, h, "ImportCertificate", map[string]any{
		"Certificate": testCertPEM,
		"Usage":       "SIGNING",
	})
	require.Equal(t, http.StatusOK, importRec.Code,
		"ImportCertificate failed: %s", importRec.Body.String())

	var importOut struct {
		CertificateID string `json:"CertificateId"`
	}
	require.NoError(t, json.Unmarshal(importRec.Body.Bytes(), &importOut))

	descRec := doTransferRequest(t, h, "DescribeCertificate", map[string]any{
		"CertificateId": importOut.CertificateID,
	})
	require.Equal(t, http.StatusOK, descRec.Code, "DescribeCertificate failed: %s", descRec.Body.String())

	var descOut struct {
		Certificate struct {
			Certificate string `json:"Certificate"`
		} `json:"Certificate"`
	}
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descOut))

	assert.Equal(t, testCertPEM, descOut.Certificate.Certificate,
		"DescribeCertificate must return the certificate body when present")
}

func TestHandler_DescribeCertificateIncludesArn(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doTransferRequest(t, h, "ImportCertificate", map[string]any{
		"Usage": "SIGNING",
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	certID := createResp["CertificateId"].(string)

	rec := doTransferRequest(t, h, "DescribeCertificate", map[string]any{
		"CertificateId": certID,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	cert := resp["Certificate"].(map[string]any)

	arn, hasArn := cert["Arn"].(string)
	assert.True(t, hasArn, "Arn must be present in DescribeCertificate response")
	assert.Contains(t, arn, certID, "Arn must contain CertificateId")
	assert.Contains(t, arn, "arn:aws:transfer:", "Arn must start with arn:aws:transfer:")
}

func TestHandler_ListCertificatesIncludesArn(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doTransferRequest(t, h, "ImportCertificate", map[string]any{
		"Usage": "ENCRYPTION",
	})

	rec := doTransferRequest(t, h, "ListCertificates", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	certs := resp["Certificates"].([]any)
	require.NotEmpty(t, certs)

	item := certs[0].(map[string]any)
	arn, hasArn := item["Arn"].(string)
	assert.True(t, hasArn, "Arn must be present in ListCertificates items")
	assert.Contains(t, arn, "arn:aws:transfer:", "Arn must start with arn:aws:transfer:")
}

func TestHandler_ImportCertificateUsageValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		usage    string
		wantCode int
	}{
		{"SIGNING accepted", "SIGNING", http.StatusOK},
		{"ENCRYPTION accepted", "ENCRYPTION", http.StatusOK},
		{"empty usage rejected", "", http.StatusBadRequest},
		{"invalid usage rejected", "INVALID", http.StatusBadRequest},
		{"lowercase rejected", "signing", http.StatusBadRequest},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			body := map[string]any{}
			if tc.usage != "" {
				body["Usage"] = tc.usage
			}
			rec := doTransferRequest(t, h, "ImportCertificate", body)
			assert.Equal(t, tc.wantCode, rec.Code)
		})
	}
}

func TestHandler_CertificateARNContainsAccountAndRegion(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doTransferRequest(t, h, "ImportCertificate", map[string]any{
		"Usage": "SIGNING",
	})
	require.Equal(t, http.StatusOK, createRec.Code)
	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	certID := createResp["CertificateId"].(string)

	rec := doTransferRequest(t, h, "DescribeCertificate", map[string]any{
		"CertificateId": certID,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	cert := resp["Certificate"].(map[string]any)

	// newTestHandler uses testAccountID = "123456789012" and testRegion = "us-east-1"
	arn := cert["Arn"].(string)
	assert.Contains(t, arn, "123456789012")
	assert.Contains(t, arn, "us-east-1")
	assert.Contains(t, arn, "certificate/"+certID)
}

func TestHandler_DeleteCertificate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(*transfer.InMemoryBackend) string
		name     string
		wantCode int
	}{
		{
			name: "success",
			setup: func(b *transfer.InMemoryBackend) string {
				certID := "cert-abc123"
				b.AddCertificateInternal(certID)

				return certID
			},
			wantCode: http.StatusOK,
		},
		{
			name: "not found",
			setup: func(_ *transfer.InMemoryBackend) string {
				return "cert-doesnotexist"
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "missing certificate id",
			setup: func(_ *transfer.InMemoryBackend) string {
				return ""
			},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := transfer.NewInMemoryBackend(t.Context(), testAccountID, testRegion)
			h := transfer.NewHandler(b)
			certID := tt.setup(b)

			rec := doTransferRequest(t, h, "DeleteCertificate", map[string]any{
				"CertificateId": certID,
			})
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestHandler_ImportCertificate(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	// Use no body so PEM parsing is skipped; provide dates as fallback.
	rec := doTransferRequest(t, h, "ImportCertificate", map[string]any{
		"Usage":         "SIGNING",
		"NotBeforeDate": "2024-01-01T00:00:00Z",
		"NotAfterDate":  "2025-01-01T00:00:00Z",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_DescribeCertificate(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doTransferRequest(t, h, "ImportCertificate", map[string]any{
		"Usage":         "SIGNING",
		"NotBeforeDate": "2024-01-01T00:00:00Z",
		"NotAfterDate":  "2025-01-01T00:00:00Z",
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	certID := createResp["CertificateId"].(string)

	rec := doTransferRequest(t, h, "DescribeCertificate", map[string]any{
		"CertificateId": certID,
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_ListCertificates(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doTransferRequest(t, h, "ListCertificates", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_UpdateCertificate(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doTransferRequest(t, h, "ImportCertificate", map[string]any{
		"Usage":         "SIGNING",
		"NotBeforeDate": "2024-06-01T00:00:00Z",
		"NotAfterDate":  "2025-06-01T00:00:00Z",
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	certID := createResp["CertificateId"].(string)

	rec := doTransferRequest(t, h, "UpdateCertificate", map[string]any{
		"CertificateId": certID,
		"Description":   "updated cert",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// validTestCertPEM is a self-signed certificate used to test PEM parsing.
const validTestCertPEM = `-----BEGIN CERTIFICATE-----
MIIC/zCCAeegAwIBAgIUfG/4OODArqXXrUB2bYyGYAIeWIMwDQYJKoZIhvcNAQEL
BQAwDzENMAsGA1UEAwwEdGVzdDAeFw0yNjA1MTcxOTM2NThaFw0yNzA1MTcxOTM2
NThaMA8xDTALBgNVBAMMBHRlc3QwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEK
AoIBAQC1oEoKtOIzJESaXBa6XuCUVU4vfafDlAvGUwxTyARcd+xB7QGT7Y3ilvFM
kv9Ha5bIcUtppZYTI/t/+5ZFF1By6WIpJcWeKUm5ueXDf5LKZOC2M8ANMBFgRmll
bi5hyvyf0WcK7G07n0eNIV6vfPSvzWbJuOplHPlyj6wgyfglbCUoXRdvYxv9vrKc
19wI/VKqjRrgHeQ8Kjgs/Do9U2oQIes2810Dm/haGF4bb65AdoowFPwKAfzRbxq3
U+T0RKdkMg+N3+hCUCJyTOhjiGNzl0Pe1vHqYJJk+gCy0KPZtJqXx5VWtQOsv63c
GJgyXGQtF9upqlUfazkGMwV5bmFbAgMBAAGjUzBRMB0GA1UdDgQWBBRRJauAqr0R
xOkIKwIZzJ4ag+95oTAfBgNVHSMEGDAWgBRRJauAqr0RxOkIKwIZzJ4ag+95oTAP
BgNVHRMBAf8EBTADAQH/MA0GCSqGSIb3DQEBCwUAA4IBAQBbHxRRXX7imnCLlVzh
BWb6rQRxMMDb214ppKkMrYacAWJUs8O35DYf/CHoh+lqp9pUX41AJS5oHnSJIVLI
n4X3/Fk3mi4bGYfqeegRC+QnxNMnHVacDg6zht/4PZxb6f+QmXq+nSgXCjrUO8rY
5XQsPS4p8itAYDPnpfrj9FyUlG8mVvkylVO3A6F+LwmJZwFBqMlX798MOHtyKNbn
Iuht/fgge8OmzBTrL5qWmS3JGFFRaWZyIuU2FrOZ/LQJqY7Pgj+WxmR3VsSr82an
BDvp/A/LIWFKk/yswpG/O5gw0vDilojojeyB9A/tcOW36xNgygOKan7YSu8P3t7s
s7B6
-----END CERTIFICATE-----`

// Test 17: ImportCertificate with malformed PEM returns 400.
func TestHandler_ImportCertificateMalformedPEM(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doTransferRequest(t, h, "ImportCertificate", map[string]any{
		"Usage":       "SIGNING",
		"Certificate": "-----BEGIN CERTIFICATE-----\nNOTVALIDBASE64!!!\n-----END CERTIFICATE-----",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "InvalidRequestException", resp["__type"])
}

// Test 18: ImportCertificate with valid PEM returns NotBeforeDate/NotAfterDate.
func TestHandler_ImportCertificateValidPEMExtractsDates(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doTransferRequest(t, h, "ImportCertificate", map[string]any{
		"Usage":       "SIGNING",
		"Certificate": validTestCertPEM,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	certID := createResp["CertificateId"].(string)

	descRec := doTransferRequest(t, h, "DescribeCertificate", map[string]any{
		"CertificateId": certID,
	})
	require.Equal(t, http.StatusOK, descRec.Code)

	var descResp map[string]any
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descResp))
	cert := descResp["Certificate"].(map[string]any)

	assert.NotEmpty(t, cert["NotBeforeDate"], "NotBeforeDate must be set from PEM")
	assert.NotEmpty(t, cert["NotAfterDate"], "NotAfterDate must be set from PEM")
}
