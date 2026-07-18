package acm_test

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/acm"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestACMHandler_ImportCertificate(t *testing.T) {
	t.Parallel()

	b := acm.NewInMemoryBackend("000000000000", "us-east-1")
	// Request cert to get a PEM body and key
	cert, err := b.RequestCertificate(context.Background(), "import-test.example.com", "", "", "", "", "", "", nil)
	require.NoError(t, err)

	certPEM := cert.CertificateBody
	keyPEM := cert.PrivateKey

	tests := []struct {
		name         string
		body         string
		wantContains []string
		wantCode     int
	}{
		{
			name: "success",
			body: func() string {
				in, _ := json.Marshal(map[string]string{
					"Certificate": certPEM,
					"PrivateKey":  keyPEM,
				})

				return string(in)
			}(),
			wantCode:     http.StatusOK,
			wantContains: []string{"CertificateArn", "arn:aws:acm:"},
		},
		{
			name:     "missing_cert",
			body:     `{"PrivateKey":"dummy"}`,
			wantCode: http.StatusBadRequest,
		},
		{
			name: "missing_key",
			body: func() string {
				in, _ := json.Marshal(map[string]string{"Certificate": certPEM})

				return string(in)
			}(),
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newACMHandler()
			rec := postACMJSON(t, h, "ImportCertificate", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

func TestACMHandler_RenewCertificate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		body     string
		wantCode int
	}{
		{
			name:     "not_found",
			body:     `{"CertificateArn":"arn:aws:acm:us-east-1:000000000000:certificate/none"}`,
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newACMHandler()
			rec := postACMJSON(t, h, "RenewCertificate", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestACMHandler_ExportCertificate(t *testing.T) {
	t.Parallel()

	b := acm.NewInMemoryBackend("000000000000", "us-east-1")
	cert, err := b.RequestCertificate(context.Background(), "export-test.example.com", "", "", "", "", "", "", nil)
	require.NoError(t, err)

	importedCert, err := b.ImportCertificate(context.Background(), cert.CertificateBody, cert.PrivateKey, "", "")
	require.NoError(t, err)

	tests := []struct {
		name         string
		certARN      string
		wantContains []string
		wantCode     int
	}{
		{
			name:         "success_imported",
			certARN:      importedCert.ARN,
			wantCode:     http.StatusOK,
			wantContains: []string{"Certificate", "PrivateKey"},
		},
		{
			name:     "fails_amazon_issued",
			certARN:  cert.ARN,
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "not_found",
			certARN:  "arn:aws:acm:us-east-1:000000000000:certificate/none",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := acm.NewHandler(b)
			body, _ := json.Marshal(map[string]string{
				"CertificateArn": tt.certARN,
				"Passphrase":     "dGVzdA==",
			})
			rec := postACMJSON(t, h, "ExportCertificate", string(body))
			assert.Equal(t, tt.wantCode, rec.Code)
			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

func TestACMHandler_GetCertificate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		wantContains []string
		wantCode     int
	}{
		{
			name:         "success",
			wantCode:     http.StatusOK,
			wantContains: []string{"Certificate", "BEGIN CERTIFICATE"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newACMHandler()
			reqRec := postACMJSON(t, h, "RequestCertificate", `{"DomainName":"get-cert.example.com"}`)
			require.Equal(t, http.StatusOK, reqRec.Code)

			var reqOut struct {
				CertificateArn string `json:"CertificateArn"`
			}
			require.NoError(t, json.Unmarshal(reqRec.Body.Bytes(), &reqOut))

			body, _ := json.Marshal(map[string]string{"CertificateArn": reqOut.CertificateArn})
			rec := postACMJSON(t, h, "GetCertificate", string(body))
			assert.Equal(t, tt.wantCode, rec.Code)
			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

// TestACMHandler_DescribeCertificate_RealistFields verifies that DescribeCertificate
// returns the new realism fields: Serial, Subject, Issuer, KeyAlgorithm, SignatureAlgorithm, IssuedAt.
func TestACMHandler_DescribeCertificate_RealistFields(t *testing.T) {
	t.Parallel()

	h := newACMHandler()

	reqRec := postACMJSON(t, h, "RequestCertificate", `{"DomainName":"realism.example.com"}`)
	require.Equal(t, http.StatusOK, reqRec.Code)

	var reqOut struct {
		CertificateArn string `json:"CertificateArn"`
	}
	require.NoError(t, json.Unmarshal(reqRec.Body.Bytes(), &reqOut))

	descBody, _ := json.Marshal(map[string]string{"CertificateArn": reqOut.CertificateArn})
	descRec := postACMJSON(t, h, "DescribeCertificate", string(descBody))
	require.Equal(t, http.StatusOK, descRec.Code)

	var descOut struct {
		Certificate struct {
			IssuedAt           *int64 `json:"IssuedAt"`
			Serial             string `json:"Serial"`
			Subject            string `json:"Subject"`
			Issuer             string `json:"Issuer"`
			KeyAlgorithm       string `json:"KeyAlgorithm"`
			SignatureAlgorithm string `json:"SignatureAlgorithm"`
		} `json:"Certificate"`
	}
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descOut))
	assert.NotEmpty(t, descOut.Certificate.Serial, "Serial should be set")
	assert.Contains(t, descOut.Certificate.Subject, "realism.example.com", "Subject should contain domain")
	assert.Contains(t, descOut.Certificate.Issuer, "realism.example.com", "Issuer should contain domain (self-signed)")
	assert.Equal(t, "EC_prime256v1", descOut.Certificate.KeyAlgorithm)
	assert.Equal(t, "SHA256WITHECDSA", descOut.Certificate.SignatureAlgorithm)
	assert.NotNil(t, descOut.Certificate.IssuedAt, "IssuedAt should be set for ISSUED cert")
}

// TestACMHandler_ImportCertificate_RealistFields verifies ImportedAt is set.
func TestACMHandler_ImportCertificate_RealistFields(t *testing.T) {
	t.Parallel()

	h := newACMHandler()

	// First create a cert to get PEM material
	b := acm.NewInMemoryBackend("000000000000", "us-east-1")
	src, err := b.RequestCertificate(context.Background(), "import-realism.example.com", "", "", "", "", "", "", nil)
	require.NoError(t, err)

	body, _ := json.Marshal(map[string]string{
		"Certificate": src.CertificateBody,
		"PrivateKey":  src.PrivateKey,
	})
	importRec := postACMJSON(t, h, "ImportCertificate", string(body))
	require.Equal(t, http.StatusOK, importRec.Code)

	var importOut struct {
		CertificateArn string `json:"CertificateArn"`
	}
	require.NoError(t, json.Unmarshal(importRec.Body.Bytes(), &importOut))

	descBody, _ := json.Marshal(map[string]string{"CertificateArn": importOut.CertificateArn})
	descRec := postACMJSON(t, h, "DescribeCertificate", string(descBody))
	require.Equal(t, http.StatusOK, descRec.Code)

	var descOut struct {
		Certificate struct {
			ImportedAt *int64 `json:"ImportedAt"`
			Type       string `json:"Type"`
		} `json:"Certificate"`
	}
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descOut))
	assert.Equal(t, "IMPORTED", descOut.Certificate.Type)
	assert.NotNil(t, descOut.Certificate.ImportedAt, "ImportedAt should be set for imported cert")
}

// TestACMHandler_ImportCertificate_ReImport verifies that passing CertificateArn re-imports.
func TestACMHandler_ImportCertificate_ReImport(t *testing.T) {
	t.Parallel()

	h := newACMHandler()
	b := acm.NewInMemoryBackend("000000000000", "us-east-1")

	// Create two certs to get two sets of PEM material
	src1, err := b.RequestCertificate(context.Background(), "reimport.example.com", "", "", "", "", "", "", nil)
	require.NoError(t, err)
	src2, err := b.RequestCertificate(context.Background(), "reimport2.example.com", "", "", "", "", "", "", nil)
	require.NoError(t, err)

	// Import first cert
	body1, _ := json.Marshal(map[string]string{
		"Certificate": src1.CertificateBody,
		"PrivateKey":  src1.PrivateKey,
	})
	importRec := postACMJSON(t, h, "ImportCertificate", string(body1))
	require.Equal(t, http.StatusOK, importRec.Code)

	var importOut struct {
		CertificateArn string `json:"CertificateArn"`
	}
	require.NoError(t, json.Unmarshal(importRec.Body.Bytes(), &importOut))
	originalARN := importOut.CertificateArn

	// Re-import using the same ARN with new cert material
	body2, _ := json.Marshal(map[string]string{
		"CertificateArn": originalARN,
		"Certificate":    src2.CertificateBody,
		"PrivateKey":     src2.PrivateKey,
	})
	reImportRec := postACMJSON(t, h, "ImportCertificate", string(body2))
	require.Equal(t, http.StatusOK, reImportRec.Code)

	// Should return the same ARN
	var reImportOut struct {
		CertificateArn string `json:"CertificateArn"`
	}
	require.NoError(t, json.Unmarshal(reImportRec.Body.Bytes(), &reImportOut))
	assert.Equal(t, originalARN, reImportOut.CertificateArn, "re-import should return same ARN")
}

// TestACMHandler_RequestCertificate_IdempotencyToken verifies idempotent cert creation.
func TestACMHandler_RequestCertificate_IdempotencyToken(t *testing.T) {
	t.Parallel()

	h := newACMHandler()

	body := `{"DomainName":"idem.example.com","IdempotencyToken":"tok-abc123"}`

	// First call
	rec1 := postACMJSON(t, h, "RequestCertificate", body)
	require.Equal(t, http.StatusOK, rec1.Code)

	var out1 struct {
		CertificateArn string `json:"CertificateArn"`
	}
	require.NoError(t, json.Unmarshal(rec1.Body.Bytes(), &out1))

	// Second call with same token should return same ARN
	rec2 := postACMJSON(t, h, "RequestCertificate", body)
	require.Equal(t, http.StatusOK, rec2.Code)

	var out2 struct {
		CertificateArn string `json:"CertificateArn"`
	}
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &out2))

	assert.Equal(t, out1.CertificateArn, out2.CertificateArn,
		"repeated RequestCertificate with same IdempotencyToken must return same ARN")
}

// TestACMHandler_RequestCertificate_DomainValidation verifies domain name validation errors.
func TestACMHandler_RequestCertificate_DomainValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		domain string
	}{
		{
			name:   "empty_domain",
			domain: "",
		},
		{
			name:   "domain_too_long",
			domain: "a." + strings.Repeat("b", 252) + ".com",
		},
		{
			name:   "label_too_long",
			domain: strings.Repeat("x", 64) + ".example.com",
		},
		{
			name:   "empty_label",
			domain: "foo..example.com",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newACMHandler()
			body, _ := json.Marshal(map[string]string{"DomainName": tt.domain})
			rec := postACMJSON(t, h, "RequestCertificate", string(body))
			assert.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}
}

// TestACMHandler_DescribeCertificate_KeyUsageAndInUseBy verifies new fields in DescribeCertificate.
func TestACMHandler_DescribeCertificate_KeyUsageAndInUseBy(t *testing.T) {
	t.Parallel()

	h := newACMHandler()

	reqRec := postACMJSON(t, h, "RequestCertificate", `{"DomainName":"keyusage.example.com"}`)
	require.Equal(t, http.StatusOK, reqRec.Code)

	var reqOut struct {
		CertificateArn string `json:"CertificateArn"`
	}
	require.NoError(t, json.Unmarshal(reqRec.Body.Bytes(), &reqOut))

	descBody, _ := json.Marshal(map[string]string{"CertificateArn": reqOut.CertificateArn})
	descRec := postACMJSON(t, h, "DescribeCertificate", string(descBody))
	require.Equal(t, http.StatusOK, descRec.Code)

	var descOut struct {
		Certificate struct {
			KeyUsage         []map[string]string `json:"KeyUsages"`
			ExtendedKeyUsage []map[string]string `json:"ExtendedKeyUsages"`
			InUseBy          []string            `json:"InUseBy"`
		} `json:"Certificate"`
	}
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descOut))

	require.NotEmpty(t, descOut.Certificate.KeyUsage, "KeyUsage should be set")
	assert.Equal(t, "DIGITAL_SIGNATURE", descOut.Certificate.KeyUsage[0]["Name"])

	require.NotEmpty(t, descOut.Certificate.ExtendedKeyUsage, "ExtendedKeyUsage should be set")
	assert.Equal(t, "TLS_WEB_SERVER_AUTHENTICATION", descOut.Certificate.ExtendedKeyUsage[0]["Name"])
}

// TestACMHandler_RequestCertificate_EMAIL_ValidationEmails verifies ValidationEmails for EMAIL method.
func TestACMHandler_RequestCertificate_EMAIL_ValidationEmails(t *testing.T) {
	t.Parallel()

	h := newACMHandler()

	reqRec := postACMJSON(t, h, "RequestCertificate",
		`{"DomainName":"email-val.example.com","ValidationMethod":"EMAIL"}`)
	require.Equal(t, http.StatusOK, reqRec.Code)

	var reqOut struct {
		CertificateArn string `json:"CertificateArn"`
	}
	require.NoError(t, json.Unmarshal(reqRec.Body.Bytes(), &reqOut))

	descBody, _ := json.Marshal(map[string]string{"CertificateArn": reqOut.CertificateArn})
	descRec := postACMJSON(t, h, "DescribeCertificate", string(descBody))
	require.Equal(t, http.StatusOK, descRec.Code)

	var descOut struct {
		Certificate struct {
			DomainValidationOptions []struct {
				ValidationMethod string   `json:"ValidationMethod"`
				ValidationEmails []string `json:"ValidationEmails"`
			} `json:"DomainValidationOptions"`
		} `json:"Certificate"`
	}
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descOut))
	require.NotEmpty(t, descOut.Certificate.DomainValidationOptions)

	dvo := descOut.Certificate.DomainValidationOptions[0]
	assert.Equal(t, "EMAIL", dvo.ValidationMethod)
	assert.NotEmpty(t, dvo.ValidationEmails, "ValidationEmails should be set for EMAIL validation")
	assert.Contains(t, dvo.ValidationEmails, "admin@email-val.example.com")
}

// TestACMHandler_WildcardCertificate_Accepted verifies wildcard domains are accepted.
func TestACMHandler_WildcardCertificate_Accepted(t *testing.T) {
	t.Parallel()

	h := newACMHandler()

	rec := postACMJSON(t, h, "RequestCertificate", `{"DomainName":"*.example.com"}`)
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestACMHandler_ExportCertificate_PassphraseRequired verifies that Passphrase is required.
func TestACMHandler_ExportCertificate_PassphraseRequired(t *testing.T) {
	t.Parallel()

	b := acm.NewInMemoryBackend("000000000000", "us-east-1")
	src, err := b.RequestCertificate(context.Background(), "export-pass.example.com", "", "", "", "", "", "", nil)
	require.NoError(t, err)

	importedCert, err := b.ImportCertificate(context.Background(), src.CertificateBody, src.PrivateKey, "", "")
	require.NoError(t, err)

	tests := []struct {
		name         string
		body         string
		wantContains []string
		wantCode     int
	}{
		{
			name: "missing_passphrase_rejected",
			body: func() string {
				b2, _ := json.Marshal(map[string]string{"CertificateArn": importedCert.ARN})

				return string(b2)
			}(),
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"ValidationException", "Passphrase"},
		},
		{
			name: "with_passphrase_success_encrypted_key",
			body: func() string {
				b2, _ := json.Marshal(map[string]string{
					"CertificateArn": importedCert.ARN,
					"Passphrase":     "dGVzdC1wYXNz", // base64("test-pass")
				})

				return string(b2)
			}(),
			wantCode:     http.StatusOK,
			wantContains: []string{"ENCRYPTED PRIVATE KEY"},
		},
		{
			name: "invalid_base64_passphrase",
			body: func() string {
				b2, _ := json.Marshal(map[string]string{
					"CertificateArn": importedCert.ARN,
					"Passphrase":     "not-valid-base64!!!",
				})

				return string(b2)
			}(),
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"ValidationException"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := acm.NewHandler(b)
			rec := postACMJSON(t, h, "ExportCertificate", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

// TestACMHandler_ExportCertificate_CertificateChainAlwaysPresent verifies chain is always returned.
func TestACMHandler_ExportCertificate_CertificateChainAlwaysPresent(t *testing.T) {
	t.Parallel()

	b := acm.NewInMemoryBackend("000000000000", "us-east-1")
	src, err := b.RequestCertificate(context.Background(), "chain-test.example.com", "", "", "", "", "", "", nil)
	require.NoError(t, err)

	// Import without chain
	imported, err := b.ImportCertificate(context.Background(), src.CertificateBody, src.PrivateKey, "", "")
	require.NoError(t, err)

	h := acm.NewHandler(b)
	exportBody, _ := json.Marshal(map[string]string{
		"CertificateArn": imported.ARN,
		"Passphrase":     "dGVzdA==", // base64("test")
	})
	rec := postACMJSON(t, h, "ExportCertificate", string(exportBody))
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		CertificateChain string `json:"CertificateChain"`
		Certificate      string `json:"Certificate"`
		PrivateKey       string `json:"PrivateKey"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.NotEmpty(t, out.CertificateChain, "CertificateChain should always be present on export")
	assert.NotEmpty(t, out.Certificate)
	assert.NotEmpty(t, out.PrivateKey)
}

func mustMarshal(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	require.NoError(t, err)

	return string(b)
}

// TestACMHandler_SubjectAlternativeNames_IncludesPrimaryDomain verifies that
// DescribeCertificate always includes the primary domain as the first entry in
// SubjectAlternativeNames, matching real AWS ACM behavior.
func TestACMHandler_SubjectAlternativeNames_IncludesPrimaryDomain(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		domainName string
		sans       []string
		wantFirst  string
		wantAll    []string
	}{
		{
			name:       "no_extra_sans",
			domainName: "example.com",
			sans:       nil,
			wantFirst:  "example.com",
			wantAll:    []string{"example.com"},
		},
		{
			name:       "with_extra_sans",
			domainName: "example.com",
			sans:       []string{"www.example.com", "api.example.com"},
			wantFirst:  "example.com",
			wantAll:    []string{"example.com", "www.example.com", "api.example.com"},
		},
		{
			name:       "wildcard",
			domainName: "*.example.com",
			sans:       nil,
			wantFirst:  "*.example.com",
			wantAll:    []string{"*.example.com"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newACMHandler()
			reqBody := map[string]any{"DomainName": tt.domainName}
			if len(tt.sans) > 0 {
				reqBody["SubjectAlternativeNames"] = tt.sans
			}

			reqRec := postACMJSON(t, h, "RequestCertificate", mustMarshal(t, reqBody))
			require.Equal(t, http.StatusOK, reqRec.Code)

			var reqOut struct {
				CertificateArn string `json:"CertificateArn"`
			}
			require.NoError(t, json.Unmarshal(reqRec.Body.Bytes(), &reqOut))

			descBody := mustMarshal(t, map[string]string{"CertificateArn": reqOut.CertificateArn})
			descRec := postACMJSON(t, h, "DescribeCertificate", descBody)
			require.Equal(t, http.StatusOK, descRec.Code)

			var descOut struct {
				Certificate struct {
					SubjectAlternativeNames []string `json:"SubjectAlternativeNames"`
				} `json:"Certificate"`
			}
			require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descOut))

			sans := descOut.Certificate.SubjectAlternativeNames
			require.NotEmpty(t, sans, "SubjectAlternativeNames must not be empty")
			assert.Equal(t, tt.wantFirst, sans[0],
				"first SAN must be the primary domain")
			assert.Equal(t, tt.wantAll, sans,
				"SubjectAlternativeNames must include primary domain + extras")
		})
	}
}

// TestACMHandler_DescribeCertificate_InUseByIsAlwaysArray verifies that InUseBy in
// DescribeCertificate is always a JSON array (possibly empty), never null or omitted.
func TestACMHandler_DescribeCertificate_InUseByIsAlwaysArray(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		body string
	}{
		{
			name: "issued_cert",
			body: `{"DomainName":"inuse-check.example.com"}`,
		},
		{
			name: "dns_pending_cert",
			body: `{"DomainName":"pending-inuse-check.example.com","ValidationMethod":"DNS"}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newACMHandler()
			reqRec := postACMJSON(t, h, "RequestCertificate", tt.body)
			require.Equal(t, http.StatusOK, reqRec.Code)

			var reqOut struct {
				CertificateArn string `json:"CertificateArn"`
			}
			require.NoError(t, json.Unmarshal(reqRec.Body.Bytes(), &reqOut))

			descBody := mustMarshal(t, map[string]string{"CertificateArn": reqOut.CertificateArn})
			descRec := postACMJSON(t, h, "DescribeCertificate", descBody)
			require.Equal(t, http.StatusOK, descRec.Code)

			// Use raw JSON to verify InUseBy is an array, not null or missing.
			var raw map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &raw))

			certRaw, ok := raw["Certificate"]
			require.True(t, ok, "response must have Certificate field")

			var certMap map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(certRaw, &certMap))

			inUseByRaw, present := certMap["InUseBy"]
			require.True(t, present,
				"InUseBy must always be present in DescribeCertificate response")
			assert.NotEqual(t, "null", string(inUseByRaw),
				"InUseBy must be [] not null")
			assert.True(t, strings.HasPrefix(string(inUseByRaw), "["),
				"InUseBy must be a JSON array, got: %s", string(inUseByRaw))
		})
	}
}

// TestACMHandler_Serial_ColonSeparatedHexFormat verifies that certificate serial numbers
// use colon-separated hex pairs (e.g. "1a:2b:3c"), matching real AWS ACM serial format.
func TestACMHandler_Serial_ColonSeparatedHexFormat(t *testing.T) {
	t.Parallel()

	h := newACMHandler()
	reqRec := postACMJSON(t, h, "RequestCertificate",
		`{"DomainName":"serial-format.example.com"}`)
	require.Equal(t, http.StatusOK, reqRec.Code)

	var reqOut struct {
		CertificateArn string `json:"CertificateArn"`
	}
	require.NoError(t, json.Unmarshal(reqRec.Body.Bytes(), &reqOut))

	descBody := mustMarshal(t, map[string]string{"CertificateArn": reqOut.CertificateArn})
	descRec := postACMJSON(t, h, "DescribeCertificate", descBody)
	require.Equal(t, http.StatusOK, descRec.Code)

	var descOut struct {
		Certificate struct {
			Serial string `json:"Serial"`
		} `json:"Certificate"`
	}
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descOut))

	serial := descOut.Certificate.Serial
	require.NotEmpty(t, serial, "Serial must be set for an issued certificate")
	assert.Contains(t, serial, ":",
		"Serial must use colon-separated hex pairs (e.g. 1a:2b:3c), got %q", serial)

	// Each segment between colons must be exactly 2 lowercase hex chars.
	for part := range strings.SplitSeq(serial, ":") {
		assert.Len(t, part, 2,
			"each serial segment must be exactly 2 hex chars, got %q in %q", part, serial)

		for _, c := range part {
			assert.True(t,
				(c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'),
				"serial segment %q must contain only lowercase hex chars", part)
		}
	}
}

// TestACMHandler_ImportedCertificate_SerialColonFormat verifies that imported certificates
// also have colon-separated serial numbers.
func TestACMHandler_ImportedCertificate_SerialColonFormat(t *testing.T) {
	t.Parallel()

	h := newACMHandler()
	b := acm.NewInMemoryBackend("000000000000", "us-east-1")
	src, err := b.RequestCertificate(
		context.Background(), "serial-import.example.com", "", "", "", "", "", "", nil,
	)
	require.NoError(t, err)

	importBody := mustMarshal(t, map[string]string{
		"Certificate": src.CertificateBody,
		"PrivateKey":  src.PrivateKey,
	})
	importRec := postACMJSON(t, h, "ImportCertificate", importBody)
	require.Equal(t, http.StatusOK, importRec.Code)

	var importOut struct {
		CertificateArn string `json:"CertificateArn"`
	}
	require.NoError(t, json.Unmarshal(importRec.Body.Bytes(), &importOut))

	descBody := mustMarshal(t, map[string]string{"CertificateArn": importOut.CertificateArn})
	descRec := postACMJSON(t, h, "DescribeCertificate", descBody)
	require.Equal(t, http.StatusOK, descRec.Code)

	var descOut struct {
		Certificate struct {
			Serial string `json:"Serial"`
		} `json:"Certificate"`
	}
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descOut))

	serial := descOut.Certificate.Serial
	require.NotEmpty(t, serial, "imported cert Serial must be set")
	assert.Contains(t, serial, ":",
		"imported cert serial must use colon-separated hex pairs, got %q", serial)
}
