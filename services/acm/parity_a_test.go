package acm_test

// parity_a_test.go — §A parity fixes:
// 1. SubjectAlternativeNames always includes the primary domain as the first entry.
// 2. DescribeCertificate InUseBy is always [] not null/omitted.
// 3. Serial number uses colon-separated hex pairs (e.g. "1a:2b:3c").

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/acm"
)

func mustMarshal(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	require.NoError(t, err)

	return string(b)
}

// TestParity_SubjectAlternativeNames_IncludesPrimaryDomain verifies that
// DescribeCertificate always includes the primary domain as the first entry in
// SubjectAlternativeNames, matching real AWS ACM behavior.
func TestParity_SubjectAlternativeNames_IncludesPrimaryDomain(t *testing.T) {
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

// TestParity_ListCertificates_SubjectAlternativeNameSummaries_IncludesPrimary verifies that
// ListCertificates SubjectAlternativeNameSummaries includes the primary domain.
func TestParity_ListCertificates_SubjectAlternativeNameSummaries_IncludesPrimary(t *testing.T) {
	t.Parallel()

	h := newACMHandler()
	reqBody := mustMarshal(t, map[string]any{
		"DomainName":              "primary.example.com",
		"SubjectAlternativeNames": []string{"www.primary.example.com"},
	})
	reqRec := postACMJSON(t, h, "RequestCertificate", reqBody)
	require.Equal(t, http.StatusOK, reqRec.Code)

	listRec := postACMJSON(t, h, "ListCertificates", "{}")
	require.Equal(t, http.StatusOK, listRec.Code)

	var listOut struct {
		CertificateSummaryList []struct {
			DomainName                      string   `json:"DomainName"`
			SubjectAlternativeNameSummaries []string `json:"SubjectAlternativeNameSummaries"`
		} `json:"CertificateSummaryList"`
	}
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listOut))
	require.NotEmpty(t, listOut.CertificateSummaryList)

	summaries := listOut.CertificateSummaryList[0].SubjectAlternativeNameSummaries
	assert.Contains(t, summaries, "primary.example.com",
		"SubjectAlternativeNameSummaries must include the primary domain")
	assert.Contains(t, summaries, "www.primary.example.com",
		"SubjectAlternativeNameSummaries must include extra SANs")
}

// TestParity_DescribeCertificate_InUseByIsAlwaysArray verifies that InUseBy in
// DescribeCertificate is always a JSON array (possibly empty), never null or omitted.
func TestParity_DescribeCertificate_InUseByIsAlwaysArray(t *testing.T) {
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

// TestParity_Serial_ColonSeparatedHexFormat verifies that certificate serial numbers
// use colon-separated hex pairs (e.g. "1a:2b:3c"), matching real AWS ACM serial format.
func TestParity_Serial_ColonSeparatedHexFormat(t *testing.T) {
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

// TestParity_ImportedCertificate_SerialColonFormat verifies that imported certificates
// also have colon-separated serial numbers.
func TestParity_ImportedCertificate_SerialColonFormat(t *testing.T) {
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
