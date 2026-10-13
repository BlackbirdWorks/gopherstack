package acm_test

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestACMHandler_RequestCertificate_InputRealism(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		body     string
		wantType string
	}{
		{
			name:     "underscore_domain",
			body:     `{"DomainName":"bad_domain.example.com"}`,
			wantType: "InvalidParameterException",
		},
		{name: "double_wildcard", body: `{"DomainName":"*.*.example.com"}`, wantType: "InvalidParameterException"},
		{name: "leading_hyphen", body: `{"DomainName":"-a.example.com"}`, wantType: "InvalidParameterException"},
		{
			name:     "cn_over_64",
			body:     `{"DomainName":"` + strings.Repeat("a", 40) + "." + strings.Repeat("b", 30) + `.com"}`,
			wantType: "InvalidParameterException",
		},
		{name: "bad_validation_method", body: `{"DomainName":"a.example.com","ValidationMethod":"FAX"}`,
			wantType: "InvalidParameterException"},
		{name: "bad_key_algorithm", body: `{"DomainName":"a.example.com","KeyAlgorithm":"FOO"}`,
			wantType: "InvalidParameterException"},
		{
			name:     "bad_transparency",
			body:     `{"DomainName":"a.example.com","Options":{"CertificateTransparencyLoggingPreference":"X"}}`,
			wantType: "InvalidParameterException",
		},
		{
			name:     "bad_token",
			body:     `{"DomainName":"a.example.com","IdempotencyToken":"a b"}`,
			wantType: "InvalidParameterException",
		},
		{name: "ok", body: `{"DomainName":"*.a.example.com","ValidationMethod":"DNS"}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newACMHandler()
			rec := postACMJSON(t, h, "RequestCertificate", tt.body)

			if tt.wantType == "" {
				require.Equal(t, http.StatusOK, rec.Code)

				return
			}

			require.Equal(t, http.StatusBadRequest, rec.Code)

			var errResp struct {
				Type    string `json:"__type"`
				Message string `json:"message"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
			assert.Equal(t, tt.wantType, errResp.Type)
			assert.NotContains(t, errResp.Message, "Exception")
		})
	}
}

func TestACMHandler_WildcardValidationRecordAndNotFound(t *testing.T) {
	t.Parallel()

	h := newACMHandler()
	rec := postACMJSON(t, h, "RequestCertificate", `{"DomainName":"*.w.example.com","ValidationMethod":"DNS"}`)
	require.Equal(t, http.StatusOK, rec.Code)

	var created struct {
		CertificateArn string `json:"CertificateArn"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &created))

	rec = postACMJSON(t, h, "DescribeCertificate", `{"CertificateArn":"`+created.CertificateArn+`"}`)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.NotContains(t, rec.Body.String(), `.*.w.example.com.`)
	assert.Contains(t, rec.Body.String(), `.w.example.com.`)

	rec = postACMJSON(t, h, "DescribeCertificate",
		`{"CertificateArn":"arn:aws:acm:us-east-1:123456789012:certificate/00000000-0000-0000-0000-000000000000"}`)
	require.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "Could not find certificate")
	assert.NotContains(t, rec.Body.String(), "ResourceNotFoundException:")

	rec = postACMJSON(t, h, "ListCertificates", `{"MaxItems":1001}`)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "InvalidArgsException")
}
