package docdb_test

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHandler_DescribeCertificates(t *testing.T) {
	t.Parallel()

	tests := []struct {
		vals         url.Values
		name         string
		wantContains string
		wantStatus   int
	}{
		{
			name: "describe_all_certificates",
			vals: url.Values{
				"Action":  {"DescribeCertificates"},
				"Version": {"2014-10-31"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "rds-ca-2019",
		},
		{
			name: "describe_certificate_by_id",
			vals: url.Values{
				"Action":                {"DescribeCertificates"},
				"Version":               {"2014-10-31"},
				"CertificateIdentifier": {"rds-ca-rsa2048-g1"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "rds-ca-rsa2048-g1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rr := doRequest(t, h, tt.vals)
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

func TestDescribeCertificates_Filter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		certID          string
		wantContains    string
		wantNotContains string
		wantStatus      int
	}{
		{
			name:         "no_filter_returns_all_certs",
			wantStatus:   http.StatusOK,
			wantContains: "rds-ca-2019",
		},
		{
			name:            "filter_by_known_id",
			certID:          "rds-ca-2019",
			wantStatus:      http.StatusOK,
			wantContains:    "rds-ca-2019",
			wantNotContains: "rds-ca-rsa2048-g1",
		},
		{
			name:            "filter_by_second_known_id",
			certID:          "rds-ca-rsa2048-g1",
			wantStatus:      http.StatusOK,
			wantContains:    "rds-ca-rsa2048-g1",
			wantNotContains: "rds-ca-2019",
		},
		{
			name:         "unknown_cert_id_returns_empty",
			certID:       "rds-ca-does-not-exist",
			wantStatus:   http.StatusOK,
			wantContains: "DescribeCertificatesResponse",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			vals := url.Values{
				"Action":  {"DescribeCertificates"},
				"Version": {"2014-10-31"},
			}
			if tt.certID != "" {
				vals.Set("CertificateIdentifier", tt.certID)
			}
			rr := doRequest(t, h, vals)
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
			if tt.wantNotContains != "" {
				assert.NotContains(t, rr.Body.String(), tt.wantNotContains)
			}
		})
	}
}
