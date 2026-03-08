package acm_test

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/acm"
)

func TestACMBackend_RequestCertificate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		domain           string
		validationMethod string
		wantErr          error
		wantDomain       string
		wantStatus       string
		wantType         string
		wantPendingFirst bool
	}{
		{
			name:       "success_no_validation",
			domain:     "example.com",
			wantDomain: "example.com",
			wantStatus: "ISSUED",
			wantType:   "AMAZON_ISSUED",
		},
		{
			name:             "dns_validation_pending",
			domain:           "dns.example.com",
			validationMethod: "DNS",
			wantDomain:       "dns.example.com",
			wantStatus:       "PENDING_VALIDATION",
			wantType:         "AMAZON_ISSUED",
			wantPendingFirst: true,
		},
		{
			name:             "email_validation_pending",
			domain:           "email.example.com",
			validationMethod: "EMAIL",
			wantDomain:       "email.example.com",
			wantStatus:       "PENDING_VALIDATION",
			wantType:         "AMAZON_ISSUED",
			wantPendingFirst: true,
		},
		{
			name:    "empty_domain",
			domain:  "",
			wantErr: acm.ErrInvalidParameter,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := acm.NewInMemoryBackend("000000000000", "us-east-1")
			cert, err := b.RequestCertificate(tt.domain, "", tt.validationMethod, nil)

			if tt.wantErr != nil {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
			assert.Contains(t, cert.ARN, "arn:aws:acm:")
			assert.Equal(t, tt.wantDomain, cert.DomainName)
			assert.Equal(t, tt.wantStatus, cert.Status)
			assert.Equal(t, tt.wantType, cert.Type)
			assert.NotEmpty(t, cert.CertificateBody, "CertificateBody should be set")

			if tt.wantPendingFirst {
				// Wait for auto-validation
				require.Eventually(t, func() bool {
					c, descErr := b.DescribeCertificate(cert.ARN)

					return descErr == nil && c.Status == "ISSUED"
				}, 2*time.Second, 50*time.Millisecond, "certificate should transition to ISSUED")
			}
		})
	}
}

func TestACMBackend_RequestCertificate_WithSANs(t *testing.T) {
	t.Parallel()

	b := acm.NewInMemoryBackend("000000000000", "us-east-1")
	cert, err := b.RequestCertificate("example.com", "", "DNS", []string{"www.example.com", "api.example.com"})
	require.NoError(t, err)

	assert.Equal(t, "example.com", cert.DomainName)
	assert.Equal(t, []string{"www.example.com", "api.example.com"}, cert.SubjectAlternativeNames)
	assert.Len(t, cert.DomainValidationOptions, 3, "should have DVOs for primary + 2 SANs")
}

func TestACMBackend_RequestCertificate_DNSValidationOptions(t *testing.T) {
	t.Parallel()

	b := acm.NewInMemoryBackend("000000000000", "us-east-1")
	cert, err := b.RequestCertificate("example.com", "", "DNS", nil)
	require.NoError(t, err)

	require.Len(t, cert.DomainValidationOptions, 1)
	dvo := cert.DomainValidationOptions[0]
	assert.Equal(t, "example.com", dvo.DomainName)
	assert.Equal(t, "PENDING_VALIDATION", dvo.ValidationStatus)
	assert.Equal(t, "DNS", dvo.ValidationMethod)
	require.NotNil(t, dvo.ResourceRecord)
	assert.Equal(t, "CNAME", dvo.ResourceRecord.Type)
	assert.Contains(t, dvo.ResourceRecord.Name, "example.com")
	assert.Contains(t, dvo.ResourceRecord.Value, "acm-validations.aws")
}

func TestACMBackend_DescribeCertificate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr error
		name    string
		arn     string
	}{
		{
			name:    "not_found",
			arn:     "arn:aws:acm:us-east-1:000000000000:certificate/nonexistent",
			wantErr: acm.ErrCertNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := acm.NewInMemoryBackend("000000000000", "us-east-1")
			_, err := b.DescribeCertificate(tt.arn)

			if tt.wantErr != nil {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestACMBackend_DeleteCertificate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr error
		setup   func(t *testing.T, b *acm.InMemoryBackend) string
		name    string
	}{
		{
			name: "success",
			setup: func(t *testing.T, b *acm.InMemoryBackend) string {
				t.Helper()
				cert, err := b.RequestCertificate("delete-me.com", "", "", nil)
				require.NoError(t, err)

				return cert.ARN
			},
		},
		{
			name:    "not_found",
			setup:   func(*testing.T, *acm.InMemoryBackend) string { return "nonexistent-arn" },
			wantErr: acm.ErrCertNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := acm.NewInMemoryBackend("000000000000", "us-east-1")
			arn := tt.setup(t, b)
			err := b.DeleteCertificate(arn)

			if tt.wantErr != nil {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestACMBackend_ListCertificates(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup     func(t *testing.T, b *acm.InMemoryBackend)
		name      string
		wantCount int
	}{
		{
			name:      "empty",
			wantCount: 0,
		},
		{
			name: "two_certs",
			setup: func(t *testing.T, b *acm.InMemoryBackend) {
				t.Helper()
				_, err := b.RequestCertificate("a.com", "", "", nil)
				require.NoError(t, err)
				_, err = b.RequestCertificate("b.com", "", "", nil)
				require.NoError(t, err)
			},
			wantCount: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := acm.NewInMemoryBackend("000000000000", "us-east-1")
			if tt.setup != nil {
				tt.setup(t, b)
			}

			certs := b.ListCertificates("", 0).Data
			assert.Len(t, certs, tt.wantCount)
		})
	}
}

func TestACMBackend_ImportCertificate(t *testing.T) {
	t.Parallel()

	certPEM, keyPEM := generateTestCert(t)

	tests := []struct {
		wantErr    error
		name       string
		certBody   string
		privateKey string
		certChain  string
		wantType   string
		wantStatus string
	}{
		{
			name:       "success",
			certBody:   certPEM,
			privateKey: keyPEM,
			wantType:   "IMPORTED",
			wantStatus: "ISSUED",
		},
		{
			name:       "with_chain",
			certBody:   certPEM,
			privateKey: keyPEM,
			certChain:  certPEM,
			wantType:   "IMPORTED",
			wantStatus: "ISSUED",
		},
		{
			name:       "missing_cert",
			certBody:   "",
			privateKey: keyPEM,
			wantErr:    acm.ErrInvalidParameter,
		},
		{
			name:       "missing_key",
			certBody:   certPEM,
			privateKey: "",
			wantErr:    acm.ErrInvalidParameter,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := acm.NewInMemoryBackend("000000000000", "us-east-1")
			cert, err := b.ImportCertificate(tt.certBody, tt.privateKey, tt.certChain)

			if tt.wantErr != nil {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
			assert.Contains(t, cert.ARN, "arn:aws:acm:")
			assert.Equal(t, tt.wantType, cert.Type)
			assert.Equal(t, tt.wantStatus, cert.Status)
			assert.Equal(t, tt.certBody, cert.CertificateBody)
			assert.Equal(t, tt.privateKey, cert.PrivateKey)
			assert.Equal(t, tt.certChain, cert.CertificateChain)
		})
	}
}

func TestACMBackend_RenewCertificate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr error
		setup   func(t *testing.T, b *acm.InMemoryBackend) string
		name    string
	}{
		{
			name: "success",
			setup: func(t *testing.T, b *acm.InMemoryBackend) string {
				t.Helper()
				cert, err := b.RequestCertificate("renew.example.com", "", "", nil)
				require.NoError(t, err)

				return cert.ARN
			},
		},
		{
			name:    "not_found",
			setup:   func(*testing.T, *acm.InMemoryBackend) string { return "nonexistent-arn" },
			wantErr: acm.ErrCertNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := acm.NewInMemoryBackend("000000000000", "us-east-1")
			certARN := tt.setup(t, b)
			err := b.RenewCertificate(certARN)

			if tt.wantErr != nil {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestACMBackend_ExportCertificate(t *testing.T) {
	t.Parallel()

	certPEM, keyPEM := generateTestCert(t)

	tests := []struct {
		wantErr error
		setup   func(t *testing.T, b *acm.InMemoryBackend) string
		name    string
	}{
		{
			name: "success_imported",
			setup: func(t *testing.T, b *acm.InMemoryBackend) string {
				t.Helper()
				cert, err := b.ImportCertificate(certPEM, keyPEM, "")
				require.NoError(t, err)

				return cert.ARN
			},
		},
		{
			name: "fails_amazon_issued",
			setup: func(t *testing.T, b *acm.InMemoryBackend) string {
				t.Helper()
				cert, err := b.RequestCertificate("amazon.example.com", "", "", nil)
				require.NoError(t, err)

				return cert.ARN
			},
			wantErr: acm.ErrNotEligible,
		},
		{
			name:    "not_found",
			setup:   func(*testing.T, *acm.InMemoryBackend) string { return "nonexistent-arn" },
			wantErr: acm.ErrCertNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := acm.NewInMemoryBackend("000000000000", "us-east-1")
			certARN := tt.setup(t, b)
			cert, err := b.ExportCertificate(certARN)

			if tt.wantErr != nil {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, certPEM, cert.CertificateBody)
			assert.Equal(t, keyPEM, cert.PrivateKey)
		})
	}
}

func TestACMBackend_GetCertificate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr error
		setup   func(t *testing.T, b *acm.InMemoryBackend) string
		name    string
	}{
		{
			name: "success_amazon_issued",
			setup: func(t *testing.T, b *acm.InMemoryBackend) string {
				t.Helper()
				cert, err := b.RequestCertificate("get.example.com", "", "", nil)
				require.NoError(t, err)

				return cert.ARN
			},
		},
		{
			name:    "not_found",
			setup:   func(*testing.T, *acm.InMemoryBackend) string { return "nonexistent-arn" },
			wantErr: acm.ErrCertNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := acm.NewInMemoryBackend("000000000000", "us-east-1")
			certARN := tt.setup(t, b)
			certBody, _, err := b.GetCertificate(certARN)

			if tt.wantErr != nil {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
			assert.NotEmpty(t, certBody)
			assert.Contains(t, certBody, "BEGIN CERTIFICATE")
		})
	}
}

// generateTestCert creates a test domain, generates a self-signed cert via the
// backend, and returns the certificate PEM and private key PEM.
func generateTestCert(t *testing.T) (string, string) {
	t.Helper()

	b := acm.NewInMemoryBackend("000000000000", "us-east-1")
	cert, err := b.RequestCertificate("test.example.com", "", "", nil)
	require.NoError(t, err)

	// Retrieve stored PEM data via GetCertificate
	certBody, _, getCertErr := b.GetCertificate(cert.ARN)
	require.NoError(t, getCertErr)
	require.NotEmpty(t, certBody)

	// Use cert body from describe to get PEM and key
	described, descErr := b.DescribeCertificate(cert.ARN)
	require.NoError(t, descErr)

	return described.CertificateBody, described.PrivateKey
}

// TestACMBackend_AutoValidation verifies the DNS validation auto-transition.
func TestACMBackend_AutoValidation(t *testing.T) {
	t.Parallel()

	b := acm.NewInMemoryBackend("000000000000", "us-east-1")
	cert, err := b.RequestCertificate("auto.example.com", "", "DNS", nil)
	require.NoError(t, err)
	assert.Equal(t, "PENDING_VALIDATION", cert.Status)

	// Wait for auto-validation (should happen within 500ms)
	require.Eventually(t, func() bool {
		c, descErr := b.DescribeCertificate(cert.ARN)
		if descErr != nil {
			return false
		}

		if c.Status != "ISSUED" {
			return false
		}

		for _, dvo := range c.DomainValidationOptions {
			if dvo.ValidationStatus != "SUCCESS" {
				return false
			}
		}

		return true
	}, 2*time.Second, 50*time.Millisecond)
}

// TestACMBackend_CertificateBodyIsPEM verifies the generated cert body is valid PEM.
func TestACMBackend_CertificateBodyIsPEM(t *testing.T) {
	t.Parallel()

	b := acm.NewInMemoryBackend("000000000000", "us-east-1")
	cert, err := b.RequestCertificate("pem.example.com", "", "", nil)
	require.NoError(t, err)

	assert.True(t, strings.HasPrefix(cert.CertificateBody, "-----BEGIN CERTIFICATE-----"))
	assert.True(t, strings.HasPrefix(cert.PrivateKey, "-----BEGIN EC PRIVATE KEY-----"))
}
