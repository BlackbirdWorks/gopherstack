package acmpca_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/acmpca"
)

const (
	testAccountID = "000000000000"
	testRegion    = "us-east-1"
)

func newTestBackend() *acmpca.InMemoryBackend {
	return acmpca.NewInMemoryBackend(testAccountID, testRegion)
}

func TestInMemoryBackend_CreateCertificateAuthority(t *testing.T) {
	t.Parallel()

	tests := []struct {
		cfg        acmpca.CertificateAuthorityConfiguration
		name       string
		caType     string
		wantStatus string
		wantErr    bool
	}{
		{
			name:   "root CA defaults",
			caType: "ROOT",
			cfg: acmpca.CertificateAuthorityConfiguration{
				Subject:          acmpca.CertificateAuthoritySubject{CommonName: "Test Root CA"},
				KeyAlgorithm:     "EC_prime256v1",
				SigningAlgorithm: "SHA256WITHECDSA",
			},
			wantStatus: "ACTIVE",
		},
		{
			name:   "subordinate CA stays creating",
			caType: "SUBORDINATE",
			cfg: acmpca.CertificateAuthorityConfiguration{
				Subject: acmpca.CertificateAuthoritySubject{CommonName: "Sub CA"},
			},
			wantStatus: "CREATING",
		},
		{
			name:    "invalid type",
			caType:  "INVALID",
			wantErr: true,
		},
		{
			name:   "empty type defaults to ROOT",
			caType: "",
			cfg: acmpca.CertificateAuthorityConfiguration{
				Subject: acmpca.CertificateAuthoritySubject{CommonName: "Default Root"},
			},
			wantStatus: "ACTIVE",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			ca, err := b.CreateCertificateAuthority(tt.caType, tt.cfg)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.NotEmpty(t, ca.ARN)
			assert.Equal(t, tt.wantStatus, ca.Status)
		})
	}
}

func TestInMemoryBackend_DescribeCertificateAuthority(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		caARN   string
		wantErr bool
	}{
		{
			name:    "existing CA",
			caARN:   "",
			wantErr: false,
		},
		{
			name:    "non-existent CA",
			caARN:   "arn:aws:acm-pca:us-east-1:000000000000:certificate-authority/nonexistent",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			var caARN string

			if tt.caARN == "" {
				ca, err := b.CreateCertificateAuthority("ROOT", acmpca.CertificateAuthorityConfiguration{
					Subject: acmpca.CertificateAuthoritySubject{CommonName: "Test CA"},
				})
				require.NoError(t, err)
				caARN = ca.ARN
			} else {
				caARN = tt.caARN
			}

			ca, err := b.DescribeCertificateAuthority(caARN)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, caARN, ca.ARN)
		})
	}
}

func TestInMemoryBackend_ListCertificateAuthorities(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		createN   int
		wantCount int
	}{
		{
			name:      "empty list",
			createN:   0,
			wantCount: 0,
		},
		{
			name:      "two CAs",
			createN:   2,
			wantCount: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()

			for i := range tt.createN {
				_, err := b.CreateCertificateAuthority("ROOT", acmpca.CertificateAuthorityConfiguration{
					Subject: acmpca.CertificateAuthoritySubject{CommonName: "CA"},
				})
				require.NoError(t, err, "creating CA %d", i)
			}

			p := b.ListCertificateAuthorities("", 0)
			assert.Len(t, p.Data, tt.wantCount)
		})
	}
}

func TestInMemoryBackend_DeleteCertificateAuthority(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		caARN   string
		wantErr bool
	}{
		{
			name:    "existing CA",
			caARN:   "",
			wantErr: false,
		},
		{
			name:    "non-existent CA",
			caARN:   "arn:aws:acm-pca:us-east-1:000000000000:certificate-authority/nonexistent",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			var caARN string

			if tt.caARN == "" {
				ca, err := b.CreateCertificateAuthority("ROOT", acmpca.CertificateAuthorityConfiguration{
					Subject: acmpca.CertificateAuthoritySubject{CommonName: "Test CA"},
				})
				require.NoError(t, err)
				caARN = ca.ARN
			} else {
				caARN = tt.caARN
			}

			err := b.DeleteCertificateAuthority(caARN)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)

			ca, err := b.DescribeCertificateAuthority(caARN)
			require.NoError(t, err)
			assert.Equal(t, "DELETED", ca.Status)
		})
	}
}

func TestInMemoryBackend_GetCertificateAuthorityCsr(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		wantErr bool
	}{
		{
			name:    "existing CA",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			ca, err := b.CreateCertificateAuthority("SUBORDINATE", acmpca.CertificateAuthorityConfiguration{
				Subject: acmpca.CertificateAuthoritySubject{CommonName: "Sub CA"},
			})
			require.NoError(t, err)

			csr, err := b.GetCertificateAuthorityCsr(ca.ARN)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Contains(t, csr, "CERTIFICATE REQUEST")
		})
	}
}

func TestInMemoryBackend_IssueCertificate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		validityDays int
		wantErr      bool
	}{
		{
			name:         "issue cert with default validity",
			validityDays: 0,
			wantErr:      false,
		},
		{
			name:         "issue cert with explicit validity",
			validityDays: 90,
			wantErr:      false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			ca, err := b.CreateCertificateAuthority("ROOT", acmpca.CertificateAuthorityConfiguration{
				Subject: acmpca.CertificateAuthoritySubject{CommonName: "Test Root CA"},
			})
			require.NoError(t, err)

			// Get the CA's CSR as the cert to issue (for simplicity we reuse the self-signed CA cert's pub key)
			subCA, err := b.CreateCertificateAuthority("SUBORDINATE", acmpca.CertificateAuthorityConfiguration{
				Subject: acmpca.CertificateAuthoritySubject{CommonName: "Test Sub CA"},
			})
			require.NoError(t, err)

			csr, err := b.GetCertificateAuthorityCsr(subCA.ARN)
			require.NoError(t, err)

			cert, err := b.IssueCertificate(ca.ARN, csr, tt.validityDays)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.NotEmpty(t, cert.ARN)
			assert.NotEmpty(t, cert.Serial)
			assert.NotEmpty(t, cert.CertBody)
		})
	}
}

func TestInMemoryBackend_TagOperations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		tags     map[string]string
		name     string
		wantTags int
	}{
		{
			name:     "add and list tags",
			tags:     map[string]string{"env": "test", "team": "platform"},
			wantTags: 2,
		},
		{
			name:     "no tags",
			tags:     map[string]string{},
			wantTags: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			ca, err := b.CreateCertificateAuthority("ROOT", acmpca.CertificateAuthorityConfiguration{
				Subject: acmpca.CertificateAuthoritySubject{CommonName: "Tag CA"},
			})
			require.NoError(t, err)

			h := acmpca.NewHandler(b)

			if len(tt.tags) > 0 {
				h.SetTagsForTest(ca.ARN, tt.tags)
			}

			got := h.GetTagsForTest(ca.ARN)
			assert.Len(t, got, tt.wantTags)
		})
	}
}
