package acm_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/acm"
)

func TestACMBackend_RequestCertificate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		domain     string
		wantErr    error
		wantDomain string
		wantStatus string
		wantType   string
	}{
		{
			name:       "success",
			domain:     "example.com",
			wantDomain: "example.com",
			wantStatus: "ISSUED",
			wantType:   "AMAZON_ISSUED",
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
			cert, err := b.RequestCertificate(tt.domain, "")

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
		})
	}
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
				cert, err := b.RequestCertificate("delete-me.com", "")
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
				_, err := b.RequestCertificate("a.com", "")
				require.NoError(t, err)
				_, err = b.RequestCertificate("b.com", "")
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
