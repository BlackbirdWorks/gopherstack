package acm_test

import (
	"context"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/acm"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
				_, err := b.RequestCertificate(context.Background(), "a.com", "", "", "", "", "", "", nil)
				require.NoError(t, err)
				_, err = b.RequestCertificate(context.Background(), "b.com", "", "", "", "", "", "", nil)
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

			p, _ := b.ListCertificates(context.Background(), acm.ListCertificatesParams{})
			certs := p.Data
			assert.Len(t, certs, tt.wantCount)
		})
	}
}

// TestACMBackend_ListCertificates_KeyUsageFilter verifies Includes.KeyUsage filtering.
func TestACMBackend_ListCertificates_KeyUsageFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		filterKeyUsage []string
		wantCount      int
		wantNonEmpty   bool
	}{
		{
			name:           "filter_digital_signature_matches",
			filterKeyUsage: []string{"DIGITAL_SIGNATURE"},
			wantNonEmpty:   true,
		},
		{
			name:           "filter_nonexistent_usage_no_match",
			filterKeyUsage: []string{"KEY_ENCIPHERMENT"},
			wantCount:      0,
		},
		{
			name:           "no_filter_returns_all",
			filterKeyUsage: nil,
			wantNonEmpty:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := acm.NewInMemoryBackend("000000000000", "us-east-1")
			_, err := b.RequestCertificate(context.Background(), "ku.example.com", "", "", "", "", "", "", nil)
			require.NoError(t, err)

			result, _ := b.ListCertificates(
				context.Background(),
				acm.ListCertificatesParams{KeyUsage: tt.filterKeyUsage},
			)

			if tt.wantNonEmpty {
				assert.NotEmpty(t, result.Data)
			} else {
				assert.Len(t, result.Data, tt.wantCount)
			}
		})
	}
}

// TestACMBackend_ListCertificates_ExtendedKeyUsageFilter verifies Includes.ExtendedKeyUsage filtering.
func TestACMBackend_ListCertificates_ExtendedKeyUsageFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		filterExtKeyUsage []string
		wantCount         int
		wantNonEmpty      bool
	}{
		{
			name:              "filter_tls_server_auth_matches",
			filterExtKeyUsage: []string{"TLS_WEB_SERVER_AUTHENTICATION"},
			wantNonEmpty:      true,
		},
		{
			name:              "filter_code_signing_no_match",
			filterExtKeyUsage: []string{"CODE_SIGNING"},
			wantCount:         0,
		},
		{
			name:              "no_filter_returns_all",
			filterExtKeyUsage: nil,
			wantNonEmpty:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := acm.NewInMemoryBackend("000000000000", "us-east-1")
			_, err := b.RequestCertificate(context.Background(), "eku.example.com", "", "", "", "", "", "", nil)
			require.NoError(t, err)

			result, _ := b.ListCertificates(
				context.Background(),
				acm.ListCertificatesParams{ExtendedKeyUsage: tt.filterExtKeyUsage},
			)

			if tt.wantNonEmpty {
				assert.NotEmpty(t, result.Data)
			} else {
				assert.Len(t, result.Data, tt.wantCount)
			}
		})
	}
}
