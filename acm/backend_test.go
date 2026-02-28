package acm_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/acm"
)

func TestACMBackend(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		run  func(t *testing.T, b *acm.InMemoryBackend)
	}{
		{
			name: "RequestCertificate/success",
			run: func(t *testing.T, b *acm.InMemoryBackend) {
				cert, err := b.RequestCertificate("example.com", "")
				require.NoError(t, err)
				assert.Contains(t, cert.ARN, "arn:aws:acm:")
				assert.Equal(t, "example.com", cert.DomainName)
				assert.Equal(t, "ISSUED", cert.Status)
				assert.Equal(t, "AMAZON_ISSUED", cert.Type)
			},
		},
		{
			name: "RequestCertificate/empty_domain",
			run: func(t *testing.T, b *acm.InMemoryBackend) {
				_, err := b.RequestCertificate("", "")
				require.Error(t, err)
				assert.ErrorIs(t, err, acm.ErrInvalidParameter)
			},
		},
		{
			name: "DescribeCertificate/not_found",
			run: func(t *testing.T, b *acm.InMemoryBackend) {
				_, err := b.DescribeCertificate("arn:aws:acm:us-east-1:000000000000:certificate/nonexistent")
				require.Error(t, err)
				assert.ErrorIs(t, err, acm.ErrCertNotFound)
			},
		},
		{
			name: "DeleteCertificate/success",
			run: func(t *testing.T, b *acm.InMemoryBackend) {
				cert, err := b.RequestCertificate("delete-me.com", "")
				require.NoError(t, err)

				err = b.DeleteCertificate(cert.ARN)
				require.NoError(t, err)

				_, err = b.DescribeCertificate(cert.ARN)
				require.Error(t, err)
				assert.ErrorIs(t, err, acm.ErrCertNotFound)
			},
		},
		{
			name: "ListCertificates/success",
			run: func(t *testing.T, b *acm.InMemoryBackend) {
				_, err := b.RequestCertificate("a.com", "")
				require.NoError(t, err)
				_, err = b.RequestCertificate("b.com", "")
				require.NoError(t, err)

				certs := b.ListCertificates()
				assert.Len(t, certs, 2)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := acm.NewInMemoryBackend("000000000000", "us-east-1")
			tt.run(t, b)
		})
	}
}
