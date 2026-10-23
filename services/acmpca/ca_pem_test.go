package acmpca_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/acmpca"
)

func TestCertificatePEMForARN(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		caType  string
		arn     string
		wantErr bool
	}{
		{name: "root_ca", caType: "ROOT"},
		{name: "subordinate_without_certificate", caType: "SUBORDINATE", wantErr: true},
		{name: "unknown_arn", arn: "arn:aws:acm-pca:us-east-1:000000000000:certificate-authority/nope", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newACMPCAHandler()
			arn := tt.arn

			if tt.caType != "" {
				ca, err := h.Backend.CreateCertificateAuthority(context.Background(), tt.caType,
					acmpca.CertificateAuthorityConfiguration{
						Subject: acmpca.CertificateAuthoritySubject{CommonName: "Test CA"},
					})
				require.NoError(t, err)

				arn = ca.ARN
			}

			pem, err := h.Backend.CertificatePEMForARN(t.Context(), arn)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Contains(t, string(pem), "BEGIN CERTIFICATE")
		})
	}
}
