package acmpca_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/acmpca"
)

func TestInMemoryBackend_SnapshotRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(b *acmpca.InMemoryBackend) string
		verify func(t *testing.T, b *acmpca.InMemoryBackend, id string)
		name   string
	}{
		{
			name: "root_ca_round_trip",
			setup: func(b *acmpca.InMemoryBackend) string {
				ca, err := b.CreateCertificateAuthority(
					context.Background(),
					"ROOT",
					acmpca.CertificateAuthorityConfiguration{
						Subject: acmpca.CertificateAuthoritySubject{CommonName: "Test CA"},
					},
				)
				if err != nil {
					return ""
				}

				return ca.ARN
			},
			verify: func(t *testing.T, b *acmpca.InMemoryBackend, id string) {
				t.Helper()

				ca, err := b.DescribeCertificateAuthority(context.Background(), id)
				require.NoError(t, err)
				assert.Equal(t, "ACTIVE", ca.Status)
				assert.Equal(t, "ROOT", ca.Type)
			},
		},
		{
			name: "issued_cert_round_trip",
			setup: func(b *acmpca.InMemoryBackend) string {
				ca, err := b.CreateCertificateAuthority(
					context.Background(),
					"ROOT",
					acmpca.CertificateAuthorityConfiguration{
						Subject: acmpca.CertificateAuthoritySubject{CommonName: "Test CA"},
					},
				)
				if err != nil {
					return ""
				}

				csr, err := b.GetCertificateAuthorityCsr(context.Background(), ca.ARN)
				if err != nil {
					return ""
				}

				cert, err := b.IssueCertificate(context.Background(), ca.ARN, csr, 365)
				if err != nil {
					return ""
				}

				return cert.ARN
			},
			verify: func(t *testing.T, b *acmpca.InMemoryBackend, _ string) {
				t.Helper()

				// IssuedCertificate ARN contains the CA ARN as a prefix
				// Find the cert by listing all CAs first
				cas := b.ListCertificateAuthorities(context.Background(), "", 0).Data
				require.NotEmpty(t, cas)

				certs := b.ListCertificates(context.Background(), cas[0].ARN, "", 0).Data
				require.NotEmpty(t, certs, "issued certificate should be restored")
			},
		},
		{
			name:  "empty_backend_round_trip",
			setup: func(_ *acmpca.InMemoryBackend) string { return "" },
			verify: func(t *testing.T, b *acmpca.InMemoryBackend, _ string) {
				t.Helper()

				cas := b.ListCertificateAuthorities(context.Background(), "", 0).Data
				assert.Empty(t, cas)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			original := acmpca.NewInMemoryBackend(testAccountID, testRegion)
			id := tt.setup(original)

			snap := original.Snapshot()
			require.NotNil(t, snap)

			fresh := acmpca.NewInMemoryBackend(testAccountID, testRegion)
			require.NoError(t, fresh.Restore(snap))

			tt.verify(t, fresh, id)
		})
	}
}

func TestInMemoryBackend_RestoreInvalidData(t *testing.T) {
	t.Parallel()

	b := acmpca.NewInMemoryBackend(testAccountID, testRegion)
	err := b.Restore([]byte("not-valid-json"))
	require.Error(t, err)
}

func TestInMemoryBackend_GetCertificate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		wantErr bool
	}{
		{name: "existing_cert", wantErr: false},
		{name: "non_existent_cert", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := acmpca.NewInMemoryBackend(testAccountID, testRegion)

			ca, err := b.CreateCertificateAuthority(
				context.Background(),
				"ROOT",
				acmpca.CertificateAuthorityConfiguration{
					Subject: acmpca.CertificateAuthoritySubject{CommonName: "Test CA"},
				},
			)
			require.NoError(t, err)

			csr, err := b.GetCertificateAuthorityCsr(context.Background(), ca.ARN)
			require.NoError(t, err)

			issuedCert, err := b.IssueCertificate(context.Background(), ca.ARN, csr, 365)
			require.NoError(t, err)

			if tt.wantErr {
				_, err = b.GetCertificate(context.Background(), ca.ARN, "nonexistent-arn")
				require.Error(t, err)
			} else {
				var cert *acmpca.IssuedCertificate
				cert, err = b.GetCertificate(context.Background(), ca.ARN, issuedCert.ARN)
				require.NoError(t, err)
				assert.Equal(t, issuedCert.ARN, cert.ARN)
				assert.Equal(t, ca.ARN, cert.CAARN)
			}
		})
	}
}

func TestInMemoryBackend_RevokeCertificate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		serial  string
		wantErr bool
	}{
		{name: "revoke_existing", wantErr: false},
		{name: "revoke_nonexistent_serial", serial: "badserial", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := acmpca.NewInMemoryBackend(testAccountID, testRegion)

			ca, err := b.CreateCertificateAuthority(
				context.Background(),
				"ROOT",
				acmpca.CertificateAuthorityConfiguration{
					Subject: acmpca.CertificateAuthoritySubject{CommonName: "Test CA"},
				},
			)
			require.NoError(t, err)

			csr, err := b.GetCertificateAuthorityCsr(context.Background(), ca.ARN)
			require.NoError(t, err)

			cert, err := b.IssueCertificate(context.Background(), ca.ARN, csr, 365)
			require.NoError(t, err)

			serial := tt.serial
			if serial == "" {
				serial = cert.Serial
			}

			err = b.RevokeCertificate(context.Background(), ca.ARN, serial, "KEY_COMPROMISE")

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)

				var got *acmpca.IssuedCertificate
				got, err = b.GetCertificate(context.Background(), ca.ARN, cert.ARN)
				require.NoError(t, err)
				assert.Equal(t, "REVOKED", got.Status)
			}
		})
	}
}

func TestInMemoryBackend_ListCertificates(t *testing.T) {
	t.Parallel()

	b := acmpca.NewInMemoryBackend(testAccountID, testRegion)

	ca, err := b.CreateCertificateAuthority(context.Background(), "ROOT", acmpca.CertificateAuthorityConfiguration{
		Subject: acmpca.CertificateAuthoritySubject{CommonName: "Test CA"},
	})
	require.NoError(t, err)

	csr, err := b.GetCertificateAuthorityCsr(context.Background(), ca.ARN)
	require.NoError(t, err)

	_, err = b.IssueCertificate(context.Background(), ca.ARN, csr, 365)
	require.NoError(t, err)

	certs := b.ListCertificates(context.Background(), ca.ARN, "", 0).Data
	assert.Len(t, certs, 1)

	// Non-existent CA returns empty list.
	empty := b.ListCertificates(context.Background(), "nonexistent", "", 0).Data
	assert.Empty(t, empty)
}

func TestInMemoryBackend_UpdateCertificateAuthority(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		caARN   string
		status  string
		wantErr bool
	}{
		{name: "disable_existing", status: "DISABLED", wantErr: false},
		{name: "update_nonexistent", caARN: "arn:nonexistent", status: "DISABLED", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := acmpca.NewInMemoryBackend(testAccountID, testRegion)

			ca, err := b.CreateCertificateAuthority(
				context.Background(),
				"ROOT",
				acmpca.CertificateAuthorityConfiguration{
					Subject: acmpca.CertificateAuthoritySubject{CommonName: "Test CA"},
				},
			)
			require.NoError(t, err)

			caARN := tt.caARN
			if caARN == "" {
				caARN = ca.ARN
			}

			err = b.UpdateCertificateAuthority(context.Background(), caARN, tt.status)

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)

				var got *acmpca.CertificateAuthority
				got, err = b.DescribeCertificateAuthority(context.Background(), caARN)
				require.NoError(t, err)
				assert.Equal(t, tt.status, got.Status)
			}
		})
	}
}

func TestInMemoryBackend_ImportCertificateAuthorityCertificate(t *testing.T) {
	t.Parallel()

	b := acmpca.NewInMemoryBackend(testAccountID, testRegion)

	// For a ROOT CA, self-sign is automatic. Test that GetCertificateAuthorityCertificate works.
	ca, err := b.CreateCertificateAuthority(context.Background(), "ROOT", acmpca.CertificateAuthorityConfiguration{
		Subject: acmpca.CertificateAuthoritySubject{CommonName: "Root CA"},
	})
	require.NoError(t, err)

	certPEM, chainPEM, err := b.GetCertificateAuthorityCertificate(context.Background(), ca.ARN)
	require.NoError(t, err)
	assert.NotEmpty(t, certPEM)
	assert.Empty(t, chainPEM) // Root CA has no chain
}

func TestInMemoryBackend_Region(t *testing.T) {
	t.Parallel()

	b := acmpca.NewInMemoryBackend(testAccountID, testRegion)
	assert.Equal(t, testRegion, b.Region())
}

func TestACMPCAHandler_Persistence(t *testing.T) {
	t.Parallel()

	backend := acmpca.NewInMemoryBackend(testAccountID, testRegion)
	h := acmpca.NewHandler(backend)

	_, err := backend.CreateCertificateAuthority(context.Background(), "ROOT", acmpca.CertificateAuthorityConfiguration{
		Subject: acmpca.CertificateAuthoritySubject{CommonName: "Test CA"},
	})
	require.NoError(t, err)

	snap := h.Snapshot()
	require.NotNil(t, snap)

	fresh := acmpca.NewInMemoryBackend(testAccountID, testRegion)
	freshH := acmpca.NewHandler(fresh)
	require.NoError(t, freshH.Restore(snap))

	cas := fresh.ListCertificateAuthorities(context.Background(), "", 0).Data
	assert.Len(t, cas, 1)
}

func TestInMemoryBackend_SnapshotRestore_AdditionalState(t *testing.T) {
	t.Parallel()

	tests := []struct {
		verify func(t *testing.T, b *acmpca.InMemoryBackend, caARN, reportID string)
		name   string
	}{
		{
			name: "permissions_policies_and_reports",
			verify: func(t *testing.T, b *acmpca.InMemoryBackend, caARN, reportID string) {
				t.Helper()

				perms, err := b.ListPermissions(context.Background(), caARN, "", 0)
				require.NoError(t, err)
				require.Len(t, perms.Data, 1)
				assert.Equal(t, "acm.amazonaws.com", perms.Data[0].Principal)

				policy, err := b.GetPolicy(context.Background(), caARN)
				require.NoError(t, err)
				assert.JSONEq(t, `{"Version":"2012-10-17","Statement":[]}`, policy)

				report, err := b.DescribeCertificateAuthorityAuditReport(context.Background(), caARN, reportID)
				require.NoError(t, err)
				assert.Equal(t, "audit-bucket", report.S3BucketName)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			original := acmpca.NewInMemoryBackend(testAccountID, testRegion)
			ca, err := original.CreateCertificateAuthority(
				context.Background(),
				"ROOT",
				acmpca.CertificateAuthorityConfiguration{
					Subject: acmpca.CertificateAuthoritySubject{CommonName: "Persist CA"},
				},
			)
			require.NoError(t, err)

			_, err = original.CreatePermission(
				context.Background(),
				ca.ARN,
				"acm.amazonaws.com",
				testAccountID,
				[]string{"IssueCertificate"},
			)
			require.NoError(t, err)
			require.NoError(
				t,
				original.PutPolicy(context.Background(), ca.ARN, `{"Version":"2012-10-17","Statement":[]}`),
			)

			report, err := original.CreateCertificateAuthorityAuditReport(
				context.Background(),
				ca.ARN,
				"audit-bucket",
				"JSON",
			)
			require.NoError(t, err)

			fresh := acmpca.NewInMemoryBackend(testAccountID, testRegion)
			require.NoError(t, fresh.Restore(original.Snapshot()))

			tt.verify(t, fresh, ca.ARN, report.AuditReportID)
		})
	}
}
