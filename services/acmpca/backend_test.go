package acmpca_test

import (
	"context"
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
			name:   "subordinate CA starts pending certificate",
			caType: "SUBORDINATE",
			cfg: acmpca.CertificateAuthorityConfiguration{
				Subject: acmpca.CertificateAuthoritySubject{CommonName: "Sub CA"},
			},
			wantStatus: "PENDING_CERTIFICATE",
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
			ca, err := b.CreateCertificateAuthority(context.Background(), tt.caType, tt.cfg)

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
				ca, err := b.CreateCertificateAuthority(
					context.Background(),
					"ROOT",
					acmpca.CertificateAuthorityConfiguration{
						Subject: acmpca.CertificateAuthoritySubject{CommonName: "Test CA"},
					},
				)
				require.NoError(t, err)
				caARN = ca.ARN
			} else {
				caARN = tt.caARN
			}

			ca, err := b.DescribeCertificateAuthority(context.Background(), caARN)

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
				_, err := b.CreateCertificateAuthority(
					context.Background(),
					"ROOT",
					acmpca.CertificateAuthorityConfiguration{
						Subject: acmpca.CertificateAuthoritySubject{CommonName: "CA"},
					},
				)
				require.NoError(t, err, "creating CA %d", i)
			}

			p := b.ListCertificateAuthorities(context.Background(), "", 0)
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
			name:    "existing CA after disable",
			caARN:   "",
			wantErr: false,
		},
		{
			name:    "non-existent CA",
			caARN:   "arn:aws:acm-pca:us-east-1:000000000000:certificate-authority/nonexistent",
			wantErr: true,
		},
		{
			name:    "active CA without disabling first",
			caARN:   "active",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			var caARN string

			switch tt.caARN {
			case "":
				ca, err := b.CreateCertificateAuthority(
					context.Background(),
					"ROOT",
					acmpca.CertificateAuthorityConfiguration{
						Subject: acmpca.CertificateAuthoritySubject{CommonName: "Test CA"},
					},
				)
				require.NoError(t, err)
				caARN = ca.ARN
				// Disable the CA first (AWS requirement before deletion).
				require.NoError(t, b.UpdateCertificateAuthority(context.Background(), caARN, "DISABLED"))
			case "active":
				ca, err := b.CreateCertificateAuthority(
					context.Background(),
					"ROOT",
					acmpca.CertificateAuthorityConfiguration{
						Subject: acmpca.CertificateAuthoritySubject{CommonName: "Active CA"},
					},
				)
				require.NoError(t, err)
				caARN = ca.ARN
				// Do NOT disable — deletion should fail.
			default:
				caARN = tt.caARN
			}

			err := b.DeleteCertificateAuthority(context.Background(), caARN, 0)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)

			ca, err := b.DescribeCertificateAuthority(context.Background(), caARN)
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
			ca, err := b.CreateCertificateAuthority(
				context.Background(),
				"SUBORDINATE",
				acmpca.CertificateAuthorityConfiguration{
					Subject: acmpca.CertificateAuthoritySubject{CommonName: "Sub CA"},
				},
			)
			require.NoError(t, err)

			csr, err := b.GetCertificateAuthorityCsr(context.Background(), ca.ARN)

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
			ca, err := b.CreateCertificateAuthority(
				context.Background(),
				"ROOT",
				acmpca.CertificateAuthorityConfiguration{
					Subject: acmpca.CertificateAuthoritySubject{CommonName: "Test Root CA"},
				},
			)
			require.NoError(t, err)

			// Get the CA's CSR as the cert to issue (for simplicity we reuse the self-signed CA cert's pub key)
			subCA, err := b.CreateCertificateAuthority(
				context.Background(),
				"SUBORDINATE",
				acmpca.CertificateAuthorityConfiguration{
					Subject: acmpca.CertificateAuthoritySubject{CommonName: "Test Sub CA"},
				},
			)
			require.NoError(t, err)

			csr, err := b.GetCertificateAuthorityCsr(context.Background(), subCA.ARN)
			require.NoError(t, err)

			cert, err := b.IssueCertificate(context.Background(), ca.ARN, csr, tt.validityDays)

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
			ca, err := b.CreateCertificateAuthority(
				context.Background(),
				"ROOT",
				acmpca.CertificateAuthorityConfiguration{
					Subject: acmpca.CertificateAuthoritySubject{CommonName: "Tag CA"},
				},
			)
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

func TestInMemoryBackend_PermissionsAndPolicies(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, b *acmpca.InMemoryBackend, caARN string)
		name string
	}{
		{
			name: "permissions_crud",
			run: func(t *testing.T, b *acmpca.InMemoryBackend, caARN string) {
				t.Helper()

				created, err := b.CreatePermission(
					context.Background(),
					caARN,
					"acm.amazonaws.com",
					testAccountID,
					[]string{"IssueCertificate", "GetCertificate"},
				)
				require.NoError(t, err)
				assert.Equal(t, caARN, created.CertificateAuthorityArn)
				assert.Equal(t, "acm.amazonaws.com", created.Principal)

				list, err := b.ListPermissions(context.Background(), caARN, "", 0)
				require.NoError(t, err)
				require.Len(t, list.Data, 1)
				assert.Equal(t, []string{"IssueCertificate", "GetCertificate"}, list.Data[0].Actions)

				require.NoError(t, b.DeletePermission(context.Background(), caARN, "acm.amazonaws.com", testAccountID))
				list, err = b.ListPermissions(context.Background(), caARN, "", 0)
				require.NoError(t, err)
				assert.Empty(t, list.Data)
			},
		},
		{
			name: "policy_crud",
			run: func(t *testing.T, b *acmpca.InMemoryBackend, caARN string) {
				t.Helper()

				policy := `{"Version":"2012-10-17","Statement":[]}`
				require.NoError(t, b.PutPolicy(context.Background(), caARN, policy))

				got, err := b.GetPolicy(context.Background(), caARN)
				require.NoError(t, err)
				assert.Equal(t, policy, got)

				require.NoError(t, b.DeletePolicy(context.Background(), caARN))
				_, err = b.GetPolicy(context.Background(), caARN)
				require.Error(t, err)
			},
		},
		{
			name: "audit_report_and_restore",
			run: func(t *testing.T, b *acmpca.InMemoryBackend, caARN string) {
				t.Helper()

				report, err := b.CreateCertificateAuthorityAuditReport(context.Background(), caARN, "bucket", "JSON")
				require.NoError(t, err)
				assert.Equal(t, "SUCCESS", report.Status)
				assert.Contains(t, report.S3Key, ".json")

				got, err := b.DescribeCertificateAuthorityAuditReport(context.Background(), caARN, report.AuditReportID)
				require.NoError(t, err)
				assert.Equal(t, report.AuditReportID, got.AuditReportID)

				require.NoError(t, b.UpdateCertificateAuthority(context.Background(), caARN, "DISABLED"))
				require.NoError(t, b.DeleteCertificateAuthority(context.Background(), caARN, 0))
				require.NoError(t, b.RestoreCertificateAuthority(context.Background(), caARN))

				ca, err := b.DescribeCertificateAuthority(context.Background(), caARN)
				require.NoError(t, err)
				assert.Equal(t, "DISABLED", ca.Status)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			ca, err := b.CreateCertificateAuthority(
				context.Background(),
				"ROOT",
				acmpca.CertificateAuthorityConfiguration{
					Subject: acmpca.CertificateAuthoritySubject{CommonName: "Ops CA"},
				},
			)
			require.NoError(t, err)

			tt.run(t, b, ca.ARN)
		})
	}
}

func TestInMemoryBackend_NewOperationValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, b *acmpca.InMemoryBackend)
		name string
	}{
		{
			name: "create permission requires ca arn",
			run: func(t *testing.T, b *acmpca.InMemoryBackend) {
				t.Helper()

				_, err := b.CreatePermission(
					context.Background(),
					"",
					"acm.amazonaws.com",
					testAccountID,
					[]string{"IssueCertificate"},
				)
				require.ErrorIs(t, err, acmpca.ErrInvalidParameter)
			},
		},
		{
			name: "delete permission requires principal",
			run: func(t *testing.T, b *acmpca.InMemoryBackend) {
				t.Helper()

				err := b.DeletePermission(
					context.Background(),
					"arn:aws:acm-pca:us-east-1:123456789012:certificate-authority/test",
					"",
					testAccountID,
				)
				require.ErrorIs(t, err, acmpca.ErrInvalidParameter)
			},
		},
		{
			name: "list permissions requires existing ca",
			run: func(t *testing.T, b *acmpca.InMemoryBackend) {
				t.Helper()

				_, err := b.ListPermissions(
					context.Background(),
					"arn:aws:acm-pca:us-east-1:123456789012:certificate-authority/missing",
					"",
					0,
				)
				require.ErrorIs(t, err, acmpca.ErrCANotFound)
			},
		},
		{
			name: "audit report requires report id on describe",
			run: func(t *testing.T, b *acmpca.InMemoryBackend) {
				t.Helper()

				ca, err := b.CreateCertificateAuthority(
					context.Background(),
					"ROOT",
					acmpca.CertificateAuthorityConfiguration{
						Subject: acmpca.CertificateAuthoritySubject{CommonName: "Validate CA"},
					},
				)
				require.NoError(t, err)

				_, err = b.DescribeCertificateAuthorityAuditReport(context.Background(), ca.ARN, "")
				require.ErrorIs(t, err, acmpca.ErrInvalidParameter)
			},
		},
		{
			name: "policy requires resource arn",
			run: func(t *testing.T, b *acmpca.InMemoryBackend) {
				t.Helper()

				_, err := b.GetPolicy(context.Background(), "")
				require.ErrorIs(t, err, acmpca.ErrInvalidParameter)
			},
		},
		{
			name: "restore requires ca arn",
			run: func(t *testing.T, b *acmpca.InMemoryBackend) {
				t.Helper()

				err := b.RestoreCertificateAuthority(context.Background(), "")
				require.ErrorIs(t, err, acmpca.ErrInvalidParameter)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tt.run(t, newTestBackend())
		})
	}
}

func TestInMemoryBackend_ValidationAndRevocation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, b *acmpca.InMemoryBackend)
		name string
	}{
		{
			name: "revoke sets RevokedAt and RevocationReason",
			run: func(t *testing.T, b *acmpca.InMemoryBackend) {
				t.Helper()

				ca, err := b.CreateCertificateAuthority(
					context.Background(),
					"ROOT",
					acmpca.CertificateAuthorityConfiguration{
						Subject: acmpca.CertificateAuthoritySubject{CommonName: "Revoke CA"},
					},
				)
				require.NoError(t, err)

				subCA, err := b.CreateCertificateAuthority(
					context.Background(),
					"SUBORDINATE",
					acmpca.CertificateAuthorityConfiguration{
						Subject: acmpca.CertificateAuthoritySubject{CommonName: "Sub CA"},
					},
				)
				require.NoError(t, err)

				csr, err := b.GetCertificateAuthorityCsr(context.Background(), subCA.ARN)
				require.NoError(t, err)

				cert, err := b.IssueCertificate(context.Background(), ca.ARN, csr, 365)
				require.NoError(t, err)

				err = b.RevokeCertificate(context.Background(), ca.ARN, cert.Serial, "KEY_COMPROMISE")
				require.NoError(t, err)

				got, err := b.GetCertificate(context.Background(), ca.ARN, cert.ARN)
				require.NoError(t, err)
				assert.Equal(t, "REVOKED", got.Status)
				assert.NotNil(t, got.RevokedAt)
				assert.Equal(t, "KEY_COMPROMISE", got.RevocationReason)
			},
		},
		{
			name: "revoke with invalid reason returns error",
			run: func(t *testing.T, b *acmpca.InMemoryBackend) {
				t.Helper()

				ca, err := b.CreateCertificateAuthority(
					context.Background(),
					"ROOT",
					acmpca.CertificateAuthorityConfiguration{
						Subject: acmpca.CertificateAuthoritySubject{CommonName: "Revoke CA"},
					},
				)
				require.NoError(t, err)

				err = b.RevokeCertificate(context.Background(), ca.ARN, "doesNotMatter", "INVALID_REASON")
				require.ErrorIs(t, err, acmpca.ErrInvalidParameter)
			},
		},
		{
			name: "delete from PENDING_CERTIFICATE state succeeds",
			run: func(t *testing.T, b *acmpca.InMemoryBackend) {
				t.Helper()

				// SUBORDINATE CAs start in PENDING_CERTIFICATE state (no auto-sign).
				ca, err := b.CreateCertificateAuthority(
					context.Background(),
					"SUBORDINATE",
					acmpca.CertificateAuthorityConfiguration{
						Subject: acmpca.CertificateAuthoritySubject{CommonName: "Pending CA"},
					},
				)
				require.NoError(t, err)
				assert.Equal(t, "PENDING_CERTIFICATE", ca.Status)

				err = b.DeleteCertificateAuthority(context.Background(), ca.ARN, 0)
				require.NoError(t, err)

				got, err := b.DescribeCertificateAuthority(context.Background(), ca.ARN)
				require.NoError(t, err)
				assert.Equal(t, "DELETED", got.Status)
			},
		},
		{
			name: "delete with permanentDeletionDays=5 returns error",
			run: func(t *testing.T, b *acmpca.InMemoryBackend) {
				t.Helper()

				ca, err := b.CreateCertificateAuthority(
					context.Background(),
					"ROOT",
					acmpca.CertificateAuthorityConfiguration{
						Subject: acmpca.CertificateAuthoritySubject{CommonName: "CA"},
					},
				)
				require.NoError(t, err)

				err = b.DeleteCertificateAuthority(context.Background(), ca.ARN, 5)
				require.ErrorIs(t, err, acmpca.ErrInvalidParameter)
			},
		},
		{
			name: "updateCA with invalid status returns error",
			run: func(t *testing.T, b *acmpca.InMemoryBackend) {
				t.Helper()

				ca, err := b.CreateCertificateAuthority(
					context.Background(),
					"ROOT",
					acmpca.CertificateAuthorityConfiguration{
						Subject: acmpca.CertificateAuthoritySubject{CommonName: "CA"},
					},
				)
				require.NoError(t, err)

				err = b.UpdateCertificateAuthority(context.Background(), ca.ARN, "INVALID_STATUS")
				require.ErrorIs(t, err, acmpca.ErrInvalidParameter)
			},
		},
		{
			name: "issueCA with empty CSR returns error",
			run: func(t *testing.T, b *acmpca.InMemoryBackend) {
				t.Helper()

				ca, err := b.CreateCertificateAuthority(
					context.Background(),
					"ROOT",
					acmpca.CertificateAuthorityConfiguration{
						Subject: acmpca.CertificateAuthoritySubject{CommonName: "CA"},
					},
				)
				require.NoError(t, err)

				_, err = b.IssueCertificate(context.Background(), ca.ARN, "", 365)
				require.ErrorIs(t, err, acmpca.ErrInvalidParameter)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tt.run(t, newTestBackend())
		})
	}
}
