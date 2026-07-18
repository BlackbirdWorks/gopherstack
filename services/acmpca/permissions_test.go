package acmpca_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/acmpca"
)

func TestInMemoryBackend_Permissions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, b *acmpca.InMemoryBackend, caARN string)
		name string
	}{
		{
			name: "create_permission_duplicate_rejected",
			run: func(t *testing.T, b *acmpca.InMemoryBackend, caARN string) {
				t.Helper()

				_, err := b.CreatePermission(
					context.Background(),
					caARN,
					"acm.amazonaws.com",
					testAccountID,
					[]string{"IssueCertificate"},
				)
				require.NoError(t, err)

				// Granting the same principal/source-account pair again must be
				// rejected with PermissionAlreadyExistsException, matching real
				// AWS ACM PCA behavior.
				_, err = b.CreatePermission(
					context.Background(),
					caARN,
					"acm.amazonaws.com",
					testAccountID,
					[]string{"IssueCertificate", "GetCertificate"},
				)
				require.ErrorIs(t, err, acmpca.ErrPermissionAlreadyExists)

				// A different source account is a distinct permission and must succeed.
				_, err = b.CreatePermission(
					context.Background(),
					caARN,
					"acm.amazonaws.com",
					"111111111111",
					[]string{"IssueCertificate"},
				)
				require.NoError(t, err)
			},
		},
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

// TestInMemoryBackend_PermissionValidation covers permission validation edge cases.
func TestInMemoryBackend_PermissionValidation(t *testing.T) {
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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tt.run(t, newTestBackend())
		})
	}
}
