package iam_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iam"
)

func TestUploadSigningCertificate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(b *iam.InMemoryBackend)
		wantErr error
		name    string
		user    string
		body    string
	}{
		{
			name: "success",
			user: "frank",
			body: "-----BEGIN CERTIFICATE-----\nMIIDxx==\n-----END CERTIFICATE-----",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateUser("frank", "/", "")
			},
		},
		{
			name:    "user not found",
			user:    "ghost",
			body:    "cert-body",
			setup:   func(*iam.InMemoryBackend) {},
			wantErr: iam.ErrUserNotFound,
		},
		{
			name: "empty body rejected",
			user: "grace",
			body: "",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateUser("grace", "/", "")
			},
			wantErr: iam.ErrMalformedPolicyDocument,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t)
			tt.setup(b)

			cert, err := b.UploadSigningCertificate(tt.user, tt.body)
			if tt.wantErr != nil {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
			assert.NotEmpty(t, cert.CertificateID)
			assert.True(t, strings.HasPrefix(cert.CertificateID, "ASCA"))
			assert.Equal(t, tt.user, cert.UserName)
			assert.Equal(t, tt.body, cert.CertificateBody)
			assert.Equal(t, "Active", cert.Status)
			assert.False(t, cert.UploadDate.IsZero())
		})
	}
}

func TestListSigningCertificates(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup     func(b *iam.InMemoryBackend)
		wantErr   error
		name      string
		user      string
		wantCount int
	}{
		{
			name: "returns only the named user's certificates",
			user: "u1",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateUser("u1", "/", "")
				_, _ = b.CreateUser("u2", "/", "")
				_, _ = b.UploadSigningCertificate("u1", "cert1")
				_, _ = b.UploadSigningCertificate("u2", "cert2")
				_, _ = b.UploadSigningCertificate("u1", "cert3")
			},
			wantCount: 2,
		},
		{
			name:    "user not found",
			user:    "ghost",
			setup:   func(*iam.InMemoryBackend) {},
			wantErr: iam.ErrUserNotFound,
		},
		{
			name: "empty user returns all certificates",
			user: "",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateUser("u3", "/", "")
				_, _ = b.CreateUser("u4", "/", "")
				_, _ = b.UploadSigningCertificate("u3", "cert-a")
				_, _ = b.UploadSigningCertificate("u4", "cert-b")
			},
			wantCount: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t)
			tt.setup(b)

			certs, err := b.ListSigningCertificates(tt.user)
			if tt.wantErr != nil {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
			assert.Len(t, certs, tt.wantCount)

			if tt.user != "" {
				for _, c := range certs {
					assert.Equal(t, tt.user, c.UserName)
				}
			}
		})
	}
}

func TestUpdateSigningCertificate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr       error
		name          string
		certificateID string
		status        string
	}{
		{name: "changes status", status: "Inactive"},
		{name: "invalid status rejected", status: "Deleted", wantErr: iam.ErrInvalidAction},
		{name: "not found", certificateID: "ASCANONEXISTENT", status: "Inactive", wantErr: iam.ErrAccessKeyNotFound},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t)
			_, _ = b.CreateUser("cert-user", "/", "")

			certID := tt.certificateID
			if certID == "" {
				cert, err := b.UploadSigningCertificate("cert-user", "body")
				require.NoError(t, err)
				certID = cert.CertificateID
			}

			err := b.UpdateSigningCertificate(certID, tt.status)
			if tt.wantErr != nil {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
			certs, err := b.ListSigningCertificates("cert-user")
			require.NoError(t, err)
			require.Len(t, certs, 1)
			assert.Equal(t, tt.status, certs[0].Status)
		})
	}
}

func TestDeleteSigningCertificate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr       error
		name          string
		certificateID string
	}{
		{name: "success"},
		{name: "not found", certificateID: "ASCANONEXISTENT", wantErr: iam.ErrAccessKeyNotFound},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t)
			_, _ = b.CreateUser("delete-user", "/", "")

			certID := tt.certificateID
			if certID == "" {
				cert, err := b.UploadSigningCertificate("delete-user", "body")
				require.NoError(t, err)
				certID = cert.CertificateID
			}

			err := b.DeleteSigningCertificate(certID)
			if tt.wantErr != nil {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
			certs, err := b.ListSigningCertificates("delete-user")
			require.NoError(t, err)
			assert.Empty(t, certs)
		})
	}
}

func TestSigningCertificate_IDUniqueness(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, _ = b.CreateUser("karl", "/", "")

	ids := make(map[string]bool)
	for range 10 {
		cert, err := b.UploadSigningCertificate("karl", "body")
		require.NoError(t, err)
		assert.False(t, ids[cert.CertificateID], "duplicate cert ID: %s", cert.CertificateID)
		ids[cert.CertificateID] = true
	}
}

func TestSigningCertificateReset_ClearsCertificates(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, _ = b.CreateUser("zoe", "/", "")
	_, _ = b.UploadSigningCertificate("zoe", "body")

	b.Reset()

	// After reset, user doesn't exist so list by user fails. Test all-certs list.
	_, _ = b.CreateUser("zoe", "/", "")
	certs, err := b.ListSigningCertificates("zoe")
	require.NoError(t, err)
	assert.Empty(t, certs)
}

func TestSigningCertificate_MultipleCertsPerUser(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, _ = b.CreateUser("multi-cert-user", "/", "")

	cert1, err := b.UploadSigningCertificate("multi-cert-user", "body-1")
	require.NoError(t, err)

	cert2, err := b.UploadSigningCertificate("multi-cert-user", "body-2")
	require.NoError(t, err)

	assert.NotEqual(t, cert1.CertificateID, cert2.CertificateID)

	certs, err := b.ListSigningCertificates("multi-cert-user")
	require.NoError(t, err)
	assert.Len(t, certs, 2)
}

func TestSigningCertificate_ToggleStatus(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, _ = b.CreateUser("toggle-user", "/", "")
	cert, _ := b.UploadSigningCertificate("toggle-user", "body")

	// Active → Inactive.
	require.NoError(t, b.UpdateSigningCertificate(cert.CertificateID, "Inactive"))

	certs, _ := b.ListSigningCertificates("toggle-user")
	assert.Equal(t, "Inactive", certs[0].Status)

	// Inactive → Active.
	require.NoError(t, b.UpdateSigningCertificate(cert.CertificateID, "Active"))

	certs, _ = b.ListSigningCertificates("toggle-user")
	assert.Equal(t, "Active", certs[0].Status)
}
