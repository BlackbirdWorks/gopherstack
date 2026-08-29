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
			wantErr: iam.ErrMalformedCertificate,
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

			p, err := b.ListSigningCertificates(tt.user, "", 0)
			if tt.wantErr != nil {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
			assert.Len(t, p.Data, tt.wantCount)

			if tt.user != "" {
				for _, c := range p.Data {
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
		updateAs      string
	}{
		{name: "changes status", status: "Inactive"},
		{name: "invalid status rejected", status: "Deleted", wantErr: iam.ErrInvalidAction},
		{name: "not found", certificateID: "ASCANONEXISTENT", status: "Inactive", wantErr: iam.ErrAccessKeyNotFound},
		{name: "wrong owner rejected", status: "Inactive", updateAs: "mallory", wantErr: iam.ErrAccessKeyNotFound},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t)
			_, _ = b.CreateUser("cert-user", "/", "")
			_, _ = b.CreateUser("mallory", "/", "")

			certID := tt.certificateID
			if certID == "" {
				cert, err := b.UploadSigningCertificate("cert-user", "body")
				require.NoError(t, err)
				certID = cert.CertificateID
			}

			updateAs := tt.updateAs
			if updateAs == "" {
				updateAs = "cert-user"
			}

			err := b.UpdateSigningCertificate(updateAs, certID, tt.status)
			if tt.wantErr != nil {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
			p, err := b.ListSigningCertificates("cert-user", "", 0)
			require.NoError(t, err)
			require.Len(t, p.Data, 1)
			assert.Equal(t, tt.status, p.Data[0].Status)
		})
	}
}

func TestDeleteSigningCertificate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr       error
		name          string
		certificateID string
		deleteAs      string
	}{
		{name: "success"},
		{name: "not found", certificateID: "ASCANONEXISTENT", wantErr: iam.ErrAccessKeyNotFound},
		{name: "wrong owner rejected", deleteAs: "mallory", wantErr: iam.ErrAccessKeyNotFound},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t)
			_, _ = b.CreateUser("delete-user", "/", "")
			_, _ = b.CreateUser("mallory", "/", "")

			certID := tt.certificateID
			if certID == "" {
				cert, err := b.UploadSigningCertificate("delete-user", "body")
				require.NoError(t, err)
				certID = cert.CertificateID
			}

			deleteAs := tt.deleteAs
			if deleteAs == "" {
				deleteAs = "delete-user"
			}

			err := b.DeleteSigningCertificate(deleteAs, certID)
			if tt.wantErr != nil {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
			p, err := b.ListSigningCertificates("delete-user", "", 0)
			require.NoError(t, err)
			assert.Empty(t, p.Data)
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
	p, err := b.ListSigningCertificates("zoe", "", 0)
	require.NoError(t, err)
	assert.Empty(t, p.Data)
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

	p, err := b.ListSigningCertificates("multi-cert-user", "", 0)
	require.NoError(t, err)
	assert.Len(t, p.Data, 2)
}

func TestSigningCertificate_ToggleStatus(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, _ = b.CreateUser("toggle-user", "/", "")
	cert, _ := b.UploadSigningCertificate("toggle-user", "body")

	// Active → Inactive.
	require.NoError(t, b.UpdateSigningCertificate("toggle-user", cert.CertificateID, "Inactive"))

	p, _ := b.ListSigningCertificates("toggle-user", "", 0)
	assert.Equal(t, "Inactive", p.Data[0].Status)

	// Inactive → Active.
	require.NoError(t, b.UpdateSigningCertificate("toggle-user", cert.CertificateID, "Active"))

	p, _ = b.ListSigningCertificates("toggle-user", "", 0)
	assert.Equal(t, "Active", p.Data[0].Status)
}
