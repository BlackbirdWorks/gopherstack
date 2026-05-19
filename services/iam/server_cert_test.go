package iam_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iam"
)

func TestServerCertificate_CRUD(t *testing.T) {
	t.Parallel()

	be := iam.NewInMemoryBackend()

	const certBody = "-----BEGIN CERTIFICATE-----\nMIIC\n-----END CERTIFICATE-----"
	const certChain = "-----BEGIN CERTIFICATE-----\nMIID\n-----END CERTIFICATE-----"

	t.Run("Upload", func(t *testing.T) {
		t.Parallel()

		b := iam.NewInMemoryBackend()

		cert, err := b.UploadServerCertificate("my-cert", "/", certBody, "")
		require.NoError(t, err)
		assert.Equal(t, "my-cert", cert.ServerCertificateName)
		assert.NotEmpty(t, cert.ServerCertificateID)
		assert.Contains(t, cert.Arn, "server-certificate")
		assert.Equal(t, "/", cert.Path)
		assert.Equal(t, certBody, cert.CertificateBody)
	})

	t.Run("UploadEmptyBody", func(t *testing.T) {
		t.Parallel()

		b := iam.NewInMemoryBackend()

		_, err := b.UploadServerCertificate("my-cert", "/", "", "")
		require.Error(t, err)
	})

	t.Run("UploadDuplicate", func(t *testing.T) {
		t.Parallel()

		b := iam.NewInMemoryBackend()
		_, err := b.UploadServerCertificate("dup-cert", "/", certBody, "")
		require.NoError(t, err)

		_, err = b.UploadServerCertificate("dup-cert", "/", certBody, "")
		require.Error(t, err)
	})

	t.Run("Get", func(t *testing.T) {
		t.Parallel()

		_, err := be.UploadServerCertificate("get-cert", "/app/", certBody, certChain)
		require.NoError(t, err)

		cert, err := be.GetServerCertificate("get-cert")
		require.NoError(t, err)
		assert.Equal(t, "get-cert", cert.ServerCertificateName)
		assert.Equal(t, certChain, cert.CertificateChain)
		assert.Equal(t, "/app/", cert.Path)
	})

	t.Run("GetNotFound", func(t *testing.T) {
		t.Parallel()

		_, err := be.GetServerCertificate("nonexistent")
		require.Error(t, err)
	})

	t.Run("List", func(t *testing.T) {
		t.Parallel()

		b := iam.NewInMemoryBackend()
		_, err := b.UploadServerCertificate("list-cert-a", "/", certBody, "")
		require.NoError(t, err)
		_, err = b.UploadServerCertificate("list-cert-b", "/prod/", certBody, "")
		require.NoError(t, err)

		all, err := b.ListServerCertificates("")
		require.NoError(t, err)
		assert.Len(t, all, 2)

		prod, err := b.ListServerCertificates("/prod/")
		require.NoError(t, err)
		assert.Len(t, prod, 1)
		assert.Equal(t, "list-cert-b", prod[0].ServerCertificateName)
	})

	t.Run("Update", func(t *testing.T) {
		t.Parallel()

		b := iam.NewInMemoryBackend()
		_, err := b.UploadServerCertificate("upd-cert", "/", certBody, "")
		require.NoError(t, err)

		err = b.UpdateServerCertificate("upd-cert", "upd-cert-renamed", "/new/")
		require.NoError(t, err)

		_, err = b.GetServerCertificate("upd-cert")
		require.Error(t, err)

		cert, err := b.GetServerCertificate("upd-cert-renamed")
		require.NoError(t, err)
		assert.Equal(t, "/new/", cert.Path)
	})

	t.Run("Delete", func(t *testing.T) {
		t.Parallel()

		b := iam.NewInMemoryBackend()
		_, err := b.UploadServerCertificate("del-cert", "/", certBody, "")
		require.NoError(t, err)

		err = b.DeleteServerCertificate("del-cert")
		require.NoError(t, err)

		_, err = b.GetServerCertificate("del-cert")
		require.Error(t, err)
	})

	t.Run("DeleteNotFound", func(t *testing.T) {
		t.Parallel()

		err := be.DeleteServerCertificate("ghost")
		require.Error(t, err)
	})
}

func TestServerCertificate_HandlerRoundtrip(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler(t)

	const certBody = "-----BEGIN CERTIFICATE-----\nMIIC\n-----END CERTIFICATE-----"

	// Upload via handler.
	rec := callIAM(t, h, "UploadServerCertificate", map[string]string{
		"ServerCertificateName": "handler-cert",
		"CertificateBody":       certBody,
		"Path":                  "/",
	})
	assert.Equal(t, 200, rec.Code)

	// Get via handler.
	rec = callIAM(t, h, "GetServerCertificate", map[string]string{
		"ServerCertificateName": "handler-cert",
	})
	assert.Equal(t, 200, rec.Code)
	assert.Contains(t, rec.Body.String(), "handler-cert")

	// List via handler.
	rec = callIAM(t, h, "ListServerCertificates", map[string]string{})
	assert.Equal(t, 200, rec.Code)
	assert.Contains(t, rec.Body.String(), "handler-cert")

	// Delete via handler.
	rec = callIAM(t, h, "DeleteServerCertificate", map[string]string{
		"ServerCertificateName": "handler-cert",
	})
	assert.Equal(t, 200, rec.Code)

	// Get after delete — NoSuchEntity returns 400 per AWS IAM semantics.
	rec = callIAM(t, h, "GetServerCertificate", map[string]string{
		"ServerCertificateName": "handler-cert",
	})
	assert.Equal(t, 400, rec.Code)
	assert.Contains(t, rec.Body.String(), "NoSuchEntity")
}

func TestSSHPublicKey_HandlerRoundtrip(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler(t)

	// Create user first.
	rec := callIAM(t, h, "CreateUser", map[string]string{"UserName": "ssh-user"})
	require.Equal(t, 200, rec.Code)

	const sshBody = "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAAAgQC test@example.com"

	// Upload SSH key.
	rec = callIAM(t, h, "UploadSSHPublicKey", map[string]string{
		"UserName":         "ssh-user",
		"SSHPublicKeyBody": sshBody,
	})
	require.Equal(t, 200, rec.Code)
	assert.Contains(t, rec.Body.String(), "ssh-user")

	// List SSH keys.
	rec = callIAM(t, h, "ListSSHPublicKeys", map[string]string{"UserName": "ssh-user"})
	require.Equal(t, 200, rec.Code)
	assert.Contains(t, rec.Body.String(), "ssh-user")
}

func TestMFADevice_HandlerRoundtrip(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler(t)

	// Create user and MFA device.
	rec := callIAM(t, h, "CreateUser", map[string]string{"UserName": "mfa-user"})
	require.Equal(t, 200, rec.Code)

	rec = callIAM(t, h, "CreateVirtualMFADevice", map[string]string{
		"VirtualMFADeviceName": "mfa-token",
		"Path":                 "/",
	})
	require.Equal(t, 200, rec.Code)
	assert.Contains(t, rec.Body.String(), "mfa-token")

	// ListMFADevices for the user (empty list — device not yet enabled for user).
	rec = callIAM(t, h, "ListMFADevices", map[string]string{"UserName": "mfa-user"})
	assert.Equal(t, 200, rec.Code)
}
