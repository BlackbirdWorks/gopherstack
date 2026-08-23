package iam

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	iamsdk "github.com/aws/aws-sdk-go-v2/service/iam"
	smithy "github.com/aws/smithy-go"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// newSigningCertTestClient mirrors newDelegationTestClient (see
// delegation_requests_whitebox_test.go) so this file needs no other
// whitebox helper.
func newSigningCertTestClient(t *testing.T, h *Handler) *iamsdk.Client {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	return iamsdk.NewFromConfig(cfg, func(o *iamsdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// TestUpdateDeleteSigningCertificate_OwnershipMismatch_NoSuchEntity covers
// gopherstack-iam-signing-cert-ownership: UpdateSigningCertificateInput and
// DeleteSigningCertificateInput both carry an optional UserName member
// (api_op_UpdateSigningCertificate.go, api_op_DeleteSigningCertificate.go),
// used by real AWS to scope the CertificateId to its owner -- the same shape
// AccessKeyId/UserName has on Update/DeleteAccessKey and SSHPublicKeyId/
// UserName has on Update/DeleteSSHPublicKey, both of which already enforce
// `key.UserName != userName` -> NoSuchEntity (access_keys.go, ssh_keys.go).
// The handler never read UserName for these two signing-certificate ops at
// all, and the backend methods took no userName parameter, so a caller could
// deactivate or delete ANY user's signing certificate by supplying only its
// CertificateId, with a mismatched (or absent) UserName silently ignored.
func TestUpdateDeleteSigningCertificate_OwnershipMismatch_NoSuchEntity(t *testing.T) {
	t.Parallel()

	t.Run("updatesigningcertificate wrong owner is nosuchentity", func(t *testing.T) {
		t.Parallel()

		b := NewInMemoryBackend()
		h := NewHandler(b)
		client := newSigningCertTestClient(t, h)

		_, _ = b.CreateUser("alice", "/", "")
		_, _ = b.CreateUser("mallory", "/", "")
		cert, err := b.UploadSigningCertificate("alice", "cert-body")
		require.NoError(t, err)

		_, err = client.UpdateSigningCertificate(t.Context(), &iamsdk.UpdateSigningCertificateInput{
			CertificateId: aws.String(cert.CertificateID),
			UserName:      aws.String("mallory"),
			Status:        "Inactive",
		})
		require.Error(t, err)

		var apiErr smithy.APIError

		require.ErrorAs(t, err, &apiErr)
		assert.Equal(t, "NoSuchEntity", apiErr.ErrorCode())

		certs, err := b.ListSigningCertificates("alice")
		require.NoError(t, err)
		require.Len(t, certs, 1)
		assert.Equal(t, "Active", certs[0].Status, "mallory must not be able to deactivate alice's certificate")
	})

	t.Run("deletesigningcertificate wrong owner is nosuchentity", func(t *testing.T) {
		t.Parallel()

		b := NewInMemoryBackend()
		h := NewHandler(b)
		client := newSigningCertTestClient(t, h)

		_, _ = b.CreateUser("bob", "/", "")
		_, _ = b.CreateUser("mallory", "/", "")
		cert, err := b.UploadSigningCertificate("bob", "cert-body")
		require.NoError(t, err)

		_, err = client.DeleteSigningCertificate(t.Context(), &iamsdk.DeleteSigningCertificateInput{
			CertificateId: aws.String(cert.CertificateID),
			UserName:      aws.String("mallory"),
		})
		require.Error(t, err)

		var apiErr smithy.APIError

		require.ErrorAs(t, err, &apiErr)
		assert.Equal(t, "NoSuchEntity", apiErr.ErrorCode())

		certs, err := b.ListSigningCertificates("bob")
		require.NoError(t, err)
		require.Len(t, certs, 1, "mallory must not be able to delete bob's certificate")
	})
}
