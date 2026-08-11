package iam_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	iamsdk "github.com/aws/aws-sdk-go-v2/service/iam"
	"github.com/aws/aws-sdk-go-v2/service/iam/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/iam"
)

// newTestIAMClient stands up the real aws-sdk-go-v2 IAM client against an
// httptest server running this package's Handler, wired through the same
// pkgs/service registry/router used in production.
func newTestIAMClient(t *testing.T, h *iam.Handler) *iamsdk.Client {
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

// TestCreateOps_TagsRoundTrip drives every IAM Create op whose real Input
// struct accepts Tags (iam@v1.58.1) through the real SDK client and asserts
// each resource's own List*Tags op sees what was supplied at creation
// (gopherstack-2mwl). CreateInstanceProfile (api_op_CreateInstanceProfile.go:74),
// CreateSAMLProvider (api_op_CreateSAMLProvider.go:92), CreateOpenIDConnectProvider
// (api_op_CreateOpenIDConnectProvider.go:104), CreateVirtualMFADevice
// (api_op_CreateVirtualMFADevice.go:81) and UploadServerCertificate
// (api_op_UploadServerCertificate.go:154) never decoded Tags at all -- a pure
// decode-drop, gone before the backend saw it. CreateUser/CreateRole/CreatePolicy
// were already wired; kept here as regression coverage. CreateGroup deliberately
// has no Tags parameter in the real SDK, so it is not covered.
func TestCreateOps_TagsRoundTrip(t *testing.T) {
	t.Parallel()

	tag := types.Tag{Key: aws.String("env"), Value: aws.String("prod")}

	t.Run("createuser", func(t *testing.T) {
		t.Parallel()

		h := iam.NewHandler(iam.NewInMemoryBackend())
		client := newTestIAMClient(t, h)

		_, err := client.CreateUser(t.Context(), &iamsdk.CreateUserInput{
			UserName: aws.String("tagged-user"),
			Tags:     []types.Tag{tag},
		})
		require.NoError(t, err)

		out, err := client.ListUserTags(t.Context(), &iamsdk.ListUserTagsInput{UserName: aws.String("tagged-user")})
		require.NoError(t, err)
		require.Len(t, out.Tags, 1)
		assert.Equal(t, "env", aws.ToString(out.Tags[0].Key))
		assert.Equal(t, "prod", aws.ToString(out.Tags[0].Value))
	})

	t.Run("createrole", func(t *testing.T) {
		t.Parallel()

		h := iam.NewHandler(iam.NewInMemoryBackend())
		client := newTestIAMClient(t, h)

		_, err := client.CreateRole(t.Context(), &iamsdk.CreateRoleInput{
			RoleName:                 aws.String("tagged-role"),
			AssumeRolePolicyDocument: aws.String("{}"),
			Tags:                     []types.Tag{tag},
		})
		require.NoError(t, err)

		out, err := client.ListRoleTags(t.Context(), &iamsdk.ListRoleTagsInput{RoleName: aws.String("tagged-role")})
		require.NoError(t, err)
		require.Len(t, out.Tags, 1)
		assert.Equal(t, "env", aws.ToString(out.Tags[0].Key))
	})

	t.Run("createpolicy", func(t *testing.T) {
		t.Parallel()

		h := iam.NewHandler(iam.NewInMemoryBackend())
		client := newTestIAMClient(t, h)

		created, err := client.CreatePolicy(t.Context(), &iamsdk.CreatePolicyInput{
			PolicyName: aws.String("tagged-policy"),
			PolicyDocument: aws.String(
				`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`,
			),
			Tags: []types.Tag{tag},
		})
		require.NoError(t, err)

		out, err := client.ListPolicyTags(t.Context(), &iamsdk.ListPolicyTagsInput{PolicyArn: created.Policy.Arn})
		require.NoError(t, err)
		require.Len(t, out.Tags, 1)
		assert.Equal(t, "env", aws.ToString(out.Tags[0].Key))
	})

	t.Run("createinstanceprofile", func(t *testing.T) {
		t.Parallel()

		h := iam.NewHandler(iam.NewInMemoryBackend())
		client := newTestIAMClient(t, h)

		_, err := client.CreateInstanceProfile(t.Context(), &iamsdk.CreateInstanceProfileInput{
			InstanceProfileName: aws.String("tagged-ip"),
			Tags:                []types.Tag{tag},
		})
		require.NoError(t, err)

		out, err := client.ListInstanceProfileTags(t.Context(), &iamsdk.ListInstanceProfileTagsInput{
			InstanceProfileName: aws.String("tagged-ip"),
		})
		require.NoError(t, err)
		require.Len(t, out.Tags, 1)
		assert.Equal(t, "env", aws.ToString(out.Tags[0].Key))
		assert.Equal(t, "prod", aws.ToString(out.Tags[0].Value))
	})

	t.Run("createsamlprovider", func(t *testing.T) {
		t.Parallel()

		h := iam.NewHandler(iam.NewInMemoryBackend())
		client := newTestIAMClient(t, h)

		created, err := client.CreateSAMLProvider(t.Context(), &iamsdk.CreateSAMLProviderInput{
			Name:                 aws.String("tagged-saml"),
			SAMLMetadataDocument: aws.String("<md/>"),
			Tags:                 []types.Tag{tag},
		})
		require.NoError(t, err)

		out, err := client.ListSAMLProviderTags(t.Context(), &iamsdk.ListSAMLProviderTagsInput{
			SAMLProviderArn: created.SAMLProviderArn,
		})
		require.NoError(t, err)
		require.Len(t, out.Tags, 1)
		assert.Equal(t, "env", aws.ToString(out.Tags[0].Key))
		assert.Equal(t, "prod", aws.ToString(out.Tags[0].Value))
	})

	t.Run("createopenidconnectprovider", func(t *testing.T) {
		t.Parallel()

		h := iam.NewHandler(iam.NewInMemoryBackend())
		client := newTestIAMClient(t, h)

		created, err := client.CreateOpenIDConnectProvider(t.Context(), &iamsdk.CreateOpenIDConnectProviderInput{
			Url:            aws.String("https://oidc.example.com"),
			ThumbprintList: []string{"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
			Tags:           []types.Tag{tag},
		})
		require.NoError(t, err)

		out, err := client.ListOpenIDConnectProviderTags(t.Context(), &iamsdk.ListOpenIDConnectProviderTagsInput{
			OpenIDConnectProviderArn: created.OpenIDConnectProviderArn,
		})
		require.NoError(t, err)
		require.Len(t, out.Tags, 1)
		assert.Equal(t, "env", aws.ToString(out.Tags[0].Key))
		assert.Equal(t, "prod", aws.ToString(out.Tags[0].Value))
	})

	t.Run("createvirtualmfadevice", func(t *testing.T) {
		t.Parallel()

		h := iam.NewHandler(iam.NewInMemoryBackend())
		client := newTestIAMClient(t, h)

		created, err := client.CreateVirtualMFADevice(t.Context(), &iamsdk.CreateVirtualMFADeviceInput{
			VirtualMFADeviceName: aws.String("tagged-mfa"),
			Tags:                 []types.Tag{tag},
		})
		require.NoError(t, err)

		out, err := client.ListMFADeviceTags(t.Context(), &iamsdk.ListMFADeviceTagsInput{
			SerialNumber: created.VirtualMFADevice.SerialNumber,
		})
		require.NoError(t, err)
		require.Len(t, out.Tags, 1)
		assert.Equal(t, "env", aws.ToString(out.Tags[0].Key))
		assert.Equal(t, "prod", aws.ToString(out.Tags[0].Value))
	})

	t.Run("uploadservercertificate", func(t *testing.T) {
		t.Parallel()

		h := iam.NewHandler(iam.NewInMemoryBackend())
		client := newTestIAMClient(t, h)

		_, err := client.UploadServerCertificate(t.Context(), &iamsdk.UploadServerCertificateInput{
			ServerCertificateName: aws.String("tagged-cert"),
			CertificateBody:       aws.String("-----BEGIN CERTIFICATE-----\nMA==\n-----END CERTIFICATE-----"),
			PrivateKey:            aws.String("-----BEGIN PRIVATE KEY-----\nMA==\n-----END PRIVATE KEY-----"),
			Tags:                  []types.Tag{tag},
		})
		require.NoError(t, err)

		out, err := client.ListServerCertificateTags(t.Context(), &iamsdk.ListServerCertificateTagsInput{
			ServerCertificateName: aws.String("tagged-cert"),
		})
		require.NoError(t, err)
		require.Len(t, out.Tags, 1)
		assert.Equal(t, "env", aws.ToString(out.Tags[0].Key))
		assert.Equal(t, "prod", aws.ToString(out.Tags[0].Value))
	})
}
