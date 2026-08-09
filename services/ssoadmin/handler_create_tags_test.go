package ssoadmin_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	ssoadminsdk "github.com/aws/aws-sdk-go-v2/service/ssoadmin"
	ssoadmintypes "github.com/aws/aws-sdk-go-v2/service/ssoadmin/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/ssoadmin"
)

const tagsRTRegion = "us-east-1"

// newTestSSOAdminClient stands up the real aws-sdk-go-v2 ssoadmin client
// against an httptest server running this package's Handler, wired through
// the same pkgs/service registry/router used in production.
func newTestSSOAdminClient(t *testing.T, h *ssoadmin.Handler) *ssoadminsdk.Client {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion(tagsRTRegion),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	return ssoadminsdk.NewFromConfig(cfg, func(o *ssoadminsdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// TestCreateOpsWithTags_RoundTrip drives every ssoadmin Create op whose real
// Input struct accepts Tags (ssoadmin@v1.43.1: api_op_CreatePermissionSet.go:56,
// api_op_CreateInstance.go:59, api_op_CreateApplication.go:84,
// api_op_CreateTrustedTokenIssuer.go:77) through the real SDK client and
// asserts ListTagsForResource sees what was supplied at creation
// (gopherstack-2mwl).
//
// Before the fix, CreateInstance's wire handler (handler_instances.go)
// decoded no Tags field at all -- a pure decode-drop, the same shape as
// elasticache's six creates -- even though Instance.Tags existed and
// TagResource/ListTagsForResource already correctly dispatched to it.
// CreatePermissionSet/CreateApplication/CreateTrustedTokenIssuer were already
// correct.
func TestCreateOpsWithTags_RoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup func(t *testing.T, client *ssoadminsdk.Client) (instanceArn, resourceArn string)
		name  string
	}{
		{
			name: "instance",
			setup: func(t *testing.T, client *ssoadminsdk.Client) (string, string) {
				t.Helper()

				out, err := client.CreateInstance(t.Context(), &ssoadminsdk.CreateInstanceInput{
					Name: aws.String("tagged-instance"),
					Tags: []ssoadmintypes.Tag{{Key: aws.String("env"), Value: aws.String("test")}},
				})
				require.NoError(t, err)

				return aws.ToString(out.InstanceArn), aws.ToString(out.InstanceArn)
			},
		},
		{
			name: "permission set",
			setup: func(t *testing.T, client *ssoadminsdk.Client) (string, string) {
				t.Helper()

				inst, err := client.CreateInstance(t.Context(), &ssoadminsdk.CreateInstanceInput{
					Name: aws.String("ps-host-instance"),
				})
				require.NoError(t, err)

				out, err := client.CreatePermissionSet(t.Context(), &ssoadminsdk.CreatePermissionSetInput{
					InstanceArn: inst.InstanceArn,
					Name:        aws.String("tagged-permission-set"),
					Tags:        []ssoadmintypes.Tag{{Key: aws.String("env"), Value: aws.String("test")}},
				})
				require.NoError(t, err)

				return aws.ToString(inst.InstanceArn), aws.ToString(out.PermissionSet.PermissionSetArn)
			},
		},
		{
			name: "application",
			setup: func(t *testing.T, client *ssoadminsdk.Client) (string, string) {
				t.Helper()

				inst, err := client.CreateInstance(t.Context(), &ssoadminsdk.CreateInstanceInput{
					Name: aws.String("app-host-instance"),
				})
				require.NoError(t, err)

				out, err := client.CreateApplication(t.Context(), &ssoadminsdk.CreateApplicationInput{
					InstanceArn:            inst.InstanceArn,
					ApplicationProviderArn: aws.String("arn:aws:sso::aws:applicationProvider/custom"),
					Name:                   aws.String("tagged-application"),
					Tags:                   []ssoadmintypes.Tag{{Key: aws.String("env"), Value: aws.String("test")}},
				})
				require.NoError(t, err)

				return aws.ToString(inst.InstanceArn), aws.ToString(out.ApplicationArn)
			},
		},
		{
			name: "trusted token issuer",
			setup: func(t *testing.T, client *ssoadminsdk.Client) (string, string) {
				t.Helper()

				inst, err := client.CreateInstance(t.Context(), &ssoadminsdk.CreateInstanceInput{
					Name: aws.String("tti-host-instance"),
				})
				require.NoError(t, err)

				out, err := client.CreateTrustedTokenIssuer(
					t.Context(),
					&ssoadminsdk.CreateTrustedTokenIssuerInput{
						InstanceArn:            inst.InstanceArn,
						Name:                   aws.String("tagged-tti"),
						TrustedTokenIssuerType: ssoadmintypes.TrustedTokenIssuerTypeOidcJwt,
						TrustedTokenIssuerConfiguration: &ssoadmintypes.TrustedTokenIssuerConfigurationMemberOidcJwtConfiguration{
							Value: ssoadmintypes.OidcJwtConfiguration{
								ClaimAttributePath:         aws.String("email"),
								IdentityStoreAttributePath: aws.String("emails.value"),
								IssuerUrl:                  aws.String("https://issuer.example.com"),
								JwksRetrievalOption:        ssoadmintypes.JwksRetrievalOptionOpenIdDiscovery,
							},
						},
						Tags: []ssoadmintypes.Tag{{Key: aws.String("env"), Value: aws.String("test")}},
					},
				)
				require.NoError(t, err)

				return aws.ToString(inst.InstanceArn), aws.ToString(out.TrustedTokenIssuerArn)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := ssoadmin.NewInMemoryBackend("000000000000", tagsRTRegion)
			client := newTestSSOAdminClient(t, ssoadmin.NewHandler(backend))

			instanceArn, resourceArn := tt.setup(t, client)
			require.NotEmpty(t, resourceArn)

			got, err := client.ListTagsForResource(t.Context(), &ssoadminsdk.ListTagsForResourceInput{
				InstanceArn: aws.String(instanceArn),
				ResourceArn: aws.String(resourceArn),
			})
			require.NoError(t, err)
			require.Len(t, got.Tags, 1)
			assert.Equal(t, "env", aws.ToString(got.Tags[0].Key))
			assert.Equal(t, "test", aws.ToString(got.Tags[0].Value))
		})
	}
}
