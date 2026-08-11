package organizations_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	organizationssdk "github.com/aws/aws-sdk-go-v2/service/organizations"
	organizationstypes "github.com/aws/aws-sdk-go-v2/service/organizations/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/organizations"
)

const tagsRTRegion = "us-east-1"

// newTestOrganizationsClient stands up the real aws-sdk-go-v2 organizations
// client against an httptest server running this package's Handler, wired
// through the same pkgs/service registry/router used in production.
func newTestOrganizationsClient(t *testing.T, h *organizations.Handler) *organizationssdk.Client {
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

	return organizationssdk.NewFromConfig(cfg, func(o *organizationssdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// TestCreateOpsWithTags_RoundTrip drives every organizations Create op whose
// real Input struct accepts Tags (organizations@v1.53.5:
// api_op_CreateAccount.go:188, api_op_CreateOrganizationalUnit.go:74,
// api_op_CreatePolicy.go:121 -- CreateGovCloudAccount also accepts Tags but
// requires a real GovCloud-linked account to exercise) through the real SDK
// client and asserts ListTagsForResource sees what was supplied at creation
// (gopherstack-2mwl). ListTagsForResource keys by bare resourceId (account
// ID, ou-id, or policy-id), not ARN -- verified against
// api_op_ListTagsForResource.go:59.
func TestCreateOpsWithTags_RoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup func(t *testing.T, client *organizationssdk.Client) string
		name  string
	}{
		{
			name: "account",
			setup: func(t *testing.T, client *organizationssdk.Client) string {
				t.Helper()

				out, err := client.CreateAccount(t.Context(), &organizationssdk.CreateAccountInput{
					AccountName: aws.String("tagged-account"),
					Email:       aws.String("tagged-account@example.com"),
					Tags:        []organizationstypes.Tag{{Key: aws.String("env"), Value: aws.String("test")}},
				})
				require.NoError(t, err)

				return aws.ToString(out.CreateAccountStatus.AccountId)
			},
		},
		{
			name: "organizational unit",
			setup: func(t *testing.T, client *organizationssdk.Client) string {
				t.Helper()

				roots, err := client.ListRoots(t.Context(), &organizationssdk.ListRootsInput{})
				require.NoError(t, err)
				require.NotEmpty(t, roots.Roots)

				out, err := client.CreateOrganizationalUnit(
					t.Context(),
					&organizationssdk.CreateOrganizationalUnitInput{
						ParentId: roots.Roots[0].Id,
						Name:     aws.String("tagged-ou"),
						Tags:     []organizationstypes.Tag{{Key: aws.String("env"), Value: aws.String("test")}},
					},
				)
				require.NoError(t, err)

				return aws.ToString(out.OrganizationalUnit.Id)
			},
		},
		{
			name: "policy",
			setup: func(t *testing.T, client *organizationssdk.Client) string {
				t.Helper()

				out, err := client.CreatePolicy(t.Context(), &organizationssdk.CreatePolicyInput{
					Name:        aws.String("tagged-policy"),
					Description: aws.String("a tagged policy"),
					Type:        organizationstypes.PolicyTypeServiceControlPolicy,
					Content: aws.String(
						`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`,
					),
					Tags: []organizationstypes.Tag{{Key: aws.String("env"), Value: aws.String("test")}},
				})
				require.NoError(t, err)

				return aws.ToString(out.Policy.PolicySummary.Id)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := organizations.NewInMemoryBackend("000000000000", tagsRTRegion)
			client := newTestOrganizationsClient(t, organizations.NewHandler(backend))

			_, err := client.CreateOrganization(t.Context(), &organizationssdk.CreateOrganizationInput{})
			require.NoError(t, err)

			resourceID := tt.setup(t, client)
			require.NotEmpty(t, resourceID)

			got, err := client.ListTagsForResource(
				t.Context(),
				&organizationssdk.ListTagsForResourceInput{ResourceId: aws.String(resourceID)},
			)
			require.NoError(t, err)
			require.Len(t, got.Tags, 1)
			assert.Equal(t, "env", aws.ToString(got.Tags[0].Key))
			assert.Equal(t, "test", aws.ToString(got.Tags[0].Value))
		})
	}
}
