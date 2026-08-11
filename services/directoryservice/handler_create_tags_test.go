package directoryservice_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	directoryservicesdk "github.com/aws/aws-sdk-go-v2/service/directoryservice"
	directoryservicetypes "github.com/aws/aws-sdk-go-v2/service/directoryservice/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/directoryservice"
)

const tagsRTRegion = "us-east-1"

// newTestDirectoryServiceClient stands up the real aws-sdk-go-v2
// directoryservice client against an httptest server running this package's
// Handler, wired through the same pkgs/service registry/router used in
// production.
func newTestDirectoryServiceClient(t *testing.T, h *directoryservice.Handler) *directoryservicesdk.Client {
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

	return directoryservicesdk.NewFromConfig(cfg, func(o *directoryservicesdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// TestCreateOpsWithTags_RoundTrip drives every directoryservice Create op
// whose real Input struct accepts Tags (directoryservice@v1.41.4:
// api_op_CreateDirectory.go, api_op_CreateMicrosoftAD.go,
// api_op_CreateHybridAD.go) through the real SDK client and asserts
// ListTagsForResource sees what was supplied at creation (gopherstack-2mwl).
func TestCreateOpsWithTags_RoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup func(t *testing.T, client *directoryservicesdk.Client) string
		name  string
	}{
		{
			name: "simple ad",
			setup: func(t *testing.T, client *directoryservicesdk.Client) string {
				t.Helper()

				out, err := client.CreateDirectory(t.Context(), &directoryservicesdk.CreateDirectoryInput{
					Name:     aws.String("simplead.example.com"),
					Password: aws.String("Sup3rSecret!Passw0rd"),
					Size:     directoryservicetypes.DirectorySizeSmall,
					Tags:     []directoryservicetypes.Tag{{Key: aws.String("env"), Value: aws.String("test")}},
				})
				require.NoError(t, err)

				return aws.ToString(out.DirectoryId)
			},
		},
		{
			name: "microsoft ad",
			setup: func(t *testing.T, client *directoryservicesdk.Client) string {
				t.Helper()

				out, err := client.CreateMicrosoftAD(t.Context(), &directoryservicesdk.CreateMicrosoftADInput{
					Name:     aws.String("msad.example.com"),
					Password: aws.String("Sup3rSecret!Passw0rd"),
					VpcSettings: &directoryservicetypes.DirectoryVpcSettings{
						VpcId:     aws.String("vpc-0123456789abcdef0"),
						SubnetIds: []string{"subnet-0123456789abcdef0", "subnet-0123456789abcdef1"},
					},
					Tags: []directoryservicetypes.Tag{{Key: aws.String("env"), Value: aws.String("test")}},
				})
				require.NoError(t, err)

				return aws.ToString(out.DirectoryId)
			},
		},
		{
			name: "hybrid ad",
			setup: func(t *testing.T, client *directoryservicesdk.Client) string {
				t.Helper()

				src, err := client.CreateDirectory(t.Context(), &directoryservicesdk.CreateDirectoryInput{
					Name:     aws.String("hybridsource.example.com"),
					Password: aws.String("Sup3rSecret!Passw0rd"),
					Size:     directoryservicetypes.DirectorySizeSmall,
				})
				require.NoError(t, err)

				assessment, err := client.StartADAssessment(t.Context(), &directoryservicesdk.StartADAssessmentInput{
					DirectoryId: src.DirectoryId,
				})
				require.NoError(t, err)

				out, err := client.CreateHybridAD(t.Context(), &directoryservicesdk.CreateHybridADInput{
					AssessmentId: assessment.AssessmentId,
					SecretArn:    aws.String("arn:aws:secretsmanager:us-east-1:000000000000:secret:my-secret-abc123"),
					Tags:         []directoryservicetypes.Tag{{Key: aws.String("env"), Value: aws.String("test")}},
				})
				require.NoError(t, err)

				return aws.ToString(out.DirectoryId)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := directoryservice.NewInMemoryBackend("000000000000", tagsRTRegion)
			client := newTestDirectoryServiceClient(t, directoryservice.NewHandler(backend))

			directoryID := tt.setup(t, client)
			require.NotEmpty(t, directoryID)

			got, err := client.ListTagsForResource(t.Context(), &directoryservicesdk.ListTagsForResourceInput{
				ResourceId: aws.String(directoryID),
			})
			require.NoError(t, err)
			require.Len(t, got.Tags, 1)
			assert.Equal(t, "env", aws.ToString(got.Tags[0].Key))
			assert.Equal(t, "test", aws.ToString(got.Tags[0].Value))
		})
	}
}
