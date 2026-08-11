package codebuild_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	codebuildsdk "github.com/aws/aws-sdk-go-v2/service/codebuild"
	"github.com/aws/aws-sdk-go-v2/service/codebuild/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/codebuild"
)

// newTestCodeBuildClient stands up the real aws-sdk-go-v2 CodeBuild client
// against an httptest server running this package's Handler, wired through
// the same pkgs/service registry/router used in production.
func newTestCodeBuildClient(t *testing.T, h *codebuild.Handler) *codebuildsdk.Client {
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

	return codebuildsdk.NewFromConfig(cfg, func(o *codebuildsdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// TestCreateOps_TagsRoundTrip drives every CodeBuild Create op that accepts
// Tags in the real SDK (codebuild@v1.72.4: CreateProject, CreateFleet,
// CreateReportGroup) through a real SDK client and asserts the resource's own
// BatchGet response renders the tags supplied at creation (gopherstack-2mwl).
// CodeBuild has no ListTagsForResource/TagResource API; tags round-trip only
// through the resource's own Batch/Get response.
func TestCreateOps_TagsRoundTrip(t *testing.T) {
	t.Parallel()

	tags := []types.Tag{{Key: aws.String("env"), Value: aws.String("prod")}}

	newClient := func(t *testing.T) *codebuildsdk.Client {
		t.Helper()

		h := codebuild.NewHandler(codebuild.NewInMemoryBackend("123456789012", "us-east-1"))

		return newTestCodeBuildClient(t, h)
	}

	t.Run("createproject", func(t *testing.T) {
		t.Parallel()

		client := newClient(t)

		_, err := client.CreateProject(t.Context(), &codebuildsdk.CreateProjectInput{
			Name:        aws.String("tagged-project"),
			ServiceRole: aws.String("arn:aws:iam::123456789012:role/service-role"),
			Source:      &types.ProjectSource{Type: types.SourceTypeNoSource},
			Artifacts:   &types.ProjectArtifacts{Type: types.ArtifactsTypeNoArtifacts},
			Environment: &types.ProjectEnvironment{
				Type:        types.EnvironmentTypeLinuxContainer,
				Image:       aws.String("aws/codebuild/standard:7.0"),
				ComputeType: types.ComputeTypeBuildGeneral1Small,
			},
			Tags: tags,
		})
		require.NoError(t, err)

		out, err := client.BatchGetProjects(t.Context(), &codebuildsdk.BatchGetProjectsInput{
			Names: []string{"tagged-project"},
		})
		require.NoError(t, err)
		require.Len(t, out.Projects, 1)
		require.Len(t, out.Projects[0].Tags, 1)
		assert.Equal(t, "env", aws.ToString(out.Projects[0].Tags[0].Key))
		assert.Equal(t, "prod", aws.ToString(out.Projects[0].Tags[0].Value))
	})

	t.Run("createfleet", func(t *testing.T) {
		t.Parallel()

		client := newClient(t)

		_, err := client.CreateFleet(t.Context(), &codebuildsdk.CreateFleetInput{
			Name:            aws.String("tagged-fleet"),
			BaseCapacity:    aws.Int32(1),
			EnvironmentType: types.EnvironmentTypeLinuxContainer,
			ComputeType:     types.ComputeTypeBuildGeneral1Small,
			Tags:            tags,
		})
		require.NoError(t, err)

		out, err := client.BatchGetFleets(t.Context(), &codebuildsdk.BatchGetFleetsInput{
			Names: []string{"tagged-fleet"},
		})
		require.NoError(t, err)
		require.Len(t, out.Fleets, 1)
		require.Len(t, out.Fleets[0].Tags, 1)
		assert.Equal(t, "env", aws.ToString(out.Fleets[0].Tags[0].Key))
		assert.Equal(t, "prod", aws.ToString(out.Fleets[0].Tags[0].Value))
	})

	t.Run("createreportgroup", func(t *testing.T) {
		t.Parallel()

		client := newClient(t)

		_, err := client.CreateReportGroup(t.Context(), &codebuildsdk.CreateReportGroupInput{
			Name: aws.String("tagged-report-group"),
			Type: types.ReportTypeTest,
			ExportConfig: &types.ReportExportConfig{
				ExportConfigType: types.ReportExportConfigTypeNoExport,
			},
			Tags: tags,
		})
		require.NoError(t, err)

		out, err := client.BatchGetReportGroups(t.Context(), &codebuildsdk.BatchGetReportGroupsInput{
			ReportGroupArns: []string{"arn:aws:codebuild:us-east-1:123456789012:report-group/tagged-report-group"},
		})
		require.NoError(t, err)
		require.Len(t, out.ReportGroups, 1)
		require.Len(t, out.ReportGroups[0].Tags, 1)
		assert.Equal(t, "env", aws.ToString(out.ReportGroups[0].Tags[0].Key))
		assert.Equal(t, "prod", aws.ToString(out.ReportGroups[0].Tags[0].Value))
	})
}
