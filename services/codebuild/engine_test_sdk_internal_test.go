package codebuild

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	codebuildsdk "github.com/aws/aws-sdk-go-v2/service/codebuild"
	cbtypes "github.com/aws/aws-sdk-go-v2/service/codebuild/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// sdkClientFor serves b through the production router and returns a real SDK client.
func sdkClientFor(t *testing.T, b *InMemoryBackend) *codebuildsdk.Client {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(NewHandler(b)))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	require.NoError(t, err)

	return codebuildsdk.NewFromConfig(cfg, func(o *codebuildsdk.Options) { o.BaseEndpoint = aws.String(srv.URL) })
}

type sdkProject struct {
	source *cbtypes.ProjectSource
	cache  *cbtypes.ProjectCache
	name   string
}

func createSDKProject(t *testing.T, c *codebuildsdk.Client, p sdkProject) {
	t.Helper()

	if p.name == "" {
		p.name = "proj"
	}

	_, err := c.CreateProject(t.Context(), &codebuildsdk.CreateProjectInput{
		Name:        aws.String(p.name),
		Source:      p.source,
		Cache:       p.cache,
		ServiceRole: aws.String("arn:aws:iam::000000000000:role/svc"),
		Artifacts:   &cbtypes.ProjectArtifacts{Type: cbtypes.ArtifactsTypeNoArtifacts},
		Environment: &cbtypes.ProjectEnvironment{
			Type:        cbtypes.EnvironmentTypeLinuxContainer,
			Image:       aws.String("aws/codebuild/standard:7.0"),
			ComputeType: cbtypes.ComputeTypeBuildGeneral1Small,
		},
	})
	require.NoError(t, err)
}

// startAndWait starts a build through the SDK and returns it once the engine finished.
func startAndWait(
	t *testing.T, h *engineHarness, c *codebuildsdk.Client, in *codebuildsdk.StartBuildInput,
) cbtypes.Build {
	t.Helper()

	if in.ProjectName == nil {
		in.ProjectName = aws.String("proj")
	}

	out, err := c.StartBuild(t.Context(), in)
	require.NoError(t, err)

	h.b.WaitBuilds()

	got, err := c.BatchGetBuilds(
		t.Context(),
		&codebuildsdk.BatchGetBuildsInput{Ids: []string{aws.ToString(out.Build.Id)}},
	)
	require.NoError(t, err)
	require.Len(t, got.Builds, 1)

	return got.Builds[0]
}

func failedPhaseMessage(b cbtypes.Build) string {
	for _, p := range b.Phases {
		if p.PhaseStatus == cbtypes.StatusTypeFailed && len(p.Contexts) > 0 {
			return aws.ToString(p.Contexts[0].Message)
		}
	}

	return ""
}
