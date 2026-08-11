package appstream_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	appstreamsdk "github.com/aws/aws-sdk-go-v2/service/appstream"
	"github.com/aws/aws-sdk-go-v2/service/appstream/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/appstream"
)

// newTestAppStreamClient stands up the real aws-sdk-go-v2 AppStream client
// against an httptest server running this package's Handler, wired through
// the same pkgs/service registry/router used in production. AppStream's
// pinned SDK (appstream@v1.64.5) has ONLY a smithy rpc-v2-cbor serializer --
// no JSON fallback -- so this exercises gopherstack's CBOR bridge
// (rpcv2cbor.go), not just its JSON handlers.
func newTestAppStreamClient(t *testing.T, h *appstream.Handler) *appstreamsdk.Client {
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

	return appstreamsdk.NewFromConfig(cfg, func(o *appstreamsdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// TestCreateOps_TagsRoundTrip drives every AppStream Create op that accepts
// Tags in the real SDK (appstream@v1.64.5) through a real SDK client and
// asserts ListTagsForResource sees what was supplied at creation
// (gopherstack-2mwl). Tags-acceptance verified by grepping each op's
// serializeCBOR_*Input for a "Tags" key write in serializers.go: CreateStack,
// CreateFleet, CreateAppBlock, CreateAppBlockBuilder, CreateApplication,
// CreateImageBuilder, CreateImportedImage all have one; CreateDirectoryConfig,
// CreateEntitlement, CreateUsageReportSubscription, CreateUser do not.
func TestCreateOps_TagsRoundTrip(t *testing.T) {
	t.Parallel()

	tags := map[string]string{"env": "prod"}

	requireTags := func(t *testing.T, client *appstreamsdk.Client, resourceARN string) {
		t.Helper()
		out, err := client.ListTagsForResource(t.Context(), &appstreamsdk.ListTagsForResourceInput{
			ResourceArn: aws.String(resourceARN),
		})
		require.NoError(t, err)
		assert.Equal(t, tags, out.Tags)
	}

	t.Run("createstack", func(t *testing.T) {
		t.Parallel()

		h := appstream.NewHandler(appstream.NewInMemoryBackend("123456789012", "us-east-1"))
		client := newTestAppStreamClient(t, h)

		out, err := client.CreateStack(t.Context(), &appstreamsdk.CreateStackInput{
			Name: aws.String("tagged-stack"),
			Tags: tags,
		})
		require.NoError(t, err)
		requireTags(t, client, aws.ToString(out.Stack.Arn))
	})

	t.Run("createfleet", func(t *testing.T) {
		t.Parallel()

		h := appstream.NewHandler(appstream.NewInMemoryBackend("123456789012", "us-east-1"))
		client := newTestAppStreamClient(t, h)

		out, err := client.CreateFleet(t.Context(), &appstreamsdk.CreateFleetInput{
			Name:         aws.String("tagged-fleet"),
			InstanceType: aws.String("stream.standard.medium"),
			ImageName:    aws.String("some-image"),
			ComputeCapacity: &types.ComputeCapacity{
				DesiredInstances: aws.Int32(1),
			},
			Tags: tags,
		})
		require.NoError(t, err)
		requireTags(t, client, aws.ToString(out.Fleet.Arn))
	})

	t.Run("createappblock", func(t *testing.T) {
		t.Parallel()

		h := appstream.NewHandler(appstream.NewInMemoryBackend("123456789012", "us-east-1"))
		client := newTestAppStreamClient(t, h)

		out, err := client.CreateAppBlock(t.Context(), &appstreamsdk.CreateAppBlockInput{
			Name: aws.String("tagged-appblock"),
			SourceS3Location: &types.S3Location{
				S3Bucket: aws.String("some-bucket"),
				S3Key:    aws.String("some-key"),
			},
			Tags: tags,
		})
		require.NoError(t, err)
		requireTags(t, client, aws.ToString(out.AppBlock.Arn))
	})

	t.Run("createappblockbuilder", func(t *testing.T) {
		t.Parallel()

		h := appstream.NewHandler(appstream.NewInMemoryBackend("123456789012", "us-east-1"))
		client := newTestAppStreamClient(t, h)

		out, err := client.CreateAppBlockBuilder(t.Context(), &appstreamsdk.CreateAppBlockBuilderInput{
			Name:         aws.String("tagged-appblockbuilder"),
			Platform:     types.AppBlockBuilderPlatformTypeWindowsServer2019,
			InstanceType: aws.String("stream.standard.medium"),
			VpcConfig:    &types.VpcConfig{},
			Tags:         tags,
		})
		require.NoError(t, err)
		requireTags(t, client, aws.ToString(out.AppBlockBuilder.Arn))
	})

	t.Run("createapplication", func(t *testing.T) {
		t.Parallel()

		h := appstream.NewHandler(appstream.NewInMemoryBackend("123456789012", "us-east-1"))
		client := newTestAppStreamClient(t, h)

		out, err := client.CreateApplication(t.Context(), &appstreamsdk.CreateApplicationInput{
			Name: aws.String("tagged-app"),
			IconS3Location: &types.S3Location{
				S3Bucket: aws.String("some-bucket"),
				S3Key:    aws.String("some-key"),
			},
			LaunchPath:       aws.String("C:\\app.exe"),
			Platforms:        []types.PlatformType{types.PlatformTypeWindowsServer2019},
			InstanceFamilies: []string{"GENERAL_PURPOSE"},
			AppBlockArn:      aws.String("arn:aws:appstream:us-east-1:123456789012:app-block/tagged-appblock"),
			Tags:             tags,
		})
		require.NoError(t, err)
		requireTags(t, client, aws.ToString(out.Application.Arn))
	})

	t.Run("createimagebuilder", func(t *testing.T) {
		t.Parallel()

		h := appstream.NewHandler(appstream.NewInMemoryBackend("123456789012", "us-east-1"))
		client := newTestAppStreamClient(t, h)

		out, err := client.CreateImageBuilder(t.Context(), &appstreamsdk.CreateImageBuilderInput{
			Name:         aws.String("tagged-imagebuilder"),
			InstanceType: aws.String("stream.standard.medium"),
			ImageName:    aws.String("some-image"),
			Tags:         tags,
		})
		require.NoError(t, err)
		requireTags(t, client, aws.ToString(out.ImageBuilder.Arn))
	})

	t.Run("createimportedimage", func(t *testing.T) {
		t.Parallel()

		h := appstream.NewHandler(appstream.NewInMemoryBackend("123456789012", "us-east-1"))
		client := newTestAppStreamClient(t, h)

		_, err := client.CreateImportedImage(t.Context(), &appstreamsdk.CreateImportedImageInput{
			Name:        aws.String("tagged-image"),
			Description: aws.String("test"),
			Tags:        tags,
		})
		require.NoError(t, err)

		out, err := client.DescribeImages(t.Context(), &appstreamsdk.DescribeImagesInput{
			Names: []string{"tagged-image"},
		})
		require.NoError(t, err)
		require.Len(t, out.Images, 1)
		requireTags(t, client, aws.ToString(out.Images[0].Arn))
	})
}
