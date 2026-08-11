package elasticbeanstalk_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	ebsdk "github.com/aws/aws-sdk-go-v2/service/elasticbeanstalk"
	"github.com/aws/aws-sdk-go-v2/service/elasticbeanstalk/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/elasticbeanstalk"
)

// newTestEBClient stands up the real aws-sdk-go-v2 Elastic Beanstalk client
// against an httptest server running this package's Handler, wired through
// the same pkgs/service registry/router used in production.
func newTestEBClient(t *testing.T, h *elasticbeanstalk.Handler) *ebsdk.Client {
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

	return ebsdk.NewFromConfig(cfg, func(o *ebsdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// TestCreateOps_TagsRoundTrip drives every Elastic Beanstalk Create op that
// accepts Tags in the real SDK (elasticbeanstalk@v1.37.4: CreateApplication,
// CreateApplicationVersion, CreateConfigurationTemplate, CreateEnvironment,
// CreatePlatformVersion) through a real SDK client and asserts
// ListTagsForResource sees what was supplied at creation (gopherstack-2mwl).
func TestCreateOps_TagsRoundTrip(t *testing.T) {
	t.Parallel()

	tags := []types.Tag{{Key: aws.String("env"), Value: aws.String("prod")}}

	newClient := func(t *testing.T) *ebsdk.Client {
		t.Helper()

		h := elasticbeanstalk.NewHandler(elasticbeanstalk.NewInMemoryBackend("123456789012", "us-east-1"))

		return newTestEBClient(t, h)
	}

	requireTags := func(t *testing.T, client *ebsdk.Client, resourceARN *string) {
		t.Helper()

		out, err := client.ListTagsForResource(t.Context(), &ebsdk.ListTagsForResourceInput{
			ResourceArn: resourceARN,
		})
		require.NoError(t, err)
		require.Len(t, out.ResourceTags, 1)
		assert.Equal(t, "env", aws.ToString(out.ResourceTags[0].Key))
		assert.Equal(t, "prod", aws.ToString(out.ResourceTags[0].Value))
	}

	t.Run("createapplication", func(t *testing.T) {
		t.Parallel()

		client := newClient(t)

		out, err := client.CreateApplication(t.Context(), &ebsdk.CreateApplicationInput{
			ApplicationName: aws.String("tagged-app"),
			Tags:            tags,
		})
		require.NoError(t, err)
		requireTags(t, client, out.Application.ApplicationArn)
	})

	t.Run("createapplicationversion", func(t *testing.T) {
		t.Parallel()

		client := newClient(t)

		_, err := client.CreateApplication(t.Context(), &ebsdk.CreateApplicationInput{
			ApplicationName: aws.String("tagged-appver-app"),
		})
		require.NoError(t, err)

		out, err := client.CreateApplicationVersion(t.Context(), &ebsdk.CreateApplicationVersionInput{
			ApplicationName: aws.String("tagged-appver-app"),
			VersionLabel:    aws.String("v1"),
			Tags:            tags,
		})
		require.NoError(t, err)
		requireTags(t, client, out.ApplicationVersion.ApplicationVersionArn)
	})

	t.Run("createconfigurationtemplate", func(t *testing.T) {
		t.Parallel()

		client := newClient(t)

		_, err := client.CreateApplication(t.Context(), &ebsdk.CreateApplicationInput{
			ApplicationName: aws.String("tagged-tmpl-app"),
		})
		require.NoError(t, err)

		_, err = client.CreateConfigurationTemplate(t.Context(), &ebsdk.CreateConfigurationTemplateInput{
			ApplicationName:   aws.String("tagged-tmpl-app"),
			TemplateName:      aws.String("tagged-tmpl"),
			SolutionStackName: aws.String("64bit Amazon Linux 2023 v4.0.0 running Python 3.11"),
			Tags:              tags,
		})
		require.NoError(t, err)

		// CreateConfigurationTemplateOutput carries no ARN (verified against
		// elasticbeanstalk@v1.37.4 ConfigurationSettingsDescription, which has
		// no ARN field) -- a real caller constructs it from the documented
		// "configurationtemplate/{app}/{template}" resource path, same as
		// gopherstack's own CreateConfigurationTemplate (configuration_templates.go:44).
		tmplARN := "arn:aws:elasticbeanstalk:us-east-1:123456789012:configurationtemplate/tagged-tmpl-app/tagged-tmpl"
		requireTags(t, client, aws.String(tmplARN))
	})

	t.Run("createenvironment", func(t *testing.T) {
		t.Parallel()

		client := newClient(t)

		_, err := client.CreateApplication(t.Context(), &ebsdk.CreateApplicationInput{
			ApplicationName: aws.String("tagged-env-app"),
		})
		require.NoError(t, err)

		out, err := client.CreateEnvironment(t.Context(), &ebsdk.CreateEnvironmentInput{
			ApplicationName:   aws.String("tagged-env-app"),
			EnvironmentName:   aws.String("tagged-env"),
			SolutionStackName: aws.String("64bit Amazon Linux 2023 v4.0.0 running Python 3.11"),
			Tags:              tags,
		})
		require.NoError(t, err)
		requireTags(t, client, out.EnvironmentArn)
	})

	t.Run("createplatformversion", func(t *testing.T) {
		t.Parallel()

		client := newClient(t)

		out, err := client.CreatePlatformVersion(t.Context(), &ebsdk.CreatePlatformVersionInput{
			PlatformName:    aws.String("tagged-platform"),
			PlatformVersion: aws.String("1.0.0"),
			PlatformDefinitionBundle: &types.S3Location{
				S3Bucket: aws.String("bucket"),
				S3Key:    aws.String("key"),
			},
			Tags: tags,
		})
		require.NoError(t, err)
		requireTags(t, client, out.PlatformSummary.PlatformArn)
	})
}
