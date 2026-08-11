package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	elasticbeanstalksdk "github.com/aws/aws-sdk-go-v2/service/elasticbeanstalk"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIntegration_ElasticBeanstalk_ApplicationAndVersionLifecycle exercises
// the core Elastic Beanstalk control-plane workflow via the AWS SDK v2:
// create an application, describe/list it, create an application version,
// describe versions, then delete. Primary integration coverage for the EB
// XML query-string handler.
func TestIntegration_ElasticBeanstalk_ApplicationAndVersionLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createElasticBeanstalkClient(t)
	ctx := t.Context()

	const (
		appName     = "it-eb-app"
		versionName = "it-eb-v1"
	)

	// CreateApplication.
	createOut, err := client.CreateApplication(ctx, &elasticbeanstalksdk.CreateApplicationInput{
		ApplicationName: aws.String(appName),
		Description:     aws.String("integration test app"),
	})
	require.NoError(t, err)
	require.NotNil(t, createOut.Application)
	assert.Equal(t, appName, aws.ToString(createOut.Application.ApplicationName))

	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext(t)
		defer cancel()

		_, _ = client.DeleteApplication(cleanupCtx, &elasticbeanstalksdk.DeleteApplicationInput{
			ApplicationName:     aws.String(appName),
			TerminateEnvByForce: aws.Bool(true),
		})
	})

	// DescribeApplications.
	descOut, err := client.DescribeApplications(ctx, &elasticbeanstalksdk.DescribeApplicationsInput{
		ApplicationNames: []string{appName},
	})
	require.NoError(t, err)
	require.Len(t, descOut.Applications, 1)
	assert.Equal(t, appName, aws.ToString(descOut.Applications[0].ApplicationName))

	// CreateApplicationVersion.
	createVerOut, err := client.CreateApplicationVersion(
		ctx,
		&elasticbeanstalksdk.CreateApplicationVersionInput{
			ApplicationName: aws.String(appName),
			VersionLabel:    aws.String(versionName),
			Description:     aws.String("v1"),
		},
	)
	require.NoError(t, err)
	require.NotNil(t, createVerOut.ApplicationVersion)
	assert.Equal(t, versionName, aws.ToString(createVerOut.ApplicationVersion.VersionLabel))

	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext(t)
		defer cancel()

		_, _ = client.DeleteApplicationVersion(cleanupCtx, &elasticbeanstalksdk.DeleteApplicationVersionInput{
			ApplicationName: aws.String(appName),
			VersionLabel:    aws.String(versionName),
		})
	})

	// DescribeApplicationVersions.
	descVerOut, err := client.DescribeApplicationVersions(
		ctx,
		&elasticbeanstalksdk.DescribeApplicationVersionsInput{
			ApplicationName: aws.String(appName),
			VersionLabels:   []string{versionName},
		},
	)
	require.NoError(t, err)
	require.Len(t, descVerOut.ApplicationVersions, 1)
	assert.Equal(t, versionName, aws.ToString(descVerOut.ApplicationVersions[0].VersionLabel))
}
