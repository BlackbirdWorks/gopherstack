package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cloudcontrolsdk "github.com/aws/aws-sdk-go-v2/service/cloudcontrol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIntegration_CloudControl_ResourceLifecycle exercises the core
// AWS CloudControl API workflow via the AWS SDK v2: create a resource,
// get it back, list resources of that type. Primary integration coverage
// for the CloudControl JSON-RPC handler.
func TestIntegration_CloudControl_ResourceLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createCloudControlClient(t)
	ctx := t.Context()

	const (
		typeName     = "AWS::S3::Bucket"
		desiredState = `{"BucketName":"it-cloudcontrol-bucket"}`
		identifier   = "it-cloudcontrol-bucket"
	)

	// CreateResource.
	createOut, err := client.CreateResource(ctx, &cloudcontrolsdk.CreateResourceInput{
		TypeName:     aws.String(typeName),
		DesiredState: aws.String(desiredState),
	})
	require.NoError(t, err)
	require.NotNil(t, createOut.ProgressEvent)

	t.Cleanup(func() {
		_, _ = client.DeleteResource(ctx, &cloudcontrolsdk.DeleteResourceInput{
			TypeName:   aws.String(typeName),
			Identifier: aws.String(identifier),
		})
	})

	// GetResource.
	getOut, err := client.GetResource(ctx, &cloudcontrolsdk.GetResourceInput{
		TypeName:   aws.String(typeName),
		Identifier: aws.String(identifier),
	})
	require.NoError(t, err)
	require.NotNil(t, getOut.ResourceDescription)
	assert.Equal(t, identifier, aws.ToString(getOut.ResourceDescription.Identifier))

	// ListResources should include the newly created resource.
	listOut, err := client.ListResources(ctx, &cloudcontrolsdk.ListResourcesInput{
		TypeName: aws.String(typeName),
	})
	require.NoError(t, err)

	foundResource := false

	for _, r := range listOut.ResourceDescriptions {
		if aws.ToString(r.Identifier) == identifier {
			foundResource = true

			break
		}
	}

	assert.True(t, foundResource, "newly created resource should be listed")
}
