package cloudformation_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cfnsdk "github.com/aws/aws-sdk-go-v2/service/cloudformation"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestListStackSetAutoDeploymentTargets_WireShape guards against
// gopherstack-6flj: ListStackSetAutoDeploymentTargets wrapped its list under
// "Targets", but the real response (cloudformation@v1.76.1 deserializers.go:
// awsAwsquery_deserializeOpDocumentListStackSetAutoDeploymentTargetsOutput)
// wraps it under "Summaries" -- a real client always decoded an empty slice
// no matter how many deployment targets existed.
func TestListStackSetAutoDeploymentTargets_WireShape(t *testing.T) {
	t.Parallel()

	client := newTestHandlerAndClient(t)

	_, err := client.CreateStackSet(t.Context(), &cfnsdk.CreateStackSetInput{
		StackSetName: aws.String("wire-shape-autodeploy"),
		TemplateBody: aws.String(simpleTemplate),
	})
	require.NoError(t, err)

	_, err = client.CreateStackInstances(t.Context(), &cfnsdk.CreateStackInstancesInput{
		StackSetName: aws.String("wire-shape-autodeploy"),
		Accounts:     []string{"123456789012"},
		Regions:      []string{"us-east-1"},
	})
	require.NoError(t, err)

	out, err := client.ListStackSetAutoDeploymentTargets(t.Context(), &cfnsdk.ListStackSetAutoDeploymentTargetsInput{
		StackSetName: aws.String("wire-shape-autodeploy"),
	})
	require.NoError(t, err)
	require.Len(t, out.Summaries, 1)
	assert.Equal(t, "123456789012", aws.ToString(out.Summaries[0].OrganizationalUnitId))
	assert.Equal(t, []string{"us-east-1"}, out.Summaries[0].Regions)
}

// TestListTypeVersions_WireShape guards against gopherstack-6flj:
// ListTypeVersions emitted the extension version ARN under "TypeArn", but the
// real TypeVersionSummary member (cloudformation@v1.76.1 types/types.go:3578)
// names it "Arn" -- a real client decoded Arn to nil for every version.
func TestListTypeVersions_WireShape(t *testing.T) {
	t.Parallel()

	backend, client := newTestHandlerAndClientWithBackend(t)

	_, err := backend.RegisterType("Acme::Widget::Thing", "s3://pkg.zip")
	require.NoError(t, err)

	out, err := client.ListTypeVersions(t.Context(), &cfnsdk.ListTypeVersionsInput{
		TypeName: aws.String("Acme::Widget::Thing"),
	})
	require.NoError(t, err)
	require.Len(t, out.TypeVersionSummaries, 1)
	assert.NotEmpty(t, aws.ToString(out.TypeVersionSummaries[0].Arn))
	assert.Equal(t, "00000001", aws.ToString(out.TypeVersionSummaries[0].VersionId))
}

// TestListResourceScanResources_WireShape guards against gopherstack-6flj:
// ScannedResource.ResourceIdentifier was emitted as a bare string under
// "ResourceIdentifier>member", but the real member is a string-to-string map
// serialized as "ResourceIdentifier>entry>key"/"entry>value"
// (cloudformation@v1.76.1 types/types.go:1470; deserializers.go:
// awsAwsquery_deserializeDocumentJazzResourceIdentifierPropertiesUnwrapped) --
// a real client decoded an empty map for every scanned resource.
func TestListResourceScanResources_WireShape(t *testing.T) {
	t.Parallel()

	client := newTestHandlerAndClient(t)

	scan, err := client.StartResourceScan(t.Context(), &cfnsdk.StartResourceScanInput{})
	require.NoError(t, err)

	out, err := client.ListResourceScanResources(t.Context(), &cfnsdk.ListResourceScanResourcesInput{
		ResourceScanId: scan.ResourceScanId,
	})
	require.NoError(t, err)
	require.Len(t, out.Resources, 1)
	assert.Equal(t, map[string]string{"Id": "example-bucket"}, out.Resources[0].ResourceIdentifier)
}
