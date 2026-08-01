package grafana_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	grafanasdk "github.com/aws/aws-sdk-go-v2/service/grafana"
	"github.com/stretchr/testify/require"
)

// TestTagResourceRoundTrip_ARNContainsSlash proves the /tags/{resourceArn}
// route survives a real Grafana workspace ARN, which embeds a literal "/"
// inside its resource segment (arn:aws:grafana:region:account:/workspaces/id
// -- see store.go's WorkspaceARN). The real aws-sdk-go-v2 client
// percent-encodes that "/" as %2F when building the request URI; if
// rawPathSegments naively split on literal "/" without per-segment
// url.PathUnescape, this would route as extra path segments and 404 instead
// of reaching TagResource/UntagResource/ListTagsForResource.
func TestTagResourceRoundTrip_ARNContainsSlash(t *testing.T) {
	t.Parallel()

	h, client := newTestHandlerAndClient(t)
	id := createActiveWorkspace(t, client, minimalCreateWorkspaceInput())

	resourceArn := h.Backend.WorkspaceARN(id)
	require.Contains(t, resourceArn, ":/workspaces/", "sanity check: the ARN really does embed a literal slash")

	_, err := client.TagResource(t.Context(), &grafanasdk.TagResourceInput{
		ResourceArn: aws.String(resourceArn),
		Tags:        map[string]string{"env": "prod", "team": "observability"},
	})
	require.NoError(t, err)

	list, err := client.ListTagsForResource(t.Context(), &grafanasdk.ListTagsForResourceInput{
		ResourceArn: aws.String(resourceArn),
	})
	require.NoError(t, err)
	require.Equal(t, map[string]string{"env": "prod", "team": "observability"}, list.Tags)

	// The workspace's own describe response must reflect the same tags.
	desc, err := client.DescribeWorkspace(t.Context(), &grafanasdk.DescribeWorkspaceInput{WorkspaceId: aws.String(id)})
	require.NoError(t, err)
	require.Equal(t, "prod", desc.Workspace.Tags["env"])

	_, err = client.UntagResource(t.Context(), &grafanasdk.UntagResourceInput{
		ResourceArn: aws.String(resourceArn),
		TagKeys:     []string{"team"},
	})
	require.NoError(t, err)

	after, err := client.ListTagsForResource(t.Context(), &grafanasdk.ListTagsForResourceInput{
		ResourceArn: aws.String(resourceArn),
	})
	require.NoError(t, err)
	require.Equal(t, map[string]string{"env": "prod"}, after.Tags)
}

// TestCreateWorkspace_InitialTags proves CreateWorkspaceInput.Tags seeds the
// same tag store TagResource/ListTagsForResource operate on.
func TestCreateWorkspace_InitialTags(t *testing.T) {
	t.Parallel()

	h, client := newTestHandlerAndClient(t)

	in := minimalCreateWorkspaceInput()
	in.Tags = map[string]string{"created-by": "ci"}

	out, err := client.CreateWorkspace(t.Context(), in)
	require.NoError(t, err)
	require.Equal(t, "ci", out.Workspace.Tags["created-by"])

	resourceArn := h.Backend.WorkspaceARN(aws.ToString(out.Workspace.Id))

	list, err := client.ListTagsForResource(t.Context(), &grafanasdk.ListTagsForResourceInput{
		ResourceArn: aws.String(resourceArn),
	})
	require.NoError(t, err)
	require.Equal(t, "ci", list.Tags["created-by"])
}

// TestTaggedResources_ResourceGroupsTaggingAPIIntegrationPoint proves the
// backend method cli.go's wireTaggingGrafana would call
// (resourcegroupstaggingapi's cross-service tag aggregation) returns every
// tagged workspace's ARN and tags -- the native-API round trip above proves
// the wire shape; this proves the cross-service integration point itself.
func TestTaggedResources_ResourceGroupsTaggingAPIIntegrationPoint(t *testing.T) {
	t.Parallel()

	h, client := newTestHandlerAndClient(t)
	id := createActiveWorkspace(t, client, minimalCreateWorkspaceInput())
	resourceArn := h.Backend.WorkspaceARN(id)

	_, err := client.TagResource(t.Context(), &grafanasdk.TagResourceInput{
		ResourceArn: aws.String(resourceArn),
		Tags:        map[string]string{"owner": "sre"},
	})
	require.NoError(t, err)

	entries := h.Backend.TaggedResources()
	require.Len(t, entries, 1)
	require.Equal(t, resourceArn, entries[0].ARN)
	require.Equal(t, "sre", entries[0].Tags["owner"])
}
