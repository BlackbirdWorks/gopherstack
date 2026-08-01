package outposts_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	outpostssdk "github.com/aws/aws-sdk-go-v2/service/outposts"
	"github.com/stretchr/testify/require"
)

func TestTagResource_OutpostAndSite(t *testing.T) {
	t.Parallel()

	h, client := newTestHandlerAndClient(t)
	siteID := createTestSite(t, client)
	created := createTestOutpost(t, client, siteID)

	_, err := client.TagResource(t.Context(), &outpostssdk.TagResourceInput{
		ResourceArn: created.OutpostArn,
		Tags:        map[string]string{"env": "prod"},
	})
	require.NoError(t, err)

	got, err := client.ListTagsForResource(t.Context(), &outpostssdk.ListTagsForResourceInput{
		ResourceArn: created.OutpostArn,
	})
	require.NoError(t, err)
	require.Equal(t, map[string]string{"env": "prod"}, got.Tags)

	site, err := client.GetSite(t.Context(), &outpostssdk.GetSiteInput{SiteId: aws.String(siteID)})
	require.NoError(t, err)

	_, err = client.TagResource(t.Context(), &outpostssdk.TagResourceInput{
		ResourceArn: site.Site.SiteArn,
		Tags:        map[string]string{"team": "platform"},
	})
	require.NoError(t, err)

	siteTags, err := client.ListTagsForResource(t.Context(), &outpostssdk.ListTagsForResourceInput{
		ResourceArn: site.Site.SiteArn,
	})
	require.NoError(t, err)
	require.Equal(t, map[string]string{"team": "platform"}, siteTags.Tags)

	// TaggedResources (the resourcegroupstaggingapi cross-service hook) must
	// see both.
	all := h.Backend.TaggedResources()
	require.Len(t, all, 2)
}

func TestUntagResource(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)
	siteID := createTestSite(t, client)
	created := createTestOutpost(t, client, siteID)

	_, err := client.TagResource(t.Context(), &outpostssdk.TagResourceInput{
		ResourceArn: created.OutpostArn,
		Tags:        map[string]string{"a": "1", "b": "2"},
	})
	require.NoError(t, err)

	_, err = client.UntagResource(t.Context(), &outpostssdk.UntagResourceInput{
		ResourceArn: created.OutpostArn,
		TagKeys:     []string{"a"},
	})
	require.NoError(t, err)

	got, err := client.ListTagsForResource(t.Context(), &outpostssdk.ListTagsForResourceInput{
		ResourceArn: created.OutpostArn,
	})
	require.NoError(t, err)
	require.Equal(t, map[string]string{"b": "2"}, got.Tags)
}

func TestTagResource_UnknownARN(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)

	_, err := client.TagResource(t.Context(), &outpostssdk.TagResourceInput{
		ResourceArn: aws.String("arn:aws:outposts:us-east-1:000000000000:outpost/op-does-not-exist"),
		Tags:        map[string]string{"a": "1"},
	})
	require.Error(t, err)
}
