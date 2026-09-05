package dax_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	daxsdk "github.com/aws/aws-sdk-go-v2/service/dax"
	daxtypes "github.com/aws/aws-sdk-go-v2/service/dax/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/dax"
)

// TestListTags_SDKRoundTrip_PaginationSurvivesUntagBetweenPages drives
// ListTags through the real aws-sdk-go-v2 dax client, walking every page,
// deleting the tag the returned NextToken names in between page fetches --
// mirroring the arithmetic-level bug this pass found and fixed in
// ListTags (services/dax/tags.go): an exact-match cursor lookup that fell
// back to offset 0 (restarting from the beginning) whenever the named tag
// key was no longer present, instead of resuming at the next key in sorted
// order. Ties the pkgs/page-shaped unit tests in
// pagination_arithmetic_test.go / tags_test.go to observable client
// behaviour through the typed SDK client and its own deserializer.
func TestListTags_SDKRoundTrip_PaginationSurvivesUntagBetweenPages(t *testing.T) {
	t.Parallel()

	backend := dax.NewInMemoryBackend("123456789012", "us-east-1")
	h := dax.NewHandler(backend)
	client := newTestDAXSDKClient(t, h)

	const clusterName = "sdk-tag-pagination"

	tags := make(map[string]string, 15)
	for i := range 15 {
		tags[string([]byte{'a' + byte(i)})+"-key"] = "v"
	}

	created, err := client.CreateCluster(t.Context(), &daxsdk.CreateClusterInput{
		ClusterName:       aws.String(clusterName),
		NodeType:          aws.String("dax.r5.large"),
		IamRoleArn:        aws.String("arn:aws:iam::123456789012:role/DAXRole"),
		ReplicationFactor: 1,
		Tags:              toDaxTagSlice(tags),
	})
	require.NoError(t, err)
	clusterArn := aws.ToString(created.Cluster.ClusterArn)

	page1, err := client.ListTags(t.Context(), &daxsdk.ListTagsInput{ResourceName: aws.String(clusterArn)})
	require.NoError(t, err)
	require.Len(t, page1.Tags, 10)
	require.NotNil(t, page1.NextToken)

	// Delete the tag the cursor names before fetching the next page -- the
	// scenario the fixed bug mishandled.
	staleKey := aws.ToString(page1.NextToken)
	_, err = client.UntagResource(t.Context(), &daxsdk.UntagResourceInput{
		ResourceName: aws.String(clusterArn),
		TagKeys:      []string{staleKey},
	})
	require.NoError(t, err)

	page2, err := client.ListTags(t.Context(), &daxsdk.ListTagsInput{
		ResourceName: aws.String(clusterArn),
		NextToken:    page1.NextToken,
	})
	require.NoError(t, err)

	seen := make(map[string]bool, len(page1.Tags))
	for _, tg := range page1.Tags {
		seen[aws.ToString(tg.Key)] = true
	}

	for _, tg := range page2.Tags {
		assert.False(t, seen[aws.ToString(tg.Key)],
			"page2 must not repeat key %q already returned in page1", aws.ToString(tg.Key))
	}
}

func toDaxTagSlice(m map[string]string) []daxtypes.Tag {
	out := make([]daxtypes.Tag, 0, len(m))
	for k, v := range m {
		out = append(out, daxtypes.Tag{Key: aws.String(k), Value: aws.String(v)})
	}

	return out
}
