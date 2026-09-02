package sns_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	snssdk "github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sns/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/sns"
)

// TestTagResourceFamily_SDKRoundTrip drives TagResource, UntagResource, and
// ListTagsForResource through the real aws-sdk-go-v2 client
// (sns@v1.42.4, Query protocol) instead of hand-constructing form values, to
// prove the Query-encoded wire shape (Tags.member.N.Key/Value,
// TagKeys.member.N) the SDK actually sends decodes correctly end to end.
func TestTagResourceFamily_SDKRoundTrip(t *testing.T) {
	t.Parallel()

	b := sns.NewInMemoryBackend()
	topic, err := b.CreateTopic("tagfamily-topic", nil)
	require.NoError(t, err)
	topicArn := topic.TopicArn
	client := newTestSNSClient(t, sns.NewHandler(b))

	_, err = client.TagResource(t.Context(), &snssdk.TagResourceInput{
		ResourceArn: aws.String(topicArn),
		Tags: []types.Tag{
			{Key: aws.String("env"), Value: aws.String("prod")},
			{Key: aws.String("team"), Value: aws.String("platform")},
		},
	})
	require.NoError(t, err)

	listOut, err := client.ListTagsForResource(t.Context(), &snssdk.ListTagsForResourceInput{
		ResourceArn: aws.String(topicArn),
	})
	require.NoError(t, err)
	require.Len(t, listOut.Tags, 2)

	got := map[string]string{}
	for _, tag := range listOut.Tags {
		got[aws.ToString(tag.Key)] = aws.ToString(tag.Value)
	}
	assert.Equal(t, map[string]string{"env": "prod", "team": "platform"}, got)

	_, err = client.UntagResource(t.Context(), &snssdk.UntagResourceInput{
		ResourceArn: aws.String(topicArn),
		TagKeys:     []string{"team"},
	})
	require.NoError(t, err)

	listOut2, err := client.ListTagsForResource(t.Context(), &snssdk.ListTagsForResourceInput{
		ResourceArn: aws.String(topicArn),
	})
	require.NoError(t, err)
	require.Len(t, listOut2.Tags, 1)
	assert.Equal(t, "env", aws.ToString(listOut2.Tags[0].Key))
	assert.Equal(t, "prod", aws.ToString(listOut2.Tags[0].Value))
}
