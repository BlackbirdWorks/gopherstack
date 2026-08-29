package rds_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	rdssdk "github.com/aws/aws-sdk-go-v2/service/rds"
	"github.com/aws/aws-sdk-go-v2/service/rds/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTagResourceFamily_SDKRoundTrip drives AddTagsToResource,
// RemoveTagsFromResource, and ListTagsForResource through the real
// aws-sdk-go-v2 client (rds@v1.124.1, Query protocol) instead of
// hand-constructing form values, to prove the Query-encoded wire shape
// (Tags.Tag.N.Key/Value, TagKeys.member.N) the SDK actually sends decodes
// correctly end to end.
func TestTagResourceFamily_SDKRoundTrip(t *testing.T) {
	t.Parallel()

	client := newTestRDSClient(t, newTestRDSHandler())

	out, err := client.CreateDBInstance(t.Context(), &rdssdk.CreateDBInstanceInput{
		DBInstanceIdentifier: aws.String("tagfamily-db"),
		DBInstanceClass:      aws.String("db.t3.micro"),
		Engine:               aws.String("postgres"),
	})
	require.NoError(t, err)
	arn := out.DBInstance.DBInstanceArn

	_, err = client.AddTagsToResource(t.Context(), &rdssdk.AddTagsToResourceInput{
		ResourceName: arn,
		Tags: []types.Tag{
			{Key: aws.String("env"), Value: aws.String("prod")},
			{Key: aws.String("team"), Value: aws.String("platform")},
		},
	})
	require.NoError(t, err)

	listOut, err := client.ListTagsForResource(t.Context(), &rdssdk.ListTagsForResourceInput{ResourceName: arn})
	require.NoError(t, err)
	require.Len(t, listOut.TagList, 2)

	got := map[string]string{}
	for _, tag := range listOut.TagList {
		got[aws.ToString(tag.Key)] = aws.ToString(tag.Value)
	}
	assert.Equal(t, map[string]string{"env": "prod", "team": "platform"}, got)

	_, err = client.RemoveTagsFromResource(t.Context(), &rdssdk.RemoveTagsFromResourceInput{
		ResourceName: arn,
		TagKeys:      []string{"team"},
	})
	require.NoError(t, err)

	listOut2, err := client.ListTagsForResource(t.Context(), &rdssdk.ListTagsForResourceInput{ResourceName: arn})
	require.NoError(t, err)
	require.Len(t, listOut2.TagList, 1)
	assert.Equal(t, "env", aws.ToString(listOut2.TagList[0].Key))
	assert.Equal(t, "prod", aws.ToString(listOut2.TagList[0].Value))
}
