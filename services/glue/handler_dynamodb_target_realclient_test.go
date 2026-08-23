package glue_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	gluesdk "github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSDKRoundTrip_GetCrawler_DynamoDBTargetScanFields drives CreateCrawler
// and GetCrawler through the real aws-sdk-go-v2 client and proves
// DynamoDBTarget.ScanAll and .ScanRate decode non-nil. Real Glue tags these
// "scanAll"/"scanRate" (lowerCamelCase) even though the sibling "Path"
// field stays PascalCase within the same type
// (deserializeDocumentDynamoDBTarget, glue@v1.152.0) -- gopherstack tagged
// both "ScanAll"/"ScanRate", so a real client's request AND response both
// silently dropped these two fields. Refs: gopherstack-v4a4.
func TestSDKRoundTrip_GetCrawler_DynamoDBTargetScanFields(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestGlueClient(t, h)

	dbRec := doGlueRequest(t, h, "CreateDatabase", map[string]any{
		"DatabaseInput": map[string]any{"Name": "db1"},
	})
	require.Equal(t, 200, dbRec.Code)

	_, err := client.CreateCrawler(t.Context(), &gluesdk.CreateCrawlerInput{
		Name:         aws.String("c1"),
		Role:         aws.String("arn:aws:iam::000000000000:role/glue"),
		DatabaseName: aws.String("db1"),
		Targets: &types.CrawlerTargets{
			DynamoDBTargets: []types.DynamoDBTarget{
				{
					Path:     aws.String("my-table"),
					ScanAll:  aws.Bool(true),
					ScanRate: aws.Float64(0.5),
				},
			},
		},
	})
	require.NoError(t, err)

	out, err := client.GetCrawler(t.Context(), &gluesdk.GetCrawlerInput{Name: aws.String("c1")})
	require.NoError(t, err)
	require.NotNil(t, out.Crawler)
	require.NotNil(t, out.Crawler.Targets)
	require.Len(t, out.Crawler.Targets.DynamoDBTargets, 1)

	target := out.Crawler.Targets.DynamoDBTargets[0]
	assert.Equal(t, "my-table", aws.ToString(target.Path))
	require.NotNil(t, target.ScanAll, "ScanAll must decode non-nil against the real SDK deserializer")
	assert.True(t, aws.ToBool(target.ScanAll))
	require.NotNil(t, target.ScanRate, "ScanRate must decode non-nil against the real SDK deserializer")
	assert.InDelta(t, 0.5, aws.ToFloat64(target.ScanRate), 0.0001)
}
