package forecast_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	forecastsdk "github.com/aws/aws-sdk-go-v2/service/forecast"
	"github.com/aws/aws-sdk-go-v2/service/forecast/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDescribeDatasetGroup_NoTagsLeak covers gopherstack-21my:
// DescribeDatasetGroupOutput (forecast@v1.44.4 api_op_DescribeDatasetGroup.go)
// declares no Tags member -- every forecast Create*Input accepts an optional
// Tags list, which the backend's create() stored verbatim in resource.Data,
// and resourceOutput (handler.go) clones resource.Data unscoped for
// Describe/Create-echo responses. A tagged resource's Describe response
// therefore carried a fabricated "Tags" member no real client would ever
// see; tags are only readable via ListTagsForResource, confirmed against
// every Describe*/Summary type in this module (none declares Tags).
func TestDescribeDatasetGroup_NoTagsLeak(t *testing.T) {
	t.Parallel()

	h := newHandler()
	client := newTestForecastClient(t, h)
	ctx := t.Context()

	created, err := client.CreateDatasetGroup(ctx, &forecastsdk.CreateDatasetGroupInput{
		DatasetGroupName: aws.String("no_tags_leak_dsg"),
		Domain:           types.DomainRetail,
		Tags:             []types.Tag{{Key: aws.String("env"), Value: aws.String("prod")}},
	})
	require.NoError(t, err)

	rec := doRequest(t, h, "DescribeDatasetGroup", map[string]any{
		"DatasetGroupArn": aws.ToString(created.DatasetGroupArn),
	})
	require.Equal(t, 200, rec.Code)
	assert.NotContains(t, rec.Body.String(), "Tags",
		"DescribeDatasetGroupOutput has no Tags member; tags are only readable via ListTagsForResource")

	tagsOut, err := client.ListTagsForResource(ctx, &forecastsdk.ListTagsForResourceInput{
		ResourceArn: created.DatasetGroupArn,
	})
	require.NoError(t, err)
	require.Len(t, tagsOut.Tags, 1)
	assert.Equal(t, "env", aws.ToString(tagsOut.Tags[0].Key))
}
