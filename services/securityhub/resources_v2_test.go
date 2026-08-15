package securityhub_test

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	securityhubsdk "github.com/aws/aws-sdk-go-v2/service/securityhub"
	securityhubtypes "github.com/aws/aws-sdk-go-v2/service/securityhub/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/securityhub"
)

func TestGetResourcesV2_Empty(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/resourcesv2", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	resources, _ := resp["Resources"].([]any)
	assert.Empty(t, resources)
}

// seedResourceFindings imports two findings whose resources carry a Region
// field, through the raw ASFF BatchImportFindings wire (POST
// /findings/import) -- resources V2's group-by aggregation is derived from
// finding.Resources[], so this is the only way to populate it.
func seedResourceFindings(t *testing.T, h *securityhub.Handler) {
	t.Helper()

	rec := doRequest(t, h, http.MethodPost, "/findings/import", map[string]any{
		"Findings": []any{
			securityhub.ValidFinding(map[string]any{
				"Id": "resource-finding-1",
				"Resources": []any{
					map[string]any{"Type": "AwsEc2Instance", "Id": "i-1", "Region": "us-east-1"},
				},
			}),
			securityhub.ValidFinding(map[string]any{
				"Id": "resource-finding-2",
				"Resources": []any{
					map[string]any{"Type": "AwsS3Bucket", "Id": "bucket-2", "Region": "us-west-2"},
				},
			}),
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)
}

// TestGetResourcesStatisticsV2_RoundTrip drives GetResourcesStatisticsV2
// through the real SDK client. Before the fix, the handler read a fabricated
// body["GroupByAttributes"] ([]string) where the real required input member
// is GroupByRules ([]types.ResourceGroupByRule), and emitted "ResourceStatistics"
// where the real required output key is "GroupByResults" (securityhub@v1.75.4
// api_op_GetResourcesStatisticsV2.go:22-58) -- a real client's request
// grouped by nothing, and its response decoded a nil slice regardless.
func TestGetResourcesStatisticsV2_RoundTrip(t *testing.T) {
	t.Parallel()

	backend := securityhub.NewInMemoryBackend("000000000000", "us-east-1")
	h := securityhub.NewHandler(backend)
	seedResourceFindings(t, h)
	client := newTestSecurityHubClient(t, h)

	out, err := client.GetResourcesStatisticsV2(t.Context(), &securityhubsdk.GetResourcesStatisticsV2Input{
		GroupByRules: []securityhubtypes.ResourceGroupByRule{
			{GroupByField: securityhubtypes.ResourceGroupByFieldRegion},
		},
	})
	require.NoError(t, err)
	require.NotEmpty(t, out.GroupByResults,
		"unfixed handler emits ResourceStatistics where the real key is GroupByResults; SDK decodes a nil slice")

	result := out.GroupByResults[0]
	assert.Equal(t, "Region", aws.ToString(result.GroupByField))
	require.NotEmpty(t, result.GroupByValues)

	var total int32

	byRegion := make(map[string]int32)

	for _, v := range result.GroupByValues {
		total += aws.ToInt32(v.Count)
		byRegion[aws.ToString(v.FieldValue)] = aws.ToInt32(v.Count)
	}

	assert.Equal(t, int32(2), total)
	assert.Equal(t, int32(1), byRegion["us-east-1"])
	assert.Equal(t, int32(1), byRegion["us-west-2"])
}

// TestGetResourcesTrendsV2_RoundTrip drives GetResourcesTrendsV2 through the
// real SDK client. Before the fix, the handler read a fabricated
// body["GroupByAttribute"] (which the real GetResourcesTrendsV2Input doesn't
// have at all) and emitted "ResourcesTrends", dropping the required
// Granularity and TrendsMetrics members (securityhub@v1.75.4
// api_op_GetResourcesTrendsV2.go:22-58) -- a real client decoded a nil slice
// and an empty Granularity string.
func TestGetResourcesTrendsV2_RoundTrip(t *testing.T) {
	t.Parallel()

	backend := securityhub.NewInMemoryBackend("000000000000", "us-east-1")
	h := securityhub.NewHandler(backend)
	seedResourceFindings(t, h)
	client := newTestSecurityHubClient(t, h)

	start, err := time.Parse(time.RFC3339, "2024-01-01T00:00:00Z")
	require.NoError(t, err)
	end, err := time.Parse(time.RFC3339, "2024-01-02T00:00:00Z")
	require.NoError(t, err)

	out, err := client.GetResourcesTrendsV2(t.Context(), &securityhubsdk.GetResourcesTrendsV2Input{
		StartTime: aws.Time(start),
		EndTime:   aws.Time(end),
	})
	require.NoError(t, err)
	assert.NotEmpty(t, out.Granularity, "Granularity is required on the real wire")
	require.NotEmpty(t, out.TrendsMetrics,
		"unfixed handler emits ResourcesTrends where the real key is TrendsMetrics; SDK decodes a nil slice")

	point := out.TrendsMetrics[0]
	require.NotNil(t, point.TrendsValues)
	require.NotNil(t, point.TrendsValues.ResourcesCount)
	assert.Equal(t, int64(2), aws.ToInt64(point.TrendsValues.ResourcesCount.AllResources))
}
