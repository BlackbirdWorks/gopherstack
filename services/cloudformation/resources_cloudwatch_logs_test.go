package cloudformation_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudformation"
	cwlogsbackend "github.com/blackbirdworks/gopherstack/services/cloudwatchlogs"
)

// TestResourceCreator_Extra_LogsResources verifies that Logs child resources are created in
// the real CloudWatch Logs backend and removed on delete.
func TestResourceCreator_Extra_LogsResources(t *testing.T) {
	t.Parallel()

	backends := newDependentServiceBackends(t)
	rc := cloudformation.NewResourceCreator(backends)
	ctx := t.Context()
	cw, ok := backends.CloudWatchLogs.Backend.(*cwlogsbackend.InMemoryBackend)
	require.True(t, ok)

	const group = "/aws/cfn/phase5"
	_, err := cw.CreateLogGroup(ctx, group, "", "")
	require.NoError(t, err)

	// LogStream round trip.
	streamPhys, err := rc.Create(ctx, "MyStream", "AWS::Logs::LogStream",
		map[string]any{"LogGroupName": group, "LogStreamName": "app-logs"}, nil, nil)
	require.NoError(t, err)

	streams, _, err := cw.DescribeLogStreams(ctx, group, "", "", "", false, 0)
	require.NoError(t, err)
	require.Len(t, streams, 1)
	assert.Equal(t, "app-logs", streams[0].LogStreamName)

	require.NoError(t, rc.Delete(ctx, "AWS::Logs::LogStream", streamPhys, nil))
	streams, _, err = cw.DescribeLogStreams(ctx, group, "", "", "", false, 0)
	require.NoError(t, err)
	assert.Empty(t, streams)

	// MetricFilter round trip.
	mfPhys, err := rc.Create(ctx, "MyMF", "AWS::Logs::MetricFilter",
		map[string]any{
			"LogGroupName":  group,
			"FilterName":    "errors",
			"FilterPattern": "ERROR",
			"MetricTransformations": []any{
				map[string]any{"MetricName": "ErrorCount", "MetricNamespace": "App", "MetricValue": "1"},
			},
		}, nil, nil)
	require.NoError(t, err)

	filters, _, err := cw.DescribeMetricFilters(ctx, group, "", "", "", "", 0)
	require.NoError(t, err)
	require.Len(t, filters, 1)
	assert.Equal(t, "errors", filters[0].FilterName)

	require.NoError(t, rc.Delete(ctx, "AWS::Logs::MetricFilter", mfPhys, nil))
	filters, _, err = cw.DescribeMetricFilters(ctx, group, "", "", "", "", 0)
	require.NoError(t, err)
	assert.Empty(t, filters)

	// QueryDefinition round trip.
	qdPhys, err := rc.Create(ctx, "MyQD", "AWS::Logs::QueryDefinition",
		map[string]any{"Name": "slow-queries", "QueryString": "fields @message", "LogGroupNames": []any{group}},
		nil, nil)
	require.NoError(t, err)
	require.NotEmpty(t, qdPhys)

	defs, _, err := cw.DescribeQueryDefinitions("", 0, "")
	require.NoError(t, err)
	require.Len(t, defs, 1)

	require.NoError(t, rc.Delete(ctx, "AWS::Logs::QueryDefinition", qdPhys, nil))
	defs, _, err = cw.DescribeQueryDefinitions("", 0, "")
	require.NoError(t, err)
	assert.Empty(t, defs)
}
