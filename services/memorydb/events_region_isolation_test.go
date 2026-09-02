package memorydb_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	memorydbsdk "github.com/aws/aws-sdk-go-v2/service/memorydb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDescribeEvents_RegionIsolation_RealClient proves DescribeEvents leaks
// every region's events to every caller. AddEvent/appendEventLocked already
// store each event under the region that generated it (CreateCluster et al.
// all call appendEventLocked(region, ...) with the request's own region),
// but DescribeEvents (events.go) discards its context parameter entirely and
// ranges over b.events -- a map keyed by region -- with no per-region scope
// at all. A real client in one region therefore sees every other region's
// event log too, not just a filtered slice of its own. This mirrors the
// cloudwatchlogs lookup-table region bug's second consequence (the listing
// side), not the identifier-collision side: memorydb's per-region resource
// creation is already region-correct, only this read path is not.
func TestDescribeEvents_RegionIsolation_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	eastClient := newMemorydbSDKClient(t, h, "us-east-1")
	westClient := newMemorydbSDKClient(t, h, "us-west-2")
	ctx := t.Context()

	_, err := eastClient.CreateCluster(ctx, &memorydbsdk.CreateClusterInput{
		ClusterName: aws.String("evt-iso-east"),
		NodeType:    aws.String("db.r6g.large"),
		ACLName:     aws.String("open-access"),
	})
	require.NoError(t, err)

	_, err = westClient.CreateCluster(ctx, &memorydbsdk.CreateClusterInput{
		ClusterName: aws.String("evt-iso-west"),
		NodeType:    aws.String("db.r6g.large"),
		ACLName:     aws.String("open-access"),
	})
	require.NoError(t, err)

	eastOut, err := eastClient.DescribeEvents(ctx, &memorydbsdk.DescribeEventsInput{})
	require.NoError(t, err)

	eastSources := make([]string, 0, len(eastOut.Events))
	for _, ev := range eastOut.Events {
		eastSources = append(eastSources, aws.ToString(ev.SourceName))
	}

	assert.Contains(t, eastSources, "evt-iso-east", "us-east-1 must see its own cluster's event")
	assert.NotContains(t, eastSources, "evt-iso-west", "us-east-1 must not see us-west-2's event")

	westOut, err := westClient.DescribeEvents(ctx, &memorydbsdk.DescribeEventsInput{})
	require.NoError(t, err)

	westSources := make([]string, 0, len(westOut.Events))
	for _, ev := range westOut.Events {
		westSources = append(westSources, aws.ToString(ev.SourceName))
	}

	assert.Contains(t, westSources, "evt-iso-west", "us-west-2 must see its own cluster's event")
	assert.NotContains(t, westSources, "evt-iso-east", "us-west-2 must not see us-east-1's event")
}
