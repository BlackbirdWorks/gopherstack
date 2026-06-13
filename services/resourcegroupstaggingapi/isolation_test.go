package resourcegroupstaggingapi //nolint:testpackage // needs access to unexported regionContextKey.

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ctxRegion returns a context carrying the given AWS region under regionContextKey,
// mirroring what the HTTP handler injects from the SigV4 credential scope.
func ctxRegion(region string) context.Context {
	return context.WithValue(context.Background(), regionContextKey{}, region)
}

// TestResourceGroupsTaggingAPIRegionIsolation proves that same-named resources
// registered via providers in two regions are fully isolated: each region sees only
// its own resources, and report-creation state is isolated per region.
func TestResourceGroupsTaggingAPIRegionIsolation(t *testing.T) {
	t.Parallel()

	backend := NewInMemoryBackend("000000000000", "us-east-1")

	ctxEast := ctxRegion("us-east-1")
	ctxWest := ctxRegion("us-west-2")

	const queueName = "shared-queue"

	eastARN := "arn:aws:sqs:us-east-1:000000000000:" + queueName
	westARN := "arn:aws:sqs:us-west-2:000000000000:" + queueName

	// Register a region-aware provider that returns resources based on ctx region.
	backend.RegisterProvider(func(ctx context.Context) []TaggedResource {
		switch getRegion(ctx, backend.defaultRegion) {
		case "us-east-1":
			return []TaggedResource{{
				ResourceARN:  eastARN,
				ResourceType: "sqs:queue",
				Tags:         map[string]string{"env": "east"},
			}}
		case "us-west-2":
			return []TaggedResource{{
				ResourceARN:  westARN,
				ResourceType: "sqs:queue",
				Tags:         map[string]string{"env": "west"},
			}}
		default:
			return nil
		}
	})

	// 1. Each region sees only its own resource.
	eastOut, err := backend.GetResources(ctxEast, &GetResourcesInput{})
	require.NoError(t, err)
	require.Len(t, eastOut.ResourceTagMappingList, 1)
	assert.Equal(t, eastARN, eastOut.ResourceTagMappingList[0].ResourceARN)
	require.Len(t, eastOut.ResourceTagMappingList[0].Tags, 1)
	assert.Equal(t, "east", eastOut.ResourceTagMappingList[0].Tags[0].Value)

	westOut, err := backend.GetResources(ctxWest, &GetResourcesInput{})
	require.NoError(t, err)
	require.Len(t, westOut.ResourceTagMappingList, 1)
	assert.Equal(t, westARN, westOut.ResourceTagMappingList[0].ResourceARN)
	require.Len(t, westOut.ResourceTagMappingList[0].Tags, 1)
	assert.Equal(t, "west", westOut.ResourceTagMappingList[0].Tags[0].Value)

	// 2. GetTagKeys is isolated: each region returns tag keys from its own resources.
	eastKeys := backend.GetTagKeys(ctxEast, &GetTagKeysInput{})
	assert.Equal(t, []string{"env"}, eastKeys.TagKeys)

	westKeys := backend.GetTagKeys(ctxWest, &GetTagKeysInput{})
	assert.Equal(t, []string{"env"}, westKeys.TagKeys)

	// 3. GetTagValues is isolated: us-east-1 sees "east", us-west-2 sees "west".
	envKey := "env"
	eastVals := backend.GetTagValues(ctxEast, &GetTagValuesInput{Key: &envKey})
	assert.Equal(t, []string{"east"}, eastVals.TagValues)

	westVals := backend.GetTagValues(ctxWest, &GetTagValuesInput{Key: &envKey})
	assert.Equal(t, []string{"west"}, westVals.TagValues)

	// 4. StartReportCreation/DescribeReportCreation are isolated per region.
	_, err = backend.StartReportCreation(ctxEast, &StartReportCreationInput{S3Bucket: "east-bucket"})
	require.NoError(t, err)

	eastReport := backend.DescribeReportCreation(ctxEast)
	require.NotNil(t, eastReport.Status)
	assert.Equal(t, reportStatusSucceeded, *eastReport.Status)
	require.NotNil(t, eastReport.S3Location)
	assert.Contains(t, *eastReport.S3Location, "east-bucket")

	// us-west-2 has no report yet.
	westReport := backend.DescribeReportCreation(ctxWest)
	require.NotNil(t, westReport.Status)
	assert.Equal(t, reportStatusNoReport, *westReport.Status)

	// 5. Reset clears all regions.
	backend.Reset()
	assert.Equal(t, reportStatusNoReport, *backend.DescribeReportCreation(ctxEast).Status)
}
