package guardduty_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	guarddutysdk "github.com/aws/aws-sdk-go-v2/service/guardduty"
	"github.com/aws/aws-sdk-go-v2/service/guardduty/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestGetCoverageStatistics_RoundTrip drives GetCoverageStatistics through a
// real SDK client and proves StatisticsType is actually honored -- requesting
// only COUNT_BY_RESOURCE_TYPE must not also compute/return
// countByCoverageStatus, instead of the pre-fix behavior where
// StatisticsType was ignored and everything was always computed regardless
// of what was requested (gopherstack-h910).
func TestGetCoverageStatistics_RoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestGuardDutyClient(t, h)

	created, err := client.CreateDetector(t.Context(), &guarddutysdk.CreateDetectorInput{
		Enable: aws.Bool(true),
	})
	require.NoError(t, err)
	detectorID := aws.ToString(created.DetectorId)

	byResourceType, err := client.GetCoverageStatistics(t.Context(), &guarddutysdk.GetCoverageStatisticsInput{
		DetectorId:     aws.String(detectorID),
		StatisticsType: []types.CoverageStatisticsType{types.CoverageStatisticsTypeCountByResourceType},
	})
	require.NoError(t, err)
	require.NotNil(t, byResourceType.CoverageStatistics)
	assert.NotNil(t, byResourceType.CoverageStatistics.CountByResourceType)
	assert.Nil(t, byResourceType.CoverageStatistics.CountByCoverageStatus)

	byCoverageStatus, err := client.GetCoverageStatistics(t.Context(), &guarddutysdk.GetCoverageStatisticsInput{
		DetectorId:     aws.String(detectorID),
		StatisticsType: []types.CoverageStatisticsType{types.CoverageStatisticsTypeCountByCoverageStatus},
	})
	require.NoError(t, err)
	require.NotNil(t, byCoverageStatus.CoverageStatistics)
	assert.NotNil(t, byCoverageStatus.CoverageStatistics.CountByCoverageStatus)
	assert.Nil(t, byCoverageStatus.CoverageStatistics.CountByResourceType)
}

// TestGetCoverageStatistics_ClientRequiresStatisticsType proves the real SDK
// client itself refuses to send GetCoverageStatistics without StatisticsType,
// confirming it is genuinely a required member on the pinned SDK, not an
// assumption (gopherstack-h910).
func TestGetCoverageStatistics_ClientRequiresStatisticsType(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestGuardDutyClient(t, h)

	_, err := client.GetCoverageStatistics(t.Context(), &guarddutysdk.GetCoverageStatisticsInput{
		DetectorId: aws.String("some-detector-id"),
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "StatisticsType")
}
