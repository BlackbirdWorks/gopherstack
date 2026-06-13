package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	guarddutysdk "github.com/aws/aws-sdk-go-v2/service/guardduty"
	guarddutytypes "github.com/aws/aws-sdk-go-v2/service/guardduty/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createGuardDutyClient returns a GuardDuty client pointed at the shared test container.
func createGuardDutyClient(t *testing.T) *guarddutysdk.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err, "unable to load SDK config")

	return guarddutysdk.NewFromConfig(cfg, func(o *guarddutysdk.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// TestIntegration_GuardDuty_DetectorLifecycle drives create→get→list→delete of a detector.
func TestIntegration_GuardDuty_DetectorLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	tests := []struct {
		name string
	}{
		{name: "full_lifecycle"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			client := createGuardDutyClient(t)

			createOut, err := client.CreateDetector(ctx, &guarddutysdk.CreateDetectorInput{
				Enable: aws.Bool(true),
			})
			require.NoError(t, err, "CreateDetector should succeed")
			detectorID := aws.ToString(createOut.DetectorId)
			require.NotEmpty(t, detectorID, "detector id must be returned")

			t.Cleanup(func() {
				_, _ = client.DeleteDetector(ctx, &guarddutysdk.DeleteDetectorInput{DetectorId: aws.String(detectorID)})
			})

			getOut, err := client.GetDetector(ctx, &guarddutysdk.GetDetectorInput{DetectorId: aws.String(detectorID)})
			require.NoError(t, err, "GetDetector should succeed")
			assert.Equal(t, guarddutytypes.DetectorStatusEnabled, getOut.Status)

			listOut, err := client.ListDetectors(ctx, &guarddutysdk.ListDetectorsInput{})
			require.NoError(t, err, "ListDetectors should succeed")
			assert.Contains(t, listOut.DetectorIds, detectorID, "created detector should appear in list")

			_, err = client.DeleteDetector(ctx, &guarddutysdk.DeleteDetectorInput{DetectorId: aws.String(detectorID)})
			require.NoError(t, err, "DeleteDetector should succeed")
		})
	}
}

// TestIntegration_GuardDuty_FilterLifecycle drives detector→filter create→get→list→delete.
func TestIntegration_GuardDuty_FilterLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	tests := []struct {
		name       string
		filterName string
	}{
		{name: "full_lifecycle", filterName: "integ-filter"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			client := createGuardDutyClient(t)

			detOut, err := client.CreateDetector(ctx, &guarddutysdk.CreateDetectorInput{Enable: aws.Bool(true)})
			require.NoError(t, err, "CreateDetector should succeed")
			detectorID := aws.ToString(detOut.DetectorId)

			t.Cleanup(func() {
				_, _ = client.DeleteDetector(ctx, &guarddutysdk.DeleteDetectorInput{DetectorId: aws.String(detectorID)})
			})

			_, err = client.CreateFilter(ctx, &guarddutysdk.CreateFilterInput{
				DetectorId: aws.String(detectorID),
				Name:       aws.String(tt.filterName),
				FindingCriteria: &guarddutytypes.FindingCriteria{
					Criterion: map[string]guarddutytypes.Condition{
						"severity": {GreaterThanOrEqual: aws.Int64(7)},
					},
				},
			})
			require.NoError(t, err, "CreateFilter should succeed")

			getOut, err := client.GetFilter(ctx, &guarddutysdk.GetFilterInput{
				DetectorId: aws.String(detectorID),
				FilterName: aws.String(tt.filterName),
			})
			require.NoError(t, err, "GetFilter should succeed")
			assert.Equal(t, tt.filterName, aws.ToString(getOut.Name))

			listOut, err := client.ListFilters(ctx, &guarddutysdk.ListFiltersInput{DetectorId: aws.String(detectorID)})
			require.NoError(t, err, "ListFilters should succeed")
			assert.Contains(t, listOut.FilterNames, tt.filterName, "created filter should appear in list")

			_, err = client.DeleteFilter(ctx, &guarddutysdk.DeleteFilterInput{
				DetectorId: aws.String(detectorID),
				FilterName: aws.String(tt.filterName),
			})
			require.NoError(t, err, "DeleteFilter should succeed")
		})
	}
}
