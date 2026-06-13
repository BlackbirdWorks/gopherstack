package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	forecastsdk "github.com/aws/aws-sdk-go-v2/service/forecast"
	forecasttypes "github.com/aws/aws-sdk-go-v2/service/forecast/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createForecastClient returns a Forecast client pointed at the shared test container.
func createForecastClient(t *testing.T) *forecastsdk.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err, "unable to load SDK config")

	return forecastsdk.NewFromConfig(cfg, func(o *forecastsdk.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// TestIntegration_Forecast_DatasetGroupLifecycle drives create→describe→list→delete of a
// dataset group.
func TestIntegration_Forecast_DatasetGroupLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	tests := []struct {
		name      string
		groupName string
		domain    forecasttypes.Domain
	}{
		{name: "retail_group", groupName: "integ_group", domain: forecasttypes.DomainRetail},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			client := createForecastClient(t)

			createOut, err := client.CreateDatasetGroup(ctx, &forecastsdk.CreateDatasetGroupInput{
				DatasetGroupName: aws.String(tt.groupName),
				Domain:           tt.domain,
			})
			require.NoError(t, err, "CreateDatasetGroup should succeed")
			arn := aws.ToString(createOut.DatasetGroupArn)
			require.NotEmpty(t, arn, "dataset group ARN must be returned")

			t.Cleanup(func() {
				_, _ = client.DeleteDatasetGroup(
					ctx,
					&forecastsdk.DeleteDatasetGroupInput{DatasetGroupArn: aws.String(arn)},
				)
			})

			descOut, err := client.DescribeDatasetGroup(ctx, &forecastsdk.DescribeDatasetGroupInput{
				DatasetGroupArn: aws.String(arn),
			})
			require.NoError(t, err, "DescribeDatasetGroup should succeed")
			assert.Equal(t, tt.groupName, aws.ToString(descOut.DatasetGroupName))
			assert.Equal(t, tt.domain, descOut.Domain)

			listOut, err := client.ListDatasetGroups(ctx, &forecastsdk.ListDatasetGroupsInput{})
			require.NoError(t, err, "ListDatasetGroups should succeed")

			found := false
			for _, g := range listOut.DatasetGroups {
				if aws.ToString(g.DatasetGroupArn) == arn {
					found = true

					break
				}
			}

			assert.True(t, found, "created dataset group should appear in list")

			_, err = client.DeleteDatasetGroup(
				ctx,
				&forecastsdk.DeleteDatasetGroupInput{DatasetGroupArn: aws.String(arn)},
			)
			require.NoError(t, err, "DeleteDatasetGroup should succeed")
		})
	}
}
