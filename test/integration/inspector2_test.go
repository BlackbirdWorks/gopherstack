package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	inspector2sdk "github.com/aws/aws-sdk-go-v2/service/inspector2"
	inspector2types "github.com/aws/aws-sdk-go-v2/service/inspector2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createInspector2Client returns an Inspector2 client pointed at the shared test container.
func createInspector2Client(t *testing.T) *inspector2sdk.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err, "unable to load SDK config")

	return inspector2sdk.NewFromConfig(cfg, func(o *inspector2sdk.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// TestIntegration_Inspector2_FilterLifecycle drives create→list→delete of a suppression filter.
func TestIntegration_Inspector2_FilterLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	tests := []struct {
		name       string
		filterName string
	}{
		{name: "suppress_filter", filterName: "integ-filter"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			client := createInspector2Client(t)

			createOut, err := client.CreateFilter(ctx, &inspector2sdk.CreateFilterInput{
				Name:   aws.String(tt.filterName),
				Action: inspector2types.FilterActionSuppress,
				FilterCriteria: &inspector2types.FilterCriteria{
					Severity: []inspector2types.StringFilter{
						{Comparison: inspector2types.StringComparisonEquals, Value: aws.String("HIGH")},
					},
				},
			})
			require.NoError(t, err, "CreateFilter should succeed")
			filterArn := aws.ToString(createOut.Arn)
			require.NotEmpty(t, filterArn, "filter ARN must be returned")

			t.Cleanup(func() {
				_, _ = client.DeleteFilter(ctx, &inspector2sdk.DeleteFilterInput{Arn: aws.String(filterArn)})
			})

			listOut, err := client.ListFilters(ctx, &inspector2sdk.ListFiltersInput{})
			require.NoError(t, err, "ListFilters should succeed")

			found := false
			for _, f := range listOut.Filters {
				if aws.ToString(f.Arn) == filterArn {
					found = true
					assert.Equal(t, tt.filterName, aws.ToString(f.Name))

					break
				}
			}

			assert.True(t, found, "created filter should appear in list")

			_, err = client.DeleteFilter(ctx, &inspector2sdk.DeleteFilterInput{Arn: aws.String(filterArn)})
			require.NoError(t, err, "DeleteFilter should succeed")
		})
	}
}
