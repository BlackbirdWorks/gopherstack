package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	mediatailorsdk "github.com/aws/aws-sdk-go-v2/service/mediatailor"
	mediatailortypes "github.com/aws/aws-sdk-go-v2/service/mediatailor/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createMediaTailorClient returns a MediaTailor client pointed at the shared test container.
func createMediaTailorClient(t *testing.T) *mediatailorsdk.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err, "unable to load SDK config")

	return mediatailorsdk.NewFromConfig(cfg, func(o *mediatailorsdk.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// TestIntegration_MediaTailor_SourceLocationLifecycle drives create→describe→list→delete of a
// source location, asserting the configured HTTP base URL round-trips.
func TestIntegration_MediaTailor_SourceLocationLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	tests := []struct {
		name    string
		slName  string
		baseURL string
	}{
		{name: "full_lifecycle", slName: "integ-sl", baseURL: "https://integ.example.com/vod/"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			client := createMediaTailorClient(t)

			createOut, err := client.CreateSourceLocation(ctx, &mediatailorsdk.CreateSourceLocationInput{
				SourceLocationName: aws.String(tt.slName),
				HttpConfiguration: &mediatailortypes.HttpConfiguration{
					BaseUrl: aws.String(tt.baseURL),
				},
			})
			require.NoError(t, err, "CreateSourceLocation should succeed")
			assert.Equal(t, tt.slName, aws.ToString(createOut.SourceLocationName))

			t.Cleanup(func() {
				cleanupCtx, cancel := cleanupContext(t)
				defer cancel()

				_, _ = client.DeleteSourceLocation(cleanupCtx, &mediatailorsdk.DeleteSourceLocationInput{
					SourceLocationName: aws.String(tt.slName),
				})
			})

			descOut, err := client.DescribeSourceLocation(ctx, &mediatailorsdk.DescribeSourceLocationInput{
				SourceLocationName: aws.String(tt.slName),
			})
			require.NoError(t, err, "DescribeSourceLocation should succeed")
			assert.Equal(t, tt.slName, aws.ToString(descOut.SourceLocationName))
			require.NotNil(t, descOut.HttpConfiguration)
			assert.Equal(t, tt.baseURL, aws.ToString(descOut.HttpConfiguration.BaseUrl))

			listOut, err := client.ListSourceLocations(ctx, &mediatailorsdk.ListSourceLocationsInput{})
			require.NoError(t, err, "ListSourceLocations should succeed")

			found := false
			for _, sl := range listOut.Items {
				if aws.ToString(sl.SourceLocationName) == tt.slName {
					found = true

					break
				}
			}

			assert.True(t, found, "created source location should appear in list")

			_, err = client.DeleteSourceLocation(ctx, &mediatailorsdk.DeleteSourceLocationInput{
				SourceLocationName: aws.String(tt.slName),
			})
			require.NoError(t, err, "DeleteSourceLocation should succeed")
		})
	}
}
