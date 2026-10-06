package cloudwatchlogs_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cwlsdk "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudwatchlogs"
)

// TestUpdateLogAnomalyDetector_FilterPattern covers UpdateLogAnomalyDetectorInput.FilterPattern.
func TestUpdateLogAnomalyDetector_FilterPattern(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		pattern *string
		want    string
	}{
		{name: "replaced", pattern: aws.String("ERROR"), want: "ERROR"},
		{name: "omitted_keeps", pattern: nil, want: "WARN"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestCloudWatchLogsClient(t, cloudwatchlogs.NewHandler(cloudwatchlogs.NewInMemoryBackend()))

			created, err := client.CreateLogAnomalyDetector(t.Context(), &cwlsdk.CreateLogAnomalyDetectorInput{
				LogGroupArnList: []string{"arn:aws:logs:us-east-1:123:log-group:/app"},
				FilterPattern:   aws.String("WARN"),
			})
			require.NoError(t, err)

			_, err = client.UpdateLogAnomalyDetector(t.Context(), &cwlsdk.UpdateLogAnomalyDetectorInput{
				AnomalyDetectorArn: created.AnomalyDetectorArn,
				Enabled:            aws.Bool(true),
				FilterPattern:      tt.pattern,
			})
			require.NoError(t, err)

			got, err := client.GetLogAnomalyDetector(t.Context(), &cwlsdk.GetLogAnomalyDetectorInput{
				AnomalyDetectorArn: created.AnomalyDetectorArn,
			})
			require.NoError(t, err)
			assert.Equal(t, tt.want, aws.ToString(got.FilterPattern))
		})
	}
}
