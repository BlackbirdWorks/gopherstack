package cloudwatchlogs_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudwatchlogs"
)

func TestCloudWatchLogsBackend_LogAnomalyDetectorLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr error
		setup   func(b *cloudwatchlogs.InMemoryBackend)
		name    string
		arnToOp string
		op      string
		newFreq string
	}{
		{
			name: "list_all",
			setup: func(b *cloudwatchlogs.InMemoryBackend) {
				cloudwatchlogs.AddLogAnomalyDetectorInternal(b, cloudwatchlogs.LogAnomalyDetector{
					AnomalyDetectorArn: "arn:aws:logs:::detector/1",
					CreationTimeStamp:  1,
				})
				cloudwatchlogs.AddLogAnomalyDetectorInternal(b, cloudwatchlogs.LogAnomalyDetector{
					AnomalyDetectorArn: "arn:aws:logs:::detector/2",
					CreationTimeStamp:  2,
				})
			},
			op: "list",
		},
		{
			name: "delete_existing",
			setup: func(b *cloudwatchlogs.InMemoryBackend) {
				cloudwatchlogs.AddLogAnomalyDetectorInternal(b, cloudwatchlogs.LogAnomalyDetector{
					AnomalyDetectorArn: "arn:aws:logs:::detector/1",
				})
			},
			op:      "delete",
			arnToOp: "arn:aws:logs:::detector/1",
		},
		{
			name:    "delete_missing",
			op:      "delete",
			arnToOp: "arn:aws:logs:::detector/nonexistent",
			wantErr: cloudwatchlogs.ErrLogAnomalyDetectorNotFound,
		},
		{
			name: "update_freq",
			setup: func(b *cloudwatchlogs.InMemoryBackend) {
				cloudwatchlogs.AddLogAnomalyDetectorInternal(b, cloudwatchlogs.LogAnomalyDetector{
					AnomalyDetectorArn:  "arn:aws:logs:::detector/1",
					EvaluationFrequency: "FIVE_MIN",
				})
			},
			op:      "update",
			arnToOp: "arn:aws:logs:::detector/1",
			newFreq: "ONE_HOUR",
		},
		{
			name: "update_invalid_freq",
			setup: func(b *cloudwatchlogs.InMemoryBackend) {
				cloudwatchlogs.AddLogAnomalyDetectorInternal(b, cloudwatchlogs.LogAnomalyDetector{
					AnomalyDetectorArn: "arn:aws:logs:::detector/1",
				})
			},
			op:      "update",
			arnToOp: "arn:aws:logs:::detector/1",
			newFreq: "INVALID",
			wantErr: cloudwatchlogs.ErrValidation,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudwatchlogs.NewInMemoryBackend()
			if tt.setup != nil {
				tt.setup(b)
			}

			var err error
			switch tt.op {
			case "list":
				var detectors []cloudwatchlogs.LogAnomalyDetector
				detectors, _, err = b.ListLogAnomalyDetectors(nil, 50, "")
				require.NoError(t, err)
				assert.Len(t, detectors, 2)

				return
			case "delete":
				err = b.DeleteLogAnomalyDetector(tt.arnToOp)
			case "update":
				err = b.UpdateLogAnomalyDetector(tt.arnToOp, tt.newFreq, 0, true)
			}

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}
			require.NoError(t, err)
		})
	}
}

func TestCloudWatchLogsBackend_GetLogAnomalyDetector(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr     error
		setup       func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) string
		name        string
		detectorArn string
	}{
		{
			name: "success",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) string {
				t.Helper()
				arn, err := b.CreateLogAnomalyDetector(
					[]string{"arn:aws:logs:us-east-1:123:log-group:test"}, "det", "", "", "", 0,
				)
				require.NoError(t, err)

				return arn
			},
		},
		{
			name:        "not_found",
			detectorArn: "arn:aws:logs:us-east-1:123:log-anomaly-detector:nonexistent",
			wantErr:     cloudwatchlogs.ErrLogAnomalyDetectorNotFound,
		},
		{
			name:    "empty_arn",
			wantErr: cloudwatchlogs.ErrValidation,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudwatchlogs.NewInMemoryBackend()
			arn := tt.detectorArn
			if tt.setup != nil {
				arn = tt.setup(t, b)
			}

			d, err := b.GetLogAnomalyDetector(arn)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
			assert.NotNil(t, d)
			assert.Equal(t, arn, d.AnomalyDetectorArn)
		})
	}
}

func TestCloudWatchLogsBackend_ListAnomalies(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr            error
		setup              func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) string
		name               string
		anomalyDetectorArn string
		wantCount          int
		wantNextEmpty      bool
	}{
		{
			name:               "empty_arn_returns_empty",
			anomalyDetectorArn: "",
			wantNextEmpty:      true,
		},
		{
			name: "valid_detector_no_anomalies",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) string {
				t.Helper()
				arn, err := b.CreateLogAnomalyDetector(
					[]string{"arn:aws:logs:us-east-1:123:log-group:test"}, "det", "", "", "", 0,
				)
				require.NoError(t, err)

				return arn
			},
			wantNextEmpty: true,
		},
		{
			name: "returns_seeded_anomalies",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) string {
				t.Helper()
				detectorArn, err := b.CreateLogAnomalyDetector(
					[]string{"arn:aws:logs:us-east-1:123:log-group:test"}, "det", "", "", "", 0,
				)
				require.NoError(t, err)
				cloudwatchlogs.AddAnomalyInternal(b, cloudwatchlogs.Anomaly{
					AnomalyDetectorArn: detectorArn,
					AnomalyID:          "anomaly-1",
					Description:        "spike in errors",
					FirstSeen:          1000,
					LastSeen:           2000,
					Active:             true,
				})
				cloudwatchlogs.AddAnomalyInternal(b, cloudwatchlogs.Anomaly{
					AnomalyDetectorArn: detectorArn,
					AnomalyID:          "anomaly-2",
					Description:        "unusual pattern",
					FirstSeen:          3000,
					LastSeen:           4000,
					Active:             true,
				})

				return detectorArn
			},
			wantCount:     2,
			wantNextEmpty: true,
		},
		{
			name: "pagination_returns_token",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) string {
				t.Helper()
				detectorArn, err := b.CreateLogAnomalyDetector(
					[]string{"arn:aws:logs:us-east-1:123:log-group:test"}, "det", "", "", "", 0,
				)
				require.NoError(t, err)
				for i := range 5 {
					cloudwatchlogs.AddAnomalyInternal(b, cloudwatchlogs.Anomaly{
						AnomalyDetectorArn: detectorArn,
						AnomalyID:          fmt.Sprintf("anomaly-%d", i),
						FirstSeen:          int64(i * 1000),
					})
				}

				return detectorArn
			},
			wantCount:     2,
			wantNextEmpty: false,
		},
		{
			name:               "detector_not_found",
			anomalyDetectorArn: "arn:aws:logs:us-east-1:123:log-anomaly-detector:nonexistent",
			wantErr:            cloudwatchlogs.ErrLogAnomalyDetectorNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudwatchlogs.NewInMemoryBackend()
			arn := tt.anomalyDetectorArn
			if tt.setup != nil {
				arn = tt.setup(t, b)
			}

			limit := 10
			if tt.name == "pagination_returns_token" {
				limit = 2
			}

			anomalies, next, err := b.ListAnomalies(arn, limit, "")

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
			assert.Len(t, anomalies, tt.wantCount)
			if tt.wantNextEmpty {
				assert.Empty(t, next)
			} else {
				assert.NotEmpty(t, next)
			}
		})
	}
}

func TestCloudWatchLogsBackend_UpdateAnomaly(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr            error
		setup              func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) string
		name               string
		anomalyDetectorArn string
		anomalyID          string
		suppressionType    string
		checkSuppression   bool
	}{
		{
			// Real AWS: omitting suppressionType (empty string) ends any
			// current suppression -- there is no "NO_SUPPRESSION" enum value
			// on the wire.
			name: "success_no_suppression",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) string {
				t.Helper()
				detectorArn, err := b.CreateLogAnomalyDetector(
					[]string{"arn:aws:logs:us-east-1:123:log-group:test"}, "det", "", "", "", 0,
				)
				require.NoError(t, err)
				cloudwatchlogs.AddAnomalyInternal(b, cloudwatchlogs.Anomaly{
					AnomalyDetectorArn: detectorArn,
					AnomalyID:          "anomaly-1",
					Active:             true,
				})

				return detectorArn
			},
			anomalyID: "anomaly-1",
		},
		{
			name: "invalid_suppression_type",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) string {
				t.Helper()
				detectorArn, err := b.CreateLogAnomalyDetector(
					[]string{"arn:aws:logs:us-east-1:123:log-group:test"}, "det", "", "", "", 0,
				)
				require.NoError(t, err)
				cloudwatchlogs.AddAnomalyInternal(b, cloudwatchlogs.Anomaly{
					AnomalyDetectorArn: detectorArn,
					AnomalyID:          "anomaly-1",
					Active:             true,
				})

				return detectorArn
			},
			anomalyID:       "anomaly-1",
			suppressionType: "NO_SUPPRESSION",
			wantErr:         cloudwatchlogs.ErrValidation,
		},
		{
			name: "success_limited_suppression_clears_on_no_suppression",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) string {
				t.Helper()
				detectorArn, err := b.CreateLogAnomalyDetector(
					[]string{"arn:aws:logs:us-east-1:123:log-group:test"}, "det", "", "", "", 0,
				)
				require.NoError(t, err)
				cloudwatchlogs.AddAnomalyInternal(b, cloudwatchlogs.Anomaly{
					AnomalyDetectorArn: detectorArn,
					AnomalyID:          "anomaly-suppressed",
					Active:             true,
				})

				return detectorArn
			},
			anomalyID:        "anomaly-suppressed",
			suppressionType:  "LIMITED",
			checkSuppression: true,
		},
		{
			name: "anomaly_not_found",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) string {
				t.Helper()
				detectorArn, err := b.CreateLogAnomalyDetector(
					[]string{"arn:aws:logs:us-east-1:123:log-group:test"}, "det", "", "", "", 0,
				)
				require.NoError(t, err)

				return detectorArn
			},
			anomalyID: "nonexistent-anomaly",
			wantErr:   cloudwatchlogs.ErrLogAnomalyDetectorNotFound,
		},
		{
			name:               "detector_not_found",
			anomalyDetectorArn: "arn:aws:logs:us-east-1:123:log-anomaly-detector:nonexistent",
			anomalyID:          "anomaly-1",
			wantErr:            cloudwatchlogs.ErrLogAnomalyDetectorNotFound,
		},
		{
			name:      "empty_arn",
			anomalyID: "anomaly-1",
			wantErr:   cloudwatchlogs.ErrValidation,
		},
		{
			name:               "empty_anomaly_id",
			anomalyDetectorArn: "arn:aws:logs:us-east-1:123:log-anomaly-detector:x",
			wantErr:            cloudwatchlogs.ErrValidation,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudwatchlogs.NewInMemoryBackend()
			arn := tt.anomalyDetectorArn
			if tt.setup != nil {
				arn = tt.setup(t, b)
			}

			err := b.UpdateAnomaly(tt.anomalyID, arn, tt.suppressionType, "")

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)

			if tt.checkSuppression {
				anomalies, _, listErr := b.ListAnomalies(arn, 10, "")
				require.NoError(t, listErr)
				require.Len(t, anomalies, 1)
				assert.Equal(t, cloudwatchlogs.AnomalyStateSuppressed, anomalies[0].State)
				require.NotNil(t, anomalies[0].Suppressed)
				assert.True(t, *anomalies[0].Suppressed)
				assert.NotZero(t, anomalies[0].SuppressedDate)

				// Real AWS: calling UpdateAnomaly again and omitting
				// suppressionType ends the suppression -- this is the bug
				// this test's name refers to (a previous revision treated
				// the empty string as "still suppressed").
				require.NoError(t, b.UpdateAnomaly(tt.anomalyID, arn, "", ""))
				cleared, _, clearErr := b.ListAnomalies(arn, 10, "")
				require.NoError(t, clearErr)
				require.Len(t, cleared, 1)
				assert.Equal(t, cloudwatchlogs.AnomalyStateActive, cleared[0].State)
				require.NotNil(t, cleared[0].Suppressed)
				assert.False(t, *cleared[0].Suppressed)
				assert.Zero(t, cleared[0].SuppressedDate)
			}
		})
	}
}

func TestCloudWatchLogsBackend_CreateLogAnomalyDetector_VisibilityTimeValidation(t *testing.T) {
	t.Parallel()

	const msPerDay = 24 * 60 * 60 * 1000

	tests := []struct {
		wantErr               error
		name                  string
		wantStatus            string
		anomalyVisibilityTime int64
	}{
		{
			name:       "zero_accepted",
			wantStatus: "INITIALIZING",
		},
		{
			name:                  "7_days_min",
			anomalyVisibilityTime: 7 * msPerDay,
			wantStatus:            "INITIALIZING",
		},
		{
			name:                  "90_days_max",
			anomalyVisibilityTime: 90 * msPerDay,
			wantStatus:            "INITIALIZING",
		},
		{
			name:                  "30_days_valid",
			anomalyVisibilityTime: 30 * msPerDay,
			wantStatus:            "INITIALIZING",
		},
		{
			name:                  "6_days_too_small",
			anomalyVisibilityTime: 6 * msPerDay,
			wantErr:               cloudwatchlogs.ErrValidation,
		},
		{
			name:                  "91_days_too_large",
			anomalyVisibilityTime: 91 * msPerDay,
			wantErr:               cloudwatchlogs.ErrValidation,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudwatchlogs.NewInMemoryBackend()
			_, err := b.CreateLogGroup(context.Background(), "g", "", "")
			require.NoError(t, err)

			groupARN := "arn:aws:logs:us-east-1:123456789012:log-group:g"
			detectorARN, err := b.CreateLogAnomalyDetector(
				[]string{groupARN}, "test-detector", "", "", "",
				tt.anomalyVisibilityTime,
			)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
			assert.NotEmpty(t, detectorARN)

			detector, err := b.GetLogAnomalyDetector(detectorARN)
			require.NoError(t, err)
			assert.Equal(t, tt.wantStatus, detector.AnomalyDetectorStatus)
			assert.NotZero(t, detector.LastModifiedTimeStamp)
		})
	}
}

func TestCloudWatchLogsBackend_UpdateLogAnomalyDetector_SetsLastModified(t *testing.T) {
	t.Parallel()

	const msPerDay = 24 * 60 * 60 * 1000

	b := cloudwatchlogs.NewInMemoryBackend()
	_, err := b.CreateLogGroup(context.Background(), "g", "", "")
	require.NoError(t, err)

	groupARN := "arn:aws:logs:us-east-1:123456789012:log-group:g"
	arn, err := b.CreateLogAnomalyDetector([]string{groupARN}, "d", "", "", "", 0)
	require.NoError(t, err)

	before, err := b.GetLogAnomalyDetector(arn)
	require.NoError(t, err)
	createdAt := before.LastModifiedTimeStamp

	time.Sleep(2 * time.Millisecond)

	err = b.UpdateLogAnomalyDetector(arn, "FIVE_MIN", 30*msPerDay, true)
	require.NoError(t, err)

	after, err := b.GetLogAnomalyDetector(arn)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, after.LastModifiedTimeStamp, createdAt)
}

func TestCloudWatchLogsBackend_UpdateLogAnomalyDetector_VisibilityTimeValidation(t *testing.T) {
	t.Parallel()

	const msPerDay = 24 * 60 * 60 * 1000

	tests := []struct {
		wantErr               error
		name                  string
		anomalyVisibilityTime int64
	}{
		{
			name:                  "valid_30_days",
			anomalyVisibilityTime: 30 * msPerDay,
		},
		{
			name:                  "too_small_6_days",
			anomalyVisibilityTime: 6 * msPerDay,
			wantErr:               cloudwatchlogs.ErrValidation,
		},
		{
			name:                  "too_large_91_days",
			anomalyVisibilityTime: 91 * msPerDay,
			wantErr:               cloudwatchlogs.ErrValidation,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudwatchlogs.NewInMemoryBackend()
			arn, err := b.CreateLogAnomalyDetector(
				[]string{"arn:aws:logs:us-east-1:123:log-group:g"}, "", "", "", "", 0,
			)
			require.NoError(t, err)

			err = b.UpdateLogAnomalyDetector(arn, "", tt.anomalyVisibilityTime, true)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
		})
	}
}
