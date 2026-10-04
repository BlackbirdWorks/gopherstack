package cloudtrail_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	cloudtrailsdk "github.com/aws/aws-sdk-go-v2/service/cloudtrail"
	cttypes "github.com/aws/aws-sdk-go-v2/service/cloudtrail/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/services/cloudtrail"
)

// ListInsightsMetricData derives its series from recorded management events (api_op_ListInsightsMetricData.go).
func TestListInsightsMetricData_FromRecordedEvents(t *testing.T) {
	t.Parallel()

	base := time.Now().UTC().Truncate(time.Hour).Add(-3 * time.Hour)

	seed := func(b *cloudtrail.InMemoryBackend) {
		add := func(name, errCode string, at time.Time) {
			ev := cloudtrail.Event{EventSource: "s3.amazonaws.com", EventName: name, EventTime: at}
			if errCode != "" {
				ev.CloudTrailEvent = `{"errorCode":"` + errCode + `"}`
			}

			b.RecordEvent(ev)
		}

		add("CreateBucket", "", base.Add(5*time.Minute))
		add("CreateBucket", "", base.Add(10*time.Minute))
		add("CreateBucket", "AccessDenied", base.Add(20*time.Minute))
		add("CreateBucket", "", base.Add(2*time.Hour+time.Minute))
		add("DeleteBucket", "", base.Add(5*time.Minute))
	}

	tests := []struct {
		in         cloudtrailsdk.ListInsightsMetricDataInput
		name       string
		wantErr    string
		wantValues []float64
	}{
		{
			name: "call-rate-nonzero",
			in: cloudtrailsdk.ListInsightsMetricDataInput{
				InsightType: cttypes.InsightTypeApiCallRateInsight,
				StartTime:   aws.Time(base), EndTime: aws.Time(base.Add(3 * time.Hour)),
			},
			wantValues: []float64{3, 1},
		},
		{
			name: "call-rate-fill-zeros",
			in: cloudtrailsdk.ListInsightsMetricDataInput{
				InsightType: cttypes.InsightTypeApiCallRateInsight,
				DataType:    cttypes.InsightsMetricDataTypeFillWithZeros,
				StartTime:   aws.Time(base), EndTime: aws.Time(base.Add(3 * time.Hour)),
			},
			wantValues: []float64{3, 0, 1},
		},
		{
			name: "error-rate",
			in: cloudtrailsdk.ListInsightsMetricDataInput{
				InsightType: cttypes.InsightTypeApiErrorRateInsight, ErrorCode: aws.String("AccessDenied"),
				StartTime: aws.Time(base), EndTime: aws.Time(base.Add(3 * time.Hour)),
			},
			wantValues: []float64{1},
		},
		{
			name: "five-minute-period",
			in: cloudtrailsdk.ListInsightsMetricDataInput{
				InsightType: cttypes.InsightTypeApiCallRateInsight, Period: aws.Int32(300),
				StartTime: aws.Time(base), EndTime: aws.Time(base.Add(30 * time.Minute)),
			},
			wantValues: []float64{1, 1, 1},
		},
		{
			name: "end-exclusive",
			in: cloudtrailsdk.ListInsightsMetricDataInput{
				InsightType: cttypes.InsightTypeApiCallRateInsight,
				StartTime:   aws.Time(base), EndTime: aws.Time(base.Add(5 * time.Minute)),
			},
			wantValues: []float64{},
		},
		{
			name: "error-needs-code", wantErr: "InvalidParameterException",
			in: cloudtrailsdk.ListInsightsMetricDataInput{InsightType: cttypes.InsightTypeApiErrorRateInsight},
		},
		{
			name: "bad-period", wantErr: "InvalidParameterException",
			in: cloudtrailsdk.ListInsightsMetricDataInput{
				InsightType: cttypes.InsightTypeApiCallRateInsight, Period: aws.Int32(7),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := cloudtrail.NewInMemoryBackend("000000000000", config.DefaultRegion)
			seed(backend)

			client := newTestCloudTrailClient(t, cloudtrail.NewHandler(backend))
			in := tt.in
			in.EventSource = aws.String("s3.amazonaws.com")
			in.EventName = aws.String("CreateBucket")

			out, err := client.ListInsightsMetricData(t.Context(), &in)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantValues, append([]float64{}, out.Values...))
			assert.Len(t, out.Timestamps, len(out.Values))
		})
	}

	t.Run("pagination", func(t *testing.T) {
		t.Parallel()

		backend := cloudtrail.NewInMemoryBackend("000000000000", config.DefaultRegion)
		seed(backend)
		client := newTestCloudTrailClient(t, cloudtrail.NewHandler(backend))

		in := &cloudtrailsdk.ListInsightsMetricDataInput{
			EventSource: aws.String("s3.amazonaws.com"), EventName: aws.String("CreateBucket"),
			InsightType: cttypes.InsightTypeApiCallRateInsight, DataType: cttypes.InsightsMetricDataTypeFillWithZeros,
			StartTime: aws.Time(base), EndTime: aws.Time(base.Add(3 * time.Hour)), MaxResults: aws.Int32(2),
		}

		var got []float64

		pages := 0

		for {
			out, err := client.ListInsightsMetricData(t.Context(), in)
			require.NoError(t, err)

			got = append(got, out.Values...)
			pages++

			if out.NextToken == nil {
				break
			}

			in.NextToken = out.NextToken
		}

		assert.Equal(t, []float64{3, 0, 1}, got)
		assert.Equal(t, 2, pages)
	})
}
