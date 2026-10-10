package xray_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	xraysdk "github.com/aws/aws-sdk-go-v2/service/xray"
	xraytypes "github.com/aws/aws-sdk-go-v2/service/xray/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInsightDetection_GroupFilterExpression(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		filter     string
		queryGroup string
		insights   bool
		wantInGrp  bool
	}{
		{name: "matching filter", filter: `service("flaky-svc")`, insights: true, wantInGrp: true, queryGroup: "g"},
		{
			name:       "other service filter",
			filter:     `service("other-svc")`,
			insights:   true,
			wantInGrp:  false,
			queryGroup: "g",
		},
		{name: "insights disabled", filter: `service("flaky-svc")`, insights: false, wantInGrp: false, queryGroup: "g"},
		{name: "default group", filter: `service("other-svc")`, insights: true, wantInGrp: true, queryGroup: "default"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestXRayClient(t)
			ctx := t.Context()

			_, err := client.CreateGroup(ctx, &xraysdk.CreateGroupInput{
				GroupName:        aws.String("g"),
				FilterExpression: aws.String(tt.filter),
				InsightsConfiguration: &xraytypes.InsightsConfiguration{
					InsightsEnabled: aws.Bool(tt.insights),
				},
			})
			require.NoError(t, err)

			now := time.Now()
			segs := make([]string, 0, 12)

			for i := range 12 {
				segs = append(segs, fmt.Sprintf(
					`{"trace_id":"1-5f84c7a1-0123456789abcdef01234567","id":"%016x","name":"flaky-svc",`+
						`"start_time":%d,"end_time":%d,"fault":true}`,
					i+1, now.Unix(), now.Unix()+1,
				))
			}

			_, err = client.PutTraceSegments(ctx, &xraysdk.PutTraceSegmentsInput{TraceSegmentDocuments: segs})
			require.NoError(t, err)

			out, err := client.GetInsightSummaries(ctx, &xraysdk.GetInsightSummariesInput{
				GroupName: aws.String(tt.queryGroup),
				StartTime: aws.Time(now.Add(-time.Hour)),
				EndTime:   aws.Time(now.Add(time.Hour)),
			})
			require.NoError(t, err)

			if tt.queryGroup == "default" {
				assert.NotEmpty(t, out.InsightSummaries)

				return
			}

			assert.Equal(t, tt.wantInGrp, len(out.InsightSummaries) > 0)

			if tt.wantInGrp {
				assert.Equal(t, "g", aws.ToString(out.InsightSummaries[0].GroupName))
			}
		})
	}
}
