package xray_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	xraysdk "github.com/aws/aws-sdk-go-v2/service/xray"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetTraceSummaries_UsersAndProcessedCount(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name          string
		filter        string
		wantUserSvcs  []string
		wantProcessed int64
		wantSummaries int
	}{
		{
			name:          "users carry reporting services",
			wantUserSvcs:  []string{"frontend", "backend"},
			wantProcessed: 2,
			wantSummaries: 2,
		},
		{
			name:          "processed count includes non-matching traces",
			filter:        `annotation.user = "alice"`,
			wantUserSvcs:  []string{"frontend", "backend"},
			wantProcessed: 2,
			wantSummaries: 1,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client := newTestXRayClient(t)
			ctx := t.Context()
			now := float64(time.Now().Unix())

			seg := func(trace, id, parent, name, user string) string {
				return fmt.Sprintf(
					`{"trace_id":%q,"id":%q,"name":%q,"parent_id":%q,"start_time":%f,"end_time":%f,`+
						`"annotations":{"user":%q}}`,
					trace, id, name, parent, now-2, now-1, user)
			}
			ta, tb := "1-5f0e1a2b-aaaaaaaaaaaaaaaaaaaaaaaa", "1-5f0e1a2b-bbbbbbbbbbbbbbbbbbbbbbbb"
			docs := []string{
				seg(ta, "a000000000000001", "", "frontend", "alice"),
				seg(ta, "a000000000000002", "a000000000000001", "backend", "alice"),
				seg(tb, "b000000000000001", "", "frontend", "bob"),
			}
			_, err := client.PutTraceSegments(ctx, &xraysdk.PutTraceSegmentsInput{TraceSegmentDocuments: docs})
			require.NoError(t, err)

			in := &xraysdk.GetTraceSummariesInput{
				StartTime: aws.Time(time.Now().Add(-time.Hour)),
				EndTime:   aws.Time(time.Now().Add(time.Hour)),
			}
			if tc.filter != "" {
				in.FilterExpression = aws.String(tc.filter)
			}

			out, err := client.GetTraceSummaries(ctx, in)
			require.NoError(t, err)
			assert.Equal(t, tc.wantProcessed, aws.ToInt64(out.TracesProcessedCount))
			require.Len(t, out.TraceSummaries, tc.wantSummaries)

			for _, ts := range out.TraceSummaries {
				require.Len(t, ts.Users, 1)

				if aws.ToString(ts.Users[0].UserName) != "alice" {
					continue
				}

				var svcs []string
				for _, id := range ts.Users[0].ServiceIds {
					svcs = append(svcs, aws.ToString(id.Name))
				}

				assert.ElementsMatch(t, tc.wantUserSvcs, svcs)
			}
		})
	}
}
