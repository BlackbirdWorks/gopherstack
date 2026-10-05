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

func TestGetServiceGraph_ServiceNames(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		names []string
	}{
		{name: "frontend and backend", names: []string{"frontend", "backend"}},
		{name: "single service", names: []string{"solo"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestXRayClient(t)
			ctx := t.Context()
			now := float64(time.Now().Unix())
			var docs []string
			parent := ""
			for i, n := range tt.names {
				id := fmt.Sprintf("a00000000000000%d", i+1)
				docs = append(docs, fmt.Sprintf(
					`{"trace_id":"1-5f0e1a2b-aaaaaaaaaaaaaaaaaaaaaaaa","id":%q,"name":%q,"parent_id":%q,`+
						`"start_time":%f,"end_time":%f}`, id, n, parent, now-2, now-1))
				parent = id
			}
			_, err := client.PutTraceSegments(ctx, &xraysdk.PutTraceSegmentsInput{TraceSegmentDocuments: docs})
			require.NoError(t, err)

			out, err := client.GetServiceGraph(ctx, &xraysdk.GetServiceGraphInput{
				StartTime: aws.Time(time.Now().Add(-time.Hour)), EndTime: aws.Time(time.Now().Add(time.Hour)),
			})
			require.NoError(t, err)
			got := make([]string, 0, len(out.Services))
			for _, s := range out.Services {
				assert.Equal(t, []string{aws.ToString(s.Name)}, s.Names)
				got = append(got, aws.ToString(s.Name))
			}
			assert.ElementsMatch(t, tt.names, got)
		})
	}
}
