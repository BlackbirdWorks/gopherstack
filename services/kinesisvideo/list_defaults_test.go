package kinesisvideo_test

import (
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	kinesisvideosdk "github.com/aws/aws-sdk-go-v2/service/kinesisvideo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ListStreams defaults to 10,000 (api_op_ListStreams.go:32); ListEdgeAgentConfigurations to 5 (:39).
func TestListPageSizeDefaults(t *testing.T) {
	t.Parallel()

	tests := []struct {
		maxRes   *int32
		name     string
		kind     string
		total    int
		wantLen  int
		wantNext bool
	}{
		{name: "streams default beyond 500", kind: "streams", total: 600, wantLen: 600},
		{name: "streams explicit", kind: "streams", total: 12, maxRes: aws.Int32(5), wantLen: 5, wantNext: true},
		{name: "edge default 5", kind: "edge", total: 7, wantLen: 5, wantNext: true},
		{name: "edge under default", kind: "edge", total: 3, wantLen: 3},
		{name: "edge explicit", kind: "edge", total: 7, maxRes: aws.Int32(6), wantLen: 6, wantNext: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestClient(t, newTestHandler())
			ctx := t.Context()

			for i := range tt.total {
				_, err := client.CreateStream(ctx, &kinesisvideosdk.CreateStreamInput{
					StreamName: aws.String(fmt.Sprintf("s%04d", i)), DataRetentionInHours: aws.Int32(1),
				})
				require.NoError(t, err)

				if tt.kind == "edge" {
					_, err = client.StartEdgeConfigurationUpdate(
						ctx,
						&kinesisvideosdk.StartEdgeConfigurationUpdateInput{
							StreamName: aws.String(fmt.Sprintf("s%04d", i)), EdgeConfig: testEdgeConfig("hub-a"),
						},
					)
					require.NoError(t, err)
				}
			}

			var gotLen int

			var next *string

			if tt.kind == "streams" {
				out, err := client.ListStreams(ctx, &kinesisvideosdk.ListStreamsInput{MaxResults: tt.maxRes})
				require.NoError(t, err)

				gotLen, next = len(out.StreamInfoList), out.NextToken
			} else {
				out, err := client.ListEdgeAgentConfigurations(ctx, &kinesisvideosdk.ListEdgeAgentConfigurationsInput{
					HubDeviceArn: aws.String("hub-a"), MaxResults: tt.maxRes,
				})
				require.NoError(t, err)

				gotLen, next = len(out.EdgeConfigs), out.NextToken
			}

			assert.Equal(t, tt.wantLen, gotLen)

			if tt.wantNext {
				assert.NotEmpty(t, aws.ToString(next))
			} else {
				assert.Empty(t, aws.ToString(next))
			}
		})
	}
}
