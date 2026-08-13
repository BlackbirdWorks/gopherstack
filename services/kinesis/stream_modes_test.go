package kinesis_test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	kinesissdk "github.com/aws/aws-sdk-go-v2/service/kinesis"
	kinesissdktypes "github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/blackbirdworks/gopherstack/services/kinesis"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestUpdateStreamWarmThroughput_RoundTrip drives UpdateStreamWarmThroughput
// through the real aws-sdk-go-v2 client. Its real required Input field is
// WarmThroughputMiBps (kinesis@v1.46.4 api_op_UpdateStreamWarmThroughput.go:63-70);
// gopherstack used to decode fabricated WriteCapacityUnits/ReadCapacityUnits
// fields that don't exist on the real Input at all, so a real client's
// request silently no-op'd (gopherstack-nbg8). Proves the fix via the real
// Output shape too (StreamARN/StreamName/WarmThroughput.Current+TargetMiBps,
// api_op_UpdateStreamWarmThroughput.go:76-88), which was previously not
// modeled at all (the handler returned an empty struct{}).
func TestUpdateStreamWarmThroughput_RoundTrip(t *testing.T) {
	t.Parallel()

	backend := kinesis.NewInMemoryBackend()
	client := newTestKinesisClient(t, kinesis.NewHandler(backend))

	streamName := "warm-throughput-stream"

	_, err := client.CreateStream(t.Context(), &kinesissdk.CreateStreamInput{
		StreamName: aws.String(streamName),
		StreamModeDetails: &kinesissdktypes.StreamModeDetails{
			StreamMode: kinesissdktypes.StreamModeOnDemand,
		},
	})
	require.NoError(t, err)

	desc, err := client.DescribeStream(t.Context(), &kinesissdk.DescribeStreamInput{StreamName: aws.String(streamName)})
	require.NoError(t, err)

	out, err := client.UpdateStreamWarmThroughput(t.Context(), &kinesissdk.UpdateStreamWarmThroughputInput{
		StreamName:          aws.String(streamName),
		WarmThroughputMiBps: aws.Int32(500),
	})
	require.NoError(t, err)
	require.NotNil(t, out.WarmThroughput)
	assert.Equal(t, int32(500), aws.ToInt32(out.WarmThroughput.CurrentMiBps))
	assert.Equal(t, int32(500), aws.ToInt32(out.WarmThroughput.TargetMiBps))
	assert.Equal(t, aws.ToString(desc.StreamDescription.StreamARN), aws.ToString(out.StreamARN))
	assert.Equal(t, streamName, aws.ToString(out.StreamName))
}

// TestUpdateStreamWarmThroughput_RequiredFieldRejected verifies the required
// WarmThroughputMiBps member is enforced server-side (a raw/non-SDK client
// could still omit it or send an out-of-range value).
func TestUpdateStreamWarmThroughput_RequiredFieldRejected(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createKinesisStream(t, h, "warm-stream-missing")

	rec := doRequest(t, h, "UpdateStreamWarmThroughput", map[string]any{
		"StreamName": "warm-stream-missing",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestUpdateStreamMode_ProvisionedToOnDemand(t *testing.T) {
	t.Parallel()

	b := newParityBackend(t)
	ctx := context.Background()

	createParityStream(t, b, "mode-test", 2)

	desc0, err := b.DescribeStream(ctx, &kinesis.DescribeStreamInput{StreamName: "mode-test"})
	require.NoError(t, err)
	assert.Equal(t, "PROVISIONED", desc0.StreamMode)

	err = b.UpdateStreamMode(ctx, &kinesis.UpdateStreamModeInput{
		StreamARN: desc0.StreamARN,
		StreamModeDetails: kinesis.StreamModeDetails{
			StreamMode: "ON_DEMAND",
		},
	})
	require.NoError(t, err)

	desc1, err := b.DescribeStream(ctx, &kinesis.DescribeStreamInput{StreamName: "mode-test"})
	require.NoError(t, err)
	assert.Equal(t, "ON_DEMAND", desc1.StreamMode)

	_, err = b.UpdateShardCount(ctx, &kinesis.UpdateShardCountInput{
		StreamName:       "mode-test",
		TargetShardCount: 4,
	})
	require.Error(t, err, "UpdateShardCount must fail for ON_DEMAND streams")

	err = b.UpdateStreamMode(ctx, &kinesis.UpdateStreamModeInput{
		StreamARN: desc1.StreamARN,
		StreamModeDetails: kinesis.StreamModeDetails{
			StreamMode: "PROVISIONED",
		},
	})
	require.NoError(t, err)

	desc2, err := b.DescribeStream(ctx, &kinesis.DescribeStreamInput{StreamName: "mode-test"})
	require.NoError(t, err)
	assert.Equal(t, "PROVISIONED", desc2.StreamMode)
}

func TestUpdateStreamMode_Valid(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		newMode  string
		wantMode string
	}{
		{name: "to_on_demand", newMode: "ON_DEMAND", wantMode: "ON_DEMAND"},
		{name: "to_provisioned", newMode: "PROVISIONED", wantMode: "PROVISIONED"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			streamName := "mode-stream-" + tt.name

			rec := doRequest(t, h, "CreateStream", map[string]any{
				"StreamName": streamName,
				"ShardCount": 1,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			rec2 := doRequest(t, h, "DescribeStream", map[string]any{"StreamName": streamName})
			require.Equal(t, http.StatusOK, rec2.Code)
			var descResp struct {
				StreamDescription struct {
					StreamARN string `json:"StreamARN"`
				} `json:"StreamDescription"`
			}
			require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &descResp))

			rec3 := doRequest(t, h, "UpdateStreamMode", map[string]any{
				"StreamARN": descResp.StreamDescription.StreamARN,
				"StreamModeDetails": map[string]any{
					"StreamMode": tt.newMode,
				},
			})
			require.Equal(t, http.StatusOK, rec3.Code)

			rec4 := doRequest(t, h, "DescribeStream", map[string]any{"StreamName": streamName})
			require.Equal(t, http.StatusOK, rec4.Code)
			var verifyResp struct {
				StreamDescription struct {
					StreamModeDetails *struct {
						StreamMode string `json:"StreamMode"`
					} `json:"StreamModeDetails"`
				} `json:"StreamDescription"`
			}
			require.NoError(t, json.Unmarshal(rec4.Body.Bytes(), &verifyResp))
			require.NotNil(t, verifyResp.StreamDescription.StreamModeDetails)
			assert.Equal(t, tt.wantMode, verifyResp.StreamDescription.StreamModeDetails.StreamMode)
		})
	}
}

func TestUpdateStreamMode_InvalidMode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		mode    string
		wantErr bool
	}{
		{name: "invalid_mode", mode: "INVALID", wantErr: true},
		{name: "empty_mode", mode: "", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			b := h.Backend.(*kinesis.InMemoryBackend)

			require.NoError(t, b.CreateStream(context.Background(), &kinesis.CreateStreamInput{
				StreamName: "inv-mode-stream",
				ShardCount: 1,
			}))

			err := b.UpdateStreamMode(context.Background(), &kinesis.UpdateStreamModeInput{
				StreamARN:         "arn:aws:kinesis:us-east-1:123456789012:stream/inv-mode-stream",
				StreamModeDetails: kinesis.StreamModeDetails{StreamMode: tt.mode},
			})

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestUpdateStreamMode_OnDemandTransitionReshardsUpToFloor verifies AWS's
// documented PROVISIONED -> ON_DEMAND auto-scale behavior: a stream under the
// default on-demand shard floor (4, matching a freshly created ON_DEMAND
// stream) is resharded up to it, with the old shards retained CLOSED for
// lineage. A stream already at or above the floor is left untouched.
func TestUpdateStreamMode_OnDemandTransitionReshardsUpToFloor(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		startShards   int
		wantOpenAfter int
	}{
		{name: "below_floor_1_shard", startShards: 1, wantOpenAfter: 4},
		{name: "below_floor_2_shards", startShards: 2, wantOpenAfter: 4},
		{name: "at_floor_4_shards", startShards: 4, wantOpenAfter: 4},
		{name: "above_floor_6_shards", startShards: 6, wantOpenAfter: 6},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newParityBackend(t)
			ctx := context.Background()
			streamName := "reshard-" + tt.name

			createParityStream(t, b, streamName, tt.startShards)

			descBefore, err := b.DescribeStream(ctx, &kinesis.DescribeStreamInput{StreamName: streamName})
			require.NoError(t, err)
			require.Len(t, descBefore.Shards, tt.startShards, "sanity: initial open shard count")

			require.NoError(t, b.UpdateStreamMode(ctx, &kinesis.UpdateStreamModeInput{
				StreamARN: descBefore.StreamARN,
				StreamModeDetails: kinesis.StreamModeDetails{
					StreamMode: "ON_DEMAND",
				},
			}))

			// ListShards' default (no ShardFilter) only returns open shards, so
			// its length is exactly the new open shard count.
			openAfter, err := b.ListShards(ctx, &kinesis.ListShardsInput{StreamName: streamName})
			require.NoError(t, err)
			assert.Len(t, openAfter.Shards, tt.wantOpenAfter)

			if tt.startShards < tt.wantOpenAfter {
				// Old shards must still be visible, CLOSED, for lineage.
				listOut, listErr := b.ListShards(ctx, &kinesis.ListShardsInput{
					StreamName:  streamName,
					ShardFilter: "FROM_TRIM_HORIZON",
				})
				require.NoError(t, listErr)
				assert.Len(t, listOut.Shards, tt.startShards+tt.wantOpenAfter,
					"old closed shards plus new open shards")
			}
		})
	}
}

// TestUpdateStreamMode_OnDemandToProvisionedKeepsShardCount verifies the
// reverse transition (ON_DEMAND -> PROVISIONED) never reshards: AWS keeps the
// stream's current auto-scaled shard count as the new provisioned baseline.
func TestUpdateStreamMode_OnDemandToProvisionedKeepsShardCount(t *testing.T) {
	t.Parallel()

	b := newParityBackend(t)
	ctx := context.Background()

	require.NoError(t, b.CreateStream(ctx, &kinesis.CreateStreamInput{
		StreamName: "ondemand-to-prov",
		StreamMode: "ON_DEMAND",
	}))

	descBefore, err := b.DescribeStream(ctx, &kinesis.DescribeStreamInput{StreamName: "ondemand-to-prov"})
	require.NoError(t, err)
	openBefore := len(descBefore.Shards)

	require.NoError(t, b.UpdateStreamMode(ctx, &kinesis.UpdateStreamModeInput{
		StreamARN:         descBefore.StreamARN,
		StreamModeDetails: kinesis.StreamModeDetails{StreamMode: "PROVISIONED"},
	}))

	descAfter, err := b.DescribeStream(ctx, &kinesis.DescribeStreamInput{StreamName: "ondemand-to-prov"})
	require.NoError(t, err)
	assert.Len(t, descAfter.Shards, openBefore, "shard count must be unchanged by ON_DEMAND -> PROVISIONED")
}

func TestUpdateStreamMode_NotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		streamARN string
		wantErr   bool
	}{
		{
			name:      "nonexistent_stream",
			streamARN: "arn:aws:kinesis:us-east-1:123456789012:stream/no-such-stream",
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			b := h.Backend.(*kinesis.InMemoryBackend)

			err := b.UpdateStreamMode(context.Background(), &kinesis.UpdateStreamModeInput{
				StreamARN:         tt.streamARN,
				StreamModeDetails: kinesis.StreamModeDetails{StreamMode: kinesis.StreamModeOnDemand},
			})

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
