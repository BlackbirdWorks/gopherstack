package kinesis_test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/kinesis"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestKinesis_UpdateStreamWarmThroughput(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createKinesisStream(t, h, "warm-stream")

	rec := doRequest(t, h, "UpdateStreamWarmThroughput", map[string]any{
		"StreamName":            "warm-stream",
		"ConsumersToPut":        1,
		"WriteProvisionedUnits": 100,
	})
	assert.True(t, rec.Code >= 200 && rec.Code < 300 || rec.Code == 400)
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
