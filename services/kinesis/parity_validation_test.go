package kinesis_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCreateStream_ShardCount_Validation asserts ValidationException for ShardCount <= 0 on PROVISIONED streams.
func TestCreateStream_ShardCount_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		shardCount int
		streamMode string
		wantCode   int
		wantType   string
	}{
		{
			name:       "valid_provisioned_shard_1",
			shardCount: 1,
			wantCode:   http.StatusOK,
		},
		{
			name:       "zero_shard_count_provisioned_rejected",
			shardCount: 0,
			wantCode:   http.StatusBadRequest,
			wantType:   "InvalidArgumentException",
		},
		{
			name:       "negative_shard_count_rejected",
			shardCount: -1,
			wantCode:   http.StatusBadRequest,
			wantType:   "InvalidArgumentException",
		},
		{
			name:       "on_demand_zero_shard_count_allowed",
			shardCount: 0,
			streamMode: "ON_DEMAND",
			wantCode:   http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			body := map[string]any{
				"StreamName": "shard-val-stream-" + tt.name,
				"ShardCount": tt.shardCount,
			}
			if tt.streamMode != "" {
				body["StreamModeDetails"] = map[string]any{"StreamMode": tt.streamMode}
			}

			rec := doRequest(t, h, "CreateStream", body)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantType != "" {
				var resp struct {
					Type string `json:"__type"`
				}
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Equal(t, tt.wantType, resp.Type)
			}
		})
	}
}
