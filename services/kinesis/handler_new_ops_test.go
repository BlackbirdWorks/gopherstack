package kinesis_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDescribeLimits verifies the DescribeLimits operation returns account shard limits.
func TestDescribeLimits(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "DescribeLimits", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		ShardLimit     int `json:"ShardLimit"`
		OpenShardCount int `json:"OpenShardCount"`
	}

	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, 500, resp.ShardLimit)
	assert.Equal(t, 0, resp.OpenShardCount)
}

// TestDescribeAccountSettings verifies the DescribeAccountSettings operation.
func TestDescribeAccountSettings(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "DescribeAccountSettings", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		ShardLimit               int `json:"ShardLimit"`
		OnDemandStreamCount      int `json:"OnDemandStreamCount"`
		OnDemandStreamCountLimit int `json:"OnDemandStreamCountLimit"`
	}

	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, 500, resp.ShardLimit)
	assert.Equal(t, 0, resp.OnDemandStreamCount)
	assert.Positive(t, resp.OnDemandStreamCountLimit)
}

// TestMergeShards verifies that two adjacent shards can be merged into one.
func TestMergeShards(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create stream with 2 shards.
	rec := doRequest(t, h, "CreateStream", map[string]any{
		"StreamName": "merge-stream",
		"ShardCount": 2,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Get the shard IDs.
	rec = doRequest(t, h, "DescribeStream", map[string]any{
		"StreamName": "merge-stream",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var descResp struct {
		StreamDescription struct {
			Shards []struct {
				ShardID string `json:"ShardId"`
			} `json:"Shards"`
		} `json:"StreamDescription"`
	}

	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	require.Len(t, descResp.StreamDescription.Shards, 2)

	shard0 := descResp.StreamDescription.Shards[0].ShardID
	shard1 := descResp.StreamDescription.Shards[1].ShardID

	// Merge the two shards.
	rec = doRequest(t, h, "MergeShards", map[string]any{
		"StreamName":           "merge-stream",
		"ShardToMerge":         shard0,
		"AdjacentShardToMerge": shard1,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Verify shard count decreased to 1.
	rec = doRequest(t, h, "DescribeStream", map[string]any{
		"StreamName": "merge-stream",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	assert.Len(t, descResp.StreamDescription.Shards, 1)
}

// TestMergeShards_Errors verifies error cases for MergeShards.
func TestMergeShards_Errors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body        any
		name        string
		wantErrType string
		wantCode    int
	}{
		{
			name: "stream_not_found",
			body: map[string]any{
				"StreamName":           "no-such-stream",
				"ShardToMerge":         "s1",
				"AdjacentShardToMerge": "s2",
			},
			wantCode:    http.StatusBadRequest,
			wantErrType: "ResourceNotFoundException",
		},
		{
			name: "shard_not_found",
			body: map[string]any{
				"StreamName":           "merge-err-stream",
				"ShardToMerge":         "bad-shard",
				"AdjacentShardToMerge": "bad-shard2",
			},
			wantCode:    http.StatusBadRequest,
			wantErrType: "InvalidArgumentException",
		},
	}

	h := newTestHandler(t)
	// Create the stream used in shard_not_found test.
	setup := doRequest(t, h, "CreateStream", map[string]any{
		"StreamName": "merge-err-stream",
		"ShardCount": 1,
	})
	require.Equal(t, http.StatusOK, setup.Code)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := doRequest(t, h, "MergeShards", tt.body)
			assert.Equal(t, tt.wantCode, got.Code)

			if tt.wantErrType != "" {
				var errResp struct {
					Type string `json:"__type"`
				}

				require.NoError(t, json.Unmarshal(got.Body.Bytes(), &errResp))
				assert.Equal(t, tt.wantErrType, errResp.Type)
			}
		})
	}
}

// TestSplitShard verifies that a shard can be split into two at a given hash key.
func TestSplitShard(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create stream with 1 shard.
	rec := doRequest(t, h, "CreateStream", map[string]any{
		"StreamName": "split-stream",
		"ShardCount": 1,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Get shard details.
	rec = doRequest(t, h, "DescribeStream", map[string]any{"StreamName": "split-stream"})
	require.Equal(t, http.StatusOK, rec.Code)

	var descResp struct {
		StreamDescription struct {
			Shards []struct {
				ShardID string `json:"ShardId"`
			} `json:"Shards"`
		} `json:"StreamDescription"`
	}

	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	require.Len(t, descResp.StreamDescription.Shards, 1)

	shardID := descResp.StreamDescription.Shards[0].ShardID

	// Split at midpoint (half of 2^128).
	const midKey = "170141183460469231731687303715884105728"

	rec = doRequest(t, h, "SplitShard", map[string]any{
		"StreamName":         "split-stream",
		"ShardToSplit":       shardID,
		"NewStartingHashKey": midKey,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Verify there are now 2 shards.
	rec = doRequest(t, h, "DescribeStream", map[string]any{"StreamName": "split-stream"})
	require.Equal(t, http.StatusOK, rec.Code)
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	assert.Len(t, descResp.StreamDescription.Shards, 2)
}

// TestSplitShard_Errors verifies error cases for SplitShard.
func TestSplitShard_Errors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body    any
		name    string
		errType string
		code    int
	}{
		{
			name: "stream_not_found",
			body: map[string]any{
				"StreamName":         "no-stream",
				"ShardToSplit":       "s1",
				"NewStartingHashKey": "100",
			},
			code:    http.StatusBadRequest,
			errType: "ResourceNotFoundException",
		},
		{
			name: "shard_not_found",
			body: map[string]any{
				"StreamName":         "split-err-stream",
				"ShardToSplit":       "bad-id",
				"NewStartingHashKey": "100",
			},
			code:    http.StatusBadRequest,
			errType: "InvalidArgumentException",
		},
	}

	h := newTestHandler(t)
	setup := doRequest(t, h, "CreateStream", map[string]any{
		"StreamName": "split-err-stream",
		"ShardCount": 1,
	})
	require.Equal(t, http.StatusOK, setup.Code)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := doRequest(t, h, "SplitShard", tt.body)
			assert.Equal(t, tt.code, got.Code)

			if tt.errType != "" {
				var errResp struct {
					Type string `json:"__type"`
				}

				require.NoError(t, json.Unmarshal(got.Body.Bytes(), &errResp))
				assert.Equal(t, tt.errType, errResp.Type)
			}
		})
	}
}

// TestStreamEncryption verifies StartStreamEncryption and StopStreamEncryption.
func TestStreamEncryption(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		streamName    string
		encType       string
		keyID         string
		wantStartCode int
		wantStopCode  int
	}{
		{
			name:          "kms_encryption_roundtrip",
			streamName:    "enc-stream-kms",
			encType:       "KMS",
			keyID:         "arn:aws:kms:us-east-1:123456789012:key/test-key-id",
			wantStartCode: http.StatusOK,
			wantStopCode:  http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			rec := doRequest(t, h, "CreateStream", map[string]any{
				"StreamName": tt.streamName,
				"ShardCount": 1,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			// Start encryption.
			rec = doRequest(t, h, "StartStreamEncryption", map[string]any{
				"StreamName":     tt.streamName,
				"EncryptionType": tt.encType,
				"KeyId":          tt.keyID,
			})
			assert.Equal(t, tt.wantStartCode, rec.Code)

			// Stop encryption.
			rec = doRequest(t, h, "StopStreamEncryption", map[string]any{
				"StreamName":     tt.streamName,
				"EncryptionType": tt.encType,
				"KeyId":          tt.keyID,
			})
			assert.Equal(t, tt.wantStopCode, rec.Code)
		})
	}
}

// TestStreamEncryption_Errors verifies error cases for encryption operations.
func TestStreamEncryption_Errors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body        any
		name        string
		action      string
		wantErrType string
		wantCode    int
	}{
		{
			name:        "start_stream_not_found",
			action:      "StartStreamEncryption",
			body:        map[string]any{"StreamName": "no-such", "EncryptionType": "KMS", "KeyId": "key-id"},
			wantCode:    http.StatusBadRequest,
			wantErrType: "ResourceNotFoundException",
		},
		{
			name:        "stop_stream_not_found",
			action:      "StopStreamEncryption",
			body:        map[string]any{"StreamName": "no-such", "EncryptionType": "KMS", "KeyId": "key-id"},
			wantCode:    http.StatusBadRequest,
			wantErrType: "ResourceNotFoundException",
		},
		{
			name:   "start_invalid_encryption_type",
			action: "StartStreamEncryption",
			body: map[string]any{
				"StreamName":     "enc-err-stream",
				"EncryptionType": "INVALID",
				"KeyId":          "key-id",
			},
			wantCode:    http.StatusBadRequest,
			wantErrType: "InvalidArgumentException",
		},
	}

	h := newTestHandler(t)
	setup := doRequest(t, h, "CreateStream", map[string]any{
		"StreamName": "enc-err-stream",
		"ShardCount": 1,
	})
	require.Equal(t, http.StatusOK, setup.Code)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := doRequest(t, h, tt.action, tt.body)
			assert.Equal(t, tt.wantCode, got.Code)

			if tt.wantErrType != "" {
				var errResp struct {
					Type string `json:"__type"`
				}

				require.NoError(t, json.Unmarshal(got.Body.Bytes(), &errResp))
				assert.Equal(t, tt.wantErrType, errResp.Type)
			}
		})
	}
}

// testPolicy is a sample resource policy for testing.
const testPolicy = `{"Version":"2012-10-17","Statement":[` +
	`{"Effect":"Allow","Principal":{"AWS":"*"},"Action":"kinesis:*"}]}`

// TestResourcePolicyLifecycle verifies Put/Get/Delete of resource policies.
func TestResourcePolicyLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		resourceARN string
		policy      string
	}{
		{
			name:        "stream_resource_policy",
			resourceARN: "arn:aws:kinesis:us-east-1:123456789012:stream/my-stream",
			policy:      testPolicy,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			// GetResourcePolicy on non-existent policy should 400.
			rec := doRequest(t, h, "GetResourcePolicy", map[string]any{
				"ResourceARN": tt.resourceARN,
			})
			assert.Equal(t, http.StatusBadRequest, rec.Code)

			// PutResourcePolicy.
			rec = doRequest(t, h, "PutResourcePolicy", map[string]any{
				"ResourceARN": tt.resourceARN,
				"Policy":      tt.policy,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			// GetResourcePolicy should return the stored policy.
			rec = doRequest(t, h, "GetResourcePolicy", map[string]any{
				"ResourceARN": tt.resourceARN,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var policyResp struct {
				Policy string `json:"Policy"`
			}

			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &policyResp))
			assert.Equal(t, tt.policy, policyResp.Policy)

			// DeleteResourcePolicy.
			rec = doRequest(t, h, "DeleteResourcePolicy", map[string]any{
				"ResourceARN": tt.resourceARN,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			// GetResourcePolicy after deletion should 400.
			rec = doRequest(t, h, "GetResourcePolicy", map[string]any{
				"ResourceARN": tt.resourceARN,
			})
			assert.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}
}

// TestDeleteResourcePolicy_NotFound verifies 400 when deleting a non-existent policy.
func TestDeleteResourcePolicy_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "DeleteResourcePolicy", map[string]any{
		"ResourceARN": "arn:aws:kinesis:us-east-1:123:stream/no-policy",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var errResp struct {
		Type string `json:"__type"`
	}

	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "ResourceNotFoundException", errResp.Type)
}

// TestListTagsForResource verifies that tags on a stream are retrievable by ARN.
func TestListTagsForResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantCode int
	}{
		{
			name:     "existing_stream_empty_tags",
			wantCode: http.StatusOK,
		},
		{
			name:     "stream_not_found",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			var arn string
			if tt.wantCode == http.StatusOK {
				arn = createStreamAndGetARN(t, h, "tag-resource-stream")
			} else {
				arn = "arn:aws:kinesis:us-east-1:123:stream/no-stream"
			}

			rec := doRequest(t, h, "ListTagsForResource", map[string]any{
				"ResourceARN": arn,
			})
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				var resp struct {
					Tags []struct {
						Key   string `json:"Key"`
						Value string `json:"Value"`
					} `json:"Tags"`
				}

				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.NotNil(t, resp.Tags)
			}
		})
	}
}

// TestNewOpsGetSupportedOperations verifies the new operations are listed as supported.
func TestNewOpsGetSupportedOperations(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ops := h.GetSupportedOperations()

	wantOps := []string{
		"DescribeLimits",
		"DescribeAccountSettings",
		"MergeShards",
		"SplitShard",
		"StartStreamEncryption",
		"StopStreamEncryption",
		"DeleteResourcePolicy",
		"GetResourcePolicy",
		"PutResourcePolicy",
		"ListTagsForResource",
	}

	for _, op := range wantOps {
		assert.Contains(t, ops, op, "expected %q to be in GetSupportedOperations()", op)
	}
}
