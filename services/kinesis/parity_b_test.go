package kinesis_test

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParity_GetRecords_SizeCap_ExcludesPartitionKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{
			name: "records_with_large_partitionkey_not_counted_toward_cap",
			run: func(t *testing.T) {
				t.Parallel()

				h := newTestHandler(t)
				streamName := "parity-b-large-pk-stream"

				rec := doRequest(t, h, "CreateStream", map[string]any{
					"StreamName": streamName,
					"ShardCount": 1,
				})
				require.Equal(t, http.StatusOK, rec.Code)

				rec = doRequest(t, h, "DescribeStream", map[string]any{"StreamName": streamName})
				require.Equal(t, http.StatusOK, rec.Code)

				var descResp struct {
					StreamDescription struct {
						Shards []struct {
							ShardID string `json:"ShardId"`
						} `json:"Shards"`
					} `json:"StreamDescription"`
				}
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
				require.NotEmpty(t, descResp.StreamDescription.Shards)
				shardID := descResp.StreamDescription.Shards[0].ShardID

				// 3 records: tiny data (10 raw bytes) + large partition key (1000 chars).
				smallData := base64.StdEncoding.EncodeToString(make([]byte, 10))
				largePK := strings.Repeat("k", 1000)

				records := make([]map[string]any, 3)
				for i := range records {
					records[i] = map[string]any{
						"Data":         smallData,
						"PartitionKey": largePK,
					}
				}

				rec = doRequest(t, h, "PutRecords", map[string]any{
					"StreamName": streamName,
					"Records":    records,
				})
				require.Equal(t, http.StatusOK, rec.Code)

				var putResp struct {
					FailedRecordCount int `json:"FailedRecordCount"`
				}
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &putResp))
				assert.Equal(t, 0, putResp.FailedRecordCount)

				rec = doRequest(t, h, "GetShardIterator", map[string]any{
					"StreamName":        streamName,
					"ShardId":           shardID,
					"ShardIteratorType": "TRIM_HORIZON",
				})
				require.Equal(t, http.StatusOK, rec.Code)

				var iterResp struct {
					ShardIterator string `json:"ShardIterator"`
				}
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &iterResp))
				require.NotEmpty(t, iterResp.ShardIterator)

				rec = doRequest(t, h, "GetRecords", map[string]any{
					"ShardIterator": iterResp.ShardIterator,
				})
				require.Equal(t, http.StatusOK, rec.Code)

				var getResp struct {
					Records []struct {
						PartitionKey string `json:"PartitionKey"`
					} `json:"Records"`
				}
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getResp))
				assert.Len(t, getResp.Records, 3,
					"all 3 records should be returned: large PartitionKey must not count toward the 10 MiB data cap")
			},
		},
		{
			name: "data_bytes_counted_toward_cap",
			run: func(t *testing.T) {
				t.Parallel()

				h := newTestHandler(t)
				streamName := "parity-b-data-cap-stream"

				rec := doRequest(t, h, "CreateStream", map[string]any{
					"StreamName": streamName,
					"ShardCount": 1,
				})
				require.Equal(t, http.StatusOK, rec.Code)

				rec = doRequest(t, h, "DescribeStream", map[string]any{"StreamName": streamName})
				require.Equal(t, http.StatusOK, rec.Code)

				var descResp struct {
					StreamDescription struct {
						Shards []struct {
							ShardID string `json:"ShardId"`
						} `json:"Shards"`
					} `json:"StreamDescription"`
				}
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
				require.NotEmpty(t, descResp.StreamDescription.Shards)
				shardID := descResp.StreamDescription.Shards[0].ShardID

				// 3 records each ~4 MiB of raw data.
				// 4 MiB * 2 = 8 MiB < 10 MiB cap; 4 MiB * 3 = 12 MiB > cap.
				const fourMiB = 4 * 1024 * 1024
				largeData := base64.StdEncoding.EncodeToString(make([]byte, fourMiB))

				for i := range 3 {
					rec = doRequest(t, h, "PutRecords", map[string]any{
						"StreamName": streamName,
						"Records": []map[string]any{
							{
								"Data":         largeData,
								"PartitionKey": fmt.Sprintf("pk-%d", i),
							},
						},
					})
					require.Equal(t, http.StatusOK, rec.Code)
				}

				rec = doRequest(t, h, "GetShardIterator", map[string]any{
					"StreamName":        streamName,
					"ShardId":           shardID,
					"ShardIteratorType": "TRIM_HORIZON",
				})
				require.Equal(t, http.StatusOK, rec.Code)

				var iterResp struct {
					ShardIterator string `json:"ShardIterator"`
				}
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &iterResp))
				require.NotEmpty(t, iterResp.ShardIterator)

				rec = doRequest(t, h, "GetRecords", map[string]any{
					"ShardIterator": iterResp.ShardIterator,
				})
				require.Equal(t, http.StatusOK, rec.Code)

				var getResp struct {
					Records []struct {
						Data []byte `json:"Data"`
					} `json:"Records"`
				}
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getResp))
				assert.LessOrEqual(t, len(getResp.Records), 2,
					"data cap (10 MiB) should stop GetRecords before the 3rd 4-MiB record")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, tc.run)
	}
}
