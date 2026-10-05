package firehose_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDescribeDeliveryStream_HTTPEndpointOmitsAccessKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		key  string
	}{
		{name: "access_key_set", key: "super-secret-key"},
		{name: "access_key_other", key: "another-key"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestFirehoseHandler(t)
			createStream(t, h, "http-stream", map[string]any{
				"HTTPEndpointDestinationConfiguration": map[string]any{
					"EndpointConfiguration": map[string]any{
						"Url": "https://example.com/ingest", "Name": "ep", "AccessKey": tt.key,
					},
					"S3Configuration": map[string]any{
						"BucketARN": "arn:aws:s3:::b", "RoleARN": "arn:aws:iam::000000000000:role/r",
					},
					"RoleARN": "arn:aws:iam::000000000000:role/r",
				},
			})

			rec := doFirehoseRequest(
				t, h, "DescribeDeliveryStream", map[string]any{"DeliveryStreamName": "http-stream"},
			)
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

			var out struct {
				DeliveryStreamDescription struct {
					Destinations []struct {
						HTTP struct {
							EndpointConfiguration map[string]any `json:"EndpointConfiguration"`
						} `json:"HttpEndpointDestinationDescription"`
					} `json:"Destinations"`
				} `json:"DeliveryStreamDescription"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			require.Len(t, out.DeliveryStreamDescription.Destinations, 1)

			ec := out.DeliveryStreamDescription.Destinations[0].HTTP.EndpointConfiguration
			assert.Equal(t, "ep", ec["Name"])
			assert.NotContains(t, ec, "AccessKey")
			assert.NotContains(t, rec.Body.String(), tt.key)
		})
	}
}
