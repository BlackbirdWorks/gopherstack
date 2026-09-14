package iotwireless_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	iotwirelesssdk "github.com/aws/aws-sdk-go-v2/service/iotwireless"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRealClient_GetPositionEstimateTimestamp covers gopherstack-xhu2t:
// GetPositionEstimateInput.Timestamp (an unixTimestamp body field per
// iotwireless@v1.59.4 serializers.go:4339-4342) was undeclared and the
// response's fabricated "timestamp" GeoJSON property always used
// time.Now(), ignoring whatever the caller requested. Also exercises the
// adjacent httpPayload-envelope fix: GetPositionEstimateOutput.GeoJsonPayload
// (deserializers.go:7445-7461) must decode as the raw GeoJSON bytes, not a
// {"GeoJsonPayload": ...} wrapper.
func TestRealClient_GetPositionEstimateTimestamp(t *testing.T) {
	t.Parallel()

	cases := []struct {
		ts   *time.Time
		name string
	}{
		{name: "explicit_timestamp", ts: aws.Time(time.Unix(1700000000, 0).UTC())},
		{name: "omitted_defaults_to_now"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			srv := newTestIoTWirelessRegistryServer(t)
			client := newTestIoTWirelessSDKClient(t, srv.URL)

			before := time.Now().UTC()

			out, err := client.GetPositionEstimate(t.Context(), &iotwirelesssdk.GetPositionEstimateInput{
				Timestamp: tc.ts,
			})
			require.NoError(t, err)
			require.NotEmpty(t, out.GeoJsonPayload)

			var geoJSON struct {
				Properties struct {
					Timestamp string `json:"timestamp"`
				} `json:"properties"`
				Type string `json:"type"`
			}
			require.NoError(t, json.Unmarshal(out.GeoJsonPayload, &geoJSON))
			assert.Equal(t, "Feature", geoJSON.Type)

			got, err := time.Parse(time.RFC3339, geoJSON.Properties.Timestamp)
			require.NoError(t, err)

			if tc.ts != nil {
				assert.WithinDuration(t, *tc.ts, got, 0)
			} else {
				assert.WithinDuration(t, before, got, 5*time.Second)
			}
		})
	}
}
