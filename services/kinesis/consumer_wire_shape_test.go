package kinesis_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestConsumerWireShape_StreamARN locks in that StreamARN only appears on
// DescribeStreamConsumer's ConsumerDescription (types.ConsumerDescription,
// deserializers.go:6353-6432) and never on RegisterStreamConsumer's or
// ListStreamConsumers' Consumer objects (types.Consumer,
// deserializers.go:6279-6349, has no StreamARN member). A prior revision
// wired one shared jsonConsumer struct with StreamARN for all three
// responses; the real SDK deserializer silently drops an unrecognized key
// via its default case, so this never broke a typed-client call -- only a
// raw-body check on the wire JSON can see it.
func TestConsumerWireShape_StreamARN(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	setupRec := doRequest(t, h, "CreateStream", map[string]any{
		"StreamName": "consumer-wire-shape-stream",
		"ShardCount": 1,
	})
	require.Equal(t, http.StatusOK, setupRec.Code)

	setupRec = doRequest(t, h, "DescribeStream", map[string]any{"StreamName": "consumer-wire-shape-stream"})
	require.Equal(t, http.StatusOK, setupRec.Code)
	var descResp struct {
		StreamDescription struct {
			StreamARN string `json:"StreamARN"`
		} `json:"StreamDescription"`
	}
	require.NoError(t, json.Unmarshal(setupRec.Body.Bytes(), &descResp))
	streamARN := descResp.StreamDescription.StreamARN
	require.NotEmpty(t, streamARN)

	setupRec = doRequest(t, h, "RegisterStreamConsumer", map[string]any{
		"StreamARN":    streamARN,
		"ConsumerName": "watcher",
	})
	require.Equal(t, http.StatusOK, setupRec.Code)

	tests := []struct {
		body        map[string]any
		wrapperKey  string
		action      string
		name        string
		wantsStream bool
	}{
		{
			name:       "register has no StreamARN",
			action:     "RegisterStreamConsumer",
			body:       map[string]any{"StreamARN": streamARN, "ConsumerName": "no-leak-consumer"},
			wrapperKey: "Consumer",
		},
		{
			name:       "list has no StreamARN",
			action:     "ListStreamConsumers",
			body:       map[string]any{"StreamARN": streamARN},
			wrapperKey: "Consumers",
		},
		{
			name:        "describe has StreamARN",
			action:      "DescribeStreamConsumer",
			body:        map[string]any{"StreamARN": streamARN, "ConsumerName": "watcher"},
			wrapperKey:  "ConsumerDescription",
			wantsStream: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			rec := doRequest(t, h, tc.action, tc.body)
			require.Equal(t, http.StatusOK, rec.Code)

			var raw map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &raw))

			consumerObj := raw[tc.wrapperKey]
			var obj map[string]any
			switch v := consumerObj.(type) {
			case map[string]any:
				obj = v
			case []any:
				require.NotEmpty(t, v)
				obj, _ = v[0].(map[string]any)
			}
			require.NotNil(t, obj)

			_, hasStreamARN := obj["StreamARN"]
			assert.Equal(t, tc.wantsStream, hasStreamARN)
		})
	}
}
