package kinesis_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestErrValidation verifies ErrValidation is returned for invalid stream name.
func TestErrValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		streamName string
	}{
		{name: "empty_name", streamName: ""},
		{name: "too_long", streamName: string(make([]byte, 129))},
		{name: "invalid_chars", streamName: "stream with spaces"},
		{name: "slash", streamName: "stream/bad"},
		{name: "ampersand", streamName: "stream&bad"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "CreateStream", map[string]any{
				"StreamName": tt.streamName,
				"ShardCount": 1,
			})
			assert.Equal(t, http.StatusBadRequest, rec.Code)

			var errResp struct {
				Type string `json:"__type"`
			}

			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
			assert.Equal(t, "ValidationException", errResp.Type)
		})
	}
}
