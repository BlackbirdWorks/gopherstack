package bedrockagentcore_test

import (
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/testclient"
)

func TestSuccessStatusMatchesAPIReference(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		path   string
		body   string
		status int
	}{
		{
			name:   "create event",
			path:   "/memories/" + testMemory + "/events",
			body:   `{"actorId":"a","sessionId":"s","eventTimestamp":1700000000,"payload":[{"blob":"x"}]}`,
			status: http.StatusCreated,
		},
		{
			name:   "batch create records",
			path:   "/memories/" + testMemory + "/memoryRecords/batchCreate",
			body:   `{"records":[{}]}`,
			status: http.StatusCreated,
		},
		{
			name:   "batch delete records",
			path:   "/memories/" + testMemory + "/memoryRecords/batchDelete",
			body:   `{"records":[{"memoryRecordId":"x"}]}`,
			status: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t, defaultControl())

			body := strings.NewReader(tt.body)

			req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, f.url+tt.path, body)
			require.NoError(t, err)
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("User-Agent", "aws-sdk-go-v2/1.41.0 api/bedrockagentcore#1.0.0")

			resp, err := testclient.New(t).Do(req)
			require.NoError(t, err)

			defer resp.Body.Close()

			raw, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			assert.Equal(t, tt.status, resp.StatusCode, string(raw))
		})
	}
}
