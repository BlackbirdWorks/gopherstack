package appsync_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// AppSync developer guide, "Logging utilities": $util.log.info/error output is
// returned in the EvaluateMappingTemplate response's logs.
func TestEvaluateMappingTemplate_Logs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		template string
		wantLogs []string
		wantErr  bool
	}{
		{name: "no logs", template: `ok`, wantLogs: []string{}},
		{name: "info and error", template: `$util.log.info("a")$util.log.error("b")ok`, wantLogs: []string{"a", "b"}},
		{
			name:     "placeholders",
			template: `$util.log.info("id={} name={}", 7, "bob")$util.log.error("extra {} {}", "x")ok`,
			wantLogs: []string{"id=7 name=bob", "extra x {}"},
		},
		{
			name:     "logs kept on error",
			template: `$util.log.info("before")$util.error("boom")`,
			wantLogs: []string{"before"},
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newTestHandler()
			rec := doRequest(t, h, http.MethodPost, "/v1/dataplane-evaluations/template",
				map[string]any{"template": tt.template, "context": "{}"})
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

			var out struct {
				Error  map[string]any `json:"error"`
				Result string         `json:"evaluationResult"`
				Logs   []string       `json:"logs"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			assert.Equal(t, tt.wantLogs, out.Logs)
			assert.Equal(t, tt.wantErr, out.Error != nil)
		})
	}
}
