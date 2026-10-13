package bedrockruntime_test

import (
	"maps"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConverse_RequestMetadataAndGuardrailValidation(t *testing.T) {
	t.Parallel()

	manyKeys := map[string]string{}
	for _, k := range []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q"} {
		manyKeys[k] = "v"
	}

	tests := []struct {
		extra      map[string]any
		name       string
		suffix     string
		wantStatus int
	}{
		{
			name:       "valid metadata",
			extra:      map[string]any{"requestMetadata": map[string]string{"team": "a:b/c"}},
			wantStatus: http.StatusOK,
		},
		{
			name:       "too many entries",
			extra:      map[string]any{"requestMetadata": manyKeys},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "bad key char",
			extra:      map[string]any{"requestMetadata": map[string]string{"k!": "v"}},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "empty key",
			extra:      map[string]any{"requestMetadata": map[string]string{"": "v"}},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "long value",
			extra:      map[string]any{"requestMetadata": map[string]string{"k": strings.Repeat("v", 257)}},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "valid guardrail",
			extra: map[string]any{"guardrailConfig": map[string]string{
				"guardrailIdentifier": "abc123", "guardrailVersion": "DRAFT", "trace": "enabled",
			}},
			wantStatus: http.StatusOK,
		},
		{
			name: "bad identifier",
			extra: map[string]any{
				"guardrailConfig": map[string]string{"guardrailIdentifier": "Bad_ID", "guardrailVersion": "1"},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "bad version",
			extra: map[string]any{
				"guardrailConfig": map[string]string{"guardrailIdentifier": "abc", "guardrailVersion": "0"},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "bad trace",
			extra: map[string]any{
				"guardrailConfig": map[string]string{"guardrailIdentifier": "abc", "trace": "full"},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "stream bad mode",
			extra:      map[string]any{"guardrailConfig": map[string]string{"streamProcessingMode": "fast"}},
			suffix:     "-stream",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			body := map[string]any{
				"messages": []map[string]any{{"role": "user", "content": []map[string]any{{"text": "hi"}}}},
			}
			maps.Copy(body, tt.extra)

			op := "/converse"
			if tt.suffix != "" {
				op = "/converse-stream"
			}

			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodPost, "/model/anthropic.claude-v2"+op, body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}
