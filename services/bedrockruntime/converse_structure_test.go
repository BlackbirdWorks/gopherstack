package bedrockruntime_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

type anyMap = map[string]any

func TestConverse_StructureValidation(t *testing.T) {
	t.Parallel()

	text := func(s string) []anyMap { return []anyMap{{"text": s}} }
	user := anyMap{"role": "user", "content": text("hi")}
	assistant := anyMap{"role": "assistant", "content": text("yo")}
	schema := anyMap{"json": anyMap{}}
	tool := anyMap{"toolSpec": anyMap{"name": "t1", "inputSchema": schema}}
	badTool := anyMap{"toolSpec": anyMap{"name": "bad name", "inputSchema": schema}}
	msgs := func(m ...anyMap) []anyMap { return m }
	withInference := func(ic anyMap) anyMap { return anyMap{"messages": msgs(user), "inferenceConfig": ic} }
	withTools := func(tc anyMap) anyMap { return anyMap{"messages": msgs(user), "toolConfig": tc} }

	tests := []struct {
		body       anyMap
		name       string
		model      string
		wantStatus int
	}{
		{name: "valid alternation", body: anyMap{"messages": msgs(user, assistant, user)}, wantStatus: http.StatusOK},
		{name: "assistant first", body: anyMap{"messages": msgs(assistant)}, wantStatus: http.StatusBadRequest},
		{name: "two users", body: anyMap{"messages": msgs(user, user)}, wantStatus: http.StatusBadRequest},
		{
			name:       "empty content",
			body:       anyMap{"messages": msgs(anyMap{"role": "user", "content": []any{}})},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "bad role",
			body:       anyMap{"messages": msgs(anyMap{"role": "bot", "content": text("x")})},
			wantStatus: http.StatusBadRequest,
		},
		{name: "temperature high", body: withInference(anyMap{"temperature": 1.5}), wantStatus: http.StatusBadRequest},
		{name: "top p high", body: withInference(anyMap{"topP": 2}), wantStatus: http.StatusBadRequest},
		{name: "max tokens zero", body: withInference(anyMap{"maxTokens": 0}), wantStatus: http.StatusBadRequest},
		{
			name:       "inference ok",
			body:       withInference(anyMap{"temperature": 0.5, "maxTokens": 10}),
			wantStatus: http.StatusOK,
		},
		{name: "no tools", body: withTools(anyMap{"tools": []any{}}), wantStatus: http.StatusBadRequest},
		{name: "bad tool name", body: withTools(anyMap{"tools": []anyMap{badTool}}), wantStatus: http.StatusBadRequest},
		{
			name: "choice names undeclared tool",
			body: withTools(anyMap{
				"tools": []anyMap{tool}, "toolChoice": anyMap{"tool": anyMap{"name": "other"}},
			}),
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "choice two members",
			body: withTools(anyMap{
				"tools": []anyMap{tool}, "toolChoice": anyMap{"auto": anyMap{}, "any": anyMap{}},
			}),
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "tool config ok",
			body: withTools(anyMap{
				"tools": []anyMap{tool}, "toolChoice": anyMap{"tool": anyMap{"name": "t1"}},
			}),
			wantStatus: http.StatusOK,
		},
		{
			name: "model id bad chars", model: "bad%20id%21", body: anyMap{"messages": msgs(user)},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			model := tt.model
			if model == "" {
				model = "anthropic.claude-3-haiku-20240307-v1:0"
			}

			h := newTestHandler(t)

			for _, op := range []string{"/converse", "/converse-stream"} {
				rec := doRequest(t, h, http.MethodPost, "/model/"+model+op, tt.body)
				assert.Equal(t, tt.wantStatus, rec.Code, op+" "+rec.Body.String())
			}
		})
	}
}
