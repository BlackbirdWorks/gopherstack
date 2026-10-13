package bedrockruntime_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/bedrock"
	"github.com/blackbirdworks/gopherstack/services/bedrockruntime"
)

type lateBedrockConfig struct {
	handler **bedrock.Handler
}

func (f lateBedrockConfig) GetBedrockHandler() service.Registerable {
	if *f.handler == nil {
		return nil
	}

	return *f.handler
}

func TestProvider_GuardrailReferencesResolveAfterInit(t *testing.T) {
	t.Parallel()

	var br *bedrock.Handler

	reg, err := (&bedrockruntime.Provider{}).Init(&service.AppContext{Config: lateBedrockConfig{handler: &br}})
	require.NoError(t, err)

	h, ok := reg.(*bedrockruntime.Handler)
	require.True(t, ok)

	converse := map[string]any{
		"messages":        []any{map[string]any{"role": "user", "content": []map[string]any{{"text": "hi"}}}},
		"guardrailConfig": map[string]string{"guardrailIdentifier": "ghost", "guardrailVersion": "DRAFT"},
	}
	apply := map[string]any{"source": "INPUT", "content": []map[string]any{{"text": map[string]string{"text": "hi"}}}}

	bb := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
	br = bedrock.NewHandler(bb)

	g, err := bb.CreateGuardrail("real", "", "blocked", "blocked", nil)
	require.NoError(t, err)

	tests := []struct {
		body     any
		name     string
		path     string
		wantCode int
	}{
		{
			name:     "converse unknown",
			path:     "/model/anthropic.claude-v2/converse",
			body:     converse,
			wantCode: http.StatusNotFound,
		},
		{
			name: "apply unknown", path: "/guardrail/ghost/version/DRAFT/apply",
			body: apply, wantCode: http.StatusNotFound,
		},
		{
			name: "apply known", path: "/guardrail/" + g.GuardrailID + "/version/DRAFT/apply",
			body: apply, wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doRequest(t, h, http.MethodPost, tt.path, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code, rec.Body.String())
		})
	}
}

func TestAsyncInvoke_ListValidationAndMessages(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		path       string
		notContain string
		wantCode   int
	}{
		{name: "max results zero", path: "/async-invoke?maxResults=0", wantCode: http.StatusBadRequest},
		{name: "max results too big", path: "/async-invoke?maxResults=1001", wantCode: http.StatusBadRequest},
		{name: "unknown next token", path: "/async-invoke?nextToken=bogus", wantCode: http.StatusBadRequest},
		{
			name: "get unknown has no code prefix", wantCode: http.StatusBadRequest,
			path:       "/async-invoke/arn:aws:bedrock:us-east-1:000000000000:async-invoke/zzzzzzzzzzzz",
			notContain: "ResourceNotFoundException:",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doRequest(t, newTestHandler(t), http.MethodGet, tt.path, nil)
			assert.Equal(t, tt.wantCode, rec.Code, rec.Body.String())

			if tt.notContain != "" {
				assert.NotContains(t, rec.Body.String(), tt.notContain)
			}
		})
	}
}
