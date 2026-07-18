package appsync_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBackend_EvaluateMappingTemplate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		template    string
		contextJSON string
		wantErr     bool
	}{
		{
			name:        "simple_template",
			template:    `{"version": "2017-02-28", "payload": {}}`,
			contextJSON: "",
		},
		{
			name:        "invalid_context_json",
			template:    `{"version": "2017-02-28"}`,
			contextJSON: "not-json",
			wantErr:     true,
		},
		{
			name:        "with_context",
			template:    `{"version": "2017-02-28", "payload": {}}`,
			contextJSON: `{"arguments": {"id": "1"}}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			out, err := b.EvaluateMappingTemplate(tt.template, tt.contextJSON)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.NotEmpty(t, out)
		})
	}
}

func TestBackend_EvaluateCode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		code    string
		wantErr bool
	}{
		{
			name: "valid_code",
			code: `export function request(ctx) { return {}; }`,
		},
		{
			name:    "empty_code",
			code:    "",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			out, err := b.EvaluateCode(tt.code, "", "", "")

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.NotEmpty(t, out)
		})
	}
}
