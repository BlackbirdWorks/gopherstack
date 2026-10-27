package docker_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/docker"
)

func TestConsumePullStream(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		stream  string
		wantErr string
	}{
		{name: "success", stream: `{"status":"Pulling"}` + "\n" + `{"status":"Downloaded"}` + "\n"},
		{name: "empty", stream: ""},
		{
			name:    "error detail",
			stream:  `{"status":"Pulling"}` + "\n" + `{"errorDetail":{"message":"manifest unknown"},"error":"manifest unknown"}` + "\n" + `{"status":"x"}`,
			wantErr: "manifest unknown",
		},
		{name: "error only", stream: `{"error":"denied: requested access"}`, wantErr: "denied: requested access"},
		{name: "garbage", stream: `{"status":"ok"}` + "\n" + `not json`, wantErr: "decode image pull stream"},
		{name: "truncated", stream: `{"status":"Pull`, wantErr: "decode image pull stream"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := docker.ConsumePullStream(strings.NewReader(tt.stream))
			if tt.wantErr == "" {
				require.NoError(t, err)

				return
			}

			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}
