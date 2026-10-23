package app

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type recordingOutbound struct{ called bool }

func (r *recordingOutbound) Get(context.Context, string, map[string]string) (int, []byte, error) {
	r.called = true

	return 200, nil, nil
}

func TestStageRequestFor(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		url   string
		api   string
		stage string
		path  string
		ok    bool
	}{
		{
			name: "execute-api host", ok: true, api: "abc123", stage: "prod",
			url:  "https://abc123.execute-api.us-east-1.amazonaws.com/prod/servers/s-1/users/u/config?protocol=SFTP",
			path: "/servers/s-1/users/u/config",
		},
		{
			name: "restapis path", ok: true, api: "abc123", stage: "dev",
			url:  "http://localhost:4566/restapis/abc123/dev/_user_request_/servers/s-1/users/u/config",
			path: "/servers/s-1/users/u/config",
		},
		{name: "external host", url: "https://idp.example.com/prod/servers/s-1"},
		{name: "no stage", url: "https://abc123.execute-api.us-east-1.amazonaws.com/"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req, ok := stageRequestFor(tt.url, map[string]string{"Password": "x"})
			require.Equal(t, tt.ok, ok)

			if !ok {
				return
			}

			assert.Equal(t, tt.api, req.APIID)
			assert.Equal(t, tt.stage, req.Stage)
			assert.Equal(t, tt.path, req.Path)
			assert.Equal(t, "x", req.Headers["Password"])
		})
	}
}

func TestTransferAPIGatewayClientFallsBackOutbound(t *testing.T) {
	t.Parallel()

	out := &recordingOutbound{}
	c := &transferAPIGatewayClient{outbound: out}

	_, _, err := c.Get(t.Context(), "https://idp.example.com/prod", nil)
	require.NoError(t, err)
	assert.True(t, out.called)
}
