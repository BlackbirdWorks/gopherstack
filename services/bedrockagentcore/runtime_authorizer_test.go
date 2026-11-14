package bedrockagentcore_test

import (
	"context"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/testclient"
	"github.com/blackbirdworks/gopherstack/services/bedrockagentcore"
)

func TestRuntimeRejectsUnauthorizedCallers(t *testing.T) {
	t.Parallel()

	const challenge = `Bearer error="invalid_request"`

	tests := []struct {
		name   string
		path   string
		code   string
		status int
	}{
		{"invoke", "/invocations", "UnauthorizedException", http.StatusUnauthorized},
		{"stop", "/stopruntimesession", "UnauthorizedException", http.StatusUnauthorized},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctl := defaultControl()
			ctl.runtimes[testRuntime+"|DEFAULT"] = bedrockagentcore.RuntimeInfo{
				ID: testRuntime, ImageURI: "img",
				Authorize: func(context.Context, *http.Request) *bedrockagentcore.AuthFailure {
					return &bedrockagentcore.AuthFailure{
						Status: http.StatusUnauthorized, Challenge: challenge, Message: "Missing bearer token",
					}
				},
			}

			f := newFixture(t, ctl)
			sess := strings.Repeat("s", 33)

			req, err := http.NewRequestWithContext(t.Context(), http.MethodPost,
				f.url+"/runtimes/"+url.PathEscape(runtimeARN)+tt.path, strings.NewReader("{}"))
			require.NoError(t, err)
			req.Header["X-Amzn-Bedrock-AgentCore-Runtime-Session-Id"] = []string{sess}
			req.Header.Set("User-Agent", "aws-sdk-go-v2/1.41.0 api/bedrockagentcore#1.0.0")

			resp, err := testclient.New(t).Do(req)
			require.NoError(t, err)

			defer resp.Body.Close()

			body, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			assert.Equal(t, tt.status, resp.StatusCode, string(body))
			assert.Equal(t, challenge, resp.Header.Get("WWW-Authenticate"))
			assert.Contains(t, resp.Header.Get("X-Amzn-Errortype"), tt.code)
		})
	}
}
