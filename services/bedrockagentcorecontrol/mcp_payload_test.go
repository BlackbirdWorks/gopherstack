package bedrockagentcorecontrol_test

import (
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/httpclient"
)

func TestGatewayMCPPayloadLimit(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		padBytes int
		rejected bool
	}{
		{name: "five mib", padBytes: 5 << 20},
		{name: "seven mib", padBytes: 7 << 20, rejected: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			m := newMCPFixture(t)
			gw := createGateway(t, m.client, "Big-Gw")
			m.clock.advance()

			pad := strings.Repeat("x", tt.padBytes)
			body := `{"jsonrpc":"2.0","id":1,"method":"ping","params":{"pad":"` + pad + `"}}`

			req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, m.mcpURL(aws.ToString(gw.GatewayId)),
				strings.NewReader(body))
			require.NoError(t, err)

			req.Header.Set("Authorization", "Bearer "+m.token(t, nil))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Accept", "application/json, text/event-stream")

			resp, err := httpclient.NewTransport().RoundTrip(req)
			require.NoError(t, err)

			defer resp.Body.Close()

			out, err := io.ReadAll(resp.Body)
			require.NoError(t, err)

			if tt.rejected {
				assert.Contains(t, string(out), "request body could not be read")

				return
			}

			assert.NotContains(t, string(out), "request body could not be read")
			assert.Contains(t, string(out), `"result"`)
		})
	}
}
