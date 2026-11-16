package bedrockagentcorecontrol_test

import (
	"encoding/base64"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/bedrockagentcorecontrol"
	"github.com/aws/aws-sdk-go-v2/service/bedrockagentcorecontrol/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/testclient"
)

const modernMeta = `"_meta":{"io.modelcontextprotocol/protocolVersion":"2026-07-28",` +
	`"io.modelcontextprotocol/clientInfo":{"name":"t","version":"1"},"io.modelcontextprotocol/clientCapabilities":{}}`

func (m *mcpFixture) searchGateway(t *testing.T, name string, search bool) string {
	t.Helper()

	cfg := &types.MCPGatewayConfiguration{SupportedVersions: []string{"2025-11-25", "2026-07-28"}}
	if search {
		cfg.SearchType = types.SearchTypeSemantic
	}

	out, err := m.client.CreateGateway(t.Context(), &sdk.CreateGatewayInput{
		Name: aws.String(name), RoleArn: aws.String(testRole), AuthorizerType: types.AuthorizerTypeCustomJwt,
		AuthorizerConfiguration: jwtAuthorizer(),
		ProtocolConfiguration:   &types.GatewayProtocolConfigurationMemberMcp{Value: *cfg},
	})
	require.NoError(t, err)

	gid := aws.ToString(out.GatewayId)
	m.createTarget(t, gid, "shop", lambdaTools("orders_lookup", "weather_forecast"))
	m.clock.advance()

	return gid
}

func (m *mcpFixture) rpc(t *testing.T, gid string, hdr map[string]string, body string) (int, string) {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, m.mcpURL(gid), strings.NewReader(body))
	require.NoError(t, err)

	req.Header.Set("Authorization", "Bearer "+m.token(t, nil))
	req.Header.Set("Content-Type", "application/json")

	for k, v := range hdr {
		req.Header.Set(k, v)
	}

	resp, err := testclient.New(t).Do(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	raw, _ := io.ReadAll(resp.Body)

	return resp.StatusCode, string(raw)
}

func TestGatewayMCPModernVersion(t *testing.T) {
	t.Parallel()

	m := newMCPFixture(t)
	gid := m.searchGateway(t, "Modern-Gw", false)
	hdr := func(method string, extra ...string) map[string]string {
		h := map[string]string{"MCP-Protocol-Version": "2026-07-28", "Mcp-Method": method}
		for i := 0; i+1 < len(extra); i += 2 {
			h[extra[i]] = extra[i+1]
		}

		return h
	}
	b64 := "=?base64?" + base64.StdEncoding.EncodeToString([]byte("shop___orders_lookup")) + "?="

	tests := []struct {
		hdr        map[string]string
		name       string
		body       string
		wantSub    string
		wantStatus int
	}{
		{
			name: "discover", hdr: hdr("server/discover"), wantStatus: 200,
			body:    `{"jsonrpc":"2.0","id":1,"method":"server/discover","params":{` + modernMeta + `}}`,
			wantSub: `"supportedVersions":["2025-11-25","2026-07-28"]`,
		},
		{
			name: "list", hdr: hdr("tools/list"), wantStatus: 200,
			body:    `{"jsonrpc":"2.0","id":1,"method":"tools/list","params":{` + modernMeta + `}}`,
			wantSub: `"resultType":"complete"`,
		},
		{
			name: "call with encoded name", hdr: hdr("tools/call", "Mcp-Name", b64), wantStatus: 200,
			body: `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"shop___orders_lookup",` +
				`"arguments":{"msg":"x"},` + modernMeta + `}}`,
			wantSub: `"resultType":"complete"`,
		},
		{
			name: "method header mismatch", hdr: hdr("tools/call"), wantStatus: 400,
			body:    `{"jsonrpc":"2.0","id":1,"method":"tools/list","params":{` + modernMeta + `}}`,
			wantSub: `"code":-32020`,
		},
		{
			name: "name header mismatch", hdr: hdr("tools/call", "Mcp-Name", "other"),
			wantStatus: 400,
			body: `{"jsonrpc":"2.0","id":1,"method":"tools/call",` +
				`"params":{"name":"shop___orders_lookup",` + modernMeta + `}}`,
			wantSub: `"code":-32020`,
		},
		{
			name: "meta required", hdr: hdr("tools/list"), wantStatus: 400,
			body:    `{"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}`,
			wantSub: `"code":-32602`,
		},
		{
			name: "initialize not served", hdr: hdr("initialize"), wantStatus: 404,
			body:    `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{` + modernMeta + `}}`,
			wantSub: `"code":-32601`,
		},
		{
			name: "unsupported version", hdr: map[string]string{"MCP-Protocol-Version": "1999-01-01"}, wantStatus: 400,
			body:    `{"jsonrpc":"2.0","id":1,"method":"ping"}`,
			wantSub: `"code":-32022`,
		},
		{
			name: "legacy initialize skips modern", hdr: map[string]string{"MCP-Protocol-Version": "2025-11-25"},
			wantStatus: 200,
			body:       `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2026-07-28"}}`,
			wantSub:    `"protocolVersion":"2025-11-25"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			status, raw := m.rpc(t, gid, tt.hdr, tt.body)
			assert.Equal(t, tt.wantStatus, status, raw)
			assert.Contains(t, raw, tt.wantSub)
		})
	}
}

func TestGatewayMCPSearchTool(t *testing.T) {
	t.Parallel()

	m := newMCPFixture(t)
	withSearch := m.searchGateway(t, "Search-Gw", true)
	noSearch := m.searchGateway(t, "Plain-Gw", false)
	call := func(q string) string {
		return `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"x_amz_bedrock_agentcore_search",` +
			`"arguments":{"query":"` + q + `"}}}`
	}

	tests := []struct {
		name       string
		gid        string
		body       string
		wantSub    string
		wantNot    string
		wantStatus int
	}{
		{
			name: "listed first", gid: withSearch, wantStatus: 200,
			body:    `{"jsonrpc":"2.0","id":1,"method":"tools/list"}`,
			wantSub: `{"result":{"tools":[{"description":"A special tool`,
		},
		{
			name: "ranks matching tool", gid: withSearch, body: call("weather forecast"), wantStatus: 200,
			wantSub: "shop___weather_forecast", wantNot: "shop___orders_lookup",
		},
		{
			name: "query is truncated", gid: withSearch, wantStatus: 200, wantSub: `"tools":[]`,
			body: call(strings.Repeat("zzz ", 300) + "weather forecast"),
		},
		{name: "no overlap", gid: withSearch, body: call("zzz"), wantStatus: 200, wantSub: `"tools":[]`},
		{name: "disabled", gid: noSearch, body: call("weather"), wantStatus: 200, wantSub: "Unknown tool"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			status, raw := m.rpc(t, tt.gid, nil, tt.body)
			assert.Equal(t, tt.wantStatus, status, raw)
			assert.Contains(t, raw, tt.wantSub)

			if tt.wantNot != "" {
				assert.NotContains(t, raw, tt.wantNot)
			}
		})
	}
}
