package bedrockagentcorecontrol_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/credentials"
	sdk "github.com/aws/aws-sdk-go-v2/service/bedrockagentcorecontrol"
	"github.com/aws/aws-sdk-go-v2/service/bedrockagentcorecontrol/types"
	"github.com/golang-jwt/jwt/v5"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/httpclient"
	"github.com/blackbirdworks/gopherstack/pkgs/testclient"
)

const (
	testIssuer = "https://idp.example.com"
	testKID    = "key-1"
)

type fakeJWKS struct{ key *rsa.PublicKey }

var errUnknownKey = errors.New("unknown key")

func (f fakeJWKS) GetJWTPublicKey(issuer, kid string) (*rsa.PublicKey, error) {
	if issuer != testIssuer || kid != testKID {
		return nil, fmt.Errorf("%w %s/%s", errUnknownKey, issuer, kid)
	}

	return f.key, nil
}

type fakeInvoker struct {
	fakeRefs
	calls *callLog
}

type invocation struct {
	arn, clientContext string
	payload            []byte
}

type callLog struct {
	byPayload map[string]invocation
	mu        sync.Mutex
}

func newCallLog() *callLog { return &callLog{byPayload: map[string]invocation{}} }

func (l *callLog) record(inv invocation) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.byPayload[string(inv.payload)] = inv
}

func (l *callLog) get(payload string) (invocation, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()

	inv, ok := l.byPayload[payload]

	return inv, ok
}

func (f fakeInvoker) InvokeLambda(
	_ context.Context, arn, clientContext string, payload []byte,
) ([]byte, string, error) {
	f.calls.record(invocation{arn: arn, clientContext: clientContext, payload: payload})

	var in map[string]any
	_ = json.Unmarshal(payload, &in)

	if in["boom"] == true {
		return []byte(`{"errorMessage":"boom"}`), "Unhandled", nil
	}

	out, _ := json.Marshal(map[string]any{"echo": in})

	return out, "", nil
}

type mcpFixture struct {
	*fixture
	key   *rsa.PrivateKey
	calls *callLog
}

func newMCPFixture(t *testing.T) *mcpFixture {
	t.Helper()

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	f := newFixture(t)
	calls := newCallLog()
	f.backend.SetReferences(fakeInvoker{functions: map[string]bool{lambdaARN: true}, calls: calls})
	f.backend.SetJWKSProvider(fakeJWKS{key: &key.PublicKey})

	return &mcpFixture{fixture: f, key: key, calls: calls}
}

func (m *mcpFixture) token(t *testing.T, claims jwt.MapClaims) string {
	t.Helper()

	base := jwt.MapClaims{"iss": testIssuer, "aud": "aud", "exp": time.Now().Add(time.Hour).Unix()}
	maps.Copy(base, claims)

	tok := jwt.NewWithClaims(jwt.SigningMethodRS256, base)
	tok.Header["kid"] = testKID

	signed, err := tok.SignedString(m.key)
	require.NoError(t, err)

	return signed
}

func (m *mcpFixture) mcpURL(id string) string { return m.url + "/agentcore/gateways/" + id + "/mcp" }

type headerRT struct {
	next          http.RoundTripper
	header, value string
}

func (h headerRT) RoundTrip(r *http.Request) (*http.Response, error) {
	r = r.Clone(r.Context())
	if h.value != "" {
		r.Header.Set(h.header, h.value)
	}

	return h.next.RoundTrip(r)
}

func connectMCP(t *testing.T, endpoint, bearer string) (*mcp.ClientSession, error) {
	t.Helper()

	hc := &http.Client{
		Transport: headerRT{next: httpclient.NewTransport(), header: "Authorization", value: bearerValue(bearer)},
	}

	return mcp.NewClient(&mcp.Implementation{Name: "test", Version: "1"}, nil).Connect(t.Context(),
		&mcp.StreamableClientTransport{
			Endpoint:             endpoint,
			HTTPClient:           hc,
			DisableStandaloneSSE: true,
			MaxRetries:           -1,
		}, nil)
}

func bearerValue(token string) string {
	if token == "" {
		return ""
	}

	return "Bearer " + token
}

func lambdaTools(names ...string) types.TargetConfiguration {
	defs := make([]types.ToolDefinition, 0, len(names))
	for _, n := range names {
		defs = append(defs, types.ToolDefinition{
			Name: aws.String(n), Description: aws.String(n + " tool"),
			InputSchema: &types.SchemaDefinition{
				Type: types.SchemaTypeObject, Required: []string{"msg"},
				Properties: map[string]types.SchemaDefinition{"msg": {Type: types.SchemaTypeString}},
			},
		})
	}

	return &types.TargetConfigurationMemberMcp{Value: &types.McpTargetConfigurationMemberLambda{
		Value: types.McpLambdaTargetConfiguration{
			LambdaArn: aws.String(lambdaARN), ToolSchema: &types.ToolSchemaMemberInlinePayload{Value: defs},
		},
	}}
}

func (m *mcpFixture) createTarget(t *testing.T, gw, name string, cfg types.TargetConfiguration) string {
	t.Helper()

	out, err := m.client.CreateGatewayTarget(t.Context(), &sdk.CreateGatewayTargetInput{
		GatewayIdentifier: aws.String(gw), Name: aws.String(name), TargetConfiguration: cfg,
		CredentialProviderConfigurations: []types.CredentialProviderConfiguration{
			{CredentialProviderType: types.CredentialProviderTypeGatewayIamRole},
		},
	})
	require.NoError(t, err)
	m.clock.advance()

	return aws.ToString(out.TargetId)
}

func TestGatewayMCPLambdaTarget(t *testing.T) {
	t.Parallel()

	m := newMCPFixture(t)
	gw := createGateway(t, m.client, "Mcp-Gw")
	gid := aws.ToString(gw.GatewayId)
	m.createTarget(t, gid, "util", lambdaTools("echo", "fail"))
	m.clock.advance()

	cs, err := connectMCP(t, m.mcpURL(gid), m.token(t, nil))
	require.NoError(t, err)

	t.Cleanup(func() { _ = cs.Close() })

	init := cs.InitializeResult()
	assert.NotEmpty(t, init.ProtocolVersion)
	assert.Equal(t, "Mcp-Gw", init.ServerInfo.Name)

	list, err := cs.ListTools(t.Context(), nil)
	require.NoError(t, err)
	require.Len(t, list.Tools, 2)
	assert.Equal(t, "util___echo", list.Tools[0].Name)
	assert.Equal(t, "echo tool", list.Tools[0].Description)

	tests := []struct {
		args    map[string]any
		name    string
		tool    string
		wantTxt string
		isError bool
	}{
		{name: "echo", tool: "util___echo", args: map[string]any{"msg": "hi"}, wantTxt: `"msg":"hi"`},
		{
			name:    "function error",
			tool:    "util___fail",
			args:    map[string]any{"boom": true},
			wantTxt: "boom",
			isError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			res, callErr := cs.CallTool(t.Context(), &mcp.CallToolParams{Name: tt.tool, Arguments: tt.args})
			require.NoError(t, callErr)
			assert.Equal(t, tt.isError, res.IsError)
			require.Len(t, res.Content, 1)
			assert.Contains(t, res.Content[0].(*mcp.TextContent).Text, tt.wantTxt)

			call, found := m.calls.get(mustJSON(t, tt.args))
			require.True(t, found)
			assert.Equal(t, lambdaARN, call.arn)
			assert.NotEmpty(t, call.clientContext)
		})
	}

	_, err = cs.CallTool(t.Context(), &mcp.CallToolParams{Name: "util___missing"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "Unknown tool")

	_, err = cs.CallTool(t.Context(), &mcp.CallToolParams{Name: "nosep"})
	require.Error(t, err)
}

func mustJSON(t *testing.T, v any) string {
	t.Helper()

	raw, err := json.Marshal(v)
	require.NoError(t, err)

	return string(raw)
}

func TestGatewayMCPClientContext(t *testing.T) {
	t.Parallel()

	m := newMCPFixture(t)
	gw := createGateway(t, m.client, "Ctx-Gw")
	gid := aws.ToString(gw.GatewayId)
	tid := m.createTarget(t, gid, "util", lambdaTools("echo"))
	m.clock.advance()

	cs, err := connectMCP(t, m.mcpURL(gid), m.token(t, nil))
	require.NoError(t, err)

	t.Cleanup(func() { _ = cs.Close() })

	_, err = cs.CallTool(t.Context(), &mcp.CallToolParams{Name: "util___echo", Arguments: map[string]any{"msg": "x"}})
	require.NoError(t, err)

	call, found := m.calls.get(`{"msg":"x"}`)
	require.True(t, found)

	raw, err := decodeB64(call.clientContext)
	require.NoError(t, err)

	var cc struct {
		Custom map[string]string `json:"custom"`
	}

	require.NoError(t, json.Unmarshal(raw, &cc))
	assert.Equal(t, "util___echo", cc.Custom["bedrockAgentCoreToolName"])
	assert.Equal(t, gid, cc.Custom["bedrockAgentCoreGatewayId"])
	assert.Equal(t, tid, cc.Custom["bedrockAgentCoreTargetId"])
}

func TestGatewayMCPPagination(t *testing.T) {
	t.Parallel()

	m := newMCPFixture(t)
	gw := createGateway(t, m.client, "Page-Gw")
	gid := aws.ToString(gw.GatewayId)

	names := make([]string, 0, 130)
	for i := range 130 {
		names = append(names, fmt.Sprintf("tool%03d", i))
	}

	m.createTarget(t, gid, "big", lambdaTools(names...))
	m.clock.advance()

	cs, err := connectMCP(t, m.mcpURL(gid), m.token(t, nil))
	require.NoError(t, err)

	t.Cleanup(func() { _ = cs.Close() })

	first, err := cs.ListTools(t.Context(), nil)
	require.NoError(t, err)
	assert.Len(t, first.Tools, 100)
	require.NotEmpty(t, first.NextCursor)

	second, err := cs.ListTools(t.Context(), &mcp.ListToolsParams{Cursor: first.NextCursor})
	require.NoError(t, err)
	assert.Len(t, second.Tools, 30)
	assert.Empty(t, second.NextCursor)

	var seen int

	for _, err := range cs.Tools(t.Context(), nil) {
		require.NoError(t, err)

		seen++
	}

	assert.Equal(t, 130, seen)
}

func TestGatewayMCPJWTAuthorizer(t *testing.T) {
	t.Parallel()

	m := newMCPFixture(t)
	gw, err := m.client.CreateGateway(t.Context(), &sdk.CreateGatewayInput{
		Name: aws.String("Jwt-Gw"), RoleArn: aws.String(testRole), AuthorizerType: types.AuthorizerTypeCustomJwt,
		AuthorizerConfiguration: &types.AuthorizerConfigurationMemberCustomJWTAuthorizer{
			Value: types.CustomJWTAuthorizerConfiguration{
				DiscoveryUrl: aws.String(testIssuer + "/.well-known/openid-configuration"),
				AllowedAudience: []string{
					"aud",
				},
				AllowedClients: []string{"client-a"},
				AllowedScopes:  []string{"mcp.read"},
				CustomClaims: []types.CustomClaimValidationType{{
					InboundTokenClaimName:      aws.String("groups"),
					InboundTokenClaimValueType: types.InboundTokenClaimValueTypeStringArray,
					AuthorizingClaimMatchValue: &types.AuthorizingClaimMatchValueType{
						ClaimMatchOperator: types.ClaimMatchOperatorTypeContains,
						ClaimMatchValue:    &types.ClaimMatchValueTypeMemberMatchValueString{Value: "admins"},
					},
				}},
			},
		},
	})
	require.NoError(t, err)

	endpoint := m.mcpURL(aws.ToString(gw.GatewayId))
	good := jwt.MapClaims{"client_id": "client-a", "scope": "openid mcp.read", "groups": []string{"admins", "dev"}}

	other := func(k string, v any) jwt.MapClaims {
		c := jwt.MapClaims{}
		maps.Copy(c, good)

		c[k] = v

		return c
	}

	tests := []struct {
		token      func() string
		name       string
		wantStatus int
	}{
		{name: "valid", token: func() string { return m.token(t, good) }, wantStatus: http.StatusOK},
		{name: "no token", token: func() string { return "" }, wantStatus: http.StatusUnauthorized},
		{name: "garbage", token: func() string { return "abc.def.ghi" }, wantStatus: http.StatusUnauthorized},
		{
			name:       "expired",
			token:      func() string { return m.token(t, other("exp", time.Now().Add(-time.Hour).Unix())) },
			wantStatus: http.StatusUnauthorized,
		},
		{
			name:       "wrong audience",
			token:      func() string { return m.token(t, other("aud", "nope")) },
			wantStatus: http.StatusUnauthorized,
		},
		{
			name:       "wrong client",
			token:      func() string { return m.token(t, other("client_id", "client-b")) },
			wantStatus: http.StatusUnauthorized,
		},
		{
			name:       "wrong issuer",
			token:      func() string { return m.token(t, other("iss", "https://evil.example.com")) },
			wantStatus: http.StatusUnauthorized,
		},
		{
			name:       "missing group",
			token:      func() string { return m.token(t, other("groups", []string{"dev"})) },
			wantStatus: http.StatusUnauthorized,
		},
		{
			name:       "missing scope",
			token:      func() string { return m.token(t, other("scope", "openid")) },
			wantStatus: http.StatusForbidden,
		},
		{name: "foreign key", token: func() string {
			other, kerr := rsa.GenerateKey(rand.Reader, 2048)
			require.NoError(t, kerr)

			tok := jwt.NewWithClaims(jwt.SigningMethodRS256, jwt.MapClaims{
				"iss": testIssuer, "aud": "aud", "client_id": "client-a", "scope": "mcp.read",
				"groups": []string{"admins"}, "exp": time.Now().Add(time.Hour).Unix(),
			})
			tok.Header["kid"] = testKID
			signed, serr := tok.SignedString(other)
			require.NoError(t, serr)

			return signed
		}, wantStatus: http.StatusUnauthorized},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			status, hdr := postRPC(t, endpoint, bearerValue(tt.token()), `{"jsonrpc":"2.0","id":1,"method":"ping"}`)
			assert.Equal(t, tt.wantStatus, status)

			if tt.wantStatus != http.StatusOK {
				assert.Contains(t, hdr.Get("WWW-Authenticate"), "Bearer")
			}
		})
	}
}

func postRPC(t *testing.T, endpoint, authz, body string) (int, http.Header) {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, endpoint, strings.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")

	if authz != "" {
		req.Header.Set("Authorization", authz)
	}

	resp, err := testclient.New(t).Do(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	_, _ = io.Copy(io.Discard, resp.Body)

	return resp.StatusCode, resp.Header
}

func TestGatewayMCPIAMAuthorizer(t *testing.T) {
	t.Parallel()

	m := newMCPFixture(t)
	m.backend.SetSigV4Secret("s3cret")

	gw, err := m.client.CreateGateway(t.Context(), &sdk.CreateGatewayInput{
		Name: aws.String("Iam-Gw"), RoleArn: aws.String(testRole), AuthorizerType: types.AuthorizerTypeAwsIam,
	})
	require.NoError(t, err)

	endpoint := m.mcpURL(aws.ToString(gw.GatewayId))
	body := `{"jsonrpc":"2.0","id":1,"method":"ping"}`

	sign := func(secret, svc string) *http.Request {
		req, rerr := http.NewRequestWithContext(t.Context(), http.MethodPost, endpoint, bytes.NewReader([]byte(body)))
		require.NoError(t, rerr)
		req.Header.Set("Content-Type", "application/json")

		sum := hex.EncodeToString(sha256Sum(body))
		creds, cerr := credentials.NewStaticCredentialsProvider("AKIA", secret, "").Retrieve(t.Context())
		require.NoError(t, cerr)
		require.NoError(t, v4.NewSigner().SignHTTP(t.Context(), creds, req, sum, svc, "us-east-1", time.Now()))

		return req
	}

	tests := []struct {
		req        func() *http.Request
		name       string
		wantStatus int
	}{
		{
			name:       "signed",
			req:        func() *http.Request { return sign("s3cret", "bedrock-agentcore") },
			wantStatus: http.StatusOK,
		},
		{
			name:       "wrong secret",
			req:        func() *http.Request { return sign("other", "bedrock-agentcore") },
			wantStatus: http.StatusForbidden,
		},
		{
			name:       "wrong service",
			req:        func() *http.Request { return sign("s3cret", "s3") },
			wantStatus: http.StatusForbidden,
		},
		{name: "unsigned", req: func() *http.Request {
			req, _ := http.NewRequestWithContext(t.Context(), http.MethodPost, endpoint, strings.NewReader(body))

			return req
		}, wantStatus: http.StatusForbidden},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			resp, derr := testclient.New(t).Do(tt.req())
			require.NoError(t, derr)

			defer resp.Body.Close()

			assert.Equal(t, tt.wantStatus, resp.StatusCode)
		})
	}
}

func TestGatewayMCPOpenAPITarget(t *testing.T) {
	t.Parallel()

	type seenReq struct{ method, path, query, body string }

	var (
		mu   sync.Mutex
		seen = map[string]seenReq{}
	)

	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)

		mu.Lock()
		seen[r.URL.Path] = seenReq{r.Method, r.URL.Path, r.URL.RawQuery, string(raw)}
		mu.Unlock()

		if strings.HasSuffix(r.URL.Path, "/missing") {
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"error":"nope"}`))

			return
		}

		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	t.Cleanup(backend.Close)

	spec := func(server string) string {
		return `{"openapi":"3.0.1","info":{"title":"p","version":"1"},"servers":[{"url":"` + server + `/v1"}],
"paths":{"/pets/{id}":{"get":{"operationId":"getPet","summary":"Get a pet","parameters":[
{"name":"id","in":"path","required":true,"schema":{"type":"string"}},
{"name":"verbose","in":"query","schema":{"type":"boolean"}}]}},
"/pets":{"post":{"operationId":"addPet","requestBody":{"required":true,"content":{"application/json":{
"schema":{"$ref":"#/components/schemas/Pet"}}}}}}},
"components":{"schemas":{"Pet":{"type":"object","required":["name"],"properties":{"name":{"type":"string"}}}}}}`
	}

	m := newMCPFixture(t)
	gw := createGateway(t, m.client, "Api-Gw")
	gid := aws.ToString(gw.GatewayId)

	target := func(server string) types.TargetConfiguration {
		return &types.TargetConfigurationMemberMcp{Value: &types.McpTargetConfigurationMemberOpenApiSchema{
			Value: &types.ApiSchemaConfigurationMemberInlinePayload{Value: spec(server)},
		}}
	}

	m.createTarget(t, gid, "pets", target(backend.URL))
	m.createTarget(t, gid, "remote", target("http://192.0.2.1:9"))
	m.clock.advance()

	cs, err := connectMCP(t, m.mcpURL(gid), m.token(t, nil))
	require.NoError(t, err)

	t.Cleanup(func() { _ = cs.Close() })

	list, err := cs.ListTools(t.Context(), nil)
	require.NoError(t, err)

	names := make([]string, 0, len(list.Tools))

	for _, tl := range list.Tools {
		names = append(names, tl.Name)
	}

	assert.ElementsMatch(t, []string{"pets___addPet", "pets___getPet", "remote___addPet", "remote___getPet"}, names)

	tests := []struct {
		args    map[string]any
		want    seenReq
		name    string
		tool    string
		wantTxt string
		isError bool
		reached bool
	}{
		{
			name: "path and query",
			tool: "pets___getPet",
			args: map[string]any{"id": "a b", "verbose": true},
			want: seenReq{
				method: "GET",
				path:   "/v1/pets/a b",
				query:  "verbose=true",
			},
			wantTxt: `"ok":true`,
			reached: true,
		},
		{
			name: "json body",
			tool: "pets___addPet",
			args: map[string]any{"name": "rex"},
			want: seenReq{
				method: "POST",
				path:   "/v1/pets",
				body:   `{"name":"rex"}`,
			},
			wantTxt: `"ok":true`,
			reached: true,
		},
		{
			name: "http error", tool: "pets___getPet", args: map[string]any{"id": "missing"},
			want: seenReq{method: "GET", path: "/v1/pets/missing"}, wantTxt: "nope", isError: true, reached: true,
		},
		{
			name: "missing param", tool: "pets___getPet", args: map[string]any{},
			wantTxt: "missing required parameter", isError: true,
		},
		{
			name: "non-loopback refused", tool: "remote___getPet", args: map[string]any{"id": "1"},
			wantTxt: "outside loopback", isError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			res, callErr := cs.CallTool(t.Context(), &mcp.CallToolParams{Name: tt.tool, Arguments: tt.args})
			require.NoError(t, callErr)
			assert.Equal(t, tt.isError, res.IsError)
			assert.Contains(t, res.Content[0].(*mcp.TextContent).Text, tt.wantTxt)

			if tt.reached {
				mu.Lock()
				got := seen[tt.want.path]
				mu.Unlock()

				assert.Equal(t, tt.want.method, got.method)
				assert.Equal(t, tt.want.path, got.path)
				assert.Equal(t, tt.want.query, got.query)
				assert.Equal(t, tt.want.body, got.body)
			}
		})
	}
}

func TestGatewayMCPServerTargetAndSync(t *testing.T) {
	t.Parallel()

	srv := mcp.NewServer(&mcp.Implementation{Name: "up", Version: "1"}, nil)
	type addIn struct {
		A int `json:"a"`
		B int `json:"b"`
	}

	mcp.AddTool(srv, &mcp.Tool{Name: "add", Description: "adds"},
		func(_ context.Context, _ *mcp.CallToolRequest, in addIn) (*mcp.CallToolResult, any, error) {
			return &mcp.CallToolResult{
				Content: []mcp.Content{&mcp.TextContent{Text: strconv.Itoa(in.A + in.B)}},
			}, nil, nil
		})

	handler := mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return srv }, nil)
	upstream := httptest.NewTLSServer(handler)
	t.Cleanup(upstream.Close)

	m := newMCPFixture(t)
	gw := createGateway(t, m.client, "Srv-Gw")
	gid := aws.ToString(gw.GatewayId)

	mcpServer := func(endpoint string) types.TargetConfiguration {
		return &types.TargetConfigurationMemberMcp{Value: &types.McpTargetConfigurationMemberMcpServer{
			Value: types.McpServerTargetConfiguration{Endpoint: aws.String(endpoint)},
		}}
	}

	good := m.createTarget(t, gid, "up", mcpServer(upstream.URL))
	bad := m.createTarget(t, gid, "down", mcpServer("https://127.0.0.1:1"))

	sync, err := m.client.SynchronizeGatewayTargets(t.Context(), &sdk.SynchronizeGatewayTargetsInput{
		GatewayIdentifier: aws.String(gid), TargetIdList: []string{good, bad},
	})
	require.NoError(t, err)
	require.Len(t, sync.Targets, 2)

	m.clock.advance()

	status := func(id string) *sdk.GetGatewayTargetOutput {
		out, gerr := m.client.GetGatewayTarget(t.Context(),
			&sdk.GetGatewayTargetInput{GatewayIdentifier: aws.String(gid), TargetId: aws.String(id)})
		require.NoError(t, gerr)

		return out
	}

	assert.Equal(t, types.TargetStatusReady, status(good).Status)
	assert.Empty(t, status(good).StatusReasons)

	failed := status(bad)
	assert.Equal(t, types.TargetStatusSynchronizeUnsuccessful, failed.Status)
	require.NotEmpty(t, failed.StatusReasons)
	assert.Contains(t, failed.StatusReasons[0], "Tool discovery failed")

	cs, err := connectMCP(t, m.mcpURL(gid), m.token(t, nil))
	require.NoError(t, err)

	t.Cleanup(func() { _ = cs.Close() })

	list, err := cs.ListTools(t.Context(), nil)
	require.NoError(t, err)
	require.Len(t, list.Tools, 1, "targets that failed synchronization expose no tools")
	assert.Equal(t, "up___add", list.Tools[0].Name)
	assert.Equal(t, "adds", list.Tools[0].Description)

	res, err := cs.CallTool(
		t.Context(),
		&mcp.CallToolParams{Name: "up___add", Arguments: map[string]any{"a": 2, "b": 3}},
	)
	require.NoError(t, err)
	assert.False(t, res.IsError)
	assert.Equal(t, "5", res.Content[0].(*mcp.TextContent).Text)
}

func TestGatewayMCPProtocol(t *testing.T) {
	t.Parallel()

	m := newMCPFixture(t)
	gw := createGateway(t, m.client, "Proto-Gw")
	gid := aws.ToString(gw.GatewayId)
	authz := "Bearer " + m.token(t, nil)

	tests := []struct {
		name       string
		method     string
		body       string
		wantSub    string
		wantStatus int
	}{
		{
			name:       "initialize echoes version",
			method:     http.MethodPost,
			wantStatus: 200,
			wantSub:    `"protocolVersion":"2025-06-18"`,
			body:       `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18"}}`,
		},
		{
			name:       "unknown version falls back",
			method:     http.MethodPost,
			wantStatus: 200,
			wantSub:    `"protocolVersion":"2025-11-25"`,
			body:       `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"1999-01-01"}}`,
		},
		{name: "notification accepted", method: http.MethodPost, wantStatus: 202,
			body: `{"jsonrpc":"2.0","method":"notifications/initialized"}`},
		{name: "unknown method", method: http.MethodPost, wantStatus: 200, wantSub: `"code":-32601`,
			body: `{"jsonrpc":"2.0","id":1,"method":"resources/list"}`},
		{name: "bad json", method: http.MethodPost, wantStatus: 200, wantSub: `"code":-32700`, body: `{`},
		{name: "bad version field", method: http.MethodPost, wantStatus: 200, wantSub: `"code":-32600`,
			body: `{"jsonrpc":"1.0","id":1,"method":"ping"}`},
		{name: "batch", method: http.MethodPost, wantStatus: 200, wantSub: `"id":2`,
			body: `[{"jsonrpc":"2.0","id":1,"method":"ping"},{"jsonrpc":"2.0","id":2,"method":"ping"}]`},
		{name: "get not allowed", method: http.MethodGet, wantStatus: 405},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req, err := http.NewRequestWithContext(t.Context(), tt.method, m.mcpURL(gid), strings.NewReader(tt.body))
			require.NoError(t, err)
			req.Header.Set("Authorization", authz)

			resp, err := testclient.New(t).Do(req)
			require.NoError(t, err)

			defer resp.Body.Close()

			raw, _ := io.ReadAll(resp.Body)
			assert.Equal(t, tt.wantStatus, resp.StatusCode)
			assert.Contains(t, string(raw), tt.wantSub)
		})
	}
}

func TestGatewayMCPHostRouting(t *testing.T) {
	t.Parallel()

	m := newMCPFixture(t)
	gw := createGateway(t, m.client, "Host-Gw")
	gid := aws.ToString(gw.GatewayId)

	tests := []struct {
		name       string
		host       string
		path       string
		wantStatus int
	}{
		{
			name:       "generated gateway url host",
			host:       gid + ".gateway.bedrock-agentcore.us-east-1.amazonaws.com",
			path:       "/mcp",
			wantStatus: 200,
		},
		{
			name:       "unknown gateway",
			host:       "nope-0000000000.gateway.bedrock-agentcore.us-east-1.amazonaws.com",
			path:       "/mcp",
			wantStatus: 404,
		},
		{
			name:       "wrong region",
			host:       gid + ".gateway.bedrock-agentcore.eu-west-1.amazonaws.com",
			path:       "/mcp",
			wantStatus: 404,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, m.url+tt.path,
				strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"ping"}`))
			require.NoError(t, err)
			req.Host = tt.host
			req.Header.Set("Authorization", "Bearer "+m.token(t, nil))

			resp, err := testclient.New(t).Do(req)
			require.NoError(t, err)

			defer resp.Body.Close()

			assert.Equal(t, tt.wantStatus, resp.StatusCode)
		})
	}

	assert.Contains(t, aws.ToString(gw.GatewayUrl), gid+".gateway.bedrock-agentcore.us-east-1.amazonaws.com/mcp")
}

func decodeB64(s string) ([]byte, error) {
	raw, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return nil, fmt.Errorf("decode: %w", err)
	}

	return raw, nil
}

func sha256Sum(s string) []byte {
	sum := sha256.Sum256([]byte(s))

	return sum[:]
}
