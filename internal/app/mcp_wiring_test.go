package app

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/chaos"
	"github.com/blackbirdworks/gopherstack/pkgs/iampolicystream"
	"github.com/blackbirdworks/gopherstack/pkgs/inithooks"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/pkgs/testclient"
)

type mcpStack struct {
	srv      *httptest.Server
	cli      *CLI
	cfg      aws.Config
	initRoot string
}

func newMCPStack(t *testing.T, mcpCfg MCPConfig) *mcpStack {
	t.Helper()

	log := buildLogger("")
	cli := CLI{AccountID: "000000000000", Region: "us-east-1", MCP: mcpCfg}
	cli.Pods.Dir = t.TempDir()
	cli.portAlloc = setupPortAllocatorWithReservations(t.Context(), log, cli)
	cli.faultStore = chaos.NewFaultStore()
	cli.localstackInit = newLocalstackInitState()
	initRoot := t.TempDir()
	cli.localstackInit.setManager(inithooks.NewManager(inithooks.ManagerConfig{Roots: []string{initRoot}}))
	_ = cli.IAMPolicyStream()

	services, err := initializeServicesT(t, &service.AppContext{
		Logger: log, Config: &cli, JanitorCtx: t.Context(), PortAlloc: cli.portAlloc,
	})
	require.NoError(t, err)

	mgr := persistence.NewManager(t.Context(), persistence.NullStore{})
	setupPersistence(t.Context(), mgr, services, false, cli.Region)

	e := buildEchoServer(t.Context(), log, mgr, services, cli)
	require.NoError(t, setupChaosAndRegistry(e, log, &cli, services))

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
		awscfg.WithBaseEndpoint(srv.URL),
	)
	require.NoError(t, err)

	return &mcpStack{srv: srv, cfg: cfg, cli: &cli, initRoot: initRoot}
}

func (s *mcpStack) httpSession(t *testing.T) *mcp.ClientSession {
	t.Helper()

	c := mcp.NewClient(&mcp.Implementation{Name: "test", Version: "0"}, nil)
	cs, err := c.Connect(t.Context(), &mcp.StreamableClientTransport{
		Endpoint:             s.srv.URL + mcpPath,
		HTTPClient:           testclient.New(t),
		DisableStandaloneSSE: true,
	}, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cs.Close() })

	return cs
}

func (s *mcpStack) session(t *testing.T, stdio bool) *mcp.ClientSession {
	t.Helper()

	if stdio {
		return s.stdioSession(t)
	}

	return s.httpSession(t)
}

func (s *mcpStack) stdioSession(t *testing.T) *mcp.ClientSession {
	t.Helper()

	ct, st := mcp.NewInMemoryTransports()
	cmd := &McpCmd{}
	cmd.Endpoint = s.srv.URL
	ctx, cancel := context.WithCancel(t.Context())

	done := make(chan error, 1)
	go func() { done <- cmd.serve(ctx, testclient.New(t), st) }()

	t.Cleanup(func() {
		cancel()
		<-done
	})

	c := mcp.NewClient(&mcp.Implementation{Name: "test", Version: "0"}, nil)
	cs, err := c.Connect(t.Context(), ct, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cs.Close() })

	return cs
}

func callTool(t *testing.T, cs *mcp.ClientSession, name string, args map[string]any) (map[string]any, string) {
	t.Helper()

	res, err := cs.CallTool(t.Context(), &mcp.CallToolParams{Name: name, Arguments: args})
	require.NoError(t, err)

	raw, err := json.Marshal(res)
	require.NoError(t, err)
	require.False(t, res.IsError, "tool %s: %s", name, raw)

	out, _ := res.StructuredContent.(map[string]any)

	return out, string(raw)
}

func mcpExpectedTools() map[string]string {
	return map[string]string{
		"gopherstack_health": "readOnly",
		"snapshot_list":      "readOnly",
		"snapshot_save":      "additive",
		"snapshot_load":      "destructive",
		"snapshot_delete":    "destructive",
		"reset_state":        "destructive",
		"chaos_list_faults":  "readOnly",
		"chaos_set_faults":   "destructive",
		"chaos_clear_faults": "destructive",
		"query_traces":       "readOnly",
		"iam_denials":        "readOnly",
		"init_hooks_list":    "readOnly",
		"init_hooks_run":     "destructive",
	}
}

func TestMCPToolsList(t *testing.T) {
	t.Parallel()

	s := newMCPStack(t, MCPConfig{Enabled: true})

	tests := []struct {
		name  string
		stdio bool
	}{
		{name: "http"},
		{name: "stdio", stdio: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cs := s.session(t, tt.stdio)
			res, err := cs.ListTools(t.Context(), nil)
			require.NoError(t, err)

			got := map[string]*mcp.Tool{}
			for _, tool := range res.Tools {
				got[tool.Name] = tool
			}

			require.Len(t, got, len(mcpExpectedTools()))

			for name, kind := range mcpExpectedTools() {
				tool, ok := got[name]
				require.True(t, ok, name)
				assert.NotEmpty(t, tool.Description, name)
				assert.NotNil(t, tool.InputSchema, name)
				assert.NotNil(t, tool.OutputSchema, name)
				require.NotNil(t, tool.Annotations, name)

				switch kind {
				case "readOnly":
					assert.True(t, tool.Annotations.ReadOnlyHint, name)
				case "destructive":
					assert.False(t, tool.Annotations.ReadOnlyHint, name)
					require.NotNil(t, tool.Annotations.DestructiveHint, name)
					assert.True(t, *tool.Annotations.DestructiveHint, name)
				default:
					assert.False(t, tool.Annotations.ReadOnlyHint, name)
					require.NotNil(t, tool.Annotations.DestructiveHint, name)
					assert.False(t, *tool.Annotations.DestructiveHint, name)
				}
			}
		})
	}
}

func TestMCPToolsHappyPath(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		stdio bool
	}{
		{name: "http"},
		{name: "stdio", stdio: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newMCPStack(t, MCPConfig{Enabled: true})
			cs := s.session(t, tt.stdio)

			out, _ := callTool(t, cs, "gopherstack_health", nil)
			assert.Equal(t, "ok", out["status"])
			assert.Contains(t, out["services"], "SQS")

			queue, err := sqs.NewFromConfig(s.cfg).
				CreateQueue(t.Context(), &sqs.CreateQueueInput{QueueName: aws.String("mcp-q")})
			require.NoError(t, err)
			require.NotNil(t, queue.QueueUrl)

			_, _ = callTool(t, cs, "snapshot_save", map[string]any{"name": "snap1", "message": "m"})

			list, raw := callTool(t, cs, "snapshot_list", nil)
			assert.Contains(t, raw, "snap1")
			assert.NotNil(t, list["data"])

			_, _ = callTool(t, cs, "reset_state", map[string]any{"service": "sqs"})

			q, err := sqs.NewFromConfig(s.cfg).ListQueues(t.Context(), &sqs.ListQueuesInput{})
			require.NoError(t, err)
			assert.Empty(t, q.QueueUrls)

			_, _ = callTool(t, cs, "snapshot_load", map[string]any{"name": "snap1"})

			q, err = sqs.NewFromConfig(s.cfg).ListQueues(t.Context(), &sqs.ListQueuesInput{})
			require.NoError(t, err)
			assert.Len(t, q.QueueUrls, 1)

			_, _ = callTool(t, cs, "snapshot_delete", map[string]any{"name": "snap1"})
			_, raw = callTool(t, cs, "snapshot_list", nil)
			assert.NotContains(t, raw, "snap1")

			_, _ = callTool(t, cs, "chaos_set_faults", map[string]any{"rules": []map[string]any{
				{
					"service":   "sqs",
					"operation": "ListQueues",
					"error":     map[string]any{"code": "Throttling", "statusCode": 429},
				},
			}})
			faults, _ := callTool(t, cs, "chaos_list_faults", nil)
			assert.Len(t, faults["rules"], 1)

			_, _ = sqs.NewFromConfig(s.cfg).ListQueues(t.Context(), &sqs.ListQueuesInput{})

			traces, _ := callTool(
				t,
				cs,
				"query_traces",
				map[string]any{"service": "sqs", "operation": "ListQueues", "status": "4xx", "limit": 5},
			)
			require.NotEmpty(t, traces["traces"])

			_, _ = callTool(t, cs, "chaos_clear_faults", nil)
			faults, _ = callTool(t, cs, "chaos_list_faults", nil)
			assert.Empty(t, faults["rules"])

			s.cli.IAMPolicyStream().Record(iampolicystream.Event{
				Service: "s3", Action: "s3:GetObject", Resource: "*", Principal: "arn:aws:iam::000000000000:user/alice",
				Decision: iampolicystream.DecisionDeny, Reason: iampolicystream.ReasonImplicitDeny,
			})
			s.cli.IAMPolicyStream().Record(iampolicystream.Event{
				Service: "s3", Action: "s3:PutObject", Resource: "*", Principal: "bob",
				Decision: iampolicystream.DecisionAllow, Reason: iampolicystream.ReasonIdentityPolicy,
			})

			denials, _ := callTool(t, cs, "iam_denials", map[string]any{"service": "s3"})
			assert.Len(t, denials["events"], 1)

			hooks, _ := callTool(t, cs, "init_hooks_list", nil)
			assert.NotNil(t, hooks["data"])

			res, err := cs.ReadResource(t.Context(), &mcp.ReadResourceParams{URI: "gopherstack://info"})
			require.NoError(t, err)
			require.Len(t, res.Contents, 1)
			assert.Contains(t, res.Contents[0].Text, `"status":"ok"`)
		})
	}
}

func TestMCPToolErrors(t *testing.T) {
	t.Parallel()

	s := newMCPStack(t, MCPConfig{Enabled: true})
	cs := s.httpSession(t)

	tests := []struct {
		args map[string]any
		name string
		tool string
	}{
		{name: "missing_snapshot", tool: "snapshot_load", args: map[string]any{"name": "nope"}},
		{name: "bad_status", tool: "query_traces", args: map[string]any{"status": "weird"}},
		{name: "bad_since", tool: "iam_denials", args: map[string]any{"since": "yesterday"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			res, err := cs.CallTool(t.Context(), &mcp.CallToolParams{Name: tt.tool, Arguments: tt.args})
			require.NoError(t, err)
			assert.True(t, res.IsError)
		})
	}
}

func TestMCPEndpointMountAndAuth(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		token      string
		cfg        MCPConfig
		wantStatus int
	}{
		{name: "disabled", cfg: MCPConfig{}, wantStatus: http.StatusNotFound},
		{name: "loopback_no_token", cfg: MCPConfig{Enabled: true}, wantStatus: http.StatusOK},
		{name: "token_missing", cfg: MCPConfig{Enabled: true, Token: "s3cret"}, wantStatus: http.StatusUnauthorized},
		{
			name:       "token_wrong",
			cfg:        MCPConfig{Enabled: true, Token: "s3cret"},
			token:      "nope",
			wantStatus: http.StatusUnauthorized,
		},
		{name: "token_ok", cfg: MCPConfig{Enabled: true, Token: "s3cret"}, token: "s3cret", wantStatus: http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newMCPStack(t, tt.cfg)
			body := `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{` +
				`"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"t","version":"0"}}}`
			req, err := http.NewRequestWithContext(
				t.Context(),
				http.MethodPost,
				s.srv.URL+mcpPath,
				strings.NewReader(body),
			)
			require.NoError(t, err)
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Accept", "application/json, text/event-stream")

			if tt.token != "" {
				req.Header.Set("Authorization", "Bearer "+tt.token)
			}

			resp, err := testclient.New(t).Do(req)
			require.NoError(t, err)

			defer resp.Body.Close()

			assert.Equal(t, tt.wantStatus, resp.StatusCode)
		})
	}
}

func TestMCPNeverLeaksSecrets(t *testing.T) {
	t.Parallel()

	s := newMCPStack(t, MCPConfig{Enabled: true})
	cs := s.httpSession(t)
	iamc := iam.NewFromConfig(s.cfg)

	_, err := iamc.CreateUser(t.Context(), &iam.CreateUserInput{UserName: aws.String("leaky")})
	require.NoError(t, err)

	key, err := iamc.CreateAccessKey(t.Context(), &iam.CreateAccessKeyInput{UserName: aws.String("leaky")})
	require.NoError(t, err)

	secret := aws.ToString(key.AccessKey.SecretAccessKey)
	require.NotEmpty(t, secret)

	presigned := s.srv.URL + "/mcp-bucket/key?X-Amz-Signature=sigvalue123" +
		"&X-Amz-Credential=credvalue456&X-Amz-Security-Token=tokvalue789"
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, presigned, http.NoBody)
	require.NoError(t, err)
	req.Header.Set(
		"Authorization",
		"AWS4-HMAC-SHA256 Credential=AKIAHEADERKEY/20250101/us-east-1/s3/aws4_request, Signature=headersig",
	)
	req.Header.Set("X-Amz-Security-Token", "headertoken")

	resp, err := testclient.New(t).Do(req)
	require.NoError(t, err)
	_ = resp.Body.Close()

	_, _ = callTool(t, cs, "snapshot_save", map[string]any{"name": "leaktest"})

	tests := []struct {
		args map[string]any
		tool string
	}{
		{tool: "gopherstack_health"},
		{tool: "snapshot_list"},
		{tool: "query_traces", args: map[string]any{"limit": 100}},
		{tool: "iam_denials"},
		{tool: "init_hooks_list"},
		{tool: "chaos_list_faults"},
	}

	for _, tt := range tests {
		t.Run(tt.tool, func(t *testing.T) {
			t.Parallel()

			_, raw := callTool(t, cs, tt.tool, tt.args)
			for _, banned := range []string{secret, "sigvalue123", "credvalue456", "tokvalue789", "headertoken", "headersig"} {
				assert.NotContains(t, raw, banned)
			}
		})
	}
}

func TestMCPInitHooksRun(t *testing.T) {
	t.Parallel()

	tests := []struct {
		args     map[string]any
		name     string
		wantOut  string
		wantErr  bool
		wantExit float64
	}{
		{name: "named_ok", args: map[string]any{"stage": "ready", "script": "ok.sh"}, wantOut: "hello"},
		{name: "named_fail", args: map[string]any{"stage": "READY", "script": "fail.sh"}, wantOut: "boom", wantExit: 3},
		{name: "whole_stage", args: map[string]any{"stage": "READY"}, wantOut: "hello"},
		{name: "unknown_script", args: map[string]any{"stage": "READY", "script": "nope.sh"}, wantErr: true},
		{name: "traversal", args: map[string]any{"stage": "READY", "script": "../ready.d/ok.sh"}, wantErr: true},
		{name: "absolute", args: map[string]any{"stage": "READY", "script": "/bin/true"}, wantErr: true},
		{name: "bad_stage", args: map[string]any{"stage": "NOPE"}, wantErr: true},
	}

	for _, tt := range tests {
		for _, stdio := range []bool{false, true} {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()

				s := newMCPStack(t, MCPConfig{Enabled: true})
				dir := filepath.Join(s.initRoot, "ready.d")
				require.NoError(t, os.MkdirAll(dir, 0o750))
				require.NoError(t, os.WriteFile(filepath.Join(dir, "ok.sh"), []byte("echo hello\n"), 0o600))
				require.NoError(t, os.WriteFile(filepath.Join(dir, "fail.sh"), []byte("echo boom\nexit 3\n"), 0o600))

				cs := s.session(t, stdio)

				if tt.wantErr {
					res, err := cs.CallTool(t.Context(), &mcp.CallToolParams{Name: "init_hooks_run", Arguments: tt.args})
					require.NoError(t, err)
					assert.True(t, res.IsError)

					return
				}

				out, raw := callTool(t, cs, "init_hooks_run", tt.args)
				assert.Contains(t, raw, tt.wantOut)

				data, _ := out["data"].(map[string]any)
				results, _ := data["results"].([]any)
				require.NotEmpty(t, results)

				if tt.name != "whole_stage" {
					first, _ := results[0].(map[string]any)
					assert.InDelta(t, tt.wantExit, first["exit_code"], 0)
				}
			})
		}
	}
}

func TestMCPReadOnlyHidesMutatingTools(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		stdio bool
	}{{name: "http"}, {name: "stdio", stdio: true}}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newMCPStack(t, MCPConfig{Enabled: true, ReadOnly: true})
			cs := s.httpSession(t)

			if tt.stdio {
				cmd := &McpCmd{ReadOnly: true}
				cmd.Endpoint = s.srv.URL
				ct, st := mcp.NewInMemoryTransports()

				go func() { _ = cmd.serve(t.Context(), testclient.New(t), st) }()

				c := mcp.NewClient(&mcp.Implementation{Name: "t", Version: "0"}, nil)
				var err error
				cs, err = c.Connect(t.Context(), ct, nil)
				require.NoError(t, err)
				t.Cleanup(func() { _ = cs.Close() })
			}

			res, err := cs.ListTools(t.Context(), nil)
			require.NoError(t, err)
			require.NotEmpty(t, res.Tools)

			for _, tool := range res.Tools {
				assert.True(t, tool.Annotations.ReadOnlyHint, tool.Name)
			}
		})
	}
}
