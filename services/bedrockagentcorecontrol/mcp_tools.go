package bedrockagentcorecontrol

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/url"
	"strings"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const (
	toolNameSep   = "___"
	targetLambda  = "lambda"
	targetOpenAPI = "openApiSchema"
	targetMCPSrv  = "mcpServer"
)

var (
	errUnsupportedTarget = errors.New("target type is not supported by the emulator")
	errSchemaUnreadable  = errors.New("tool schema could not be read")
)

// targetView is a deep copy of a gateway target, safe to use after the backend lock is released.
type targetView struct {
	doc    map[string]any
	id     string
	name   string
	status string
	sc     scope
	tools  []any
}

// gatewayView is a deep copy of a gateway and its targets.
type gatewayView struct {
	doc     map[string]any
	id      string
	arn     string
	sc      scope
	targets []targetView
}

func deepCopy(m map[string]any) map[string]any {
	raw, err := json.Marshal(m)
	if err != nil {
		return map[string]any{}
	}

	var out map[string]any
	if err = json.Unmarshal(raw, &out); err != nil {
		return map[string]any{}
	}

	return out
}

func (b *InMemoryBackend) targetViewLocked(t *Resource) targetView {
	var tools []any

	if l, ok := t.Aux[keyTools].([]any); ok {
		tools = deepCopyList(l)
	}

	return targetView{
		id: t.ID, name: strOf(t.Doc, keyName), doc: deepCopy(t.Doc), tools: tools,
		status: b.visible(t), sc: scope{account: t.Account, region: t.Region},
	}
}

func deepCopyList(l []any) []any {
	raw, _ := json.Marshal(l)

	var out []any

	_ = json.Unmarshal(raw, &out)

	return out
}

// gatewayByID finds a gateway by id (and Region when known) across accounts.
func (b *InMemoryBackend) gatewayByID(id, region string) (*gatewayView, bool) {
	b.mu.RLock("gatewayByID")
	defer b.mu.RUnlock()

	for _, r := range b.resources.All() {
		if r.Kind != kindGateway || r.ID != id || !b.live(r) || (region != "" && r.Region != region) {
			continue
		}

		sc := scope{account: r.Account, region: r.Region}
		gv := &gatewayView{id: r.ID, arn: r.ARN, doc: deepCopy(r.Doc), sc: sc}

		for _, t := range b.scoped(sc, b.kinds[kindGatewayTarget], r.ID) {
			gv.targets = append(gv.targets, b.targetViewLocked(t))
		}

		return gv, true
	}

	return nil, false
}

func mcpConfig(t targetView) (string, map[string]any) {
	mcpCfg := objOf(objOf(t.doc, "targetConfiguration"), "mcp")
	for _, kind := range mcpTargetTypes() {
		if v, ok := mcpCfg[kind]; ok {
			m, _ := v.(map[string]any)

			return kind, m
		}
	}

	return "", nil
}

// toolEntry is one MCP tool exposed by a target.
type toolEntry struct {
	def  map[string]any
	name string
}

// listTools returns the tools a target exposes under their bare names.
func (b *InMemoryBackend) listTools(ctx context.Context, t targetView) ([]toolEntry, error) {
	kind, cfg := mcpConfig(t)

	switch kind {
	case targetLambda:
		defs, err := b.lambdaToolDefs(cfg)
		if err != nil {
			return nil, err
		}

		return entriesOf(defs), nil
	case targetOpenAPI:
		ops, err := b.openAPIOps(cfg)
		if err != nil {
			return nil, err
		}

		defs := make([]any, 0, len(ops))
		for _, op := range ops {
			defs = append(defs, op.toolDef())
		}

		return entriesOf(defs), nil
	case targetMCPSrv:
		return b.mcpServerTools(ctx, t, cfg)
	case "":
		return nil, nil
	default:
		return nil, fmt.Errorf("%w: %s", errUnsupportedTarget, kind)
	}
}

func entriesOf(defs []any) []toolEntry {
	out := make([]toolEntry, 0, len(defs))

	for _, d := range defs {
		m := mapOf(d)
		if name := strOf(m, keyName); name != "" {
			out = append(out, toolEntry{name: name, def: m})
		}
	}

	return out
}

func (b *InMemoryBackend) schemaPayload(cfg map[string]any, inlineKey string) ([]byte, error) {
	if s, ok := cfg[inlineKey].(string); ok {
		return []byte(s), nil
	}

	s3 := objOf(cfg, "s3")
	if s3 == nil {
		return nil, fmt.Errorf("%w: neither inline payload nor S3 location is set", errSchemaUnreadable)
	}

	u, err := url.Parse(strOf(s3, "uri"))
	if err != nil || u.Scheme != "s3" || u.Host == "" || strings.TrimPrefix(u.Path, "/") == "" {
		return nil, fmt.Errorf("%w: invalid S3 uri %q", errSchemaUnreadable, strOf(s3, "uri"))
	}

	b.mu.RLock("schemaPayload")
	refs := b.refs
	b.mu.RUnlock()

	if refs == nil {
		return nil, fmt.Errorf("%w: S3 is not available", errSchemaUnreadable)
	}

	data, ok := refs.ReadObject(u.Host, strings.TrimPrefix(u.Path, "/"))
	if !ok {
		return nil, fmt.Errorf("%w: s3://%s%s does not exist", errSchemaUnreadable, u.Host, u.Path)
	}

	return data, nil
}

func (b *InMemoryBackend) lambdaToolDefs(cfg map[string]any) ([]any, error) {
	ts := objOf(cfg, "toolSchema")
	if inline, ok := ts["inlinePayload"].([]any); ok {
		return inline, nil
	}

	raw, err := b.schemaPayload(ts, "")
	if err != nil {
		return nil, err
	}

	var defs []any
	if err = json.Unmarshal(raw, &defs); err != nil {
		return nil, fmt.Errorf("%w: not a JSON array of tool definitions: %w", errSchemaUnreadable, err)
	}

	return defs, nil
}

func (b *InMemoryBackend) mcpServerTools(ctx context.Context, t targetView, cfg map[string]any) ([]toolEntry, error) {
	if static := objOf(cfg, "mcpToolSchema"); static != nil {
		raw, err := b.schemaPayload(static, "inlinePayload")
		if err != nil {
			return nil, err
		}

		var doc struct {
			Tools []any `json:"tools"`
		}

		if err = json.Unmarshal(raw, &doc); err != nil {
			return nil, fmt.Errorf("%w: not an MCP tools/list result: %w", errSchemaUnreadable, err)
		}

		return entriesOf(doc.Tools), nil
	}

	if strOf(cfg, "listingMode") != "DYNAMIC" && t.tools != nil {
		return entriesOf(t.tools), nil
	}

	return b.discoverMCPServer(ctx, t, cfg)
}

func (b *InMemoryBackend) discoverMCPServer(
	ctx context.Context,
	t targetView,
	cfg map[string]any,
) ([]toolEntry, error) {
	var out []toolEntry

	err := b.withMCPSession(ctx, t, cfg, func(cs *mcp.ClientSession) error {
		for tool, terr := range cs.Tools(ctx, nil) {
			if terr != nil {
				return terr
			}

			raw, merr := json.Marshal(tool)
			if merr != nil {
				return merr
			}

			var def map[string]any
			if merr = json.Unmarshal(raw, &def); merr != nil {
				return merr
			}

			out = append(out, toolEntry{name: tool.Name, def: def})
		}

		return nil
	})

	return out, err
}

func (b *InMemoryBackend) withMCPSession(
	ctx context.Context, t targetView, cfg map[string]any, fn func(*mcp.ClientSession) error,
) error {
	hc, err := b.targetClient(ctx, t)
	if err != nil {
		return err
	}

	endpoint := strOf(cfg, "endpoint")
	if _, perr := url.Parse(endpoint); perr != nil {
		return fmt.Errorf("invalid MCP server endpoint: %w", perr)
	}

	client := mcp.NewClient(&mcp.Implementation{Name: "bedrock-agentcore-gateway", Version: "1.0.0"}, nil)

	cs, err := client.Connect(ctx, &mcp.StreamableClientTransport{
		Endpoint: endpoint, HTTPClient: hc, DisableStandaloneSSE: true, MaxRetries: -1,
	}, nil)
	if err != nil {
		return fmt.Errorf("connect to MCP server: %w", err)
	}

	defer func() { _ = cs.Close() }()

	return fn(cs)
}

// prefixed returns the tool definition with the gateway-visible name target___tool.
func prefixed(target string, e toolEntry) map[string]any {
	out := make(map[string]any, len(e.def))
	maps.Copy(out, e.def)

	out[keyName] = target + toolNameSep + e.name

	if _, ok := out["inputSchema"]; !ok {
		out["inputSchema"] = map[string]any{"type": "object"}
	}

	return out
}
