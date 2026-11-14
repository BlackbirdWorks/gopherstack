package bedrockagentcorecontrol

import (
	"context"
	"encoding/base64"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"io"
	"net/http"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const (
	pathGatewayPrefix = "/agentcore/gateways/"
	pathMCP           = "/mcp"
	opGatewayMCP      = "InvokeGatewayMCP"
	maxMCPBody        = 4 << 20
	toolsPageSize     = 100
	jsonRPCVersion    = "2.0"

	rpcParseError     = -32700
	rpcInvalidRequest = -32600
	rpcMethodNotFound = -32601
	rpcInvalidParams  = -32602
	rpcInternal       = -32603
)

var gatewayHost = regexp.MustCompile(
	`^([a-z0-9][a-z0-9-]*)\.gateway\.bedrock-agentcore\.([a-z0-9-]+)\.(?:amazonaws\.com|localhost)(?::\d+)?$`)

// defaultMCPVersions are the protocol revisions the gateway negotiates, newest first.
func defaultMCPVersions() []string { return []string{"2025-11-25", "2025-06-18", "2025-03-26"} }

// gatewayRoute reports the gateway a request addresses: <id>.gateway.bedrock-agentcore.<region>.amazonaws.com/mcp
// (the generated gatewayUrl) or <emulator>/agentcore/gateways/<id>/mcp.
func gatewayRoute(r *http.Request) (string, string, bool) {
	if m := gatewayHost.FindStringSubmatch(strings.ToLower(r.Host)); m != nil && r.URL.Path == pathMCP {
		return m[1], m[2], true
	}

	rest, found := strings.CutPrefix(r.URL.Path, pathGatewayPrefix)
	if !found {
		return "", "", false
	}

	id, tail, found := strings.Cut(rest, "/")
	if !found || id == "" || "/"+tail != pathMCP {
		return "", "", false
	}

	return strings.ToLower(id), "", true
}

type rpcRequest struct {
	Method  string         `json:"method"`
	JSONRPC string         `json:"jsonrpc"`
	ID      jsontext.Value `json:"id,omitempty"`
	Params  jsontext.Value `json:"params,omitempty"`
}

type rpcError struct {
	Message string `json:"message"`
	Code    int    `json:"code"`
}

type rpcResponse struct {
	Result  any            `json:"result,omitempty"`
	Error   *rpcError      `json:"error,omitempty"`
	JSONRPC string         `json:"jsonrpc"`
	ID      jsontext.Value `json:"id"`
}

func rpcFail(id jsontext.Value, code int, msg string) rpcResponse {
	if id == nil {
		id = jsontext.Value("null")
	}

	return rpcResponse{JSONRPC: jsonRPCVersion, ID: id, Error: &rpcError{Code: code, Message: msg}}
}

func writeJSON(c *echo.Context, status int, v any) error {
	c.Response().Header().Set("Content-Type", "application/json")
	c.Response().WriteHeader(status)

	return json.MarshalEncode(jsontext.NewEncoder(c.Response(), jsonx.V1()), v)
}

// serveGateway answers one MCP streamable-HTTP request addressed to a gateway.
func (h *Handler) serveGateway(c *echo.Context, gatewayID, region string) error {
	r := c.Request()

	gw, ok := h.Backend.gatewayByID(gatewayID, region)
	if !ok {
		return writeJSON(c, http.StatusNotFound, map[string]string{"message": "Gateway not found"})
	}

	if fail := h.Backend.authorize(r.Context(), r, gw); fail != nil {
		if fail.challenge != "" {
			c.Response().Header().Set("WWW-Authenticate", fail.challenge)
		}

		return writeJSON(c, fail.status, map[string]string{"message": fail.message})
	}

	if r.Method != http.MethodPost {
		c.Response().Header().Set("Allow", http.MethodPost)

		return c.NoContent(http.StatusMethodNotAllowed)
	}

	body, err := io.ReadAll(http.MaxBytesReader(c.Response(), r.Body, maxMCPBody))
	if err != nil {
		return writeJSON(c, http.StatusOK, rpcFail(nil, rpcParseError, "request body could not be read"))
	}

	trimmed := strings.TrimSpace(string(body))
	if strings.HasPrefix(trimmed, "[") {
		return h.serveBatch(c, gw, []byte(trimmed))
	}

	resp, reply := h.Backend.handleRPC(r.Context(), gw, []byte(trimmed))
	if !reply {
		return c.NoContent(http.StatusAccepted)
	}

	return writeJSON(c, http.StatusOK, resp)
}

func (h *Handler) serveBatch(c *echo.Context, gw *gatewayView, body []byte) error {
	var items []jsontext.Value
	if err := json.Unmarshal(body, &items, jsonx.V1()); err != nil || len(items) == 0 {
		return writeJSON(c, http.StatusOK, rpcFail(nil, rpcInvalidRequest, "invalid batch"))
	}

	out := make([]rpcResponse, 0, len(items))

	for _, it := range items {
		if resp, reply := h.Backend.handleRPC(c.Request().Context(), gw, it); reply {
			out = append(out, resp)
		}
	}

	if len(out) == 0 {
		return c.NoContent(http.StatusAccepted)
	}

	return writeJSON(c, http.StatusOK, out)
}

// handleRPC processes one JSON-RPC message; reply is false for notifications and client responses.
func (b *InMemoryBackend) handleRPC(ctx context.Context, gw *gatewayView, raw []byte) (rpcResponse, bool) {
	var req rpcRequest
	if err := json.Unmarshal(raw, &req, jsonx.V1()); err != nil {
		return rpcFail(nil, rpcParseError, "parse error"), true
	}

	if req.JSONRPC != jsonRPCVersion {
		return rpcFail(req.ID, rpcInvalidRequest, "jsonrpc must be 2.0"), true
	}

	if len(req.ID) == 0 {
		return rpcResponse{}, false
	}

	if req.Method == "" {
		return rpcResponse{}, false
	}

	var (
		result any
		rerr   *rpcError
	)

	switch req.Method {
	case "initialize":
		result, rerr = mcpInitialize(gw, req.Params)
	case "ping":
		result = map[string]any{}
	case "tools/list":
		result, rerr = b.mcpToolsList(ctx, gw, req.Params)
	case "tools/call":
		result, rerr = b.mcpToolsCall(ctx, gw, req)
	default:
		rerr = &rpcError{Code: rpcMethodNotFound, Message: "method not found: " + req.Method}
	}

	if rerr != nil {
		return rpcResponse{JSONRPC: jsonRPCVersion, ID: req.ID, Error: rerr}, true
	}

	return rpcResponse{JSONRPC: jsonRPCVersion, ID: req.ID, Result: result}, true
}

func mcpInitialize(gw *gatewayView, params jsontext.Value) (any, *rpcError) {
	var p struct {
		ProtocolVersion string `json:"protocolVersion"`
	}

	if len(params) > 0 {
		if err := json.Unmarshal(params, &p, jsonx.V1()); err != nil {
			return nil, &rpcError{Code: rpcInvalidParams, Message: "invalid initialize params"}
		}
	}

	proto := objOf(objOf(gw.doc, "protocolConfiguration"), "mcp")

	versions := stringsOf(proto["supportedVersions"])
	if len(versions) == 0 {
		versions = defaultMCPVersions()
	}

	chosen := latestVersion(versions)
	if slices.Contains(versions, p.ProtocolVersion) {
		chosen = p.ProtocolVersion
	}

	res := map[string]any{
		"protocolVersion": chosen,
		"capabilities":    map[string]any{keyTools: map[string]any{"listChanged": false}},
		"serverInfo":      map[string]any{"name": strOf(gw.doc, keyName), "version": "1.0.0"},
	}

	if ins := strOf(proto, "instructions"); ins != "" {
		res["instructions"] = ins
	}

	return res, nil
}

func latestVersion(versions []string) string {
	best := versions[0]

	for _, v := range versions {
		if v > best {
			best = v
		}
	}

	return best
}

func (b *InMemoryBackend) mcpToolsList(ctx context.Context, gw *gatewayView, params jsontext.Value) (any, *rpcError) {
	var p struct {
		Cursor string `json:"cursor"`
	}

	if len(params) > 0 {
		if err := json.Unmarshal(params, &p, jsonx.V1()); err != nil {
			return nil, &rpcError{Code: rpcInvalidParams, Message: "invalid tools/list params"}
		}
	}

	all := make([]any, 0)

	for _, t := range gw.targets {
		if t.status != stateReady {
			continue
		}

		entries, err := b.listTools(ctx, t)
		if err != nil {
			logger.Load(ctx).WarnContext(ctx, "bedrockagentcorecontrol: target tools unavailable",
				"gateway", gw.id, "target", t.name, "error", err)

			continue
		}

		for _, e := range entries {
			all = append(all, prefixed(t.name, e))
		}
	}

	proto := objOf(objOf(gw.doc, "protocolConfiguration"), "mcp")
	if off, _ := proto["disableMcpListToolsPagination"].(bool); off {
		return map[string]any{keyTools: all}, nil
	}

	start := 0

	if p.Cursor != "" {
		raw, derr := base64.RawURLEncoding.DecodeString(p.Cursor)

		n, aerr := strconv.Atoi(string(raw))
		if derr != nil || aerr != nil || n < 0 || n > len(all) {
			return nil, &rpcError{Code: rpcInvalidParams, Message: "invalid cursor"}
		}

		start = n
	}

	end := min(start+toolsPageSize, len(all))
	res := map[string]any{keyTools: all[start:end]}

	if end < len(all) {
		res["nextCursor"] = base64.RawURLEncoding.EncodeToString([]byte(strconv.Itoa(end)))
	}

	return res, nil
}

func (b *InMemoryBackend) mcpToolsCall(ctx context.Context, gw *gatewayView, req rpcRequest) (any, *rpcError) {
	var p struct {
		Arguments map[string]any `json:"arguments"`
		Name      string         `json:"name"`
	}

	if err := json.Unmarshal(req.Params, &p, jsonx.V1()); err != nil || p.Name == "" {
		return nil, &rpcError{Code: rpcInvalidParams, Message: "tools/call requires a tool name"}
	}

	if p.Arguments == nil {
		p.Arguments = map[string]any{}
	}

	res, err := b.callTool(ctx, gw, callInfo{
		gatewayID: gw.id, toolFull: p.Name, args: p.Arguments, messageID: strings.Trim(string(req.ID), `"`),
	})

	switch {
	case errors.Is(err, errUnknownTool):
		return nil, &rpcError{Code: rpcInvalidParams, Message: "Unknown tool: " + p.Name}
	case err != nil:
		return nil, &rpcError{Code: rpcInternal, Message: "tool call failed"}
	default:
		return res, nil
	}
}
