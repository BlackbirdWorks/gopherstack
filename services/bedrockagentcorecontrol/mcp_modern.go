package bedrockagentcorecontrol

import (
	"context"
	"encoding/base64"
	"encoding/json/v2"
	"io"
	"maps"
	"net/http"
	"slices"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

const (
	serverVersion    = "1.0.0"
	modernMCPVersion = "2026-07-28"
	hdrProtocol      = "Mcp-Protocol-Version"
	hdrMethod        = "Mcp-Method"
	hdrName          = "Mcp-Name"
	metaVersion      = "io.modelcontextprotocol/protocolVersion"
	metaClientInfo   = "io.modelcontextprotocol/clientInfo"
	metaClientCaps   = "io.modelcontextprotocol/clientCapabilities"
	metaServerInfo   = "io.modelcontextprotocol/serverInfo"
	methodDiscover   = "server/discover"
	methodToolsCall  = "tools/call"
	resultComplete   = "complete"
	b64Prefix        = "=?base64?"
	b64Suffix        = "?="

	rpcHeaderMismatch      = -32020
	rpcUnsupportedProtocol = -32022
)

// gatewayVersions are the MCP protocol versions the gateway accepts (protocolConfiguration.mcp.supportedVersions).
func gatewayVersions(gw *gatewayView) []string {
	if v := stringsOf(objOf(objOf(gw.doc, "protocolConfiguration"), "mcp")["supportedVersions"]); len(v) > 0 {
		return v
	}

	return defaultMCPVersions()
}

// legacyVersions are the supported versions negotiated through initialize.
func legacyVersions(gw *gatewayView) []string {
	out := slices.DeleteFunc(slices.Clone(gatewayVersions(gw)), func(v string) bool { return v == modernMCPVersion })
	if len(out) == 0 {
		return defaultMCPVersions()
	}

	return out
}

type modernError struct {
	data    any
	message string
	code    int
	status  int
}

func (e *modernError) respond(c *echo.Context, id []byte) error {
	body := map[string]any{"jsonrpc": jsonRPCVersion, "id": rawID(id), "error": rpcErrorBody(e)}

	return writeJSON(c, e.status, body)
}

func rpcErrorBody(e *modernError) map[string]any {
	out := map[string]any{"code": e.code, "message": e.message}
	if e.data != nil {
		out["data"] = e.data
	}

	return out
}

func rawID(id []byte) any {
	if len(id) == 0 {
		return nil
	}

	var v any
	if json.Unmarshal(id, &v, jsonx.V1()) != nil {
		return nil
	}

	return v
}

func unsupportedVersion(requested string, supported []string) *modernError {
	return &modernError{
		status: http.StatusBadRequest, code: rpcUnsupportedProtocol, message: "Unsupported protocol version",
		data: map[string]any{"supported": supported, "requested": requested},
	}
}

func headerMismatch(format, got, want string) *modernError {
	return &modernError{
		status: http.StatusBadRequest, code: rpcHeaderMismatch,
		message: "Header mismatch: " + format + " header value '" + got + "' does not match body value '" + want + "'",
	}
}

func decodeHeaderValue(v string) string {
	if raw, ok := strings.CutPrefix(v, b64Prefix); ok {
		if enc, ok2 := strings.CutSuffix(raw, b64Suffix); ok2 {
			if dec, err := base64.StdEncoding.DecodeString(enc); err == nil {
				return string(dec)
			}
		}
	}

	return v
}

// versionGate rejects a request whose MCP-Protocol-Version the gateway does not accept; the 400 status and
// error code come from the 2026-07-28 Streamable HTTP "Protocol Version Header" section.
func (h *Handler) versionGate(c *echo.Context, gw *gatewayView) (bool, error) {
	ver := c.Request().Header.Get(hdrProtocol)
	if ver == "" || slices.Contains(gatewayVersions(gw), ver) {
		return false, nil
	}

	return true, unsupportedVersion(ver, gatewayVersions(gw)).respond(c, nil)
}

func modernMeta(params map[string]any) *modernError {
	meta := objOf(params, "_meta")
	if strOf(meta, metaVersion) == "" || meta[metaClientInfo] == nil || meta[metaClientCaps] == nil {
		return &modernError{
			status: http.StatusBadRequest, code: rpcInvalidParams,
			message: "params._meta must carry " + metaVersion + ", " + metaClientInfo + " and " + metaClientCaps,
		}
	}

	return nil
}

func checkModernHeaders(r *http.Request, req rpcRequest, params map[string]any) *modernError {
	if got := r.Header.Get(hdrMethod); got != req.Method {
		return headerMismatch(hdrMethod, got, req.Method)
	}

	if got := strOf(objOf(params, "_meta"), metaVersion); got != modernMCPVersion {
		return headerMismatch(hdrProtocol, modernMCPVersion, got)
	}

	if req.Method == methodToolsCall {
		if got := decodeHeaderValue(r.Header.Get(hdrName)); got != strOf(params, "name") {
			return headerMismatch(hdrName, got, strOf(params, "name"))
		}
	}

	return nil
}

// serveModern answers one stateless 2026-07-28 request: no initialize handshake, every request carries its
// version, client info and capabilities in params._meta.
func (h *Handler) serveModern(c *echo.Context, gw *gatewayView) error {
	r := c.Request()

	body, err := io.ReadAll(http.MaxBytesReader(c.Response(), r.Body, maxMCPBody))
	if err != nil {
		return badRequest(rpcParseError, "request body could not be read").respond(c, nil)
	}

	var req rpcRequest
	if err = json.Unmarshal(body, &req, jsonx.V1()); err != nil || req.JSONRPC != jsonRPCVersion || req.Method == "" {
		return badRequest(rpcInvalidRequest, "invalid request").respond(c, nil)
	}

	var params map[string]any
	if len(req.Params) > 0 {
		_ = json.Unmarshal(req.Params, &params, jsonx.V1())
	}

	if merr := modernMeta(params); merr != nil {
		return merr.respond(c, req.ID)
	}

	if merr := checkModernHeaders(r, req, params); merr != nil {
		return merr.respond(c, req.ID)
	}

	if len(req.ID) == 0 {
		return c.NoContent(http.StatusAccepted)
	}

	result, rerr := h.Backend.modernDispatch(r.Context(), gw, req)
	if rerr != nil {
		status := http.StatusOK
		if rerr.Code == rpcMethodNotFound {
			status = http.StatusNotFound
		}

		return writeJSON(c, status, rpcResponse{JSONRPC: jsonRPCVersion, ID: req.ID, Error: rerr})
	}

	return writeJSON(c, http.StatusOK, rpcResponse{JSONRPC: jsonRPCVersion, ID: req.ID, Result: withResultType(result)})
}

func withResultType(result any) any {
	m, ok := result.(map[string]any)
	if !ok {
		return result
	}

	out := make(map[string]any, len(m)+1)
	maps.Copy(out, m)

	out["resultType"] = resultComplete

	return out
}

func (b *InMemoryBackend) modernDispatch(ctx context.Context, gw *gatewayView, req rpcRequest) (any, *rpcError) {
	switch req.Method {
	case methodDiscover:
		return discoverResult(gw), nil
	case "ping":
		return map[string]any{}, nil
	case "tools/list":
		return b.mcpToolsList(ctx, gw, req.Params)
	case methodToolsCall:
		return b.mcpToolsCall(ctx, gw, req)
	default:
		return nil, &rpcError{Code: rpcMethodNotFound, Message: "method not found: " + req.Method}
	}
}

// discoverResult is the server/discover DiscoverResult (modelcontextprotocol.io 2026-07-28 server/discover).
func discoverResult(gw *gatewayView) map[string]any {
	res := map[string]any{
		"supportedVersions": gatewayVersions(gw),
		"capabilities":      map[string]any{keyTools: map[string]any{"listChanged": false}},
		"_meta": map[string]any{metaServerInfo: map[string]any{
			"name": strOf(gw.doc, keyName), "version": serverVersion,
		}},
	}

	if ins := strOf(objOf(objOf(gw.doc, "protocolConfiguration"), "mcp"), "instructions"); ins != "" {
		res["instructions"] = ins
	}

	return res
}

func badRequest(code int, msg string) *modernError {
	return &modernError{status: http.StatusBadRequest, code: code, message: msg}
}
