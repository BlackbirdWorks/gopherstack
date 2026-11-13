package bedrockagentcorecontrol

import (
	"context"
	"encoding/base64"
	"encoding/json/v2"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

const gatewayMessageVersion = "1.0"

var (
	errUnknownTool  = errors.New("unknown tool")
	errNoLambdaHost = errors.New("lambda is not available")
)

// LambdaInvoker runs a Lambda function in-process; functionError is the X-Amz-Function-Error value.
type LambdaInvoker interface {
	InvokeLambda(ctx context.Context, functionARN, clientContext string, payload []byte) (
		result []byte, functionError string, err error)
}

func textResult(text string, isErr bool) map[string]any {
	out := map[string]any{"content": []any{map[string]any{keyType: "text", "text": text}}}
	if isErr {
		out["isError"] = true
	}

	return out
}

// splitToolName splits target___tool at the first separator (target names cannot contain underscores).
func splitToolName(full string) (string, string, bool) {
	target, tool, ok := strings.Cut(full, toolNameSep)

	return target, tool, ok && target != "" && tool != ""
}

type callInfo struct {
	args      map[string]any
	gatewayID string
	toolFull  string
	messageID string
}

// callTool dispatches tools/call to the target that owns the tool.
func (b *InMemoryBackend) callTool(ctx context.Context, gw *gatewayView, ci callInfo) (map[string]any, error) {
	targetName, tool, ok := splitToolName(ci.toolFull)
	if !ok {
		return nil, errUnknownTool
	}

	idx := slices.IndexFunc(gw.targets, func(t targetView) bool { return t.name == targetName })
	if idx < 0 || gw.targets[idx].status != stateReady {
		return nil, errUnknownTool
	}

	t := gw.targets[idx]
	kind, cfg := mcpConfig(t)

	switch kind {
	case targetLambda:
		return b.callLambda(ctx, t, cfg, tool, ci)
	case targetOpenAPI:
		return b.callOpenAPI(ctx, t, cfg, tool, ci.args)
	case targetMCPSrv:
		return b.callMCPServer(ctx, t, cfg, tool, ci.args)
	default:
		return nil, errUnknownTool
	}
}

func (b *InMemoryBackend) callLambda(
	ctx context.Context, t targetView, cfg map[string]any, tool string, ci callInfo,
) (map[string]any, error) {
	defs, err := b.lambdaToolDefs(cfg)
	if err != nil {
		return textResult(err.Error(), true), nil
	}

	idx := slices.IndexFunc(defs, func(d any) bool { return strOf(mapOf(d), keyName) == tool })
	if idx < 0 {
		return nil, errUnknownTool
	}

	b.mu.RLock("callLambda")
	refs := b.refs
	b.mu.RUnlock()

	inv, ok := refs.(LambdaInvoker)
	if !ok {
		return textResult(errNoLambdaHost.Error(), true), nil
	}

	payload, _ := json.Marshal(ci.args, jsonx.V1())

	clientCtx, _ := json.Marshal(map[string]any{"custom": map[string]string{
		"bedrockAgentCoreToolName":       ci.toolFull,
		"bedrockAgentCoreGatewayId":      ci.gatewayID,
		"bedrockAgentCoreTargetId":       t.id,
		"bedrockAgentCoreMessageVersion": gatewayMessageVersion,
		"bedrockAgentCoreAwsRequestId":   uuid.NewString(),
		"bedrockAgentCoreMcpMessageId":   ci.messageID,
	}}, jsonx.V1())

	out, fnErr, err := inv.InvokeLambda(ctx, strOf(cfg, "lambdaArn"),
		base64.StdEncoding.EncodeToString(clientCtx), payload)
	if err != nil {
		return textResult(fmt.Sprintf("lambda invocation failed: %v", err), true), nil
	}

	res := textResult(string(out), fnErr != "")

	if fnErr == "" && mapOf(defs[idx])["outputSchema"] != nil {
		var structured map[string]any
		if json.Unmarshal(out, &structured, jsonx.V1()) == nil && structured != nil {
			res["structuredContent"] = structured
		}
	}

	return res, nil
}

func (b *InMemoryBackend) callMCPServer(
	ctx context.Context, t targetView, cfg map[string]any, tool string, args map[string]any,
) (map[string]any, error) {
	var res map[string]any

	err := b.withMCPSession(ctx, t, cfg, func(cs *mcp.ClientSession) error {
		out, cerr := cs.CallTool(ctx, &mcp.CallToolParams{Name: tool, Arguments: args})
		if cerr != nil {
			return cerr
		}

		raw, merr := json.Marshal(out, jsonx.V1())
		if merr != nil {
			return merr
		}

		return json.Unmarshal(raw, &res, jsonx.V1())
	})
	if err != nil {
		return textResult("MCP server call failed: "+err.Error(), true), nil //nolint:nilerr // tool error
	}

	return res, nil
}
