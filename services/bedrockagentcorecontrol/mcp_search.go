package bedrockagentcorecontrol

import (
	"context"
	"encoding/json/v2"
	"slices"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	dataplane "github.com/blackbirdworks/gopherstack/services/bedrockagentcore"
)

const (
	searchToolName    = "x_amz_bedrock_agentcore_search"
	searchTypeSem     = "SEMANTIC"
	searchResultLimit = 10
	toolTextFixed     = 2
	keyQuery          = "query"
	maxSearchQuery    = 1024
	typeObject        = "object"
)

func searchEnabled(gw *gatewayView) bool {
	return strOf(objOf(objOf(gw.doc, "protocolConfiguration"), "mcp"), "searchType") == searchTypeSem
}

// searchToolDef is the built-in search tool listed first when the gateway has semantic search enabled
// (gateway-using-mcp-list.html, gateway-using-mcp-semantic-search.html).
func searchToolDef() map[string]any {
	return map[string]any{
		keyName: searchToolName,
		"description": "A special tool that returns a trimmed down list of tools given a context. " +
			"Use it when many tools are available and you want the ones relevant to a natural language query.",
		"inputSchema": map[string]any{
			keyType: typeObject,
			"properties": map[string]any{
				keyQuery: map[string]any{
					keyType: "string", "description": "A natural language query to search for tools.",
				},
			},
			"required": []any{keyQuery},
		},
	}
}

// gatewayTools lists the prefixed definitions of every tool of every ready target.
func (b *InMemoryBackend) gatewayTools(ctx context.Context, gw *gatewayView) []map[string]any {
	var all []map[string]any

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

	return all
}

func toolText(def map[string]any) string {
	props := objOf(objOf(def, "inputSchema"), "properties")
	parts := make([]string, 0, len(props)*2+toolTextFixed)
	parts = append(parts, strOf(def, keyName), strOf(def, "description"))
	for name, p := range props {
		parts = append(parts, name, strOf(mapOf(p), "description"))
	}

	return strings.NewReplacer("_", " ", "-", " ").Replace(strings.Join(parts, " "))
}

// searchTools ranks the gateway's tools against the query. The ranking is lexical BM25: the emulator has no
// embedding model, so tools sharing no term with the query are not returned.
func (b *InMemoryBackend) searchTools(ctx context.Context, gw *gatewayView, args map[string]any) (any, *rpcError) {
	query, _ := args[keyQuery].(string)
	if strings.TrimSpace(query) == "" {
		return nil, &rpcError{Code: rpcInvalidParams, Message: "query is required"}
	}

	query = query[:min(len(query), maxSearchQuery)]
	defs := b.gatewayTools(ctx, gw)
	texts := make([]string, len(defs))

	for i, d := range defs {
		texts[i] = toolText(d)
	}

	scores := dataplane.RankBM25(query, texts)
	idx := make([]int, 0, len(defs))

	for i, s := range scores {
		if s > 0 {
			idx = append(idx, i)
		}
	}

	slices.SortStableFunc(idx, func(a, c int) int {
		switch {
		case scores[a] > scores[c]:
			return -1
		case scores[a] < scores[c]:
			return 1
		default:
			return 0
		}
	})

	found := make([]any, 0, searchResultLimit)

	for _, i := range idx[:min(len(idx), searchResultLimit)] {
		found = append(found, defs[i])
	}

	raw, _ := json.Marshal(map[string]any{keyTools: found}, jsonx.V1())
	res := textResult(string(raw), false)
	res["structuredContent"] = map[string]any{keyTools: found}

	return res, nil
}
