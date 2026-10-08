package bedrockagent

import (
	"slices"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// Resources whose ClientToken is hidden from the wire (json:"-") carry it through
// snapshots in a side map keyed "<table>\x00<key>".
const tokenKeySep = "\x00"

func docRequestKey(op, kbID, dsID, token string) string {
	return strings.Join([]string{op, kbID, dsID, token}, tokenKeySep)
}

func (b *InMemoryBackend) recordDocRequest(key, token string, details []KBDocumentDetail) {
	if token != "" {
		b.docRequests[key] = slices.Clone(details)
	}
}

func (b *InMemoryBackend) pruneDocRequests(kbID, dsID string) {
	for k := range b.docRequests {
		parts := strings.Split(k, tokenKeySep)
		if parts[1] == kbID && parts[2] == dsID {
			delete(b.docRequests, k)
		}
	}
}

func collectTokens[V any](out map[string]string, name string, t *store.Table[V], keyOf, tokenOf func(*V) string) {
	t.Range(func(v *V) bool {
		if tok := tokenOf(v); tok != "" {
			out[name+tokenKeySep+keyOf(v)] = tok
		}

		return true
	})
}

func applyTokens[V any](
	in map[string]string,
	name string,
	t *store.Table[V],
	keyOf func(*V) string,
	set func(*V, string),
) {
	t.Range(func(v *V) bool {
		if tok, ok := in[name+tokenKeySep+keyOf(v)]; ok {
			set(v, tok)
		}

		return true
	})
}

func (b *InMemoryBackend) collectHiddenClientTokens() map[string]string {
	out := make(map[string]string)

	collectTokens(
		out,
		"kb",
		b.knowledgeBases,
		knowledgeBaseKeyFn,
		func(v *KnowledgeBase) string { return v.ClientToken },
	)
	collectTokens(out, "ds", b.dataSources, dataSourceKeyFn, func(v *DataSource) string { return v.ClientToken })
	collectTokens(out, "job", b.ingestionJobs, ingestionJobKeyFn, func(v *IngestionJob) string { return v.ClientToken })
	collectTokens(out, "flow", b.flows, flowKeyFn, func(v *Flow) string { return v.ClientToken })
	collectTokens(
		out,
		"flowVersion",
		b.flowVersions,
		flowVersionKeyFn,
		func(v *FlowVersion) string { return v.ClientToken },
	)
	collectTokens(out, "flowAlias", b.flowAliases, flowAliasKeyFn, func(v *FlowAlias) string { return v.ClientToken })
	collectTokens(out, "prompt", b.prompts, promptKeyFn, func(v *Prompt) string { return v.ClientToken })
	collectTokens(
		out,
		"promptVersion",
		b.promptVersions,
		promptVersionKeyFn,
		func(v *PromptVersion) string { return v.ClientToken },
	)

	return out
}

func (b *InMemoryBackend) applyHiddenClientTokens(in map[string]string) {
	if len(in) == 0 {
		return
	}

	applyTokens(in, "kb", b.knowledgeBases, knowledgeBaseKeyFn, func(v *KnowledgeBase, t string) { v.ClientToken = t })
	applyTokens(in, "ds", b.dataSources, dataSourceKeyFn, func(v *DataSource, t string) { v.ClientToken = t })
	applyTokens(in, "job", b.ingestionJobs, ingestionJobKeyFn, func(v *IngestionJob, t string) { v.ClientToken = t })
	applyTokens(in, "flow", b.flows, flowKeyFn, func(v *Flow, t string) { v.ClientToken = t })
	applyTokens(
		in,
		"flowVersion",
		b.flowVersions,
		flowVersionKeyFn,
		func(v *FlowVersion, t string) { v.ClientToken = t },
	)
	applyTokens(in, "flowAlias", b.flowAliases, flowAliasKeyFn, func(v *FlowAlias, t string) { v.ClientToken = t })
	applyTokens(in, "prompt", b.prompts, promptKeyFn, func(v *Prompt, t string) { v.ClientToken = t })
	applyTokens(
		in,
		"promptVersion",
		b.promptVersions,
		promptVersionKeyFn,
		func(v *PromptVersion, t string) { v.ClientToken = t },
	)
}
