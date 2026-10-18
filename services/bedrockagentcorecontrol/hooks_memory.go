package bedrockagentcorecontrol

import (
	"slices"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/awsid"
	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
)

const (
	minExpiryDays = 7
	maxExpiryDays = 365
)

func strategyTypes() map[string]string {
	return map[string]string{
		"semanticMemoryStrategy":       "SEMANTIC",
		"summaryMemoryStrategy":        "SUMMARIZATION",
		"userPreferenceMemoryStrategy": "USER_PREFERENCE",
		"customMemoryStrategy":         "CUSTOM",
		"episodicMemoryStrategy":       "EPISODIC",
	}
}

func strategyUnionKeys() []string {
	types := strategyTypes()
	keys := make([]string, 0, len(types))

	for k := range types {
		keys = append(keys, k)
	}

	slices.Sort(keys)

	return keys
}

func validateExpiry(v any) error {
	if f, ok := v.(float64); ok && (int(f) < minExpiryDays || int(f) > maxExpiryDays) {
		return invalid("eventExpiryDuration must be between %d and %d days", minExpiryDays, maxExpiryDays)
	}

	return nil
}

func validateStrategyInputs(list []any) error {
	for _, s := range list {
		m, _ := s.(map[string]any)

		key, ok := exactlyOne(m, strategyUnionKeys())
		if !ok {
			return invalid("each memory strategy must set exactly one of %s", strings.Join(strategyUnionKeys(), ", "))
		}

		if strOf(objOf(m, key), keyName) == "" {
			return invalid("memory strategy name is required")
		}
	}

	return nil
}

func validateMemoryInput(in args) error {
	if err := validateExpiry(in["eventExpiryDuration"]); err != nil {
		return err
	}

	return validateStrategyInputs(in.list("memoryStrategies"))
}

func (b *InMemoryBackend) newStrategy(m map[string]any) map[string]any {
	key, _ := exactlyOne(m, strategyUnionKeys())
	body := objOf(m, key)
	now := awstime.Epoch(b.now())

	s := map[string]any{
		"strategyId": awsid.NewN(strOf(body, keyName)+"-", idSuffixLen),
		keyName:      body[keyName], "type": strategyTypes()[key], keyStatus: stateActive,
		"createdAt": now, "updatedAt": now,
	}

	for _, f := range []string{keyDescription, "namespaces", "namespaceTemplates", "memoryRecordSchema", "configuration"} {
		if v, ok := body[f]; ok {
			s[f] = v
		}
	}

	return s
}

func (b *InMemoryBackend) decorateMemory(r *Resource, in args) {
	strategies := make([]any, 0, len(in.list("memoryStrategies")))

	for _, s := range in.list("memoryStrategies") {
		m, _ := s.(map[string]any)
		strategies = append(strategies, b.newStrategy(m))
	}

	delete(r.Doc, "memoryStrategies")
	r.Doc["strategies"] = strategies
}

func validateMemoryUpdate(r *Resource, patch map[string]any) error {
	if err := validateExpiry(patch["eventExpiryDuration"]); err != nil {
		return err
	}

	ms, _ := patch["memoryStrategies"].(map[string]any)
	if ms == nil {
		return nil
	}

	if err := validateStrategyInputs(listOf(ms, "addMemoryStrategies")); err != nil {
		return err
	}

	existing := map[string]bool{}
	for _, s := range listOf(r.Doc, "strategies") {
		existing[strOf(mapOf(s), "strategyId")] = true
	}

	for _, key := range []string{"modifyMemoryStrategies", "deleteMemoryStrategies"} {
		for _, e := range listOf(ms, key) {
			id := strOf(mapOf(e), "memoryStrategyId")
			if !existing[id] {
				return notFound("memory strategy %q not found", id)
			}
		}
	}

	return nil
}

func (b *InMemoryBackend) applyMemoryUpdate(r *Resource, patch map[string]any) {
	ms, _ := patch["memoryStrategies"].(map[string]any)
	delete(patch, "memoryStrategies")

	if add, ok := patch["addIndexedKeys"].([]any); ok {
		cur := listOf(r.Doc, "indexedKeys")
		r.Doc["indexedKeys"] = append(cur, add...)

		delete(patch, "addIndexedKeys")
	}

	mergeDoc(r, patch)

	if ms == nil {
		return
	}

	strategies := listOf(r.Doc, "strategies")
	now := awstime.Epoch(b.now())

	for _, e := range listOf(ms, "deleteMemoryStrategies") {
		id := strOf(mapOf(e), "memoryStrategyId")
		strategies = slices.DeleteFunc(strategies, func(s any) bool { return strOf(mapOf(s), "strategyId") == id })
	}

	for _, e := range listOf(ms, "modifyMemoryStrategies") {
		mod := mapOf(e)

		for _, s := range strategies {
			sm := mapOf(s)
			if sm["strategyId"] != mod["memoryStrategyId"] {
				continue
			}

			for _, f := range strategyFields() {
				if v, ok := mod[f]; ok {
					sm[f] = v
				}
			}

			sm["updatedAt"] = now
		}
	}

	for _, a := range listOf(ms, "addMemoryStrategies") {
		strategies = append(strategies, b.newStrategy(mapOf(a)))
	}

	r.Doc["strategies"] = strategies
}

func strategyFields() []string {
	return []string{keyDescription, "namespaces", "namespaceTemplates", "configuration", "memoryRecordSchema"}
}
