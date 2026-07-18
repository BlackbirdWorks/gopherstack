package cloudwatchlogs

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// PutTransformer creates or updates a log transformer.
func (b *InMemoryBackend) PutTransformer(logGroupIdentifier string, processors []map[string]any) error {
	if logGroupIdentifier == "" {
		return fmt.Errorf("%w: logGroupIdentifier is required", ErrValidation)
	}

	b.mu.Lock("PutTransformer")
	defer b.mu.Unlock()

	b.transformers.Put(&Transformer{
		LogGroupIdentifier: logGroupIdentifier,
		Processors:         processors,
		CreatedAt:          time.Now().UTC(),
	})

	return nil
}

// GetTransformer returns the transformer for a log group.
func (b *InMemoryBackend) GetTransformer(logGroupIdentifier string) (*Transformer, error) {
	b.mu.RLock("GetTransformer")
	defer b.mu.RUnlock()

	t, ok := b.transformers.Get(logGroupIdentifier)
	if !ok {
		return nil, fmt.Errorf("%w: transformer for %q not found", ErrTransformerNotFound, logGroupIdentifier)
	}
	cp := *t

	return &cp, nil
}

// DeleteTransformer removes the transformer for a log group.
func (b *InMemoryBackend) DeleteTransformer(logGroupIdentifier string) error {
	b.mu.Lock("DeleteTransformer")
	defer b.mu.Unlock()

	if !b.transformers.Delete(logGroupIdentifier) {
		return fmt.Errorf("%w: transformer for %q not found", ErrTransformerNotFound, logGroupIdentifier)
	}

	return nil
}

// ApplyTransformer applies the supplied transformer processors to the supplied
// sample log event messages and returns the transformed results. The transform
// is deterministic: processors are applied in order to each event. Supported
// processors mirror a useful subset of the AWS transformer grammar:
//
//   - addKeys: add fixed key/value entries to the (JSON) event
//   - deleteKeys: remove keys from the (JSON) event
//   - renameKeys: rename keys within the (JSON) event
//   - lowerCaseString / upperCaseString: case-fold named string fields
//   - copyValue: copy one field's value into another
//
// Events that are not JSON objects are passed through unchanged for
// JSON-oriented processors. Unknown processors are ignored.
func ApplyTransformer(
	messages []string,
	processors []map[string]any,
) []TestTransformerOutput {
	results := make([]TestTransformerOutput, 0, len(messages))

	for i, msg := range messages {
		transformed := applyProcessorsToMessage(msg, processors)
		results = append(results, TestTransformerOutput{
			EventNumber:             int64(i + 1),
			EventMessage:            msg,
			TransformedEventMessage: transformed,
		})
	}

	return results
}

func applyProcessorsToMessage(message string, processors []map[string]any) string {
	obj, isJSON := decodeJSONObject(message)

	for _, proc := range processors {
		for name, raw := range proc {
			cfg, ok := raw.(map[string]any)
			if !ok {
				continue
			}

			if isJSON {
				applyJSONProcessor(name, cfg, obj)
			}
		}
	}

	if !isJSON {
		return message
	}

	out, err := json.Marshal(obj)
	if err != nil {
		return message
	}

	return string(out)
}

func decodeJSONObject(message string) (map[string]any, bool) {
	trimmed := strings.TrimSpace(message)
	if trimmed == "" || trimmed[0] != '{' {
		return nil, false
	}

	var obj map[string]any
	if err := json.Unmarshal([]byte(trimmed), &obj); err != nil {
		return nil, false
	}

	return obj, true
}

// applyJSONProcessor dispatches a single named JSON transformer processor to
// its handler. Each case delegates to a small helper so that the dispatch
// itself stays trivial.
func applyJSONProcessor(name string, cfg map[string]any, obj map[string]any) {
	switch name {
	case "addKeys":
		applyAddKeys(cfg, obj)
	case "deleteKeys":
		applyDeleteKeys(cfg, obj)
	case "renameKeys":
		applyRenameKeys(cfg, obj)
	case "copyValue":
		applyCopyValue(cfg, obj)
	case "lowerCaseString":
		applyStringCase(cfg, obj, strings.ToLower)
	case "upperCaseString":
		applyStringCase(cfg, obj, strings.ToUpper)
	}
}

// applyAddKeys implements the "addKeys" JSON transformer processor: it adds
// each configured entry's value under its key, optionally overwriting an
// existing value when overwriteIfExists is set.
func applyAddKeys(cfg map[string]any, obj map[string]any) {
	for _, entry := range entriesField(cfg, "entries") {
		key, _ := entry["key"].(string)
		if key == "" {
			continue
		}
		_, exists := obj[key]
		overwrite, _ := entry["overwriteIfExists"].(bool)
		if !exists || overwrite {
			obj[key] = entry["value"]
		}
	}
}

// applyDeleteKeys implements the "deleteKeys" JSON transformer processor.
func applyDeleteKeys(cfg map[string]any, obj map[string]any) {
	for _, key := range stringSliceField(cfg, "withKeys") {
		delete(obj, key)
	}
}

// applyRenameKeys implements the "renameKeys" JSON transformer processor.
func applyRenameKeys(cfg map[string]any, obj map[string]any) {
	for _, entry := range entriesField(cfg, "entries") {
		key, _ := entry["key"].(string)
		renameTo, _ := entry["renameTo"].(string)
		if key == "" || renameTo == "" {
			continue
		}
		if v, exists := obj[key]; exists {
			obj[renameTo] = v
			delete(obj, key)
		}
	}
}

// applyCopyValue implements the "copyValue" JSON transformer processor.
func applyCopyValue(cfg map[string]any, obj map[string]any) {
	for _, entry := range entriesField(cfg, "entries") {
		source, _ := entry["source"].(string)
		target, _ := entry["target"].(string)
		if source == "" || target == "" {
			continue
		}
		if v, exists := obj[source]; exists {
			obj[target] = v
		}
	}
}

func applyStringCase(cfg map[string]any, obj map[string]any, fn func(string) string) {
	for _, key := range stringSliceField(cfg, "withKeys") {
		if s, ok := obj[key].(string); ok {
			obj[key] = fn(s)
		}
	}
}

func entriesField(cfg map[string]any, key string) []map[string]any {
	raw, ok := cfg[key].([]any)
	if !ok {
		return nil
	}

	entries := make([]map[string]any, 0, len(raw))
	for _, item := range raw {
		if m, isMap := item.(map[string]any); isMap {
			entries = append(entries, m)
		}
	}

	return entries
}

func stringSliceField(cfg map[string]any, key string) []string {
	raw, ok := cfg[key].([]any)
	if !ok {
		return nil
	}

	out := make([]string, 0, len(raw))
	for _, item := range raw {
		if s, isStr := item.(string); isStr {
			out = append(out, s)
		}
	}

	return out
}
