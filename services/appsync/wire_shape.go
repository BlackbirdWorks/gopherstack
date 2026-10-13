package appsync

import "encoding/json"

// wireShape re-encodes v without the internal-only keys drop from its top-level
// object (or from each element when v is a slice).
func wireShape(v any, drop ...string) any {
	raw, err := json.Marshal(v)
	if err != nil {
		return v
	}

	var decoded any
	if json.Unmarshal(raw, &decoded) != nil {
		return v
	}

	switch d := decoded.(type) {
	case map[string]any:
		deleteKeys(d, drop)
	case []any:
		for _, el := range d {
			if m, ok := el.(map[string]any); ok {
				deleteKeys(m, drop)
			}
		}
	}

	return decoded
}

func deleteKeys(m map[string]any, keys []string) {
	for _, k := range keys {
		delete(m, k)
	}
}
