package pods

import (
	"bytes"
	"encoding/json"
	"fmt"
)

type scopeKey struct{ account, region string }

// scopeOf reports the account/region of a record object carrying both fields.
func scopeOf(v any) (scopeKey, bool) {
	m, ok := v.(map[string]any)
	if !ok {
		return scopeKey{}, false
	}

	acct, aok := m["accountID"].(string)
	if !aok {
		acct, aok = m["accountId"].(string)
	}

	region, rok := m["region"].(string)

	return scopeKey{acct, region}, aok && rok
}

// collectScopes gathers the account/region scopes of nested records; the root is not a record.
func collectScopes(v any, root bool, out map[scopeKey]bool) {
	if !root {
		if k, ok := scopeOf(v); ok {
			out[k] = true

			return
		}
	}

	switch t := v.(type) {
	case map[string]any:
		for _, c := range t {
			collectScopes(c, false, out)
		}
	case []any:
		for _, c := range t {
			collectScopes(c, false, out)
		}
	}
}

func inScopes(v any, scopes map[scopeKey]bool) bool {
	k, ok := scopeOf(v)

	return ok && scopes[k]
}

func mergeArray(cur any, inc []any, scopes map[scopeKey]bool) any {
	curArr, ok := cur.([]any)
	if !ok {
		return inc
	}

	out := make([]any, 0, len(curArr)+len(inc))

	for _, e := range curArr {
		if !inScopes(e, scopes) {
			out = append(out, e)
		}
	}

	return append(out, inc...)
}

func mergeMap(cur any, inc map[string]any, scopes map[scopeKey]bool) any {
	curMap, ok := cur.(map[string]any)
	if !ok {
		return inc
	}

	out := make(map[string]any, len(curMap)+len(inc))

	for k, v := range curMap {
		if !inScopes(v, scopes) {
			out[k] = v
		}
	}

	for k, v := range inc {
		out[k] = mergeValue(curMap[k], v, scopes)
	}

	return out
}

func mergeValue(cur, inc any, scopes map[scopeKey]bool) any {
	if _, ok := scopeOf(inc); ok {
		return inc
	}

	switch t := inc.(type) {
	case []any:
		return mergeArray(cur, t, scopes)
	case map[string]any:
		return mergeMap(cur, t, scopes)
	default:
		return inc
	}
}

func decodeJSON(raw []byte) (any, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()

	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrCorrupt, err)
	}

	return v, nil
}

// MergeScoped merges the incoming service snapshot into current per account/region:
// records in an account/region present in incoming replace current's records for that
// scope, other scopes in current are kept. scoped is false when incoming has no
// account/region records; the caller must then replace the whole service state.
func MergeScoped(current, incoming []byte) ([]byte, bool, error) {
	inc, err := decodeJSON(incoming)
	if err != nil {
		return nil, false, err
	}

	scopes := map[scopeKey]bool{}
	collectScopes(inc, true, scopes)

	if len(scopes) == 0 || len(current) == 0 {
		return incoming, false, nil
	}

	cur, err := decodeJSON(current)
	if err != nil {
		return nil, false, err
	}

	merged, err := json.Marshal(mergeValue(cur, inc, scopes))
	if err != nil {
		return nil, false, fmt.Errorf("marshal merged snapshot: %w", err)
	}

	return merged, true, nil
}
