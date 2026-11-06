package gremlin

const indexerKey = "~tinkerpop.index.indexer"

func regIndex(m map[string]stepFn) {
	m["index"] = stepIndex
}

// indexAsMap reads with(WithOptions.indexer, list|map); list (0) is the default.
func indexAsMap(s *Step) (bool, error) {
	asMap := false

	for _, w := range modsNamed(s, "with") {
		if len(w.Args) != 2 || !isIndexerKey(w.Args[0]) {
			continue
		}

		switch v := w.Args[1].(type) {
		case Token:
			if v.Kind != kindWithOptions || (v.Name != "map" && v.Name != "list") {
				return false, invalidArg("index() indexer must be WithOptions.list or WithOptions.map")
			}

			asMap = v.Name == "map"
		default:
			if !isInt(v) || toInt(v) < 0 || toInt(v) > 1 {
				return false, invalidArg("index() indexer must be WithOptions.list or WithOptions.map")
			}

			asMap = toInt(v) == 1
		}
	}

	return asMap, nil
}

func isIndexerKey(a any) bool {
	if t, ok := a.(Token); ok {
		return t.Kind == kindWithOptions && t.Name == "indexer"
	}

	s, ok := a.(string)

	return ok && s == indexerKey
}

func stepIndex(_ *exec, s *Step, in []*trav) ([]*trav, error) {
	asMap, err := indexAsMap(s)
	if err != nil {
		return nil, err
	}

	return mapLocal(in, func(v any) (any, bool) {
		var items []any

		switch t := v.(type) {
		case []any:
			items = t
		case Set:
			items = t
		default:
			items = []any{v}
		}

		if asMap {
			m := NewMap()
			for i, e := range items {
				m.Put(int64(i), e)
			}

			return m, true
		}

		out := make([]any, len(items))
		for i, e := range items {
			out[i] = []any{e, int64(i)}
		}

		return out, true
	}), nil
}
