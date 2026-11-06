package gcpmonitoring

import (
	"encoding/json"
	"errors"
	"regexp"
	"strconv"
	"strings"
)

var errJSONPath = errors.New("invalid JSONPath")

type pathStep struct {
	name     string
	index    int
	kind     byte
}

const (
	stepName  byte = 'n'
	stepIndex byte = 'i'
	stepAny   byte = '*'
)

// parseJSONPath accepts $, .name, ['name'], [n] and the wildcards .* and [*].
func parseJSONPath(src string) ([]pathStep, error) {
	if !strings.HasPrefix(src, "$") {
		return nil, errJSONPath
	}

	var steps []pathStep

	rest := src[1:]

	for rest != "" {
		switch rest[0] {
		case '.':
			end := strings.IndexAny(rest[1:], ".[")
			if end < 0 {
				end = len(rest) - 1
			}

			name := rest[1 : 1+end]
			if name == "" {
				return nil, errJSONPath
			}

			if name == "*" {
				steps = append(steps, pathStep{kind: stepAny})
			} else {
				steps = append(steps, pathStep{kind: stepName, name: name})
			}

			rest = rest[1+end:]
		case '[':
			end := strings.IndexByte(rest, ']')
			if end < 0 {
				return nil, errJSONPath
			}

			step, err := bracketStep(rest[1:end])
			if err != nil {
				return nil, err
			}

			steps = append(steps, step)
			rest = rest[end+1:]
		default:
			return nil, errJSONPath
		}
	}

	return steps, nil
}

func bracketStep(inner string) (pathStep, error) {
	switch {
	case inner == "*":
		return pathStep{kind: stepAny}, nil
	case len(inner) >= 2 && (inner[0] == '\'' || inner[0] == '"') && inner[len(inner)-1] == inner[0]:
		return pathStep{kind: stepName, name: inner[1 : len(inner)-1]}, nil
	}

	n, err := strconv.Atoi(inner)
	if err != nil || n < 0 {
		return pathStep{}, errJSONPath
	}

	return pathStep{kind: stepIndex, index: n}, nil
}

func evalJSONPath(doc any, steps []pathStep) []any {
	cur := []any{doc}

	for _, st := range steps {
		var next []any

		for _, v := range cur {
			switch st.kind {
			case stepName:
				if m, ok := v.(map[string]any); ok {
					if x, has := m[st.name]; has {
						next = append(next, x)
					}
				}
			case stepIndex:
				if a, ok := v.([]any); ok && st.index < len(a) {
					next = append(next, a[st.index])
				}
			case stepAny:
				switch c := v.(type) {
				case map[string]any:
					for _, x := range c {
						next = append(next, x)
					}
				case []any:
					next = append(next, c...)
				}
			}
		}

		cur = next
	}

	return cur
}

// jsonPathMatches reports whether any value at the path equals content (exact) or matches it (regex).
func jsonPathMatches(m *uptimeMatcher, body []byte) bool {
	jm := m.GetJsonPathMatcher()

	steps, err := parseJSONPath(jm.GetJsonPath())
	if err != nil {
		return false
	}

	var doc any
	if json.Unmarshal(body, &doc) != nil {
		return false
	}

	var re *regexp.Regexp

	if jm.GetJsonMatcher() == jsonRegexMatch {
		if re, err = regexp.Compile(m.GetContent()); err != nil {
			return false
		}
	}

	for _, v := range evalJSONPath(doc, steps) {
		text := jsonScalar(v)
		if (re != nil && re.MatchString(text)) || (re == nil && text == m.GetContent()) {
			return true
		}
	}

	return false
}

func jsonScalar(v any) string {
	if v == nil {
		return "null"
	}

	return scalarText(v)
}
