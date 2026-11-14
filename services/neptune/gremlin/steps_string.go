package gremlin

import (
	"fmt"
	"strings"
	"unicode/utf8"
)

// String-manipulation steps (TinkerPop 3.7.1+, available in Neptune engine releases that ship TinkerPop 3.7/3.8).
func regString(m map[string]stepFn) {
	for name, f := range map[string]func(string) any{
		"toLower": func(s string) any { return strings.ToLower(s) },
		"toUpper": func(s string) any { return strings.ToUpper(s) },
		"trim":    func(s string) any { return strings.TrimSpace(s) },
		"lTrim":   func(s string) any { return strings.TrimLeft(s, " \t\r\n") },
		"rTrim":   func(s string) any { return strings.TrimRight(s, " \t\r\n") },
		"length":  func(s string) any { return int64(utf8.RuneCountInString(s)) },
	} {
		m[name] = perTrav(stringMap(name, func(_ *exec, _ *Step, _ *trav, s string) (any, error) { return f(s), nil }))
	}

	m["asString"] = perTrav(func(_ *exec, _ *Step, t *trav) ([]*trav, error) {
		return mapElements(t, func(v any) (any, error) {
			if v == nil {
				return nil, nil
			}

			return asStringValue(v), nil
		})
	})
	m["split"] = perTrav(stringMap("split", stepSplit))
	m["concat"] = perTrav(stringMap("concat", stepConcat))
	m["replace"] = perTrav(stringMap("replace", stepReplace))
	m["substring"] = perTrav(stringMap("substring", stepSubstring))
}

func asStringValue(v any) string {
	if s, ok := v.(string); ok {
		return s
	}

	if f, ok := v.(float64); ok {
		return formatFloat(f)
	}

	return fmt.Sprint(v)
}

// mapElements applies f to the traverser object, or to each element when it is a list.
func mapElements(t *trav, f func(any) (any, error)) ([]*trav, error) {
	if list, ok := t.v.([]any); ok {
		out := make([]any, 0, len(list))

		for _, e := range list {
			r, err := f(e)
			if err != nil {
				return nil, err
			}

			out = append(out, r)
		}

		return []*trav{t.next(out)}, nil
	}

	r, err := f(t.v)
	if err != nil {
		return nil, err
	}

	if r == nil && t.v == nil {
		return []*trav{t}, nil
	}

	return []*trav{t.next(r)}, nil
}

func stringMap(
	name string, f func(x *exec, s *Step, t *trav, in string) (any, error),
) func(x *exec, s *Step, t *trav) ([]*trav, error) {
	return func(x *exec, s *Step, t *trav) ([]*trav, error) {
		return mapElements(t, func(v any) (any, error) {
			if v == nil {
				return nil, nil
			}

			str, ok := v.(string)
			if !ok {
				return nil, malformed("The input to %s() must be a String but got %T", name, v)
			}

			return f(x, s, t, str)
		})
	}
}

func (x *exec) stringArg(s *Step, i int, t *trav) (string, error) {
	if i >= len(s.Args) {
		return "", invalidArg("%s() requires %d argument(s)", s.Name, i+1)
	}

	a := s.Args[i]
	if tr, ok := a.(*Traversal); ok {
		v, found, err := x.firstValue(tr, t)
		if err != nil {
			return "", err
		}

		if !found {
			return "", malformed("The provided traverser does not map to a value: %s", tr)
		}

		a = v
	}

	str, ok := a.(string)
	if !ok {
		return "", malformed("Argument %d of %s() must be a String", i+1, s.Name)
	}

	return str, nil
}

func stepSplit(x *exec, s *Step, t *trav, in string) (any, error) {
	sep, err := x.stringArg(s, 0, t)
	if err != nil {
		return nil, err
	}

	var parts []string

	if sep == "" {
		for _, r := range in {
			parts = append(parts, string(r))
		}
	} else {
		parts = strings.Split(in, sep)
	}

	out := make([]any, len(parts))
	for i, p := range parts {
		out[i] = p
	}

	return out, nil
}

func stepConcat(x *exec, s *Step, t *trav, in string) (any, error) {
	var sb strings.Builder

	sb.WriteString(in)

	for i := range s.Args {
		a, err := x.stringArg(s, i, t)
		if err != nil {
			return nil, err
		}

		sb.WriteString(a)
	}

	return sb.String(), nil
}

func stepReplace(x *exec, s *Step, t *trav, in string) (any, error) {
	from, err := x.stringArg(s, 0, t)
	if err != nil {
		return nil, err
	}

	to, err := x.stringArg(s, 1, t)
	if err != nil {
		return nil, err
	}

	return strings.ReplaceAll(in, from, to), nil
}

func stepSubstring(_ *exec, s *Step, _ *trav, in string) (any, error) {
	args := s.Args
	if len(args) > 0 {
		if tok, ok := args[0].(Token); ok && tok.Kind == kindScope {
			args = args[1:]
		}
	}

	if len(args) == 0 || !isInt(args[0]) {
		return nil, invalidArg("substring() expects an integer start index")
	}

	runes := []rune(in)
	start := int(toInt(args[0]))
	end := len(runes)

	if len(args) > 1 {
		if !isInt(args[1]) {
			return nil, invalidArg("substring() expects an integer length")
		}

		end = min(len(runes), start+int(toInt(args[1])))
	}

	if start < 0 {
		start = max(0, len(runes)+start)
	}

	if start > len(runes) || end < start {
		return "", nil
	}

	return string(runes[start:end]), nil
}
