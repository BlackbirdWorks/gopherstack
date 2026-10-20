package gcprest

// MatchPath compiles tmpl and matches an escaped request path against it, returning the verb and bindings.
func MatchPath(tmpl, escapedPath string) (string, [][2]string, bool, error) {
	t, err := parseTemplate(tmpl)
	if err != nil {
		return "", nil, false, err
	}

	segs, ok := splitPath(escapedPath, t.verb)
	if !ok {
		return t.verb, nil, false, nil
	}

	binds, ok := t.match(segs)
	vars := make([][2]string, 0, len(binds))

	for _, b := range binds {
		vars = append(vars, [2]string{b.field, b.value})
	}

	return t.verb, vars, ok, nil
}

// SplitPath exposes splitPath.
func SplitPath(escaped, verb string) ([]string, bool) { return splitPath(escaped, verb) }

// ErrBadTemplate exposes the template parse error.
var ErrBadTemplate = errBadTemplate
