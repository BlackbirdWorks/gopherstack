package gcpcloudbuild

import (
	"bytes"
	"encoding/json"
	"errors"
	"regexp"
	"strconv"
	"strings"
)

var errBindingMissing = errors.New("not present in the payload")

var (
	bindingRef  = regexp.MustCompile(`\$\(body((?:\.[A-Za-z0-9_-]+|\[\d+\])*)\)`)
	bindingStep = regexp.MustCompile(`\.([A-Za-z0-9_-]+)|\[(\d+)\]`)
)

// bindPayload resolves $(body.a.b[0]) references in substitution values against a webhook's JSON payload.
func bindPayload(subs map[string]string, body []byte) (map[string]string, error) {
	out := make(map[string]string, len(subs))

	var doc any

	parsed := false

	for k, v := range subs {
		if !bindingRef.MatchString(v) {
			out[k] = v

			continue
		}

		if !parsed {
			dec := json.NewDecoder(bytes.NewReader(body))
			dec.UseNumber()

			if err := dec.Decode(&doc); err != nil {
				return nil, errInvalid(
					"The webhook payload is not valid JSON, so $(body...) bindings cannot resolve: %v.", err)
			}

			parsed = true
		}

		var bindErr error

		out[k] = bindingRef.ReplaceAllStringFunc(v, func(m string) string {
			val, err := lookupBinding(doc, bindingRef.FindStringSubmatch(m)[1])
			if err != nil && bindErr == nil {
				bindErr = errInvalid("Substitution %s: binding %q: %v.", k, m, err)
			}

			return val
		})

		if bindErr != nil {
			return nil, bindErr
		}
	}

	return out, nil
}

func lookupBinding(doc any, path string) (string, error) {
	cur := doc

	for _, st := range bindingStep.FindAllStringSubmatch(path, -1) {
		var ok bool

		cur, ok = step1(cur, st[1], st[2])
		if !ok {
			return "", errBindingMissing
		}
	}

	switch v := cur.(type) {
	case nil:
		return "", nil
	case string:
		return v, nil
	case json.Number:
		return v.String(), nil
	case bool:
		return strconv.FormatBool(v), nil
	}

	raw, err := json.Marshal(cur)
	if err != nil {
		return "", errBindingMissing
	}

	return strings.TrimSpace(string(raw)), nil
}

func step1(cur any, key, idx string) (any, bool) {
	if key != "" {
		m, ok := cur.(map[string]any)
		if !ok {
			return nil, false
		}

		v, ok := m[key]

		return v, ok
	}

	arr, ok := cur.([]any)
	n, err := strconv.Atoi(idx)

	if !ok || err != nil || n >= len(arr) {
		return nil, false
	}

	return arr[n], true
}
