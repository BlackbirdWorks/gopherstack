package cloudformation

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"unicode"
)

// sam_connector_profiles.json is the SAM translator's own connector profile table
// (aws/serverless-application-model, samtranslator/model/connector_profiles/profiles.json), Apache-2.0,
// Copyright Amazon.com, Inc. or its affiliates.
//
//go:embed sam_connector_profiles.json
var samConnectorProfilesJSON []byte

type samConnProfile struct {
	Permissions           map[string]map[string]samConnProfileEntry `json:"Permissions"`
	CfnResourceProperties map[string]samConnResourceProps           `json:"CfnResourceProperties"`
}

type samConnProfileEntry struct {
	Properties map[string]any `json:"Properties"`
	Type       string         `json:"Type"`
}

type samConnResourceProps struct {
	Inputs  map[string]any `json:"Inputs"`
	Outputs map[string]any `json:"Outputs"`
}

//nolint:gochecknoglobals // lazily decoded embedded table
var samConnectorProfiles = sync.OnceValue(func() samConnProfile {
	var out samConnProfile
	if err := json.Unmarshal(samConnectorProfilesJSON, &out); err != nil {
		panic("cloudformation: invalid embedded sam_connector_profiles.json: " + err.Error())
	}

	return out
})

func rp(key string, value any) samReplacement { return samReplacement{key: key, value: value} }

var samProfileVarRe = regexp.MustCompile(`%\{[\w.]+\}`)

var samSubVarRe = regexp.MustCompile(`\$\{.+\}`)

type samReplacement struct {
	value any
	key   string
}

func sanitizeSubVar(s string) string {
	var b strings.Builder
	for _, c := range s {
		if unicode.IsLetter(c) || unicode.IsDigit(c) {
			b.WriteRune(c)
		}
	}

	return b.String()
}

// profileReplace substitutes %{Key} placeholders the way the translator's profile_replace does: an exact
// match yields the value itself, a partial match turns the string into an Fn::Sub.
func profileReplace(obj any, repl []samReplacement) (any, error) {
	switch v := obj.(type) {
	case map[string]any:
		out := make(map[string]any, len(v))
		for k, e := range v {
			r, err := profileReplace(e, repl)
			if err != nil {
				return nil, err
			}
			out[k] = r
		}

		return out, nil
	case []any:
		out := make([]any, len(v))
		for i, e := range v {
			r, err := profileReplace(e, repl)
			if err != nil {
				return nil, err
			}
			out[i] = r
		}

		return out, nil
	case string:
		return profileReplaceString(v, repl)
	default:
		return obj, nil
	}
}

func profileReplaceString(s string, repl []samReplacement) (any, error) {
	vars := map[string]any{}
	for _, r := range repl {
		pattern := "%{" + r.key + "}"
		if !strings.Contains(s, pattern) {
			continue
		}
		if r.value == nil {
			return nil, fmt.Errorf("%s is missing", r.key)
		}
		if pattern == s {
			return r.value, nil
		}
		name := sanitizeSubVar(r.key)
		s = strings.ReplaceAll(s, pattern, "${"+name+"}")
		vars[name] = r.value
	}
	if !samSubVarRe.MatchString(s) {
		return s, nil
	}
	if len(vars) > 0 {
		return map[string]any{samKeyFnSub: []any{s, vars}}, nil
	}

	return map[string]any{samKeyFnSub: s}, nil
}

func verifyProfileVarsReplaced(obj any) error {
	raw, err := json.Marshal(obj)
	if err != nil {
		return err
	}
	if m := samProfileVarRe.FindAllString(string(raw), -1); len(m) > 0 {
		return fmt.Errorf("the following variables have not been replaced: %v", m)
	}

	return nil
}
