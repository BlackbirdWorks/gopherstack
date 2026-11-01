package azurearm

import (
	"encoding/json"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

const (
	apimMaxSpecSize = 4 << 20
	apimMaxOpIDBase = 76
)

var apimOpIDCleanRE = regexp.MustCompile(`[^A-Za-z0-9._-]+`)

type apimImportedOp struct {
	props map[string]any
	id    string
}

type apimAPISpec struct {
	title      string
	desc       string
	serviceURL string
	ops        []apimImportedOp
}

func (sp *apimAPISpec) fillDefaults(out map[string]any) {
	if _, has := out[apimCDisplayName]; !has && sp.title != "" {
		out[apimCDisplayName] = sp.title
	}

	if _, has := out[webDescription]; !has && sp.desc != "" {
		out[webDescription] = sp.desc
	}

	if _, has := out[apimCServiceURL]; !has && sp.serviceURL != "" {
		out[apimCServiceURL] = sp.serviceURL
	}
}

// parseAPISpec reads an OpenAPI 3 or Swagger 2 JSON document. YAML and the -link formats (which would make the
// emulator fetch a caller-chosen URL) are refused.
func parseAPISpec(format, value string) (*apimAPISpec, error) {
	switch strings.ToLower(format) {
	case "openapi+json", "openapi", "swagger-json":
	default:
		return nil, apimInvalid(
			"The import format %q is not supported; use openapi+json, openapi (JSON content) or swagger-json.", format)
	}

	if len(value) > apimMaxSpecSize {
		return nil, apimInvalid("The API definition is larger than %d bytes.", apimMaxSpecSize)
	}

	var doc map[string]any
	if err := json.Unmarshal([]byte(value), &doc); err != nil {
		return nil, apimInvalid("The API definition is not valid JSON (YAML is not supported): %v", err)
	}

	isV3 := strings.HasPrefix(strOf(doc, "openapi"), "3.")
	if !isV3 && strOf(doc, "swagger") != apimOpenAPI2 {
		return nil, apimInvalid("The API definition must declare openapi 3.x or swagger 2.0.")
	}

	info := mapOf(doc["info"])
	sp := &apimAPISpec{title: strOf(info, "title"), desc: strOf(info, webDescription)}
	sp.serviceURL = specServiceURL(doc, isV3)

	paths := mapOf(doc["paths"])
	keys := make([]string, 0, len(paths))

	for k := range paths {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	used := map[string]bool{}

	for _, path := range keys {
		item := mapOf(paths[path])

		for _, method := range []string{"get", "put", "post", apimCDelete, "options", "head", "patch", "trace"} {
			op := mapOf(item[method])
			if op == nil {
				continue
			}

			if len(sp.ops) >= apimMaxOps {
				return nil, apimInvalid("The API definition has more than %d operations.", apimMaxOps)
			}

			sp.ops = append(sp.ops, buildImportedOp(method, path, op, used, isV3))
		}
	}

	return sp, nil
}

func specServiceURL(doc map[string]any, isV3 bool) string {
	if isV3 {
		if servers, ok := doc["servers"].([]any); ok && len(servers) > 0 {
			if u := strOf(mapOf(servers[0]), apimCUrl); validAbsURL(u, acaScaleHTTP, acaSchemeHTTPS) {
				return strings.TrimRight(u, "/")
			}
		}

		return ""
	}

	host := strOf(doc, "host")
	if host == "" {
		return ""
	}

	scheme := acaSchemeHTTPS
	if schemes, ok := doc["schemes"].([]any); ok && len(schemes) > 0 {
		if s, isStr := schemes[0].(string); isStr {
			scheme = s
		}
	}

	return strings.TrimRight(scheme+"://"+host+strOf(doc, "basePath"), "/")
}

func buildImportedOp(method, path string, op map[string]any, used map[string]bool, isV3 bool) apimImportedOp {
	id := apimOpIDCleanRE.ReplaceAllString(strOf(op, "operationId"), "-")
	if id == "" {
		id = method + "-" + strings.Trim(apimOpIDCleanRE.ReplaceAllString(path, "-"), "-")
	}

	if len(id) > apimMaxOpIDBase {
		id = id[:apimMaxOpIDBase]
	}

	base := id
	for n := 2; used[strings.ToLower(id)]; n++ {
		id = base + "-" + strconv.Itoa(n)
	}

	used[strings.ToLower(id)] = true

	display := strOf(op, "summary")
	if display == "" {
		display = id
	}

	props := map[string]any{
		apimCDisplayName: truncate(display, apimMaxName), apimCMethod: strings.ToUpper(method), apimCUrlTemplate: path,
	}

	if d := strOf(op, webDescription); d != "" {
		props[webDescription] = d
	}

	names := templateParams(path)
	params := make([]any, 0, len(names))

	for _, name := range names {
		params = append(params, map[string]any{fieldName: name, "required": true, fieldType: "string"})
	}

	props[apimCTemplateParameters] = params

	if resp := importedResponses(mapOf(op[apimCResponses]), isV3); len(resp) > 0 {
		props[apimCResponses] = resp
	}

	return apimImportedOp{id: id, props: props}
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}

	return s[:n]
}

func importedResponses(responses map[string]any, isV3 bool) []any {
	codes := make([]string, 0, len(responses))

	for c := range responses {
		if _, err := strconv.Atoi(c); err == nil {
			codes = append(codes, c)
		}
	}

	sort.Strings(codes)

	out := make([]any, 0, len(codes))

	for _, c := range codes {
		r := mapOf(responses[c])
		code, _ := strconv.Atoi(c)
		entry := map[string]any{apimCStatusCode: code}

		if d := strOf(r, webDescription); d != "" {
			entry[webDescription] = d
		}

		if isV3 {
			if reps := importedRepresentations(mapOf(r["content"])); len(reps) > 0 {
				entry["representations"] = reps
			}
		}

		out = append(out, entry)
	}

	return out
}

func importedRepresentations(content map[string]any) []any {
	types := make([]string, 0, len(content))

	for t := range content {
		types = append(types, t)
	}

	sort.Strings(types)

	reps := make([]any, 0, len(types))

	for _, t := range types {
		media := mapOf(content[t])
		rep := map[string]any{apimCContentTypeX: t}

		switch {
		case media["example"] != nil:
			rep[apimCExamples] = map[string]any{aksDefaultName: map[string]any{fieldValue: media["example"]}}
		case mapOf(media[apimCExamples]) != nil:
			ex := map[string]any{}

			for name, e := range mapOf(media[apimCExamples]) {
				if v, has := mapOf(e)[fieldValue]; has {
					ex[name] = map[string]any{fieldValue: v}
				}
			}

			if len(ex) > 0 {
				rep[apimCExamples] = ex
			}
		}

		reps = append(reps, rep)
	}

	return reps
}
