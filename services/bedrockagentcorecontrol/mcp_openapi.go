package bedrockagentcorecontrol

import (
	"bytes"
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/url"
	"slices"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

const (
	maxRefDepth   = 8
	jsonMediaType = "application/json"
)

var (
	errBadOpenAPI   = errors.New("OpenAPI schema is invalid")
	errMissingParam = errors.New("missing required parameter")
)

func httpMethods() []string {
	return []string{"get", "put", "post", "delete", "patch", "head", "options"}
}

type openAPIParam struct {
	schema   map[string]any
	name     string
	in       string
	required bool
}

type openAPIOp struct {
	body     map[string]any
	id       string
	desc     string
	method   string
	path     string
	server   string
	params   []openAPIParam
	bodyReq  bool
	hasBody  bool
	bodyJSON bool
}

func (o openAPIOp) toolDef() map[string]any {
	props := map[string]any{}
	required := []any{}

	for _, p := range o.params {
		props[p.name] = p.schema
		if p.required {
			required = append(required, p.name)
		}
	}

	for k, v := range mapOf(o.body["properties"]) {
		if _, taken := props[k]; !taken {
			props[k] = v
		}
	}

	if o.bodyReq {
		for _, r := range listOf(o.body, "required") {
			if !slices.Contains(required, r) {
				required = append(required, r)
			}
		}
	}

	schema := map[string]any{keyType: "object", "properties": props}
	if len(required) > 0 {
		schema["required"] = required
	}

	return map[string]any{keyName: o.id, keyDescription: o.desc, "inputSchema": schema}
}

func (b *InMemoryBackend) openAPIOps(cfg map[string]any) ([]openAPIOp, error) {
	raw, err := b.schemaPayload(cfg, "inlinePayload")
	if err != nil {
		return nil, err
	}

	return parseOpenAPI(raw)
}

func parseOpenAPI(raw []byte) ([]openAPIOp, error) {
	var spec map[string]any
	if err := yaml.Unmarshal(raw, &spec); err != nil {
		return nil, fmt.Errorf("%w: %w", errBadOpenAPI, err)
	}

	if _, ok := spec["openapi"].(string); !ok {
		return nil, fmt.Errorf("%w: missing openapi version", errBadOpenAPI)
	}

	server, err := serverURL(spec)
	if err != nil {
		return nil, err
	}

	paths := mapOf(spec["paths"])
	keys := make([]string, 0, len(paths))

	for k := range paths {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	var ops []openAPIOp

	for _, p := range keys {
		item := mapOf(paths[p])

		for _, m := range httpMethods() {
			opm := mapOf(item[m])
			if opm == nil {
				continue
			}

			if op, ok := buildOp(spec, server, p, m, item, opm); ok {
				ops = append(ops, op)
			}
		}
	}

	if len(ops) == 0 {
		return nil, fmt.Errorf("%w: no operations with an operationId", errBadOpenAPI)
	}

	return ops, nil
}

func serverURL(spec map[string]any) (string, error) {
	servers, _ := spec["servers"].([]any)
	if len(servers) == 0 {
		return "", fmt.Errorf("%w: servers must name the API endpoint", errBadOpenAPI)
	}

	s := mapOf(servers[0])
	u := strOf(s, "url")

	for name, v := range mapOf(s["variables"]) {
		u = strings.ReplaceAll(u, "{"+name+"}", strOf(mapOf(v), "default"))
	}

	parsed, err := url.Parse(u)
	if err != nil || (parsed.Scheme != schemeHTTP && parsed.Scheme != schemeHTTPS) || parsed.Host == "" {
		return "", fmt.Errorf("%w: server url %q must be absolute http(s)", errBadOpenAPI, u)
	}

	return strings.TrimRight(u, "/"), nil
}

func buildOp(spec map[string]any, server, path, method string, item, opm map[string]any) (openAPIOp, bool) {
	id := strOf(opm, "operationId")
	if id == "" {
		return openAPIOp{}, false
	}

	desc := strOf(opm, "description")
	if desc == "" {
		desc = strOf(opm, "summary")
	}

	op := openAPIOp{id: id, desc: desc, method: strings.ToUpper(method), path: path, server: server}

	seen := map[string]bool{}

	for _, list := range [][]any{listOf(opm, "parameters"), listOf(item, "parameters")} {
		for _, pv := range list {
			pm := resolveRef(spec, mapOf(pv), 0)
			key := strOf(pm, keyName) + "/" + strOf(pm, "in")

			if seen[key] || strOf(pm, keyName) == "" {
				continue
			}

			seen[key] = true

			schema := resolveRefs(spec, mapOf(pm["schema"]), 0)
			if schema == nil {
				schema = map[string]any{keyType: "string"}
			}

			req, _ := pm["required"].(bool)
			op.params = append(op.params, openAPIParam{
				name: strOf(pm, keyName), in: strOf(pm, "in"), required: req || strOf(pm, "in") == "path",
				schema: schema,
			})
		}
	}

	if rb := resolveRef(spec, objOf(opm, "requestBody"), 0); rb != nil {
		if media := objOf(objOf(rb, "content"), jsonMediaType); media != nil {
			op.hasBody, op.bodyJSON = true, true
			op.body = resolveRefs(spec, mapOf(media["schema"]), 0)
			op.bodyReq, _ = rb["required"].(bool)
		}
	}

	return op, true
}

func resolveRef(spec, m map[string]any, depth int) map[string]any {
	ref := strOf(m, "$ref")
	if ref == "" || depth > maxRefDepth || !strings.HasPrefix(ref, "#/") {
		return m
	}

	var cur any = spec

	for part := range strings.SplitSeq(strings.TrimPrefix(ref, "#/"), "/") {
		part = strings.ReplaceAll(strings.ReplaceAll(part, "~1", "/"), "~0", "~")
		cur = mapOf(cur)[part]
	}

	if r := mapOf(cur); r != nil {
		return resolveRef(spec, r, depth+1)
	}

	return m
}

// resolveRefs inlines local $ref members of a schema, to a bounded depth.
func resolveRefs(spec, m map[string]any, depth int) map[string]any {
	if m == nil {
		return nil
	}

	if depth > maxRefDepth {
		return map[string]any{}
	}

	m = resolveRef(spec, m, 0)
	out := make(map[string]any, len(m))

	for k, v := range m {
		out[k] = resolveValue(spec, v, depth+1)
	}

	return out
}

func resolveValue(spec map[string]any, v any, depth int) any {
	switch t := v.(type) {
	case map[string]any:
		return resolveRefs(spec, t, depth)
	case []any:
		out := make([]any, len(t))
		for i, e := range t {
			out[i] = resolveValue(spec, e, depth)
		}

		return out
	default:
		return v
	}
}

// callOpenAPI performs the HTTP request an OpenAPI tool stands for.
func (b *InMemoryBackend) callOpenAPI(
	ctx context.Context, t targetView, cfg map[string]any, tool string, args map[string]any,
) (map[string]any, error) {
	ops, err := b.openAPIOps(cfg)
	if err != nil {
		return nil, err
	}

	idx := slices.IndexFunc(ops, func(o openAPIOp) bool { return o.id == tool })
	if idx < 0 {
		return nil, errUnknownTool
	}

	req, err := ops[idx].request(ctx, args)
	if err != nil {
		return textResult(err.Error(), true), nil
	}

	hc, err := b.targetClient(ctx, t)
	if err != nil {
		return textResult("credential provider error: "+err.Error(), true), nil
	}

	resp, err := hc.Do(req)
	if err != nil {
		return textResult("request to target failed: "+err.Error(), true), nil
	}

	defer resp.Body.Close()

	body, _ := io.ReadAll(io.LimitReader(resp.Body, maxOutboundBytes))

	return textResult(string(body), resp.StatusCode >= http.StatusBadRequest), nil
}

func (o openAPIOp) request(ctx context.Context, args map[string]any) (*http.Request, error) {
	path := o.path
	query := url.Values{}
	header := http.Header{}
	used := map[string]bool{}

	for _, p := range o.params {
		v, ok := args[p.name]
		if !ok {
			if p.required {
				return nil, fmt.Errorf("%w %q", errMissingParam, p.name)
			}

			continue
		}

		used[p.name] = true
		s := paramString(v)

		switch p.in {
		case "path":
			path = strings.ReplaceAll(path, "{"+p.name+"}", url.PathEscape(s))
		case "query":
			query.Add(p.name, s)
		case "header":
			header.Set(p.name, s)
		}
	}

	var body io.Reader

	if o.hasBody {
		payload := map[string]any{}

		for k, v := range args {
			if !used[k] {
				payload[k] = v
			}
		}

		raw, err := json.Marshal(payload, jsonx.V1())
		if err != nil {
			return nil, fmt.Errorf("encode request body: %w", err)
		}

		body = bytes.NewReader(raw)
	}

	target := o.server + path
	if len(query) > 0 {
		target += "?" + query.Encode()
	}

	req, err := http.NewRequestWithContext(ctx, o.method, target, body)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}

	maps.Copy(req.Header, header)

	req.Header.Set("Accept", jsonMediaType)

	if o.hasBody {
		req.Header.Set("Content-Type", jsonMediaType)
	}

	return req, nil
}

func paramString(v any) string {
	if s, ok := v.(string); ok {
		return s
	}

	raw, _ := json.Marshal(v, jsonx.V1())

	return string(raw)
}
