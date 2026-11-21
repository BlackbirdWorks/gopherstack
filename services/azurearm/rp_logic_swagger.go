package azurearm

import (
	"net/http"
	"net/url"
	"strings"
)

const (
	logicSwaggerVersion = "2.0"
	logicKSchema        = "schema"
)

// swaggerDocument builds a Swagger 2.0 description of the workflow's Request triggers: one operation per trigger
// (method, relativePath, JSON body schema) on the emulator's invoke path, with the callback signature parameters.
func (w *logicWorkflow) swaggerDocument(base string) map[string]any {
	host, scheme := "", logicSchemeHTTPS

	if u, err := url.Parse(base); err == nil && u.Host != "" {
		host, scheme = u.Host, u.Scheme
	}

	paths := map[string]any{}

	for _, name := range sortedTriggerNames(w) {
		typ := w.triggerType(name)
		if typ != logicTrigRequest && typ != logicTrigManual {
			continue
		}

		path, method, op := w.swaggerOperation(name)

		item, _ := paths[path].(map[string]any)
		if item == nil {
			item = map[string]any{}
			paths[path] = item
		}

		item[method] = op
	}

	return map[string]any{
		"swagger":  logicSwaggerVersion,
		"info":     map[string]any{"title": w.Name, logicKVersion: w.Version},
		"host":     host,
		"basePath": strings.TrimSuffix(logicDataPrefix, "/") + "/" + w.FlowID,
		"schemes":  []any{scheme},
		"paths":    paths,
	}
}

func (w *logicWorkflow) swaggerOperation(trigger string) (string, string, map[string]any) {
	in, _ := objectOf(w.triggerDef(trigger)["inputs"])

	method := strings.ToLower(stringOf(in["method"]))
	if method == "" {
		method = strings.ToLower(http.MethodPost)
	}

	path := "/triggers/" + trigger + "/paths/invoke"
	params := []any{}

	if rel := stringOf(in["relativePath"]); rel != "" {
		path += "/" + strings.TrimPrefix(rel, "/")

		for _, p := range relativePathParams(rel) {
			params = append(params, map[string]any{
				fieldName: p, "in": "path", "required": true, fieldType: "string",
			})
		}
	}

	for _, q := range []string{logicKAPIVersion, "sp", "sv", logicKSig} {
		params = append(params, map[string]any{fieldName: q, "in": "query", "required": true, fieldType: "string"})
	}

	if schema, ok := objectOf(in[logicKSchema]); ok && method != "get" && method != "delete" {
		params = append(params, map[string]any{fieldName: logicKBody, "in": logicKBody, "schema": schema})
	}

	return path, method, map[string]any{
		"operationId": trigger,
		"parameters":  params,
		"responses": map[string]any{
			"default": map[string]any{"description": "The response of the workflow run."},
		},
	}
}

func (h *Handler) logicListSwagger(q logicReq) error {
	out, err := q.lp.read(q.ref, func(w *logicWorkflow) (any, error) { return w.swaggerDocument(q.base), nil })

	return h.logicReply(q, out, err)
}
