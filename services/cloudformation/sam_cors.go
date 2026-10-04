package cloudformation

import (
	"maps"
	"slices"
	"strings"
)

const samCorsHeaderPrefix = "method.response.header.Access-Control-"

// samCorsValue single-quotes a static CORS value as API Gateway mapping expressions require.
func samCorsValue(v any) any {
	s, ok := v.(string)
	if !ok || (strings.HasPrefix(s, "'") && strings.HasSuffix(s, "'")) {
		return v
	}

	return "'" + s + "'"
}

// samCorsSettings normalises Api Cors (an origin string or CorsConfiguration map).
func samCorsSettings(id string, cors any) (map[string]any, error) {
	switch c := cors.(type) {
	case string:
		return map[string]any{"AllowOrigin": c}, nil
	case map[string]any:
		if c["AllowOrigin"] == nil {
			return nil, samErr(id, "Cors requires 'AllowOrigin'.")
		}
		if err := rejectUnknown(id, c, keySet("AllowOrigin", "AllowMethods", "AllowHeaders", "MaxAge",
			"AllowCredentials")); err != nil {
			return nil, err
		}

		return c, nil
	default:
		return nil, samErr(id, "Cors must be an origin string or a CorsConfiguration object.")
	}
}

// addCorsPreflight adds an OPTIONS mock integration to every path lacking one.
func addCorsPreflight(id string, paths map[string]any, cors any, authorizer string) error {
	cfg, err := samCorsSettings(id, cors)
	if err != nil {
		return err
	}
	for _, path := range slices.Sorted(maps.Keys(paths)) {
		item := asMap(paths[path])
		if _, exists := item["options"]; exists {
			continue
		}
		op := corsOptionsOperation(cfg, item)
		if authorizer != "" {
			op["security"] = security(authorizer)
		}
		item["options"] = op
	}

	return nil
}

func corsOptionsOperation(cfg, item map[string]any) map[string]any {
	params := map[string]any{samCorsHeaderPrefix + "Allow-Origin": samCorsValue(cfg["AllowOrigin"])}
	methods := cfg["AllowMethods"]
	if methods == nil {
		methods = "'" + strings.Join(corsMethodsFor(item), ",") + "'"
	}
	params[samCorsHeaderPrefix+"Allow-Methods"] = samCorsValue(methods)
	if v := cfg["AllowHeaders"]; v != nil {
		params[samCorsHeaderPrefix+"Allow-Headers"] = samCorsValue(v)
	}
	if v := cfg["MaxAge"]; v != nil {
		params[samCorsHeaderPrefix+"Max-Age"] = samCorsValue(v)
	}
	if cfg["AllowCredentials"] == true {
		params[samCorsHeaderPrefix+"Allow-Credentials"] = "'true'"
	}
	headers := map[string]any{}
	for k := range params {
		headers[strings.TrimPrefix(k, "method.response.header.")] = map[string]any{samKeyTypeLower: "string"}
	}

	return map[string]any{
		"summary":  "CORS support",
		"consumes": []any{samJSONMime},
		samKeyResponses: map[string]any{"200": map[string]any{
			"description": "Default response for CORS method", "headers": headers,
		}},
		"x-amazon-apigateway-integration": map[string]any{
			samKeyTypeLower:    "mock",
			"requestTemplates": map[string]any{samJSONMime: "{\n  \"statusCode\" : 200\n}\n"},
			samKeyResponses: map[string]any{defaultEventBusName: map[string]any{
				"statusCode":         "200",
				"responseParameters": params,
				"responseTemplates":  map[string]any{samJSONMime: "{}\n"},
			}},
		},
	}
}

func corsMethodsFor(item map[string]any) []string {
	verbs := []string{"delete", "get", "head", "patch", "post", "put"}
	if _, isAny := item[samAnyMethodKey]; !isAny {
		verbs = slices.DeleteFunc(verbs, func(v string) bool {
			_, ok := item[v]

			return !ok
		})
	}
	up := make([]string, 0, len(verbs))
	for _, v := range verbs {
		up = append(up, strings.ToUpper(v))
	}

	return slices.Concat(up, []string{"OPTIONS"})
}
