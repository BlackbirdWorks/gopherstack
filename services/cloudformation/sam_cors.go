package cloudformation

import (
	"maps"
	"slices"
	"strings"
)

const samCorsHeaderPrefix = "method.response.header.Access-Control-"

// samCorsSettings normalises Api Cors (an origin string or CorsConfiguration map).
func samCorsSettings(id string, cors any) (map[string]any, error) {
	switch c := cors.(type) {
	case string:
		return map[string]any{"AllowOrigin": c}, nil
	case map[string]any:
		if isSAMIntrinsic(c) {
			return map[string]any{"AllowOrigin": c}, nil
		}
		if err := rejectUnknown(id, c, keySet("AllowOrigin", "AllowMethods", "AllowHeaders", "MaxAge",
			samKeyAllowCreds)); err != nil {
			return nil, err
		}

		out := maps.Clone(c)
		if _, has := out["AllowOrigin"]; !has {
			out["AllowOrigin"] = "'*'"
		} else if out["AllowOrigin"] == nil || out["AllowOrigin"] == "" {
			return nil, samErr(id, "Invalid input. Value for AllowedOrigins is required")
		}
		if out[samKeyAllowCreds] == true && out["AllowOrigin"] == "'*'" {
			return nil, samErr(id, "Unable to add Cors configuration because 'AllowCredentials' can not be true when "+
				"'AllowOrigin' is \"'*'\" or not set")
		}

		return out, nil
	default:
		return nil, samErr(id, "Cors must be an origin string or a CorsConfiguration object.")
	}
}

// addCorsPreflight adds an OPTIONS mock integration to every path lacking one.
func addCorsPreflight(id string, paths map[string]any, cors any) error {
	cfg, err := samCorsSettings(id, cors)
	if err != nil {
		return err
	}
	for _, path := range slices.Sorted(maps.Keys(paths)) {
		contents, ok := conditionalContents(paths[path])
		if !ok {
			return samErr(id, "Value of Fn::If must be a list.")
		}
		for _, c := range contents {
			item := asMap(c)
			if item == nil {
				return samErr(id, "Value of '%s' path must be a dictionary according to Swagger spec.", path)
			}
			if opts, exists := item[samKOptions]; exists {
				if optErr := validateOptionsResponses(path, opts); optErr != nil {
					return optErr
				}

				continue
			}
			item[samKOptions] = corsOptionsOperation(cfg, item)
		}
	}

	return nil
}

func corsOptionsOperation(cfg, item map[string]any) map[string]any {
	params := map[string]any{samCorsHeaderPrefix + "Allow-Origin": cfg["AllowOrigin"]}
	methods := cfg["AllowMethods"]
	if methods == nil {
		methods = "'" + strings.Join(corsMethodsFor(item), ",") + "'"
	}
	params[samCorsHeaderPrefix+"Allow-Methods"] = methods
	if v := cfg["AllowHeaders"]; v != nil {
		params[samCorsHeaderPrefix+"Allow-Headers"] = v
	}
	if v := cfg["MaxAge"]; v != nil {
		params[samCorsHeaderPrefix+"Max-Age"] = v
	}
	if cfg[samKeyAllowCreds] == true {
		params[samCorsHeaderPrefix+"Allow-Credentials"] = "'true'"
	}
	headers := map[string]any{}
	for k := range params {
		typ := "string"
		if strings.HasSuffix(k, "Max-Age") {
			typ = "integer"
		}
		headers[strings.TrimPrefix(k, "method.response.header.")] = map[string]any{samKeyTypeLower: typ}
	}

	return map[string]any{
		"summary":  "CORS support",
		"consumes": []any{samJSONMime},
		"produces": []any{samJSONMime},
		samKeyResponses: map[string]any{"200": map[string]any{
			samKeyDescLower: "Default response for CORS method", "headers": headers,
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
	allHTTP := []string{samKOptions, "get", "head", "post", "put", "delete", "patch"}
	verbs := []string{}
	if _, isAny := item[samAnyMethodKey]; isAny {
		verbs = allHTTP
	} else {
		for _, v := range allHTTP {
			if _, ok := item[v]; ok {
				verbs = append(verbs, v)
			}
		}
		verbs = append(verbs, samKOptions)
	}
	up := make([]string, 0, len(verbs))
	for _, v := range verbs {
		if u := strings.ToUpper(v); !slices.Contains(up, u) {
			up = append(up, u)
		}
	}
	slices.Sort(up)

	return up
}
