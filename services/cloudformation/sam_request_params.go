package cloudformation

import (
	"maps"
	"slices"
	"strings"
)

const (
	samIntegrationKey = "x-amazon-apigateway-integration"
	samSwaggerString  = "string"
	samLocHeader      = "header"
	samKeyNameLower   = "name"
	samTimeoutMin     = 50
	samTimeoutMax     = 29000
)

// samReqParam is one Api event RequestParameters entry.
type samReqParam struct {
	in, name, full string
	required       bool
	caching        bool
}

func samParamLocations() map[string]string {
	return map[string]string{"header": samLocHeader, "querystring": "query", "path": "path"}
}

// parseRequestParameters reads a list of "method.request.<loc>.<name>" strings
// and/or single-key maps {<param>: {Required, Caching}}.
func parseRequestParameters(fnID, event string, v any) ([]samReqParam, error) {
	if v == nil {
		return nil, nil
	}
	list, ok := v.([]any)
	if !ok {
		return nil, samErr(fnID, "Event [%s] RequestParameters must be a list.", event)
	}
	out := make([]samReqParam, 0, len(list))
	for _, item := range list {
		var full string
		var opts map[string]any
		switch it := item.(type) {
		case string:
			full = it
		case map[string]any:
			if len(it) != 1 {
				return nil, samErr(fnID, "Event [%s] RequestParameters entries must have exactly one parameter.", event)
			}
			full = slices.Sorted(maps.Keys(it))[0]
			opts = asMap(it[full])
			if err := rejectUnknown(fnID+event, opts, keySet("Required", "Caching")); err != nil {
				return nil, err
			}
		default:
			return nil, samErr(fnID, "Event [%s] RequestParameters entries must be strings or maps.", event)
		}
		p, err := newReqParam(fnID, event, full)
		if err != nil {
			return nil, err
		}
		p.required = opts["Required"] == true
		p.caching = opts["Caching"] == true
		out = append(out, p)
	}

	return out, nil
}

func newReqParam(fnID, event, full string) (samReqParam, error) {
	rest, ok := strings.CutPrefix(full, "method.request.")
	loc, name, found := strings.Cut(rest, ".")
	in := samParamLocations()[loc]
	if !ok || !found || in == "" || name == "" {
		return samReqParam{}, samErr(fnID,
			"Event [%s] RequestParameters entry '%s' must start with method.request.header, "+
				"method.request.querystring or method.request.path.", event, full)
	}

	return samReqParam{in: in, name: name, full: full}, nil
}

func parseAPITimeout(fnID, event string, v any) (any, error) {
	if v == nil {
		return nil, nil //nolint:nilnil // absent timeout is valid
	}
	var n float64
	switch t := v.(type) {
	case int:
		n = float64(t)
	case int64:
		n = float64(t)
	case float64:
		n = t
	default:
		return v, nil
	}
	if n < samTimeoutMin || n > samTimeoutMax {
		return nil, samErr(fnID, "Event [%s] TimeoutInMillis must be between %d and %d milliseconds.",
			event, samTimeoutMin, samTimeoutMax)
	}

	return v, nil
}
