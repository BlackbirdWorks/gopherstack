package appsync

import (
	"encoding/json"
	"strings"
)

// renderVTL renders tmpl with the given arguments and result as the context.
func renderVTL(tmpl string, args map[string]any, result any) (string, error) {
	if tmpl == "" {
		if result != nil {
			b, err := json.Marshal(result)

			return string(b), err
		}

		return "{}", nil
	}

	cc := &callCtx{}
	out, err := runVTL(tmpl, cc.ctxMap(args, nil, result, nil, true), cc.utilEnv())
	if err != nil {
		return "", err
	}

	if out.Returned {
		return returnedString(out.Value), nil
	}

	return out.Output, nil
}

func returnedString(v any) string {
	if v == nil {
		return ""
	}

	if s, ok := v.(string); ok {
		return s
	}

	b, _ := json.Marshal(v)

	return string(b)
}

// vtlOutputValue decodes a rendered template: JSON when it parses, nil for
// blank output, the raw string otherwise.
func vtlOutputValue(out vtlOutcome) any {
	if out.Returned {
		return out.Value
	}

	if strings.TrimSpace(out.Output) == "" {
		return nil
	}

	return unmarshalOrRaw(out.Output)
}

func toDynamoDBJSON(val any) string {
	b, _ := json.Marshal(ddbTyped(val))

	return string(b)
}
