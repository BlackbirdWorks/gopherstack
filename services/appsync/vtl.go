package appsync

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
)

// renderVTL evaluates a basic subset of AppSync Velocity Template Language (VTL).
// Supported patterns:
//   - $context.arguments.key / $ctx.args.key → argument values
//   - $context.result / $ctx.result / $context.result.key → resolver result
//   - $context.prev.result / $ctx.prev.result / $context.prev.result.key → the
//     previous pipeline function's result (PIPELINE resolvers only; see
//     renderVTLFull)
//   - $context.source / $ctx.source[.key] → the parent object of a nested field
//   - $util.toJson(expr) → JSON-encode the expression
//   - #return(expr) → shorthand return with expression
//   - $util.dynamodb.toDynamoDBJson($ctx.args.key) → wrap in DynamoDB format
//   - Literal strings inside quotes are preserved
//
// Unsupported constructs are passed through unchanged.
func renderVTL(tmpl string, args map[string]any, result any) (string, error) {
	return renderVTLFull(tmpl, vtlContext{args: args, result: result})
}

// renderVTLFull renders tmpl against vc: prev is the previous pipeline
// function's result ($ctx.prev.result) and source the parent object of a
// nested field resolver ($ctx.source).
func renderVTLFull(tmpl string, vc vtlContext) (string, error) {
	if tmpl == "" {
		if vc.result != nil {
			b, err := json.Marshal(vc.result)
			if err != nil {
				return "{}", err
			}

			return string(b), nil
		}

		return "{}", nil
	}

	return vc.render(tmpl)
}

type vtlContext struct {
	args   map[string]any
	result any
	prev   any
	source any
}

const (
	vtlNull     = "null"
	minSubmatch = 2
)

var (
	// reUtilToJSON matches $util.toJson(<expr>) calls.
	reUtilToJSON = regexp.MustCompile(`\$util\.toJson\(([^)]+)\)`)
	// reDynamoDBJSON matches $util.dynamodb.toDynamoDBJson(<expr>) calls.
	reDynamoDBJSON = regexp.MustCompile(`\$util\.dynamodb\.toDynamoDBJson\(([^)]+)\)`)
	// reReturn matches #return(<expr>) or #return.
	reReturn = regexp.MustCompile(`(?m)^\s*#return\(([^)]*)\)\s*$`)
	// reContextArgsDot matches $context.arguments.key or $ctx.args.key.
	reContextArgsDot = regexp.MustCompile(`\$(?:context\.arguments|ctx\.args)\.(\w+)`)
	// reContextArgs matches $context.arguments or $ctx.args (bare).
	reContextArgs = regexp.MustCompile(`\$(?:context\.arguments|ctx\.args)\b`)
	// reContextResultDot matches $context.result.key or $ctx.result.key.
	reContextResultDot = regexp.MustCompile(`\$(?:context\.result|ctx\.result)\.(\w+)`)
	// reContextResult matches $context.result or $ctx.result (bare).
	reContextResult = regexp.MustCompile(`\$(?:context\.result|ctx\.result)\b`)
	// reContextPrevResultDot matches $context.prev.result.key or $ctx.prev.result.key.
	reContextPrevResultDot = regexp.MustCompile(`\$(?:context\.prev\.result|ctx\.prev\.result)\.(\w+)`)
	// reContextPrevResult matches $context.prev.result or $ctx.prev.result (bare).
	reContextPrevResult = regexp.MustCompile(`\$(?:context\.prev\.result|ctx\.prev\.result)\b`)
	// reContextResultPath matches $context.result.a.b or $ctx.result.a.b.
	reContextResultPath = regexp.MustCompile(`\$(?:context|ctx)\.result((?:\.\w+)+)`)
	// reContextSourcePath matches $context.source[.a.b] or $ctx.source[.a.b].
	reContextSourcePath = regexp.MustCompile(`\$(?:context|ctx)\.source((?:\.\w+)*)`)
)

func (vc *vtlContext) render(tmpl string) (string, error) {
	tmpl = vc.expandReturn(tmpl)
	tmpl = vc.expandUtilToJSON(tmpl)
	tmpl = vc.expandDynamoDBJSON(tmpl)
	tmpl = vc.expandContextSource(tmpl)
	tmpl = vc.expandContextPrevResultDot(tmpl)
	tmpl = vc.expandContextPrevResult(tmpl)
	tmpl = vc.expandContextResultDot(tmpl)
	tmpl = vc.expandContextResult(tmpl)
	tmpl = vc.expandContextArgsDot(tmpl)
	tmpl = vc.expandContextArgs(tmpl)

	return strings.TrimSpace(tmpl), nil
}

// expandContextSource expands $ctx.source and $ctx.source.a.b paths.
func (vc *vtlContext) expandContextSource(tmpl string) string {
	return reContextSourcePath.ReplaceAllStringFunc(tmpl, func(match string) string {
		sub := reContextSourcePath.FindStringSubmatch(match)
		v := vc.sourcePath(sub[1])

		if sub[1] == "" {
			b, err := json.Marshal(v)
			if err != nil {
				return vtlNull
			}

			return string(b)
		}

		switch t := v.(type) {
		case nil:
			return vtlNull
		case map[string]any, []any:
			b, _ := json.Marshal(t)

			return string(b)
		default:
			return fmt.Sprintf("%v", t)
		}
	})
}

func isPathExpr(re *regexp.Regexp, expr string) bool {
	return re.FindString(expr) == expr
}

// pathExpr evaluates $ctx.result.a.b and $ctx.source[.a.b] expressions.
func (vc *vtlContext) pathExpr(expr string) any {
	if sub := reContextResultPath.FindStringSubmatch(expr); sub != nil && sub[0] == expr {
		return walkPath(vc.result, sub[1])
	}

	return vc.sourcePath(reContextSourcePath.FindStringSubmatch(expr)[1])
}

// sourcePath walks a ".a.b" path below $ctx.source.
func (vc *vtlContext) sourcePath(path string) any {
	return walkPath(vc.source, path)
}

// walkPath walks a ".a.b" path below root; a missing step yields nil.
func walkPath(root any, path string) any {
	cur := root

	for seg := range strings.SplitSeq(strings.TrimPrefix(path, "."), ".") {
		if seg == "" {
			continue
		}

		m, ok := cur.(map[string]any)
		if !ok {
			return nil
		}

		cur = m[seg]
	}

	return cur
}

// expandContextPrevResultDot expands $context.prev.result.key / $ctx.prev.result.key.
func (vc *vtlContext) expandContextPrevResultDot(tmpl string) string {
	return reContextPrevResultDot.ReplaceAllStringFunc(tmpl, func(match string) string {
		sub := reContextPrevResultDot.FindStringSubmatch(match)
		if len(sub) < minSubmatch {
			return match
		}

		if m, ok := vc.prev.(map[string]any); ok {
			if v, exists := m[sub[1]]; exists {
				return fmt.Sprintf("%v", v)
			}
		}

		return vtlNull
	})
}

// expandContextPrevResult expands bare $context.prev.result / $ctx.prev.result.
func (vc *vtlContext) expandContextPrevResult(tmpl string) string {
	return reContextPrevResult.ReplaceAllStringFunc(tmpl, func(_ string) string {
		if vc.prev == nil {
			return vtlNull
		}

		b, err := json.Marshal(vc.prev)
		if err != nil {
			return vtlNull
		}

		return string(b)
	})
}

// expandReturn handles #return(expr) lines — replaces with just the expression.
func (vc *vtlContext) expandReturn(tmpl string) string {
	return reReturn.ReplaceAllStringFunc(tmpl, func(match string) string {
		sub := reReturn.FindStringSubmatch(match)
		if len(sub) > 1 {
			return strings.TrimSpace(sub[1])
		}

		return match
	})
}

// expandUtilToJSON expands $util.toJson(...) calls.
func (vc *vtlContext) expandUtilToJSON(tmpl string) string {
	return reUtilToJSON.ReplaceAllStringFunc(tmpl, func(match string) string {
		sub := reUtilToJSON.FindStringSubmatch(match)
		if len(sub) < minSubmatch {
			return match
		}

		val := vc.resolveExpr(strings.TrimSpace(sub[1]))

		b, err := json.Marshal(val)
		if err != nil {
			return vtlNull
		}

		return string(b)
	})
}

// expandDynamoDBJSON expands $util.dynamodb.toDynamoDBJson(...) calls.
func (vc *vtlContext) expandDynamoDBJSON(tmpl string) string {
	return reDynamoDBJSON.ReplaceAllStringFunc(tmpl, func(match string) string {
		sub := reDynamoDBJSON.FindStringSubmatch(match)
		if len(sub) < minSubmatch {
			return match
		}

		return toDynamoDBJSON(vc.resolveExpr(strings.TrimSpace(sub[1])))
	})
}

// expandContextResultDot expands $context.result.key / $ctx.result.key.
func (vc *vtlContext) expandContextResultDot(tmpl string) string {
	return reContextResultDot.ReplaceAllStringFunc(tmpl, func(match string) string {
		sub := reContextResultDot.FindStringSubmatch(match)
		if len(sub) < minSubmatch {
			return match
		}

		if m, ok := vc.result.(map[string]any); ok {
			if v, exists := m[sub[1]]; exists {
				return fmt.Sprintf("%v", v)
			}
		}

		return vtlNull
	})
}

// expandContextResult expands $context.result / $ctx.result (bare).
func (vc *vtlContext) expandContextResult(tmpl string) string {
	return reContextResult.ReplaceAllStringFunc(tmpl, func(_ string) string {
		if vc.result == nil {
			return vtlNull
		}

		b, err := json.Marshal(vc.result)
		if err != nil {
			return vtlNull
		}

		return string(b)
	})
}

// expandContextArgsDot expands $context.arguments.key / $ctx.args.key.
func (vc *vtlContext) expandContextArgsDot(tmpl string) string {
	return reContextArgsDot.ReplaceAllStringFunc(tmpl, func(match string) string {
		sub := reContextArgsDot.FindStringSubmatch(match)
		if len(sub) < minSubmatch {
			return match
		}

		if v, ok := vc.args[sub[1]]; ok {
			return fmt.Sprintf("%v", v)
		}

		return vtlNull
	})
}

// expandContextArgs expands bare $context.arguments / $ctx.args.
func (vc *vtlContext) expandContextArgs(tmpl string) string {
	return reContextArgs.ReplaceAllStringFunc(tmpl, func(_ string) string {
		if vc.args == nil {
			return "{}"
		}

		b, err := json.Marshal(vc.args)
		if err != nil {
			return "{}"
		}

		return string(b)
	})
}

// resolveExpr evaluates a VTL expression against context.
func (vc *vtlContext) resolveExpr(expr string) any {
	expr = strings.TrimSpace(expr)

	switch {
	case expr == "$context.result" || expr == "$ctx.result":
		return vc.result

	case expr == "$context.prev.result" || expr == "$ctx.prev.result":
		return vc.prev

	case isPathExpr(reContextResultPath, expr) || isPathExpr(reContextSourcePath, expr):
		return vc.pathExpr(expr)

	case strings.HasPrefix(expr, "$context.arguments.") || strings.HasPrefix(expr, "$ctx.args."):
		var key string
		if after, ok := strings.CutPrefix(expr, "$context.arguments."); ok {
			key = after
		} else {
			key = strings.TrimPrefix(expr, "$ctx.args.")
		}

		if vc.args != nil {
			return vc.args[key]
		}

		return nil

	case expr == "$context.arguments" || expr == "$ctx.args":
		return vc.args

	default:
		return expr
	}
}

// toDynamoDBJSON wraps a value in the DynamoDB JSON format (e.g., {"S": "value"}).
func toDynamoDBJSON(val any) string {
	switch v := val.(type) {
	case string:
		b, _ := json.Marshal(map[string]any{"S": v})

		return string(b)
	case float64, float32, int, int64:
		b, _ := json.Marshal(map[string]any{"N": fmt.Sprintf("%v", v)})

		return string(b)
	case bool:
		b, _ := json.Marshal(map[string]any{"BOOL": v})

		return string(b)
	case nil:
		return `{"NULL":true}`
	default:
		b, _ := json.Marshal(val)

		return string(b)
	}
}
