package appsync

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

func ddbTyped(v any) any {
	switch t := v.(type) {
	case nil:
		return map[string]any{ddbTagNull: true}
	case string:
		return map[string]any{"S": t}
	case bool:
		return map[string]any{ddbTagBool: t}
	case []any:
		return map[string]any{"L": ddbTypedList(t)}
	case *vlist:
		return map[string]any{"L": ddbTypedList(t.items)}
	case map[string]any:
		return map[string]any{"M": ddbTypedMap(t)}
	}

	if _, _, ok := toNum(v); ok {
		return map[string]any{"N": fmtNum(v)}
	}

	return map[string]any{"S": vrender(v)}
}

func ddbTypedList(in []any) []any {
	out := make([]any, len(in))
	for i, x := range in {
		out[i] = ddbTyped(x)
	}

	return out
}

func ddbTypedMap(in map[string]any) map[string]any {
	out := make(map[string]any, len(in))
	for k, x := range in {
		out[k] = ddbTyped(x)
	}

	return out
}

const betweenBounds = 2

func listOf(v any) []any {
	switch t := v.(type) {
	case []any:
		return t
	case *vlist:
		return t.items
	}

	return nil
}

func ddbSet(tag string, numbers bool) utilFn {
	return func(_ *utilEnv, a []any) (any, error) {
		items := listOf(argAt(a, 0))
		out := make([]any, len(items))

		for i, it := range items {
			if numbers {
				out[i] = fmtNum(it)
			} else {
				out[i] = vrender(it)
			}
		}

		return map[string]any{tag: out}, nil
	}
}

func ddbScalars() map[string]utilFn {
	return map[string]utilFn{
		"toDynamoDB": func(_ *utilEnv, a []any) (any, error) { return ddbTyped(argAt(a, 0)), nil },
		mToString: func(_ *utilEnv, a []any) (any, error) {
			return map[string]any{"S": vrender(argAt(a, 0))}, nil
		},
		"toNumber":  func(_ *utilEnv, a []any) (any, error) { return map[string]any{"N": fmtNum(argAt(a, 0))}, nil },
		"toBinary":  func(_ *utilEnv, a []any) (any, error) { return map[string]any{"B": vrender(argAt(a, 0))}, nil },
		"toBoolean": func(_ *utilEnv, a []any) (any, error) { return map[string]any{ddbTagBool: vtruthy(argAt(a, 0))}, nil },
		"toNull":    func(_ *utilEnv, _ []any) (any, error) { return map[string]any{ddbTagNull: true}, nil },
		"toList": func(_ *utilEnv, a []any) (any, error) {
			return map[string]any{"L": ddbTypedList(listOf(argAt(a, 0)))}, nil
		},
		"toMap": func(_ *utilEnv, a []any) (any, error) {
			m, _ := argAt(a, 0).(map[string]any)

			return map[string]any{"M": ddbTypedMap(m)}, nil
		},
		"toMapValues": func(_ *utilEnv, a []any) (any, error) {
			m, _ := argAt(a, 0).(map[string]any)

			return ddbTypedMap(m), nil
		},
		"toStringSet": ddbSet("SS", false),
		"toNumberSet": ddbSet("NS", true),
		"toBinarySet": ddbSet("BS", false),
	}
}

func utilDynamo() map[string]utilFn {
	out := map[string]utilFn{}

	for name, fn := range ddbScalars() {
		out["dynamodb."+name] = fn
		out["dynamodb."+name+"Json"] = func(u *utilEnv, a []any) (any, error) {
			v, err := fn(u, a)
			if err != nil {
				return nil, err
			}

			b, err := json.Marshal(v)

			return string(b), err
		}
	}

	return out
}

func utilTransform() map[string]utilFn {
	return map[string]utilFn{
		"transform.toDynamoDBFilterExpression":    exprFn,
		"transform.toDynamoDBConditionExpression": exprFn,
	}
}

func exprFn(_ *utilEnv, a []any) (any, error) {
	f, _ := argAt(a, 0).(map[string]any)
	b := &exprBuilder{names: map[string]any{}, values: map[string]any{}}

	expr, err := b.filter(f)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"expression": expr, "expressionNames": b.names, "expressionValues": b.values,
	}, nil
}

type exprBuilder struct {
	names  map[string]any
	values map[string]any
}

func (b *exprBuilder) name(field string) string {
	key := "#" + sanitizeAttr(field)
	b.names[key] = field

	return key
}

func (b *exprBuilder) value(field, op string, v any) string {
	base := ":" + sanitizeAttr(field) + "_" + op
	key := base

	for i := 1; ; i++ {
		if _, dup := b.values[key]; !dup {
			break
		}

		key = fmt.Sprintf("%s%d", base, i)
	}

	b.values[key] = ddbTyped(v)

	return key
}

func sanitizeAttr(s string) string {
	var sb strings.Builder

	for _, r := range s {
		if r == '_' || (r >= '0' && r <= '9') || (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') {
			sb.WriteRune(r)
		} else {
			sb.WriteByte('_')
		}
	}

	return sb.String()
}

func (b *exprBuilder) filter(f map[string]any) (string, error) {
	var parts []string

	keys := make([]string, 0, len(f))
	for k := range f {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	for _, k := range keys {
		var (
			p   string
			err error
		)

		switch k {
		case "and", "or":
			p, err = b.group(k, listOf(f[k]))
		case "not":
			sub, _ := f[k].(map[string]any)
			if p, err = b.filter(sub); err == nil {
				p = "(NOT " + p + ")"
			}
		default:
			ops, _ := f[k].(map[string]any)
			p, err = b.field(k, ops)
		}

		if err != nil {
			return "", err
		}

		parts = append(parts, p)
	}

	if len(parts) == 1 {
		return parts[0], nil
	}

	return "(" + strings.Join(parts, " AND ") + ")", nil
}

func (b *exprBuilder) group(kind string, subs []any) (string, error) {
	parts := make([]string, 0, len(subs))

	for _, s := range subs {
		m, _ := s.(map[string]any)

		p, err := b.filter(m)
		if err != nil {
			return "", err
		}

		parts = append(parts, p)
	}

	return "(" + strings.Join(parts, " "+strings.ToUpper(kind)+" ") + ")", nil
}

func (b *exprBuilder) field(field string, ops map[string]any) (string, error) {
	var parts []string

	opNames := make([]string, 0, len(ops))
	for o := range ops {
		opNames = append(opNames, o)
	}

	sort.Strings(opNames)

	for _, op := range opNames {
		p, err := b.op(field, op, ops[op])
		if err != nil {
			return "", err
		}

		parts = append(parts, p)
	}

	return "(" + strings.Join(parts, " AND ") + ")", nil
}

func (b *exprBuilder) op(field, op string, v any) (string, error) {
	n := b.name(field)

	if sym, ok := map[string]string{"eq": "=", "ne": "<>", "lt": "<", "le": "<=", "gt": ">", "ge": ">="}[op]; ok {
		return n + " " + sym + " " + b.value(field, op, v), nil
	}

	switch op {
	case mContains, "beginsWith":
		fn := map[string]string{mContains: mContains, "beginsWith": "begins_with"}[op]

		return fn + "(" + n + ", " + b.value(field, op, v) + ")", nil
	case "notContains":
		return "NOT contains(" + n + ", " + b.value(field, op, v) + ")", nil
	case "between":
		bounds := listOf(v)
		if len(bounds) != betweenBounds {
			return "", fmt.Errorf("%w: between requires two values", ErrValidation)
		}

		return n + " BETWEEN " + b.value(field, op+"0", bounds[0]) + " AND " + b.value(field, op+"1", bounds[1]), nil
	case "attributeExists":
		if vtruthy(v) {
			return "attribute_exists(" + n + ")", nil
		}

		return "attribute_not_exists(" + n + ")", nil
	case "attributeType":
		return "attribute_type(" + n + ", " + b.value(field, op, v) + ")", nil
	}

	return "", fmt.Errorf("%w: unsupported condition operator %q", ErrValidation, op)
}
