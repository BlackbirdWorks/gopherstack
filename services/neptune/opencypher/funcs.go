package opencypher

import (
	"math"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	aggMin = "min"
	aggMax = "max"
)

func isAggregate(name string) bool {
	switch name {
	case "count", "collect", "sum", "avg", aggMin, aggMax:
		return true
	default:
		return false
	}
}

func containsAgg(e expr) bool {
	switch t := e.(type) {
	case callExpr:
		if isAggregate(t.name) {
			return true
		}

		return anyAgg(t.args...)
	case propExpr:
		return containsAgg(t.e)
	case indexExpr:
		return anyAgg(t.e, t.i)
	case sliceExpr:
		return anyAgg(t.e, t.from, t.to)
	case unaryExpr:
		return containsAgg(t.e)
	case binaryExpr:
		return anyAgg(t.l, t.r)
	case isNullExpr:
		return containsAgg(t.e)
	case listExpr:
		return anyAgg(t.items...)
	case mapExpr:
		return anyAgg(t.vals...)
	case caseExpr:
		return anyAgg(append(append([]expr{t.subject, t.els}, t.whens...), t.thens...)...)
	default:
		return false
	}
}

func anyAgg(es ...expr) bool {
	for _, e := range es {
		if e != nil && containsAgg(e) {
			return true
		}
	}

	return false
}

func stringify(v any) string {
	switch t := v.(type) {
	case nil:
		return ""
	case string:
		return t
	case int64:
		return strconv.FormatInt(t, 10)
	case float64:
		return strconv.FormatFloat(t, 'g', -1, 64)
	case bool:
		return strconv.FormatBool(t)
	default:
		return valueKey(v)
	}
}

func (x *exec) evalCall(c callExpr, r row, grp []row) (any, error) {
	if isAggregate(c.name) {
		if grp == nil {
			return nil, malformed("Invalid use of aggregating function %s(...) in this context", c.name)
		}

		return x.aggregate(c, grp)
	}

	args := make([]any, len(c.args))

	for i, a := range c.args {
		v, err := x.eval(a, r, grp)
		if err != nil {
			return nil, err
		}

		args[i] = v
	}

	if c.name == "startnode" || c.name == "endnode" {
		return x.relEndpoint(c.name, args)
	}

	fn, ok := x.g.funcs[c.name]
	if !ok {
		return nil, &Error{Code: CodeMalformedQuery, Detail: "Unknown function '" + c.name + "'"}
	}

	return fn(args)
}

func (x *exec) aggregate(c callExpr, grp []row) (any, error) {
	if c.star {
		return int64(len(grp)), nil
	}

	if len(c.args) != 1 {
		return nil, malformed("%s() takes exactly one argument", c.name)
	}

	var vals []any

	seen := map[string]bool{}

	for _, gr := range grp {
		v, err := x.eval(c.args[0], gr, nil)
		if err != nil {
			return nil, err
		}

		if v == nil {
			continue
		}

		if c.distinct {
			k := valueKey(v)
			if seen[k] {
				continue
			}

			seen[k] = true
		}

		vals = append(vals, v)
	}

	return foldAggregate(c.name, vals)
}

func foldAggregate(name string, vals []any) (any, error) {
	switch name {
	case "count":
		return int64(len(vals)), nil
	case "collect":
		if vals == nil {
			return []any{}, nil
		}

		return vals, nil
	case aggMin, aggMax:
		if len(vals) == 0 {
			return nil, nil //nolint:nilnil // aggregate over nothing
		}

		best := vals[0]

		for _, v := range vals[1:] {
			if c := order(v, best); (name == aggMin && c < 0) || (name == aggMax && c > 0) {
				best = v
			}
		}

		return best, nil
	}

	return foldNumeric(name, vals)
}

func foldNumeric(name string, vals []any) (any, error) {
	if len(vals) == 0 {
		if name == "sum" {
			return int64(0), nil
		}

		return nil, nil //nolint:nilnil // avg over nothing
	}

	allInt, isum, fsum := true, int64(0), 0.0

	for _, v := range vals {
		switch n := v.(type) {
		case int64:
			isum += n
			fsum += float64(n)
		case float64:
			allInt = false
			fsum += n
		default:
			return nil, malformed("Type mismatch: expected Number but was %s", typeName(v))
		}
	}

	switch {
	case name == "avg":
		return fsum / float64(len(vals)), nil
	case allInt:
		return isum, nil
	default:
		return fsum, nil
	}
}

type scalarFunc func(args []any) (any, error)

func builtinFuncs() map[string]scalarFunc {
	return map[string]scalarFunc{
		"id": funcID, "labels": funcLabels, "type": funcType, "properties": funcProperties, "keys": funcKeys,
		"size": funcSize, "length": funcSize, "coalesce": funcCoalesce, "exists": funcExists,
		"tostring": strFunc(func(v any) any { return stringify(v) }), "tointeger": funcToInteger,
		"tofloat": funcToFloat, "toboolean": funcToBoolean,
		"tolower": strFn(strings.ToLower), "toupper": strFn(strings.ToUpper), "trim": strFn(strings.TrimSpace),
		"ltrim":   strFn(func(s string) string { return strings.TrimLeft(s, " \t\r\n") }),
		"rtrim":   strFn(func(s string) string { return strings.TrimRight(s, " \t\r\n") }),
		"reverse": funcReverse, "head": funcHead, "last": funcLast, "tail": funcTail, "range": funcRange,
		"substring": funcSubstring, "replace": funcReplace, "split": funcSplit,
		"left": funcLeft, "right": funcRight,
		"abs": numFn(math.Abs), "ceil": numFn(math.Ceil), "floor": numFn(math.Floor), "round": numFn(math.Round),
		"sqrt": numFn(math.Sqrt), "sign": numFn(func(f float64) float64 {
			switch {
			case f > 0:
				return 1
			case f < 0:
				return -1
			}

			return 0
		}),
		"nodes":         funcPathNodes,
		"relationships": funcPathRels, "timestamp": func([]any) (any, error) { return time.Now().UnixMilli(), nil },
	}
}

func argc(name string, args []any, want int) error {
	if len(args) != want {
		return malformed("Function %s() takes %d argument(s)", name, want)
	}

	return nil
}

func funcID(args []any) (any, error) {
	if err := argc("id", args, 1); err != nil {
		return nil, err
	}

	switch t := args[0].(type) {
	case *Node:
		return t.ID, nil
	case *Rel:
		return t.ID, nil
	case nil:
		return null()
	}

	return nil, malformed("Type mismatch: expected Node or Relationship but was %s", typeName(args[0]))
}

func funcLabels(args []any) (any, error) {
	if err := argc("labels", args, 1); err != nil {
		return nil, err
	}

	switch t := args[0].(type) {
	case *Node:
		out := make([]any, len(t.Labels))
		for i, l := range t.Labels {
			out[i] = l
		}

		return out, nil
	case nil:
		return null()
	}

	return nil, malformed("Type mismatch: expected Node but was %s", typeName(args[0]))
}

func funcType(args []any) (any, error) {
	if err := argc("type", args, 1); err != nil {
		return nil, err
	}

	switch t := args[0].(type) {
	case *Rel:
		return t.Type, nil
	case nil:
		return null()
	}

	return nil, malformed("Type mismatch: expected Relationship but was %s", typeName(args[0]))
}

func propsOf(v any) (map[string]any, bool) {
	switch t := v.(type) {
	case *Node:
		return t.Props, true
	case *Rel:
		return t.Props, true
	case map[string]any:
		return t, true
	}

	return nil, false
}

func funcProperties(args []any) (any, error) {
	if err := argc("properties", args, 1); err != nil {
		return nil, err
	}

	if args[0] == nil {
		return null()
	}

	p, ok := propsOf(args[0])
	if !ok {
		return nil, malformed("Type mismatch: expected Node, Relationship or Map but was %s", typeName(args[0]))
	}

	return cloneProps(p), nil
}

func funcKeys(args []any) (any, error) {
	if err := argc("keys", args, 1); err != nil {
		return nil, err
	}

	if args[0] == nil {
		return null()
	}

	p, ok := propsOf(args[0])
	if !ok {
		return nil, malformed("Type mismatch: expected Node, Relationship or Map but was %s", typeName(args[0]))
	}

	keys := make([]string, 0, len(p))
	for k := range p {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	out := make([]any, len(keys))
	for i, k := range keys {
		out[i] = k
	}

	return out, nil
}

func funcSize(args []any) (any, error) {
	if len(args) != 1 {
		return nil, malformed("size() takes 1 argument")
	}

	switch t := args[0].(type) {
	case nil:
		return null()
	case []any:
		return int64(len(t)), nil
	case string:
		return int64(len([]rune(t))), nil
	case *Path:
		return int64(len(t.Rels)), nil
	}

	return nil, malformed("Type mismatch: expected List or String but was %s", typeName(args[0]))
}

func funcCoalesce(args []any) (any, error) {
	for _, a := range args {
		if a != nil {
			return a, nil
		}
	}

	return nil, nil //nolint:nilnil // all null
}

func funcExists(args []any) (any, error) {
	if err := argc("exists", args, 1); err != nil {
		return nil, err
	}

	return args[0] != nil, nil
}

func strFunc(f func(any) any) scalarFunc {
	return func(args []any) (any, error) {
		if len(args) != 1 {
			return nil, malformed("function takes 1 argument")
		}

		if args[0] == nil {
			return null()
		}

		return f(args[0]), nil
	}
}

func strFn(f func(string) string) scalarFunc {
	return func(args []any) (any, error) {
		if len(args) != 1 {
			return nil, malformed("function takes 1 argument")
		}

		switch t := args[0].(type) {
		case nil:
			return null()
		case string:
			return f(t), nil
		}

		return nil, malformed("Type mismatch: expected String but was %s", typeName(args[0]))
	}
}

func numFn(f func(float64) float64) scalarFunc {
	return func(args []any) (any, error) {
		if len(args) != 1 {
			return nil, malformed("function takes 1 argument")
		}

		switch t := args[0].(type) {
		case nil:
			return null()
		case int64:
			if r := f(float64(t)); r == math.Trunc(r) && math.Abs(r) < 1<<53 {
				return int64(r), nil
			}

			return f(float64(t)), nil
		case float64:
			return f(t), nil
		}

		return nil, malformed("Type mismatch: expected Number but was %s", typeName(args[0]))
	}
}

func funcToInteger(args []any) (any, error) {
	if err := argc("toInteger", args, 1); err != nil {
		return nil, err
	}

	switch t := args[0].(type) {
	case int64:
		return t, nil
	case float64:
		return int64(t), nil
	case string:
		if i, err := strconv.ParseInt(strings.TrimSpace(t), 10, 64); err == nil {
			return i, nil
		}

		if f, err := strconv.ParseFloat(strings.TrimSpace(t), 64); err == nil {
			return int64(f), nil
		}
	}

	return nil, nil //nolint:nilnil // not convertible
}

func funcToFloat(args []any) (any, error) {
	if err := argc("toFloat", args, 1); err != nil {
		return nil, err
	}

	switch t := args[0].(type) {
	case int64:
		return float64(t), nil
	case float64:
		return t, nil
	case string:
		if f, err := strconv.ParseFloat(strings.TrimSpace(t), 64); err == nil {
			return f, nil
		}
	}

	return nil, nil //nolint:nilnil // not convertible
}

func funcToBoolean(args []any) (any, error) {
	if err := argc("toBoolean", args, 1); err != nil {
		return nil, err
	}

	switch t := args[0].(type) {
	case bool:
		return t, nil
	case string:
		switch strings.ToLower(t) {
		case "true":
			return true, nil
		case "false":
			return false, nil
		}
	}

	return nil, nil //nolint:nilnil // not convertible
}

func listArg(name string, args []any) ([]any, bool, error) {
	if err := argc(name, args, 1); err != nil {
		return nil, false, err
	}

	if args[0] == nil {
		return nil, false, nil
	}

	l, ok := args[0].([]any)
	if !ok {
		return nil, false, malformed("Type mismatch: expected List but was %s", typeName(args[0]))
	}

	return l, true, nil
}

func funcReverse(args []any) (any, error) {
	if len(args) == 1 {
		if s, ok := args[0].(string); ok {
			rs := []rune(s)
			for i, j := 0, len(rs)-1; i < j; i, j = i+1, j-1 {
				rs[i], rs[j] = rs[j], rs[i]
			}

			return string(rs), nil
		}
	}

	l, ok, err := listArg("reverse", args)
	if !ok || err != nil {
		return nil, err
	}

	out := make([]any, len(l))
	for i, v := range l {
		out[len(l)-1-i] = v
	}

	return out, nil
}

func funcHead(args []any) (any, error) {
	l, ok, err := listArg("head", args)
	if !ok || err != nil || len(l) == 0 {
		return nil, err
	}

	return l[0], nil
}

func funcLast(args []any) (any, error) {
	l, ok, err := listArg("last", args)
	if !ok || err != nil || len(l) == 0 {
		return nil, err
	}

	return l[len(l)-1], nil
}

func funcTail(args []any) (any, error) {
	l, ok, err := listArg("tail", args)
	if !ok || err != nil {
		return nil, err
	}

	if len(l) == 0 {
		return []any{}, nil
	}

	return append([]any{}, l[1:]...), nil
}

const (
	maxRange      = 1_000_000
	rangeNoStep   = 2
	substrWithLen = 3
	argPair       = 2
)

func funcRange(args []any) (any, error) {
	if len(args) < 2 || len(args) > 3 {
		return nil, malformed("range() takes 2 or 3 arguments")
	}

	nums := make([]int64, len(args))

	for i, a := range args {
		n, ok := a.(int64)
		if !ok {
			return nil, malformed("Type mismatch: range() expects Integer arguments")
		}

		nums[i] = n
	}

	step := int64(1)
	if len(nums) > rangeNoStep {
		step = nums[rangeNoStep]
	}

	if step == 0 {
		return nil, malformed("step argument to range() cannot be zero")
	}

	out := []any{}

	for v := nums[0]; (step > 0 && v <= nums[1]) || (step < 0 && v >= nums[1]); v += step {
		if len(out) >= maxRange {
			return nil, malformed("range() result too large")
		}

		out = append(out, v)
	}

	return out, nil
}

func funcSubstring(args []any) (any, error) {
	if len(args) < 2 || len(args) > 3 {
		return nil, malformed("substring() takes 2 or 3 arguments")
	}

	s, ok := args[0].(string)
	start, ok2 := args[1].(int64)

	if !ok || !ok2 || start < 0 {
		return nil, nil //nolint:nilnil // invalid input yields null
	}

	rs := []rune(s)
	end := int64(len(rs))

	if len(args) == substrWithLen {
		n, ok3 := args[2].(int64)
		if !ok3 || n < 0 {
			return nil, nil //nolint:nilnil // invalid input yields null
		}

		end = min(end, start+n)
	}

	if start >= int64(len(rs)) || start >= end {
		return "", nil
	}

	return string(rs[start:end]), nil
}

func funcReplace(args []any) (any, error) {
	if err := argc("replace", args, substrWithLen); err != nil {
		return nil, err
	}

	s, a := args[0].(string)
	from, b := args[1].(string)
	to, c := args[2].(string)

	if !a || !b || !c {
		return nil, nil //nolint:nilnil // invalid input yields null
	}

	return strings.ReplaceAll(s, from, to), nil
}

func funcSplit(args []any) (any, error) {
	if err := argc("split", args, argPair); err != nil {
		return nil, err
	}

	s, a := args[0].(string)
	sep, b := args[1].(string)

	if !a || !b {
		return nil, nil //nolint:nilnil // invalid input yields null
	}

	parts := strings.Split(s, sep)
	out := make([]any, len(parts))

	for i, p := range parts {
		out[i] = p
	}

	return out, nil
}

func funcLeft(args []any) (any, error) {
	return edge("left", args, func(rs []rune, n int) string { return string(rs[:n]) })
}

func funcRight(args []any) (any, error) {
	return edge("right", args, func(rs []rune, n int) string { return string(rs[len(rs)-n:]) })
}

func edge(name string, args []any, f func([]rune, int) string) (any, error) {
	if err := argc(name, args, argPair); err != nil {
		return nil, err
	}

	s, ok := args[0].(string)
	n, ok2 := args[1].(int64)

	if !ok || !ok2 || n < 0 {
		return nil, nil //nolint:nilnil // invalid input yields null
	}

	rs := []rune(s)

	return f(rs, int(min(n, int64(len(rs))))), nil
}

func funcPathNodes(args []any) (any, error) {
	p, ok := pathArg("nodes", args)
	if !ok {
		return nil, nil //nolint:nilnil // not a path
	}

	out := make([]any, len(p.Nodes))
	for i, n := range p.Nodes {
		out[i] = n
	}

	return out, nil
}

func funcPathRels(args []any) (any, error) {
	p, ok := pathArg("relationships", args)
	if !ok {
		return nil, nil //nolint:nilnil // not a path
	}

	out := make([]any, len(p.Rels))
	for i, r := range p.Rels {
		out[i] = r
	}

	return out, nil
}

func pathArg(_ string, args []any) (*Path, bool) {
	if len(args) != 1 {
		return nil, false
	}

	p, ok := args[0].(*Path)

	return p, ok
}

func (x *exec) relEndpoint(name string, args []any) (any, error) {
	if err := argc(name, args, 1); err != nil {
		return nil, err
	}

	switch t := args[0].(type) {
	case nil:
		return null()
	case *Rel:
		if name == "startnode" {
			return x.g.nodes[t.Start], nil
		}

		return x.g.nodes[t.End], nil
	}

	return nil, malformed("Type mismatch: expected Relationship but was %s", typeName(args[0]))
}

func null() (any, error) {
	return nil, nil //nolint:nilnil // a null Cypher value is not an error
}
