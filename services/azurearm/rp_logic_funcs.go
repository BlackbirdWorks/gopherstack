package azurearm

import (
	"encoding/base64"
	"encoding/json"
	"maps"
	"math"
	"net/url"
	"reflect"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"unicode/utf8"

	"github.com/google/uuid"
)

const (
	argsTwo   = 2
	argsThree = 3
	argsFour  = 4

	portHTTPS = 443
	portHTTP  = 80
)

type logicFunc func(c *logicEvalCtx, a []*logicExpr) (any, error)

// eagerLogic evaluates every argument before calling f; min/max bound the argument count (max < 0 is unbounded).
func eagerLogic(name string, minArgs, maxArgs int, f func(a []any) (any, error)) logicFunc {
	return func(c *logicEvalCtx, a []*logicExpr) (any, error) {
		if len(a) < minArgs || maxArgs >= 0 && len(a) > maxArgs {
			return nil, exprErr("function '%s' has an invalid number of arguments: %d", name, len(a))
		}

		vals := make([]any, len(a))

		for i, n := range a {
			v, err := c.eval(n)
			if err != nil {
				return nil, err
			}

			vals[i] = v
		}

		return f(vals)
	}
}

func (c *logicEvalCtx) eval(n *logicExpr) (any, error) {
	switch n.kind {
	case exprLit:
		return n.lit, nil
	case exprCall:
		fn, ok := logicFuncTable()[strings.ToLower(n.name)]
		if !ok {
			return nil, exprErr("the template language function '%s' is not defined or not valid", n.name)
		}

		return fn(c, n.args)
	case exprIndex:
		return c.evalIndex(n)
	case exprProp:
		base, err := c.eval(n.base)
		if err != nil {
			return nil, err
		}

		return logicMember(base, n.name, n.safe)
	}

	return nil, exprErr("invalid expression")
}

func (c *logicEvalCtx) evalIndex(n *logicExpr) (any, error) {
	base, err := c.eval(n.base)
	if err != nil {
		return nil, err
	}

	idx, err := c.eval(n.idx)
	if err != nil {
		return nil, err
	}

	if arr, ok := base.([]any); ok {
		i, isNum := logicInt(idx)
		if !isNum {
			return nil, exprErr("array index must be an integer, got %s", logicString(idx))
		}

		if i < 0 || i >= int64(len(arr)) {
			if n.safe {
				return nil, nil //nolint:nilnil // a null-safe access of a missing element is null
			}

			return nil, exprErr("the index %d is outside the bounds of the array of length %d", i, len(arr))
		}

		return arr[i], nil
	}

	return logicMember(base, logicString(idx), n.safe)
}

func logicMember(base any, key string, safe bool) (any, error) {
	switch t := base.(type) {
	case map[string]any:
		if v, ok := t[key]; ok {
			return v, nil
		}

		for k, v := range t {
			if strings.EqualFold(k, key) {
				return v, nil
			}
		}

		if safe {
			return nil, nil //nolint:nilnil // a null-safe access of a missing property is null
		}

		return nil, exprErr("property '%s' cannot be evaluated because it does not exist", key)
	case nil:
		if safe {
			return nil, nil //nolint:nilnil // a null-safe access of null is null
		}

		return nil, exprErr("property '%s' cannot be evaluated on a null value", key)
	}

	if safe {
		return nil, nil //nolint:nilnil // a null-safe access of a scalar is null
	}

	return nil, exprErr("property '%s' cannot be evaluated on a value that is not an object", key)
}

func logicInt(v any) (int64, bool) {
	switch t := v.(type) {
	case int64:
		return t, true
	case float64:
		if t == math.Trunc(t) {
			return int64(t), true
		}
	}

	return 0, false
}

func logicNum(v any) (float64, bool, bool) {
	switch t := v.(type) {
	case int64:
		return float64(t), true, true
	case float64:
		return t, false, true
	case string:
		if i, err := strconv.ParseInt(strings.TrimSpace(t), 10, 64); err == nil {
			return float64(i), true, true
		}

		if f, err := strconv.ParseFloat(strings.TrimSpace(t), 64); err == nil {
			return f, false, true
		}
	}

	return 0, false, false
}

func logicTruthy(v any) bool {
	switch t := v.(type) {
	case bool:
		return t
	case nil:
		return false
	case int64:
		return t != 0
	case float64:
		return t != 0
	case string:
		return t != ""
	}

	return true
}

func logicEquals(a, b any) bool {
	if ab, ok := a.(bool); ok {
		if _, isNum := logicNumOnly(b); isNum {
			return ab == logicTruthy(b)
		}
	}

	if bb, ok := b.(bool); ok {
		if _, isNum := logicNumOnly(a); isNum {
			return bb == logicTruthy(a)
		}
	}

	af, aok := logicNumOnly(a)
	bf, bok := logicNumOnly(b)

	if aok && bok {
		return af == bf
	}

	return reflect.DeepEqual(normLogic(a), normLogic(b))
}

func logicNumOnly(v any) (float64, bool) {
	switch t := v.(type) {
	case int64:
		return float64(t), true
	case float64:
		return t, true
	}

	return 0, false
}

// logicCompare orders two numbers or two strings.
func logicCompare(name string, a, b any) (int, error) {
	af, aok := logicNumOnly(a)
	bf, bok := logicNumOnly(b)

	if aok && bok {
		switch {
		case af < bf:
			return -1, nil
		case af > bf:
			return 1, nil
		}

		return 0, nil
	}

	as, asOK := a.(string)
	bs, bsOK := b.(string)

	if asOK && bsOK {
		return strings.Compare(as, bs), nil
	}

	return 0, exprErr(
		"the template language function '%s' expects its parameters to be both numbers or both strings",
		name,
	)
}

func cmpFunc(name string, ok func(int) bool) logicFunc {
	return eagerLogic(name, argsTwo, argsTwo, func(a []any) (any, error) {
		r, err := logicCompare(name, a[0], a[1])
		if err != nil {
			return nil, err
		}

		return ok(r), nil
	})
}

func logicArgString(name string, v any) (string, error) {
	s, ok := v.(string)
	if !ok {
		return "", exprErr("the template language function '%s' expects a string argument", name)
	}

	return s, nil
}

func logicToBool(v any) (bool, error) {
	switch t := v.(type) {
	case bool:
		return t, nil
	case string:
		switch strings.ToLower(t) {
		case "true":
			return true, nil
		case logicLitFalse:
			return false, nil
		}
	case int64, float64:
		return logicTruthy(t), nil
	}

	return false, exprErr("cannot convert the value to a boolean")
}

func logicLength(v any) (int, bool) {
	switch t := v.(type) {
	case string:
		return utf8.RuneCountInString(t), true
	case []any:
		return len(t), true
	case map[string]any:
		return len(t), true
	}

	return 0, false
}

var (
	logicFuncOnce sync.Once            //nolint:gochecknoglobals // lazily built, read-only afterwards
	logicFuncMap  map[string]logicFunc //nolint:gochecknoglobals // lazily built, read-only afterwards
)

// logicFuncTable is the lower-cased name to implementation table; built on first use.
func logicFuncTable() map[string]logicFunc {
	logicFuncOnce.Do(func() { logicFuncMap = buildLogicFuncs() })

	return logicFuncMap
}

func buildLogicFuncs() map[string]logicFunc {
	m := map[string]logicFunc{}

	addLogicStringFuncs(m)
	addLogicCollectionFuncs(m)
	addLogicLogicalFuncs(m)
	addLogicMathFuncs(m)
	addLogicConvFuncs(m)
	addLogicTimeFuncs(m)
	addLogicURIFuncs(m)
	addLogicMoreFuncs(m)
	addLogicWorkflowFuncs(m)

	return m
}

func addLogicStringFuncs(m map[string]logicFunc) {
	m["concat"] = eagerLogic("concat", 1, -1, logicConcat)
	m["substring"] = eagerLogic("substring", argsTwo, argsThree, logicSubstring)
	m["replace"] = eagerLogic("replace", argsThree, argsThree, func(a []any) (any, error) {
		return strings.ReplaceAll(logicString(a[0]), logicString(a[1]), logicString(a[2])), nil
	})
	m["split"] = eagerLogic("split", argsTwo, argsTwo, logicSplit)
	m["tolower"] = eagerLogic(
		"toLower",
		1,
		1,
		func(a []any) (any, error) { return strings.ToLower(logicString(a[0])), nil },
	)
	m["toupper"] = eagerLogic(
		"toUpper",
		1,
		1,
		func(a []any) (any, error) { return strings.ToUpper(logicString(a[0])), nil },
	)
	m["trim"] = eagerLogic(
		"trim",
		1,
		1,
		func(a []any) (any, error) { return strings.TrimSpace(logicString(a[0])), nil },
	)
	m["startswith"] = eagerLogic("startsWith", argsTwo, argsTwo, func(a []any) (any, error) {
		return strings.HasPrefix(strings.ToLower(logicString(a[0])), strings.ToLower(logicString(a[1]))), nil
	})
	m["endswith"] = eagerLogic("endsWith", argsTwo, argsTwo, func(a []any) (any, error) {
		return strings.HasSuffix(strings.ToLower(logicString(a[0])), strings.ToLower(logicString(a[1]))), nil
	})
	m["indexof"] = eagerLogic("indexOf", argsTwo, argsTwo, func(a []any) (any, error) {
		return int64(strings.Index(strings.ToLower(logicString(a[0])), strings.ToLower(logicString(a[1])))), nil
	})
	m["lastindexof"] = eagerLogic("lastIndexOf", argsTwo, argsTwo, logicLastIndexOf)
	m["guid"] = eagerLogic("guid", 0, 1, logicGUID)
}

func logicSubstring(a []any) (any, error) {
	s, err := logicArgString("substring", a[0])
	if err != nil {
		return nil, err
	}

	start, ok := logicInt(a[1])
	if !ok {
		return nil, exprErr("substring expects an integer start index")
	}

	runes := []rune(s)
	if start < 0 || int(start) > len(runes) {
		return nil, exprErr("the start index %d is outside the bounds of the string", start)
	}

	end := len(runes)

	if len(a) == argsThree {
		n, nOK := logicInt(a[2])
		if !nOK || n < 0 {
			return nil, exprErr("substring expects a non-negative integer length")
		}

		if int(start)+int(n) > len(runes) {
			return nil, exprErr("the length %d exceeds the string", n)
		}

		end = int(start) + int(n)
	}

	return string(runes[start:end]), nil
}

func logicLastIndexOf(a []any) (any, error) {
	s, sub := strings.ToLower(logicString(a[0])), strings.ToLower(logicString(a[1]))

	switch {
	case s == "" && sub == "":
		return int64(0), nil
	case s == "":
		return int64(-1), nil
	case sub == "":
		return int64(len(s) - 1), nil
	}

	return int64(strings.LastIndex(s, sub)), nil
}

func logicGUID(a []any) (any, error) {
	id := uuid.NewString()

	format := "D"
	if len(a) == 1 {
		format = strings.ToUpper(logicString(a[0]))
	}

	switch format {
	case "N":
		return strings.ReplaceAll(id, "-", ""), nil
	case "D":
		return id, nil
	case "B":
		return "{" + id + "}", nil
	case "P":
		return "(" + id + ")", nil
	case "X":
		h := strings.ReplaceAll(id, "-", "")

		return "{0x" + h[:8] + ",0x" + h[8:12] + ",0x" + h[12:16] + ",{0x" + h[16:18] + ",0x" + h[18:20] +
			",0x" + h[20:22] + ",0x" + h[22:24] + ",0x" + h[24:26] + ",0x" + h[26:28] + ",0x" + h[28:30] +
			",0x" + h[30:32] + "}}", nil
	}

	return nil, exprErr("guid format %q is not valid", format)
}

func addLogicCollectionFuncs(m map[string]logicFunc) {
	m["length"] = eagerLogic("length", 1, 1, logicLengthFn)
	m["empty"] = eagerLogic("empty", 1, 1, logicEmpty)
	m["contains"] = eagerLogic("contains", argsTwo, argsTwo, logicContains)
	m["first"] = eagerLogic("first", 1, 1, func(a []any) (any, error) { return logicEnd(a[0], true) })
	m["last"] = eagerLogic("last", 1, 1, func(a []any) (any, error) { return logicEnd(a[0], false) })
	m["join"] = eagerLogic("join", argsTwo, argsTwo, logicJoin)
	m["createarray"] = eagerLogic("createArray", 1, -1, func(a []any) (any, error) { return slices.Clone(a), nil })
	m["union"] = eagerLogic("union", argsTwo, -1, logicUnion)
	m["intersection"] = eagerLogic("intersection", argsTwo, -1, logicIntersection)
	m["take"] = eagerLogic("take", argsTwo, argsTwo, func(a []any) (any, error) { return logicTakeSkip(a, true) })
	m["skip"] = eagerLogic("skip", argsTwo, argsTwo, func(a []any) (any, error) { return logicTakeSkip(a, false) })
	m["reverse"] = eagerLogic("reverse", 1, 1, logicReverse)
	m["sort"] = eagerLogic("sort", 1, argsTwo, logicSort)
	m["range"] = eagerLogic("range", argsTwo, argsTwo, logicRange)
	m["mergeobjects"] = eagerLogic("mergeObjects", argsTwo, -1, logicMergeObjects)
	m["setproperty"] = eagerLogic("setProperty", argsThree, argsThree, logicSetProperty)
	m["removeproperty"] = eagerLogic("removeProperty", argsTwo, argsTwo, logicRemoveProperty)
}

func logicContains(a []any) (any, error) {
	switch t := a[0].(type) {
	case string:
		return strings.Contains(t, logicString(a[1])), nil
	case []any:
		for _, v := range t {
			if logicEquals(v, a[1]) {
				return true, nil
			}
		}

		return false, nil
	case map[string]any:
		_, ok := t[logicString(a[1])]

		return ok, nil
	}

	return nil, exprErr("the function 'contains' expects a string, array or object")
}

func logicEnd(v any, first bool) (any, error) {
	switch t := v.(type) {
	case string:
		r := []rune(t)
		if len(r) == 0 {
			return nil, nil //nolint:nilnil // first/last of an empty collection is null
		}

		if first {
			return string(r[0]), nil
		}

		return string(r[len(r)-1]), nil
	case []any:
		if len(t) == 0 {
			return nil, nil //nolint:nilnil // first/last of an empty collection is null
		}

		if first {
			return t[0], nil
		}

		return t[len(t)-1], nil
	}

	return nil, exprErr("the function 'first/last' expects a string or array")
}

func logicUnion(a []any) (any, error) {
	if _, ok := a[0].(map[string]any); ok {
		out := map[string]any{}

		for _, v := range a {
			o, isObj := v.(map[string]any)
			if !isObj {
				return nil, exprErr("union expects collections of the same kind")
			}

			maps.Copy(out, o)
		}

		return out, nil
	}

	out := []any{}

	for _, v := range a {
		arr, ok := v.([]any)
		if !ok {
			return nil, exprErr("union expects arrays or objects")
		}

		for _, x := range arr {
			if !slices.ContainsFunc(out, func(y any) bool { return logicEquals(x, y) }) {
				out = append(out, x)
			}
		}
	}

	return out, nil
}

func logicIntersection(a []any) (any, error) {
	first, ok := a[0].([]any)
	if !ok {
		return nil, exprErr("intersection expects arrays")
	}

	out := []any{}

	for _, x := range first {
		inAll := true

		for _, other := range a[1:] {
			arr, isArr := other.([]any)
			if !isArr {
				return nil, exprErr("intersection expects arrays")
			}

			if !slices.ContainsFunc(arr, func(y any) bool { return logicEquals(x, y) }) {
				inAll = false

				break
			}
		}

		if inAll && !slices.ContainsFunc(out, func(y any) bool { return logicEquals(x, y) }) {
			out = append(out, x)
		}
	}

	return out, nil
}

func logicTakeSkip(a []any, take bool) (any, error) {
	n, ok := logicInt(a[1])
	if !ok || n < 0 {
		return nil, exprErr("take/skip expects a non-negative integer count")
	}

	if s, isStr := a[0].(string); isStr {
		r := []rune(s)
		k := min(int(n), len(r))

		if take {
			return string(r[:k]), nil
		}

		return string(r[k:]), nil
	}

	arr, isArr := a[0].([]any)
	if !isArr {
		return nil, exprErr("take/skip expects a string or array")
	}

	k := min(int(n), len(arr))

	if take {
		return slices.Clone(arr[:k]), nil
	}

	return slices.Clone(arr[k:]), nil
}

func logicSort(a []any) (any, error) {
	arr, ok := a[0].([]any)
	if !ok {
		return nil, exprErr("sort expects an array")
	}

	out := slices.Clone(arr)

	var sortErr error

	key := func(v any) any {
		if len(a) == argsTwo {
			o, isObj := v.(map[string]any)
			if !isObj {
				sortErr = exprErr("sort by key expects objects")

				return nil
			}

			return o[logicString(a[1])]
		}

		return v
	}

	sort.SliceStable(out, func(i, j int) bool {
		r, err := logicCompare("sort", key(out[i]), key(out[j]))
		if err != nil {
			sortErr = err
		}

		return r < 0
	})

	if sortErr != nil {
		return nil, sortErr
	}

	return out, nil
}

func addLogicLogicalFuncs(m map[string]logicFunc) {
	m["equals"] = eagerLogic(
		"equals",
		argsTwo,
		argsTwo,
		func(a []any) (any, error) { return logicEquals(a[0], a[1]), nil },
	)
	m["not"] = eagerLogic("not", 1, 1, logicNot)
	m["and"] = shortCircuit("and", true)
	m["or"] = shortCircuit("or", false)
	m["if"] = func(c *logicEvalCtx, a []*logicExpr) (any, error) {
		if len(a) != argsThree {
			return nil, exprErr("function 'if' expects 3 arguments")
		}

		cond, err := c.eval(a[0])
		if err != nil {
			return nil, err
		}

		b, err := logicToBool(cond)
		if err != nil {
			return nil, err
		}

		if b {
			return c.eval(a[1])
		}

		return c.eval(a[2])
	}
	m["coalesce"] = func(c *logicEvalCtx, a []*logicExpr) (any, error) {
		for _, n := range a {
			v, err := c.eval(n)
			if err != nil {
				return nil, err
			}

			if v != nil {
				return v, nil
			}
		}

		return nil, nil //nolint:nilnil // coalesce of only nulls is null
	}
	m["greater"] = cmpFunc("greater", func(r int) bool { return r > 0 })
	m["greaterorequals"] = cmpFunc("greaterOrEquals", func(r int) bool { return r >= 0 })
	m["less"] = cmpFunc("less", func(r int) bool { return r < 0 })
	m["lessorequals"] = cmpFunc("lessOrEquals", func(r int) bool { return r <= 0 })
}

func shortCircuit(name string, isAnd bool) logicFunc {
	return func(c *logicEvalCtx, a []*logicExpr) (any, error) {
		if len(a) < 1 {
			return nil, exprErr("function '%s' expects at least 1 argument", name)
		}

		for _, n := range a {
			v, err := c.eval(n)
			if err != nil {
				return nil, err
			}

			b, err := logicToBool(v)
			if err != nil {
				return nil, err
			}

			if b != isAnd {
				return !isAnd, nil
			}
		}

		return isAnd, nil
	}
}

func addLogicMathFuncs(m map[string]logicFunc) {
	m["add"] = mathFunc("add", func(x, y float64) float64 { return x + y }, func(x, y int64) int64 { return x + y })
	m["sub"] = mathFunc("sub", func(x, y float64) float64 { return x - y }, func(x, y int64) int64 { return x - y })
	m["mul"] = mathFunc("mul", func(x, y float64) float64 { return x * y }, func(x, y int64) int64 { return x * y })
	m["div"] = eagerLogic("div", argsTwo, argsTwo, logicDiv)
	m["mod"] = eagerLogic("mod", argsTwo, argsTwo, logicMod)
	m["min"] = eagerLogic("min", 1, -1, func(a []any) (any, error) { return logicExtreme(a, false) })
	m["max"] = eagerLogic("max", 1, -1, func(a []any) (any, error) { return logicExtreme(a, true) })
	m["pow"] = eagerLogic("pow", argsTwo, argsTwo, logicPow)
}

func mathFunc(name string, ff func(x, y float64) float64, fi func(x, y int64) int64) logicFunc {
	return eagerLogic(name, argsTwo, argsTwo, func(a []any) (any, error) {
		x, xi, ok1 := logicNum(a[0])
		y, yi, ok2 := logicNum(a[1])

		if !ok1 || !ok2 {
			return nil, exprErr("the function '%s' expects numeric arguments", name)
		}

		if xi && yi {
			return fi(int64(x), int64(y)), nil
		}

		return normLogic(ff(x, y)), nil
	})
}

func logicExtreme(a []any, wantMax bool) (any, error) {
	vals := a
	if len(a) == 1 {
		arr, ok := a[0].([]any)
		if !ok || len(arr) == 0 {
			return nil, exprErr("min/max expects numbers or a non-empty array")
		}

		vals = arr
	}

	best := vals[0]

	for _, v := range vals[1:] {
		r, err := logicCompare("min/max", v, best)
		if err != nil {
			return nil, err
		}

		if wantMax && r > 0 || !wantMax && r < 0 {
			best = v
		}
	}

	if _, ok := logicNumOnly(best); !ok {
		return nil, exprErr("min/max expects numbers")
	}

	return best, nil
}

func addLogicConvFuncs(m map[string]logicFunc) {
	m[logicVarString] = eagerLogic(logicVarString, 1, 1, func(a []any) (any, error) { return logicString(a[0]), nil })
	m["int"] = eagerLogic("int", 1, 1, logicIntFn)
	m[logicVarFloat] = eagerLogic(logicVarFloat, 1, argsTwo, logicFloat)
	m["bool"] = eagerLogic("bool", 1, 1, func(a []any) (any, error) { return logicToBool(a[0]) })
	m["json"] = eagerLogic("json", 1, 1, logicJSON)
	m["base64"] = eagerLogic("base64", 1, 1, func(a []any) (any, error) {
		return base64.StdEncoding.EncodeToString([]byte(logicString(a[0]))), nil
	})
	m["base64tostring"] = eagerLogic("base64ToString", 1, 1, logicBase64Decode)
	m["uricomponent"] = eagerLogic("uriComponent", 1, 1, func(a []any) (any, error) {
		return strings.ReplaceAll(url.QueryEscape(logicString(a[0])), "+", "%20"), nil
	})
	m["uricomponenttostring"] = eagerLogic("uriComponentToString", 1, 1, logicURIDecode)
	m["isint"] = eagerLogic("isInt", 1, 1, logicIsInt)
	m["isfloat"] = eagerLogic("isFloat", 1, argsTwo, logicIsFloat)
}

func addLogicURIFuncs(m map[string]logicFunc) {
	parse := func(name string, f func(u *url.URL) any) logicFunc {
		return eagerLogic(name, 1, 1, func(a []any) (any, error) {
			u, err := url.Parse(logicString(a[0]))
			if err != nil || u.Scheme == "" {
				return nil, exprErr("the value is not a valid absolute URI")
			}

			return f(u), nil
		})
	}

	m["urihost"] = parse("uriHost", func(u *url.URL) any { return u.Hostname() })
	m["uripath"] = parse("uriPath", func(u *url.URL) any { return u.EscapedPath() })
	m["uripathandquery"] = parse("uriPathAndQuery", func(u *url.URL) any {
		if u.RawQuery == "" {
			return u.EscapedPath()
		}

		return u.EscapedPath() + "?" + u.RawQuery
	})
	m["uriport"] = parse("uriPort", func(u *url.URL) any {
		if p, err := strconv.ParseInt(u.Port(), 10, 64); err == nil {
			return p
		}

		if u.Scheme == logicSchemeHTTPS {
			return int64(portHTTPS)
		}

		return int64(portHTTP)
	})
	m["uriquery"] = parse("uriQuery", func(u *url.URL) any {
		if u.RawQuery == "" {
			return ""
		}

		return "?" + u.RawQuery
	})
	m["urischeme"] = parse("uriScheme", func(u *url.URL) any { return u.Scheme })
}

func logicJoin(a []any) (any, error) {
	arr, ok := a[0].([]any)
	if !ok {
		return nil, exprErr("the function 'join' expects an array")
	}

	parts := make([]string, len(arr))
	for i, v := range arr {
		parts[i] = logicString(v)
	}

	return strings.Join(parts, logicString(a[1])), nil
}

func logicReverse(a []any) (any, error) {
	arr, ok := a[0].([]any)
	if !ok {
		return nil, exprErr("the function 'reverse' expects an array")
	}

	out := slices.Clone(arr)
	slices.Reverse(out)

	return out, nil
}

func logicRange(a []any) (any, error) {
	start, ok1 := logicInt(a[0])
	count, ok2 := logicInt(a[1])

	if !ok1 || !ok2 || count < 0 {
		return nil, exprErr("range expects an integer start and a non-negative count")
	}

	out := make([]any, count)
	for i := range out {
		out[i] = start + int64(i)
	}

	return out, nil
}

func logicMergeObjects(a []any) (any, error) {
	out := map[string]any{}

	for _, v := range a {
		o, ok := v.(map[string]any)
		if !ok {
			return nil, exprErr("mergeObjects expects objects")
		}

		maps.Copy(out, o)
	}

	return out, nil
}

func logicSetProperty(a []any) (any, error) {
	o, ok := a[0].(map[string]any)
	if !ok {
		return nil, exprErr("setProperty expects an object")
	}

	out := make(map[string]any, len(o)+1)
	maps.Copy(out, o)

	out[logicString(a[1])] = a[2]

	return out, nil
}

func logicRemoveProperty(a []any) (any, error) {
	o, ok := a[0].(map[string]any)
	if !ok {
		return nil, exprErr("removeProperty expects an object")
	}

	out := make(map[string]any, len(o))
	for k, x := range o {
		if k != logicString(a[1]) {
			out[k] = x
		}
	}

	return out, nil
}

func logicLengthFn(a []any) (any, error) {
	n, ok := logicLength(a[0])
	if !ok {
		return nil, exprErr("the function 'length' expects a string, array or object")
	}

	return int64(n), nil
}

func logicEmpty(a []any) (any, error) {
	if a[0] == nil {
		return true, nil
	}

	n, ok := logicLength(a[0])
	if !ok {
		return nil, exprErr("the function 'empty' expects a string, array or object")
	}

	return n == 0, nil
}

func logicIntFn(a []any) (any, error) {
	if i, ok := logicInt(a[0]); ok {
		return i, nil
	}

	if s, ok := a[0].(string); ok {
		if i, err := strconv.ParseInt(strings.TrimSpace(s), 10, 64); err == nil {
			return i, nil
		}
	}

	return nil, exprErr("the value cannot be converted to an integer")
}

func logicFloat(a []any) (any, error) {
	v := a[0]
	if str, isStr := v.(string); isStr && len(a) == 1 {
		v = strings.ReplaceAll(str, ",", "")
	}

	f, _, ok := logicNum(v)
	if !ok {
		return nil, exprErr("the value cannot be converted to a float")
	}

	return f, nil
}

func logicJSON(a []any) (any, error) {
	s, ok := a[0].(string)
	if !ok {
		return a[0], nil
	}

	var v any
	if err := json.Unmarshal([]byte(s), &v); err != nil {
		return nil, exprErr("the string is not valid JSON: %v", err)
	}

	return normLogic(v), nil
}

func logicBase64Decode(a []any) (any, error) {
	b, err := base64.StdEncoding.DecodeString(logicString(a[0]))
	if err != nil {
		return nil, exprErr("the value is not valid base64")
	}

	return string(b), nil
}

func logicURIDecode(a []any) (any, error) {
	s, err := url.QueryUnescape(logicString(a[0]))
	if err != nil {
		return nil, exprErr("the value is not a valid URI component")
	}

	return s, nil
}

func logicIsInt(a []any) (any, error) {
	s, ok := a[0].(string)
	if !ok {
		return false, nil
	}

	_, err := strconv.ParseInt(strings.TrimSpace(s), 10, 64)

	return err == nil, nil
}

func logicIsFloat(a []any) (any, error) {
	s, ok := a[0].(string)
	if !ok {
		return false, nil
	}

	_, err := strconv.ParseFloat(strings.ReplaceAll(strings.TrimSpace(s), ",", ""), 64)

	return err == nil, nil
}

func logicConcat(a []any) (any, error) {
	var sb strings.Builder
	for _, v := range a {
		sb.WriteString(logicString(v))
	}

	return sb.String(), nil
}

func logicSplit(a []any) (any, error) {
	s := logicString(a[0])
	d := logicString(a[1])

	var parts []string
	if d == "" {
		parts = []string{s}
	} else {
		parts = strings.Split(s, d)
	}

	out := make([]any, len(parts))
	for i, p := range parts {
		out[i] = p
	}

	return out, nil
}

func logicNot(a []any) (any, error) {
	b, err := logicToBool(a[0])
	if err != nil {
		return nil, err
	}

	return !b, nil
}

func logicDiv(a []any) (any, error) {
	x, xi, ok1 := logicNum(a[0])
	y, yi, ok2 := logicNum(a[1])

	if !ok1 || !ok2 {
		return nil, exprErr("the function 'div' expects numeric arguments")
	}

	if y == 0 {
		return nil, exprErr("the function 'div' cannot divide by zero")
	}

	if xi && yi {
		return int64(x) / int64(y), nil
	}

	return x / y, nil
}

func logicMod(a []any) (any, error) {
	x, xi, ok1 := logicNum(a[0])
	y, yi, ok2 := logicNum(a[1])

	if !ok1 || !ok2 || y == 0 {
		return nil, exprErr("the function 'mod' expects numeric arguments and a non-zero divisor")
	}

	if xi && yi {
		return int64(x) % int64(y), nil
	}

	return math.Mod(x, y), nil
}

func logicPow(a []any) (any, error) {
	x, _, ok1 := logicNum(a[0])
	y, _, ok2 := logicNum(a[1])

	if !ok1 || !ok2 {
		return nil, exprErr("the function 'pow' expects numeric arguments")
	}

	return math.Pow(x, y), nil
}
