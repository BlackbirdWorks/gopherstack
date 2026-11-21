package opencypher

import (
	"math"
	"math/big"
	"math/rand/v2"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
)

const (
	pairArgs       = 2
	halfTurnDeg    = 180
	datetimeLayout = "2006-01-02T15:04:05Z"
	timeHMS        = "15:04:05"
	timeHMSMillis  = "15:04:05.000"
	argOrder       = "order"
	argKey         = "key"
	orderDesc      = "desc"
)

func extraFuncs() map[string]scalarFunc {
	fns := map[string]scalarFunc{
		"e":                func([]any) (any, error) { return math.E, nil },
		"pi":               func([]any) (any, error) { return math.Pi, nil },
		"rand":             func([]any) (any, error) { return rand.Float64(), nil }, //nolint:gosec // not security relevant
		"randomuuid":       func([]any) (any, error) { return uuid.NewString(), nil },
		"atan2":            funcAtan2,
		"join":             funcJoin,
		"removekeyfrommap": funcRemoveKeyFromMap,
		"epochmillis":      funcEpochMillis,
		"datetime":         funcDatetime,
		"day":              temporalPart(func(t time.Time) int { return t.Day() }),
		"month":            temporalPart(func(t time.Time) int { return int(t.Month()) }),
		"year":             temporalPart(func(t time.Time) int { return t.Year() }),
		"textindexof":      funcTextIndexOf,
		"colltoset":        funcCollToSet,
		"collsubtract":     funcCollSubtract,
		"collintersection": funcCollIntersection,
		"collsort":         funcCollSort,
		"collsortmaps":     funcCollSortMaps,
		"collsortmulti":    funcCollSortMulti,
		"collsortnodes":    funcCollSortMaps,
	}

	for name, f := range map[string]func(float64) float64{
		"exp": math.Exp, "log": math.Log, "log10": math.Log10, "sin": math.Sin, "cos": math.Cos, "tan": math.Tan,
		"asin": math.Asin, "acos": math.Acos, "atan": math.Atan,
		"cot":     func(x float64) float64 { return 1 / math.Tan(x) },
		"degrees": func(x float64) float64 { return x * halfTurnDeg / math.Pi },
		"radians": func(x float64) float64 { return x * math.Pi / halfTurnDeg },
	} {
		fns[name] = floatFn(f)
	}

	return fns
}

// floatFn applies f to one numeric argument and always answers a float, as the trigonometric and logarithmic
// functions do.
func floatFn(f func(float64) float64) scalarFunc {
	return func(args []any) (any, error) {
		if len(args) != 1 {
			return nil, malformed("function takes 1 argument")
		}

		if args[0] == nil {
			return null()
		}

		if !isNumber(args[0]) {
			return nil, malformed("Type mismatch: expected Number but was %s", typeName(args[0]))
		}

		return f(toFloat(args[0])), nil
	}
}

func funcAtan2(args []any) (any, error) {
	if err := argc("atan2", args, pairArgs); err != nil {
		return nil, err
	}

	if args[0] == nil || args[1] == nil {
		return null()
	}

	if !isNumber(args[0]) || !isNumber(args[1]) {
		return nil, malformed("Type mismatch: expected Number")
	}

	return math.Atan2(toFloat(args[0]), toFloat(args[1])), nil
}

func funcJoin(args []any) (any, error) {
	if err := argc("join", args, pairArgs); err != nil {
		return nil, err
	}

	if args[0] == nil || args[1] == nil {
		return null()
	}

	list, ok := args[0].([]any)
	if !ok {
		return nil, malformed("Type mismatch: expected List but was %s", typeName(args[0]))
	}

	sep, ok := args[1].(string)
	if !ok {
		return nil, malformed("Type mismatch: expected String but was %s", typeName(args[1]))
	}

	parts := make([]string, len(list))

	for i, e := range list {
		s, isStr := e.(string)
		if !isStr {
			return nil, malformed("Type mismatch: expected String but was %s", typeName(e))
		}

		parts[i] = s
	}

	return strings.Join(parts, sep), nil
}

func funcRemoveKeyFromMap(args []any) (any, error) {
	if err := argc("removeKeyFromMap", args, pairArgs); err != nil {
		return nil, err
	}

	if args[0] == nil {
		return null()
	}

	m, ok := args[0].(map[string]any)
	if !ok {
		return nil, malformed("Type mismatch: expected Map but was %s", typeName(args[0]))
	}

	key, ok := args[1].(string)
	if !ok {
		return nil, malformed("Type mismatch: expected String but was %s", typeName(args[1]))
	}

	out := make(map[string]any, len(m))

	for k, v := range m {
		if k != key {
			out[k] = v
		}
	}

	return out, nil
}

func datetimeDateLayouts() []string {
	return []string{"2006-01-02", "20060102", "2006-01", "200601", "2006-002", "2006002", "2006"}
}

func datetimeTimeLayouts() []string {
	return []string{
		timeHMSMillis, timeHMS, "150405.000", "150405", "15:04", "1504", "15",
		"15:04:05Z07:00", "15:04:05.000Z07:00", "15:04Z07:00", "15Z07", "150405Z0700", "1504Z0700",
	}
}

// parseDatetime accepts the ISO-8601 date[Ttime[zone]] forms Neptune documents; the result is in UTC.
func parseDatetime(s string) (time.Time, bool) {
	datePart, _, hasTime := strings.Cut(s, "T")

	for _, dl := range datetimeDateLayouts() {
		if !hasTime {
			if t, err := time.Parse(dl, datePart); err == nil {
				return t.UTC(), true
			}

			continue
		}

		for _, tl := range datetimeTimeLayouts() {
			if t, err := time.Parse(dl+"T"+tl, s); err == nil {
				return t.UTC(), true
			}
		}

		if t, err := time.Parse(dl+"TZ07:00", s); err == nil {
			return t.UTC(), true
		}
	}

	return time.Time{}, false
}

func funcDatetime(args []any) (any, error) {
	switch len(args) {
	case 0:
		return time.Now().UTC().Format(datetimeLayout), nil
	case 1:
		if args[0] == nil {
			return null()
		}

		s, ok := args[0].(string)
		if !ok {
			return nil, malformed("Type mismatch: expected String but was %s", typeName(args[0]))
		}

		t, parsed := parseDatetime(s)
		if !parsed {
			return nil, &Error{Code: CodeInvalidParameter, Detail: "Unable to parse '" + s + "' as a datetime"}
		}

		return t.Format(datetimeLayout), nil
	default:
		return nil, malformed("Function datetime() takes at most 1 argument")
	}
}

func asDatetime(v any) (time.Time, error) {
	s, ok := v.(string)
	if !ok {
		return time.Time{}, malformed("Type mismatch: expected DateTime but was %s", typeName(v))
	}

	t, parsed := parseDatetime(s)
	if !parsed {
		return time.Time{}, malformed("Type mismatch: expected DateTime but was String")
	}

	return t, nil
}

func funcEpochMillis(args []any) (any, error) {
	if err := argc("epochMillis", args, 1); err != nil {
		return nil, err
	}

	if args[0] == nil {
		return null()
	}

	t, err := asDatetime(args[0])
	if err != nil {
		return nil, err
	}

	return t.UnixMilli(), nil
}

func temporalPart(part func(time.Time) int) scalarFunc {
	return func(args []any) (any, error) {
		if len(args) != 1 {
			return nil, malformed("function takes 1 argument")
		}

		if args[0] == nil {
			return null()
		}

		t, err := asDatetime(args[0])
		if err != nil {
			return nil, err
		}

		return int64(part(t)), nil
	}
}

func funcTextIndexOf(args []any) (any, error) {
	if len(args) < 2 || len(args) > 4 {
		return nil, malformed("Function textIndexOf() takes 2 to 4 arguments")
	}

	if args[0] == nil || args[1] == nil {
		return null()
	}

	text, ok1 := args[0].(string)
	lookup, ok2 := args[1].(string)

	if !ok1 || !ok2 {
		return nil, malformed("Type mismatch: expected String")
	}

	runes := []rune(text)
	from, to := int64(0), int64(-1)

	for i, dst := range []*int64{&from, &to} {
		if len(args) > 2+i && args[2+i] != nil {
			n, ok := args[2+i].(int64)
			if !ok {
				return nil, malformed("Type mismatch: expected Integer but was %s", typeName(args[2+i]))
			}

			*dst = n
		}
	}

	if to < 0 || to > int64(len(runes)) {
		to = int64(len(runes))
	}

	from = max(from, 0)

	if from > to {
		return int64(-1), nil
	}

	idx := strings.Index(string(runes[from:to]), lookup)
	if idx < 0 {
		return int64(-1), nil
	}

	return from + int64(len([]rune(string(runes[from:to])[:idx]))), nil
}

func collArg(name string, v any) ([]any, bool, error) {
	if v == nil {
		return nil, true, nil
	}

	l, ok := v.([]any)
	if !ok {
		return nil, false, malformed("Type mismatch: %s expected List but was %s", name, typeName(v))
	}

	return l, false, nil
}

func uniqueSorted(items []any) []any {
	seen := map[string]bool{}
	out := make([]any, 0, len(items))

	for _, v := range items {
		if k := valueKey(v); !seen[k] {
			seen[k] = true
			out = append(out, v)
		}
	}

	sort.SliceStable(out, func(i, j int) bool { return order(out[i], out[j]) < 0 })

	return out
}

func funcCollToSet(args []any) (any, error) {
	if err := argc("collToSet", args, 1); err != nil {
		return nil, err
	}

	l, isNull, err := collArg("collToSet", args[0])
	if err != nil || isNull {
		return nil, err
	}

	seen := map[string]bool{}
	out := make([]any, 0, len(l))

	for _, v := range l {
		if k := valueKey(v); !seen[k] {
			seen[k] = true
			out = append(out, v)
		}
	}

	return out, nil
}

func setOperands(name string, args []any) ([]any, []any, bool, error) {
	if err := argc(name, args, pairArgs); err != nil {
		return nil, nil, false, err
	}

	a, n1, err := collArg(name, args[0])
	if err != nil {
		return nil, nil, false, err
	}

	b, n2, err := collArg(name, args[1])
	if err != nil {
		return nil, nil, false, err
	}

	return a, b, n1 || n2, nil
}

func funcCollSubtract(args []any) (any, error) {
	a, b, isNull, err := setOperands("collSubtract", args)
	if err != nil || isNull {
		return nil, err
	}

	drop := map[string]bool{}
	for _, v := range b {
		drop[valueKey(v)] = true
	}

	var keep []any

	for _, v := range a {
		if !drop[valueKey(v)] {
			keep = append(keep, v)
		}
	}

	return uniqueSorted(keep), nil
}

func funcCollIntersection(args []any) (any, error) {
	a, b, isNull, err := setOperands("collIntersection", args)
	if err != nil || isNull {
		return nil, err
	}

	have := map[string]bool{}
	for _, v := range b {
		have[valueKey(v)] = true
	}

	var both []any

	for _, v := range a {
		if have[valueKey(v)] {
			both = append(both, v)
		}
	}

	return uniqueSorted(both), nil
}

type sortKey struct {
	key  string
	desc bool
}

func parseSortConfig(v any) (sortKey, error) {
	m, ok := v.(map[string]any)
	if !ok {
		return sortKey{}, malformed("Type mismatch: sort configuration must be a Map but was %s", typeName(v))
	}

	var k sortKey

	if s, isStr := m[argKey].(string); isStr {
		k.key = s
	}

	if s, isStr := m[argOrder].(string); isStr {
		k.desc = strings.EqualFold(s, orderDesc)
	}

	return k, nil
}

func sortValue(v any, key string) any {
	if key == "" {
		return v
	}

	if p, ok := propsOf(v); ok {
		return p[key]
	}

	return nil
}

func sortItems(items []any, keys []sortKey) []any {
	out := append([]any(nil), items...)

	sort.SliceStable(out, func(i, j int) bool {
		for _, k := range keys {
			c := order(sortValue(out[i], k.key), sortValue(out[j], k.key))
			if c == 0 {
				continue
			}

			if k.desc {
				return c > 0
			}

			return c < 0
		}

		return false
	})

	return out
}

func funcCollSort(args []any) (any, error) {
	if len(args) < 1 || len(args) > 2 {
		return nil, malformed("Function collSort() takes 1 or 2 arguments")
	}

	l, isNull, err := collArg("collSort", args[0])
	if err != nil || isNull {
		return nil, err
	}

	var key sortKey

	if len(args) == 2 && args[1] != nil {
		if key, err = parseSortConfig(args[1]); err != nil {
			return nil, err
		}
	}

	return sortItems(l, []sortKey{key}), nil
}

func funcCollSortMaps(args []any) (any, error) {
	if err := argc("collSortMaps", args, pairArgs); err != nil {
		return nil, err
	}

	l, isNull, err := collArg("collSortMaps", args[0])
	if err != nil || isNull {
		return nil, err
	}

	key, err := parseSortConfig(args[1])
	if err != nil {
		return nil, err
	}

	return sortItems(l, []sortKey{key}), nil
}

func funcCollSortMulti(args []any) (any, error) {
	if len(args) < 1 || len(args) > 4 {
		return nil, malformed("Function collSortMulti() takes 1 to 4 arguments")
	}

	l, isNull, err := collArg("collSortMulti", args[0])
	if err != nil || isNull {
		return nil, err
	}

	var keys []sortKey

	if len(args) > 1 && args[1] != nil {
		if keys, err = parseSortConfigs(args[1]); err != nil {
			return nil, err
		}
	}

	out := sortItems(l, keys)
	limit, skip := int64(-1), int64(0)

	if len(args) > 2 && args[2] != nil {
		limit, _ = args[2].(int64)
	}

	if len(args) > 3 && args[3] != nil {
		skip, _ = args[3].(int64)
	}

	skip = min(max(skip, 0), int64(len(out)))
	out = out[skip:]

	if limit >= 0 && limit < int64(len(out)) {
		out = out[:limit]
	}

	return out, nil
}

func parseSortConfigs(v any) ([]sortKey, error) {
	cfgs, _, err := collArg("collSortMulti", v)
	if err != nil {
		return nil, err
	}

	keys := make([]sortKey, 0, len(cfgs))

	for _, c := range cfgs {
		k, kerr := parseSortConfig(c)
		if kerr != nil {
			return nil, kerr
		}

		keys = append(keys, k)
	}

	return keys, nil
}

// checkedIntOp applies + or * to two integers, reporting overflow of the signed 64-bit range.
func checkedIntOp(op string, a, b int64) (int64, bool) {
	r := new(big.Int)

	if op == "*" {
		r.Mul(big.NewInt(a), big.NewInt(b))
	} else {
		r.Add(big.NewInt(a), big.NewInt(b))
	}

	if !r.IsInt64() {
		return 0, false
	}

	return r.Int64(), true
}
