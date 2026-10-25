package kql

import (
	"math"
	"slices"
	"strconv"
	"strings"
)

type aggState interface {
	add(row []any) error
	result() []any
}

type aggSpec struct {
	newState func() aggState
	cols     []Col
	slot     int
}

type aggCollector struct {
	in    *scope
	specs []*aggSpec
	width int
	byIdx map[string]bool
}

var aggNames = map[string]bool{ //nolint:gochecknoglobals // static lookup
	"count": true, "countif": true, "sum": true, "sumif": true, "avg": true, "avgif": true, "min": true,
	"max": true, "dcount": true, "make_list": true, "make_set": true, "any": true, "take_any": true,
	"arg_max": true, "arg_min": true, "percentile": true, "stdev": true, "variance": true,
}

func isAggregate(name string) bool { return aggNames[name] }

func (ac *aggCollector) add(spec *aggSpec) int {
	spec.slot = ac.width
	ac.width += len(spec.cols)
	ac.specs = append(ac.specs, spec)

	return spec.slot
}

func (ac *aggCollector) register(name string, x *callNode, _ *scope) (scalar, error) {
	if name == "arg_max" || name == "arg_min" {
		return scalar{}, unsupported(name + "() inside an expression; use it as a top-level aggregate")
	}

	spec, err := ac.build(name, x)
	if err != nil {
		return scalar{}, err
	}

	slot := ac.add(spec)

	return scalar{typ: spec.cols[0].Type, fn: func(r []any) (any, error) { return r[slot], nil }}, nil
}

func (ac *aggCollector) compileArgs(x *callNode) ([]scalar, error) {
	return ac.in.compileArgs(x.args)
}

func needArgs(x *callNode, min, max int) error {
	if len(x.args) < min || (max >= 0 && len(x.args) > max) {
		return errArgs(x.name, min, len(x.args))
	}

	return nil
}

func (ac *aggCollector) build(name string, x *callNode) (*aggSpec, error) {
	switch name {
	case "count":
		if len(x.args) > 1 || len(x.args) == 1 && !isStar(x.args[0]) {
			return nil, errArgs(x.name, 0, len(x.args))
		}

		return &aggSpec{cols: []Col{{Type: TLong}}, newState: func() aggState { return &countAgg{} }}, nil
	case "countif":
		args, err := ac.args(x, 1, 1)
		if err != nil {
			return nil, err
		}

		return &aggSpec{cols: []Col{{Type: TLong}}, newState: func() aggState {
			return &countAgg{pred: args[0].fn}
		}}, nil
	case "sum", "avg", "stdev", "variance":
		return ac.numericAgg(name, x, nil)
	case "sumif", "avgif":
		args, err := ac.args(x, 2, 2)
		if err != nil {
			return nil, err
		}

		return ac.numericAggArgs(strings.TrimSuffix(name, "if"), args[:1], args[1].fn)
	case "min", "max":
		return ac.extremeAgg(name, x)
	case "dcount":
		args, err := ac.args(x, 1, 2)
		if err != nil {
			return nil, err
		}

		return &aggSpec{cols: []Col{{Type: TLong}}, newState: func() aggState {
			return &dcountAgg{arg: args[0].fn, seen: map[string]bool{}}
		}}, nil
	case "make_list", "make_set":
		args, err := ac.args(x, 1, 2)
		if err != nil {
			return nil, err
		}

		limit := -1
		if len(args) > 1 {
			if v, ok := constOf(args[1]); ok {
				limit = int(intArg(v))
			}
		}

		set := name == "make_set"

		return &aggSpec{cols: []Col{{Type: TDynamic}}, newState: func() aggState {
			return &listAgg{arg: args[0].fn, set: set, limit: limit, seen: map[string]bool{}}
		}}, nil
	case "any", "take_any":
		args, err := ac.args(x, 1, -1)
		if err != nil {
			return nil, err
		}

		return &aggSpec{cols: []Col{{Type: args[0].typ}}, newState: func() aggState {
			return &anyAgg{arg: args[0].fn}
		}}, nil
	case "percentile":
		return ac.percentileAgg(x)
	}

	return nil, unsupported("aggregate " + name + "()")
}

func (ac *aggCollector) args(x *callNode, min, max int) ([]scalar, error) {
	if err := needArgs(x, min, max); err != nil {
		return nil, err
	}

	return ac.compileArgs(x)
}

func (ac *aggCollector) numericAgg(name string, x *callNode, pred func([]any) (any, error)) (*aggSpec, error) {
	args, err := ac.args(x, 1, 1)
	if err != nil {
		return nil, err
	}

	return ac.numericAggArgs(name, args, pred)
}

func (ac *aggCollector) numericAggArgs(name string, args []scalar, pred func([]any) (any, error)) (*aggSpec, error) {
	t := args[0].typ
	if t != TDynamic && t != "" && !isNumType(t) && !(t == TBool && false) {
		return nil, semErr("%s() requires a numeric argument but found %s", name, t)
	}

	rt := TReal
	if name == "sum" && (t == TInt || t == TLong) {
		rt = TLong
	}

	return &aggSpec{cols: []Col{{Type: rt}}, newState: func() aggState {
		return &numAgg{kind: name, arg: args[0].fn, pred: pred, realTyped: t == TReal}
	}}, nil
}

func (ac *aggCollector) extremeAgg(name string, x *callNode) (*aggSpec, error) {
	args, err := ac.args(x, 1, 1)
	if err != nil {
		return nil, err
	}

	sign := 1
	if name == "min" {
		sign = -1
	}

	return &aggSpec{cols: []Col{{Type: args[0].typ}}, newState: func() aggState {
		return &extremeAgg{arg: args[0].fn, sign: sign}
	}}, nil
}

func (ac *aggCollector) percentileAgg(x *callNode) (*aggSpec, error) {
	args, err := ac.args(x, 2, 2)
	if err != nil {
		return nil, err
	}

	pv, ok := constOf(args[1])
	if !ok {
		return nil, semErr("percentile() requires a constant percentile")
	}

	p := floatArg(pv)
	if p < 0 || p > 100 {
		return nil, semErr("percentile() percentile must be between 0 and 100")
	}

	return &aggSpec{cols: []Col{{Type: args[0].typ}}, newState: func() aggState {
		return &percentileAgg{arg: args[0].fn, p: p}
	}}, nil
}

// registerArg builds arg_max/arg_min, which yield several columns.
func (ac *aggCollector) registerArg(x *callNode) ([]Col, error) {
	name := strings.ToLower(x.name)
	if len(x.args) < 2 {
		return nil, errArgs(x.name, 2, len(x.args))
	}

	by, err := ac.in.compile(x.args[0])
	if err != nil {
		return nil, err
	}

	byName := defaultExprName(x.args[0], "")
	if byName == "" {
		byName = strings.TrimPrefix(name, "arg_") + "_"
	}

	cols := []Col{{Name: byName, Type: by.typ}}
	rets := []scalar{}

	for _, a := range x.args[1:] {
		if id, ok := a.(*identNode); ok && id.name == "*" {
			continue
		}

		s, serr := ac.in.compile(a)
		if serr != nil {
			return nil, serr
		}

		rets = append(rets, s)
		cols = append(cols, Col{Name: defaultExprName(a, "Column"), Type: s.typ})
	}

	if star := x.args[len(x.args)-1]; isStar(star) {
		rets = nil
		cols = cols[:1]

		for i, c := range ac.in.cols {
			if c.Name == byName || ac.byIdx[c.Name] {
				continue
			}

			idx := i
			rets = append(rets, scalar{typ: c.Type, fn: func(r []any) (any, error) { return r[idx], nil }})
			cols = append(cols, c)
		}
	}

	sign := 1
	if name == "arg_min" {
		sign = -1
	}

	ac.add(&aggSpec{cols: cols, newState: func() aggState { return &argAgg{by: by.fn, rets: rets, sign: sign} }})

	return cols, nil
}

func isStar(n node) bool {
	id, ok := n.(*identNode)

	return ok && id.name == "*"
}

type countAgg struct {
	pred func([]any) (any, error)
	n    int64
}

func (a *countAgg) add(row []any) error {
	if a.pred != nil {
		v, err := a.pred(row)
		if err != nil {
			return err
		}

		if b, _ := v.(bool); !b {
			return nil
		}
	}

	a.n++

	return nil
}

func (a *countAgg) result() []any { return []any{a.n} }

type numAgg struct {
	arg      func([]any) (any, error)
	pred     func([]any) (any, error)
	kind     string
	vals     []float64
	isum     int64
	fsum     float64
	n        int64
	sawFloat bool
	realTyped bool
}

func (a *numAgg) add(row []any) error {
	if a.pred != nil {
		pv, err := a.pred(row)
		if err != nil {
			return err
		}

		if b, _ := pv.(bool); !b {
			return nil
		}
	}

	v, err := a.arg(row)
	if err != nil || v == nil {
		return err
	}

	switch x := v.(type) {
	case int64:
		a.isum += x
		a.fsum += float64(x)
		a.vals = append(a.vals, float64(x))
	case float64:
		a.sawFloat = true
		a.fsum += x
		a.vals = append(a.vals, x)
	default:
		return semErr("%s() requires numeric values but found %s", a.kind, typeOfValue(v))
	}

	a.n++

	return nil
}

func (a *numAgg) result() []any {
	switch a.kind {
	case "sum":
		if a.sawFloat || a.realTyped {
			return []any{a.fsum}
		}

		return []any{a.isum}
	case "avg":
		if a.n == 0 {
			return []any{nil}
		}

		return []any{a.fsum / float64(a.n)}
	}

	if a.n < 2 {
		return []any{nil}
	}

	mean := a.fsum / float64(a.n)
	ss := 0.0

	for _, v := range a.vals {
		ss += (v - mean) * (v - mean)
	}

	variance := ss / float64(a.n-1)
	if a.kind == "variance" {
		return []any{variance}
	}

	return []any{math.Sqrt(variance)}
}

type extremeAgg struct {
	arg  func([]any) (any, error)
	best any
	sign int
}

func (a *extremeAgg) add(row []any) error {
	v, err := a.arg(row)
	if err != nil || v == nil {
		return err
	}

	if a.best == nil {
		a.best = v

		return nil
	}

	if c, ok := compareValues(v, a.best); ok && c*a.sign > 0 {
		a.best = v
	}

	return nil
}

func (a *extremeAgg) result() []any { return []any{a.best} }

type dcountAgg struct {
	arg  func([]any) (any, error)
	seen map[string]bool
}

func (a *dcountAgg) add(row []any) error {
	v, err := a.arg(row)
	if err != nil || v == nil {
		return err
	}

	a.seen[groupKey(v)] = true

	return nil
}

func (a *dcountAgg) result() []any { return []any{int64(len(a.seen))} }

type listAgg struct {
	arg   func([]any) (any, error)
	seen  map[string]bool
	items []any
	limit int
	set   bool
}

func (a *listAgg) add(row []any) error {
	v, err := a.arg(row)
	if err != nil || v == nil {
		return err
	}

	if a.limit >= 0 && len(a.items) >= a.limit {
		return nil
	}

	if a.set {
		k := groupKey(v)
		if a.seen[k] {
			return nil
		}

		a.seen[k] = true
	}

	a.items = append(a.items, v)

	return nil
}

func (a *listAgg) result() []any {
	if a.items == nil {
		return []any{[]any{}}
	}

	return []any{a.items}
}

type anyAgg struct {
	arg   func([]any) (any, error)
	value any
}

func (a *anyAgg) add(row []any) error {
	if a.value != nil {
		return nil
	}

	v, err := a.arg(row)
	a.value = v

	return err
}

func (a *anyAgg) result() []any { return []any{a.value} }

type percentileAgg struct {
	arg  func([]any) (any, error)
	vals []any
	p    float64
}

func (a *percentileAgg) add(row []any) error {
	v, err := a.arg(row)
	if err != nil || v == nil {
		return err
	}

	if !isNumeric(v) {
		return semErr("percentile() requires numeric values")
	}

	a.vals = append(a.vals, v)

	return nil
}

func (a *percentileAgg) result() []any {
	if len(a.vals) == 0 {
		return []any{nil}
	}

	slices.SortFunc(a.vals, func(x, y any) int { c, _ := compareValues(x, y); return c })

	rank := int(math.Ceil(a.p / 100 * float64(len(a.vals))))
	rank = max(rank, 1)

	return []any{a.vals[rank-1]}
}

type argAgg struct {
	by   func([]any) (any, error)
	best any
	out  []any
	rets []scalar
	sign int
	has  bool
}

func (a *argAgg) add(row []any) error {
	v, err := a.by(row)
	if err != nil || v == nil {
		return err
	}

	if a.has {
		if c, ok := compareValues(v, a.best); !ok || c*a.sign <= 0 {
			return nil
		}
	}

	out := make([]any, 0, len(a.rets)+1)
	out = append(out, v)

	for _, r := range a.rets {
		rv, rerr := r.fn(row)
		if rerr != nil {
			return rerr
		}

		out = append(out, rv)
	}

	a.best, a.out, a.has = v, out, true

	return nil
}

func (a *argAgg) result() []any {
	if !a.has {
		return make([]any, len(a.rets)+1)
	}

	return a.out
}

// defaultExprName derives the output column name KQL gives an unnamed expression.
func defaultExprName(n node, fallback string) string {
	switch x := n.(type) {
	case *identNode:
		return x.name
	case *memberNode:
		if base := defaultExprName(x.x, ""); base != "" {
			return base + "_" + x.name
		}
	case *callNode:
		if strings.EqualFold(x.name, "bin") || strings.EqualFold(x.name, "floor") {
			if len(x.args) > 0 {
				return defaultExprName(x.args[0], fallback)
			}
		}
	}

	return fallback
}

func defaultAggName(x *callNode) string {
	name := strings.ToLower(x.name)
	if len(x.args) == 0 {
		return name + "_"
	}

	arg := defaultExprName(x.args[0], "")
	if name == "percentile" && len(x.args) > 1 {
		if l, ok := x.args[1].(*litNode); ok {
			return name + "_" + arg + "_" + strings.TrimSuffix(strconv.FormatFloat(floatArg(l.v), 'f', -1, 64), ".0")
		}
	}

	return name + "_" + arg
}
