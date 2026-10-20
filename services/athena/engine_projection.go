package athena

import (
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"
)

const (
	projEnum     = "enum"
	projInteger  = "integer"
	projDate     = "date"
	projInjected = "injected"

	maxProjectedValues     = 1_000_000
	maxProjectedPartitions = 200_000
)

var errProjection = errors.New("partition projection")

type projectionError struct{ msg string }

func (e *projectionError) Error() string { return e.msg }

func projErr(format string, args ...any) error {
	return &projectionError{"INVALID_TABLE_PROPERTY: " + fmt.Sprintf(format, args...)}
}

// projProps is a table's properties keyed case-insensitively.
type projProps map[string]string

func (p projProps) get(key string) string { return strings.TrimSpace(p[strings.ToLower(key)]) }

func newProjProps(params map[string]string) projProps {
	out := make(projProps, len(params))
	for k, v := range params {
		out[strings.ToLower(k)] = v
	}

	return out
}

func projectionEnabled(def *GlueTableDef) bool {
	return len(def.PartitionKeys) > 0 &&
		strings.EqualFold(newProjProps(def.Parameters).get("projection.enabled"), "true")
}

// wherePred is one `col OP literal` conjunct from a WHERE clause.
type wherePred struct {
	op   string
	vals []string
}

// colFilter collects the conjunctive predicates on one column.
type colFilter struct {
	quals map[string]bool
	preds []wherePred
}

func (f *colFilter) usable() bool { return f != nil && len(f.quals) <= 1 && len(f.preds) > 0 }

func compareVals(a, b string, numeric bool) int {
	if numeric {
		x, xerr := strconv.ParseFloat(a, 64)
		y, yerr := strconv.ParseFloat(b, 64)

		if xerr == nil && yerr == nil {
			switch {
			case x < y:
				return -1
			case x > y:
				return 1
			default:
				return 0
			}
		}
	}

	return strings.Compare(a, b)
}

func (p wherePred) holds(v string, numeric bool) bool {
	switch p.op {
	case "=", "in":
		return slices.ContainsFunc(p.vals, func(c string) bool { return compareVals(v, c, numeric) == 0 })
	case "<":
		return compareVals(v, p.vals[0], numeric) < 0
	case "<=":
		return compareVals(v, p.vals[0], numeric) <= 0
	case ">":
		return compareVals(v, p.vals[0], numeric) > 0
	case ">=":
		return compareVals(v, p.vals[0], numeric) >= 0
	default:
		return true
	}
}

func (f *colFilter) keep(v string, numeric bool) bool {
	if !f.usable() {
		return true
	}

	for _, p := range f.preds {
		if !p.holds(v, numeric) {
			return false
		}
	}

	return true
}

// equalities returns the value set pinned by an = / IN wherePred, if any.
func (f *colFilter) equalities() ([]string, bool) {
	if !f.usable() {
		return nil, false
	}

	for _, p := range f.preds {
		if p.op == "=" || p.op == "in" {
			return p.vals, true
		}
	}

	return nil, false
}

func literalValue(toks []token) (string, bool) {
	sig := significant(toks)

	switch {
	case len(sig) == 1 && sig[0].kind == tkString:
		return stringLiteral(sig[0]), true
	case len(sig) == 1 && sig[0].kind == tkNumber:
		return sig[0].text, true
	case len(sig) == argsTwo && sig[0].isPunct("-") && sig[1].kind == tkNumber:
		return "-" + sig[1].text, true
	default:
		return "", false
	}
}

func columnRef(toks []token) (string, string, bool) {
	sig := significant(toks)

	switch {
	case len(sig) == 1 && (sig[0].kind == tkWord || sig[0].kind == tkQuoted):
		return "", sig[0].ident(), true
	case len(sig) == argsThree && sig[1].isPunct(".") && (sig[2].kind == tkWord || sig[2].kind == tkQuoted):
		return sig[0].ident(), sig[2].ident(), true
	default:
		return "", "", false
	}
}

// splitConjuncts splits a WHERE expression on top-level AND; ok=false when it has a top-level OR.
func splitConjuncts(toks []token) ([][]token, bool) {
	var (
		parts   [][]token
		cur     []token
		depth   int
		between bool
	)

	for _, t := range toks {
		switch {
		case t.isPunct("("):
			depth++
		case t.isPunct(")"):
			depth--
		case depth == 0 && t.isWord("or"):
			return nil, false
		case depth == 0 && t.isWord("between"):
			between = true
		case depth == 0 && t.isWord("and") && between:
			between = false
		case depth == 0 && t.isWord("and"):
			parts = append(parts, cur)
			cur = nil

			continue
		}

		cur = append(cur, t)
	}

	return append(parts, cur), true
}

func opSplit(toks []token) ([]token, string, []token, bool) {
	for i := range toks {
		if toks[i].kind != tkPunct {
			continue
		}

		for _, o := range []string{"<=", ">=", "=", "<", ">"} {
			txt := toks[i].text
			if len(o) == argsTwo {
				if i+1 >= len(toks) || toks[i+1].kind != tkPunct {
					continue
				}

				txt += toks[i+1].text
			}

			if txt == o {
				return toks[:i], o, toks[i+len(o):], true
			}
		}
	}

	return nil, "", nil, false
}

func (f *colFilter) add(qual string, p wherePred) {
	f.quals[qual] = true
	f.preds = append(f.preds, p)
}

type whereFilters map[string]*colFilter

func (w whereFilters) add(qual, col string, p wherePred) {
	f := w[col]
	if f == nil {
		f = &colFilter{quals: map[string]bool{}}
		w[col] = f
	}

	f.add(qual, p)
}

func (w whereFilters) conjunct(c []token) {
	if left, op, right, ok := opSplit(c); ok {
		q, col, cok := columnRef(left)
		v, vok := literalValue(right)

		if cok && vok {
			w.add(q, col, wherePred{op: op, vals: []string{v}})
		}

		return
	}

	sig := significant(c)
	if in := firstTopLevelWord(sig, "in"); in > 0 {
		w.inList(sig, in)
	} else if bt := firstTopLevelWord(sig, "between"); bt > 0 {
		w.between(sig, bt)
	}
}

func (w whereFilters) inList(sig []token, in int) {
	q, col, ok := columnRef(sig[:in])
	rest := sig[in+1:]

	if !ok || len(rest) < argsTwo || !rest[0].isPunct("(") || !rest[len(rest)-1].isPunct(")") {
		return
	}

	var vals []string

	for _, part := range splitArgs(rest[1 : len(rest)-1]) {
		v, vok := literalValue(part)
		if !vok {
			return
		}

		vals = append(vals, v)
	}

	w.add(q, col, wherePred{op: "in", vals: vals})
}

func (w whereFilters) between(sig []token, bt int) {
	q, col, ok := columnRef(sig[:bt])
	and := firstTopLevelWord(sig[bt+1:], "and")

	if !ok || and < 0 {
		return
	}

	lo, lok := literalValue(sig[bt+1 : bt+1+and])
	hi, hok := literalValue(sig[bt+1+and+1:])

	if lok && hok {
		w.add(q, col, wherePred{op: ">=", vals: []string{lo}})
		w.add(q, col, wherePred{op: "<=", vals: []string{hi}})
	}
}

// extractWhereFilters gathers simple column predicates from every WHERE clause in the query.
func extractWhereFilters(query string) whereFilters {
	out := whereFilters{}

	toks, err := tokenize(query)
	if err != nil {
		return out
	}

	for i, t := range toks {
		if !t.isWord("where") {
			continue
		}

		end := orderByLikeEnd(toks, i+1)

		conj, ok := splitConjuncts(toks[i+1 : end])
		if !ok {
			continue
		}

		for _, c := range conj {
			out.conjunct(c)
		}
	}

	return out
}

func orderByLikeEnd(toks []token, from int) int {
	depth := 0

	for i := from; i < len(toks); i++ {
		switch {
		case toks[i].isPunct("("):
			depth++
		case toks[i].isPunct(")"):
			depth--
			if depth < 0 {
				return i
			}
		case depth == 0 && toks[i].kind == tkWord && isClauseEnder(strings.ToLower(toks[i].text)):
			return i
		}
	}

	return len(toks)
}

// projectedColumn is one partition key's projection settings.
type projectedColumn struct {
	key   Column
	props projProps
	typ   string
}

func (c projectedColumn) prop(name string) string {
	return c.props.get("projection." + strings.ToLower(c.key.Name) + "." + name)
}

func (c projectedColumn) numeric() bool { return kindOf(c.key.Type) == kindInt }

func (c projectedColumn) values(f *colFilter, now time.Time) ([]string, error) {
	switch c.typ {
	case projEnum:
		return c.enumValues(f), nil
	case projInteger:
		return c.integerValues(f)
	case projDate:
		return c.dateValues(f, now)
	case projInjected:
		eq, ok := f.equalities()
		if !ok {
			return nil, projErr("Column '%s' uses injected partition projection; a constant filter "+
				"(%s = <value> or IN) is required in the WHERE clause", c.key.Name, c.key.Name)
		}

		return slices.Clone(eq), nil
	default:
		return nil, projErr("projection.%s.type must be one of enum, integer, date, injected (got '%s')",
			c.key.Name, c.typ)
	}
}

func (c projectedColumn) enumValues(f *colFilter) []string {
	var out []string

	for v := range strings.SplitSeq(c.prop("values"), ",") {
		if v = strings.TrimSpace(v); v != "" && f.keep(v, c.numeric()) {
			out = append(out, v)
		}
	}

	return out
}

func parseRange(raw string) (string, string, bool) {
	lo, hi, ok := strings.Cut(raw, ",")

	return strings.TrimSpace(lo), strings.TrimSpace(hi), ok
}

func (c projectedColumn) integerValues(f *colFilter) ([]string, error) {
	loS, hiS, ok := parseRange(c.prop("range"))
	lo, lerr := strconv.ParseInt(loS, 10, 64)
	hi, herr := strconv.ParseInt(hiS, 10, 64)
	step := int64(1)

	if s := c.prop("interval"); s != "" {
		n, err := strconv.ParseInt(s, 10, 64)
		if err != nil || n < 1 {
			return nil, projErr("projection.%s.interval must be a positive integer", c.key.Name)
		}

		step = n
	}

	if !ok || lerr != nil || herr != nil || hi < lo {
		return nil, projErr("projection.%s.range must be 'min,max' integers", c.key.Name)
	}

	digits, _ := strconv.Atoi(c.prop("digits"))

	var out []string

	for v := lo; v <= hi; v += step {
		s := fmt.Sprintf("%0*d", digits, v)
		if f.keep(strconv.FormatInt(v, 10), true) {
			out = append(out, s)
		}

		if len(out) > maxProjectedValues {
			return nil, projErr("projection.%s.range yields too many values", c.key.Name)
		}
	}

	return out, nil
}

var nowExpr = regexp.MustCompile(
	`(?i)^NOW(?:\s*([+-])\s*(\d+)\s*(YEARS?|MONTHS?|WEEKS?|DAYS?|HOURS?|MINUTES?|SECONDS?))?$`,
)

func unitDuration(unit string, n int, t time.Time) time.Time {
	switch strings.ToLower(strings.TrimSuffix(strings.ToLower(unit), "s")) {
	case unitYear:
		return t.AddDate(n, 0, 0)
	case unitMonth:
		return t.AddDate(0, n, 0)
	case unitWeek:
		return t.AddDate(0, 0, n*daysPerWeek)
	case unitDay:
		return t.AddDate(0, 0, n)
	case unitHour:
		return t.Add(time.Duration(n) * time.Hour)
	case unitMinute:
		return t.Add(time.Duration(n) * time.Minute)
	default:
		return t.Add(time.Duration(n) * time.Second)
	}
}

func parseProjDate(s, layout string, now time.Time) (time.Time, error) {
	if m := nowExpr.FindStringSubmatch(s); m != nil {
		if m[1] == "" {
			return now, nil
		}

		n, _ := strconv.Atoi(m[2])
		if m[1] == "-" {
			n = -n
		}

		return unitDuration(m[3], n, now), nil
	}

	return time.ParseInLocation(layout, s, time.UTC)
}

func finestUnit(format string) string {
	switch {
	case strings.Contains(format, "ss"):
		return unitSecond
	case strings.Contains(format, "mm"):
		return unitMinute
	case strings.Contains(format, "HH"), strings.Contains(format, "H"):
		return unitHour
	case strings.Contains(format, "d"):
		return unitDay
	case strings.Contains(format, "M"):
		return unitMonth
	default:
		return unitYear
	}
}

func (c projectedColumn) dateValues(f *colFilter, now time.Time) ([]string, error) {
	format := c.prop("format")
	if format == "" {
		return nil, projErr("projection.%s.format is required for date projection", c.key.Name)
	}

	layout := jodaToGo(format)
	loS, hiS, ok := parseRange(c.prop("range"))

	lo, lerr := parseProjDate(loS, layout, now)
	hi, herr := parseProjDate(hiS, layout, now)

	if !ok || lerr != nil || herr != nil {
		return nil, projErr("projection.%s.range must be 'start,end' dates in format %s or NOW", c.key.Name, format)
	}

	step := 1

	if s := c.prop("interval"); s != "" {
		n, err := strconv.Atoi(s)
		if err != nil || n < 1 {
			return nil, projErr("projection.%s.interval must be a positive integer", c.key.Name)
		}

		step = n
	}

	unit := orDefault(strings.ToLower(c.prop("interval.unit")), finestUnit(format))

	var out []string

	for t := lo; !t.After(hi); t = unitDuration(unit, step, t) {
		if s := t.Format(layout); f.keep(s, false) {
			out = append(out, s)
		}

		if len(out) > maxProjectedValues {
			return nil, projErr("projection.%s.range yields too many values", c.key.Name)
		}
	}

	return out, nil
}

// projectPartitions computes the partitions a projection-enabled table exposes,
// pruned by the query's WHERE predicates; the S3 prefixes are never listed otherwise.
func projectPartitions(def *GlueTableDef, filters whereFilters, now time.Time) ([]GluePartitionDef, error) {
	props := newProjProps(def.Parameters)
	lists := make([][]string, len(def.PartitionKeys))
	total := 1

	for i, k := range def.PartitionKeys {
		col := projectedColumn{key: k, props: props}
		col.typ = strings.ToLower(col.prop("type"))

		vals, err := col.values(filters[strings.ToLower(k.Name)], now)
		if err != nil {
			return nil, err
		}

		lists[i] = vals

		if total *= max(len(vals), 1); total > maxProjectedPartitions {
			return nil, &projectionError{fmt.Sprintf("EXCEEDED_LIMIT: %v would enumerate more than %d partitions; "+
				"add partition filters to the WHERE clause", errProjection, maxProjectedPartitions)}
		}
	}

	return crossPartitions(def, props.get("storage.location.template"), lists), nil
}

func crossPartitions(def *GlueTableDef, template string, lists [][]string) []GluePartitionDef {
	combos := [][]string{nil}

	for _, vals := range lists {
		next := make([][]string, 0, len(combos)*len(vals))

		for _, c := range combos {
			for _, v := range vals {
				next = append(next, append(slices.Clone(c), v))
			}
		}

		combos = next
	}

	out := make([]GluePartitionDef, 0, len(combos))
	for _, c := range combos {
		out = append(out, GluePartitionDef{Values: c, Location: projectedLocation(def, template, c)})
	}

	return out
}

func projectedLocation(def *GlueTableDef, template string, vals []string) string {
	if template == "" {
		return partitionLocation(def, GluePartitionDef{Values: vals})
	}

	loc := template
	for i, k := range def.PartitionKeys {
		loc = strings.ReplaceAll(loc, "${"+k.Name+"}", vals[i])
	}

	return loc
}

// applyProjection replaces the Glue partitions of projection-enabled tables.
func applyProjection(plan *enginePlan, query string, now time.Time) error {
	var filters whereFilters

	for _, pt := range plan.tables {
		if !projectionEnabled(pt.def) {
			continue
		}

		if filters == nil {
			filters = extractWhereFilters(query)
		}

		parts, err := projectPartitions(pt.def, ownFilters(plan, pt, filters), now)
		if err != nil {
			return err
		}

		pt.parts = parts
	}

	return nil
}

// ownFilters drops filters on columns that more than one planned table defines.
func ownFilters(plan *enginePlan, pt *planTable, filters whereFilters) whereFilters {
	out := whereFilters{}

	for _, k := range pt.def.PartitionKeys {
		name := strings.ToLower(k.Name)
		shared := 0

		for _, other := range plan.tables {
			if slices.ContainsFunc(
				tableColumns(other.def),
				func(c Column) bool { return strings.EqualFold(c.Name, name) },
			) {
				shared++
			}
		}

		if f, ok := filters[name]; ok && shared == 1 {
			out[name] = f
		}
	}

	return out
}
