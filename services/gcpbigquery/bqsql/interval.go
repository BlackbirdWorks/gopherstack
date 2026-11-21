package bqsql

import (
	"database/sql/driver"
	"fmt"
	"math"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
)

// INTERVAL values are stored as their canonical text "[sign]Y-M [sign]D [sign]H:M:S[.F]"
// (https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/data-types#interval_type).
const (
	monthsPerYear    = 12
	daysPerMonth     = 30
	ivHoursPerDay    = 24
	ivMicrosPerSec   = int64(time.Second / time.Microsecond)
	microsPerMinute  = 60 * ivMicrosPerSec
	microsPerHour    = 60 * microsPerMinute
	microsPerDay     = ivHoursPerDay * microsPerHour
	maxIntervalMon   = 10000 * monthsPerYear
	maxIntervalDays  = 3660000
	maxIntervalMic   = 87840000 * microsPerHour
	partYear         = "YEAR"
	partMonth        = "MONTH"
	partDay          = "DAY"
	partHour         = "HOUR"
	partMinute       = "MINUTE"
	partSecond       = "SECOND"
	ymYear           = 2
	ymMonth          = 3
	ymDay            = 4
	ymHour           = 6
	ymMin            = 7
	ymSec            = 8
	isoDay           = 3
	isoHour          = 4
	isoMin           = 5
	makeIntervalArgs = 6
	mulGuard         = 4
	isoMonth         = 2
)

type ival struct{ months, days, micros int64 }

func intervalRange() []string {
	return []string{partYear, partMonth, partDay, partHour, partMinute, partSecond}
}

var errIntervalRange = errBQ("Interval field value out of range")

func (v ival) check() (ival, error) {
	if v.months < -maxIntervalMon || v.months > maxIntervalMon || v.days < -maxIntervalDays ||
		v.days > maxIntervalDays || v.micros < -maxIntervalMic || v.micros > maxIntervalMic {
		return v, errIntervalRange
	}

	return v, nil
}

func (v ival) String() string {
	sign := func(neg bool) string {
		if neg {
			return "-"
		}

		return ""
	}

	am := abs64(v.months)
	ym := fmt.Sprintf("%s%d-%d", sign(v.months < 0), am/monthsPerYear, am%monthsPerYear)

	au := abs64(v.micros)
	frac := ""

	if f := au % ivMicrosPerSec; f != 0 {
		frac = "." + strings.TrimRight(fmt.Sprintf("%06d", f), "0")
	}

	tm := fmt.Sprintf("%s%d:%d:%d%s", sign(v.micros < 0), au/microsPerHour, au%microsPerHour/microsPerMinute,
		au%microsPerMinute/ivMicrosPerSec, frac)

	return ym + " " + strconv.FormatInt(v.days, 10) + " " + tm
}

func abs64(n int64) int64 {
	if n < 0 {
		return -n
	}

	return n
}

//nolint:gochecknoglobals // compiled once, read-only
var canonicalInterval = sync.OnceValue(func() *regexp.Regexp {
	return regexp.MustCompile(`^(-?)(\d+)-(\d+) ([+-]?\d+) (-?)(\d+):(\d+):(\d+)(?:\.(\d{1,6}))?$`)
})

// parseInterval reads the canonical text form.
func parseInterval(s string) (ival, error) {
	m := canonicalInterval().FindStringSubmatch(strings.TrimSpace(s))
	if m == nil {
		return ival{}, errBQ("Invalid INTERVAL value %q", s)
	}

	n := func(i int) int64 {
		v, _ := strconv.ParseInt(m[i], 10, 64)

		return v
	}

	months := n(ymYear)*monthsPerYear + n(ymMonth)
	if m[1] == "-" {
		months = -months
	}

	frac := int64(0)
	if m[9] != "" {
		frac, _ = strconv.ParseInt((m[9] + "000000")[:6], 10, 64)
	}

	micros := n(ymHour)*microsPerHour + n(ymMin)*microsPerMinute + n(ymSec)*ivMicrosPerSec + frac
	if m[5] == "-" {
		micros = -micros
	}

	return ival{months, n(ymDay), micros}.check()
}

func partIndex(p string) int {
	for i, q := range intervalRange() {
		if q == strings.ToUpper(p) {
			return i
		}
	}

	return -1
}

// intervalOf builds the interval of amount units of part.
func intervalOf(amount int64, part string) (ival, error) {
	var v ival

	if abs64(amount) > 1<<42 {
		return v, errIntervalRange
	}

	switch strings.ToUpper(part) {
	case partYear:
		v.months = amount * monthsPerYear
	case "QUARTER":
		v.months = amount * 3 //nolint:mnd // months per quarter
	case partMonth:
		v.months = amount
	case "WEEK":
		v.days = amount * 7 //nolint:mnd // days per week
	case partDay:
		v.days = amount
	case partHour:
		v.micros = amount * microsPerHour
	case partMinute:
		v.micros = amount * microsPerMinute
	case partSecond:
		v.micros = amount * ivMicrosPerSec
	case "MILLISECOND":
		v.micros = amount * 1000 //nolint:mnd // microseconds per millisecond
	case "MICROSECOND":
		v.micros = amount
	default:
		return v, errBQ("Invalid interval part %s", part)
	}

	return v.check()
}

// intervalFromText parses INTERVAL 'text' FROM TO TO, where the pieces are those of the datetime parts range.
func intervalFromText(text, from, to string) (ival, error) {
	i1, i2 := partIndex(from), partIndex(to)
	if to == "" {
		i2 = i1
	}

	if i1 < 0 || i2 < i1 {
		return ival{}, errBQ("Invalid interval range %s TO %s", from, to)
	}

	parts := intervalRange()[i1 : i2+1]
	has := func(p string) bool { return slices.Contains(parts, p) }

	var v ival

	var groups []func(string) error

	if has(partYear) || has(partMonth) {
		groups = append(groups, func(tok string) error {
			m, err := ymMonths(tok, has(partYear), has(partMonth))
			v.months = m

			return err
		})
	}

	if has(partDay) {
		groups = append(groups, func(tok string) error {
			d, err := strconv.ParseInt(tok, 10, 64)
			v.days = d

			return err
		})
	}

	if has(partHour) || has(partMinute) || has(partSecond) {
		groups = append(groups, func(tok string) error {
			m, err := timeMicros(tok, parts)
			v.micros = m

			return err
		})
	}

	fields := strings.Fields(text)
	if len(fields) != len(groups) {
		return v, errBQ("Invalid INTERVAL value %q", text)
	}

	for i, g := range groups {
		if err := g(fields[i]); err != nil {
			return v, errBQ("Invalid INTERVAL value %q", text)
		}
	}

	return v.check()
}

func ymMonths(tok string, year, month bool) (int64, error) {
	neg := strings.HasPrefix(tok, "-")
	tok = strings.TrimLeft(tok, "+-")

	var y, m int64

	var err error

	switch {
	case year && month:
		ys, ms, ok := strings.Cut(tok, "-")
		if !ok {
			return 0, errBQ("Invalid INTERVAL value %q", tok)
		}

		if y, err = strconv.ParseInt(ys, 10, 64); err == nil {
			m, err = strconv.ParseInt(ms, 10, 64)
		}
	case year:
		y, err = strconv.ParseInt(tok, 10, 64)
	default:
		m, err = strconv.ParseInt(tok, 10, 64)
	}

	if err != nil {
		return 0, errBQ("Invalid INTERVAL value %q", tok)
	}

	total := y*monthsPerYear + m
	if neg {
		total = -total
	}

	return total, nil
}

func timeMicros(tok string, parts []string) (int64, error) {
	neg := strings.HasPrefix(tok, "-")
	tok = strings.TrimLeft(tok, "+-")

	var timeParts []string

	for _, p := range parts {
		if p == partHour || p == partMinute || p == partSecond {
			timeParts = append(timeParts, p)
		}
	}

	segs := strings.Split(tok, ":")
	if len(segs) != len(timeParts) {
		return 0, errBQ("Invalid INTERVAL value %q", tok)
	}

	total := int64(0)

	for i, p := range timeParts {
		seg := segs[i]
		unit := map[string]int64{partHour: microsPerHour, partMinute: microsPerMinute, partSecond: ivMicrosPerSec}[p]

		whole, frac, hasFrac := strings.Cut(seg, ".")

		n, err := strconv.ParseInt(whole, 10, 64)
		if err != nil || hasFrac && p != partSecond {
			return 0, errBQ("Invalid INTERVAL value %q", tok)
		}

		total += n * unit

		if hasFrac {
			f, ferr := strconv.ParseInt((frac + "000000")[:6], 10, 64)
			if ferr != nil {
				return 0, errBQ("Invalid INTERVAL value %q", tok)
			}

			total += f
		}
	}

	if neg {
		total = -total
	}

	return total, nil
}

//nolint:gochecknoglobals // compiled once, read-only
var isoInterval = sync.OnceValue(func() *regexp.Regexp {
	return regexp.MustCompile(`^P(?:(-?\d+)Y)?(?:(-?\d+)M)?(?:(-?\d+)D)?` +
		`(?:T(?:(-?\d+)H)?(?:(-?\d+)M)?(?:(-?\d+(?:\.\d{1,6})?)S)?)?$`)
})

// castInterval is CAST(string AS INTERVAL): the canonical text or an ISO 8601 duration.
func castInterval(s string) (ival, error) {
	if v, err := parseInterval(s); err == nil {
		return v, nil
	}

	m := isoInterval().FindStringSubmatch(strings.TrimSpace(s))
	if m == nil || strings.TrimSpace(s) == "P" {
		return ival{}, errBQ("Invalid INTERVAL value %q", s)
	}

	n := func(i int) int64 {
		v, _ := strconv.ParseInt(m[i], 10, 64)

		return v
	}

	v := ival{
		months: n(1)*monthsPerYear + n(isoMonth), days: n(isoDay),
		micros: n(isoHour)*microsPerHour + n(isoMin)*microsPerMinute,
	}

	if m[6] != "" {
		f, _ := strconv.ParseFloat(m[6], 64)
		v.micros += int64(math.Round(f * float64(ivMicrosPerSec)))
	}

	return v.check()
}

func (v ival) add(o ival) (ival, error) {
	return ival{v.months + o.months, v.days + o.days, v.micros + o.micros}.check()
}
func (v ival) neg() ival { return ival{-v.months, -v.days, -v.micros} }

func (v ival) mul(k int64) (ival, error) {
	if k != 0 && (abs64(v.months) > mulGuard*maxIntervalMon || abs64(v.days) > mulGuard*maxIntervalDays ||
		abs64(v.micros)/abs64(k) > mulGuard*maxIntervalMic) {
		return v, errIntervalRange
	}

	return ival{v.months * k, v.days * k, v.micros * k}.check()
}

// div spills the remainder of each field into the next smaller one (a month is 30 days, a day 24 hours).
func (v ival) div(k int64) (ival, error) {
	if k == 0 {
		return v, errBQ("division by zero")
	}

	mq, mr := v.months/k, v.months%k
	days := v.days + mr*daysPerMonth
	dq, dr := days/k, days%k
	micros := (v.micros + dr*microsPerDay) / k

	return ival{mq, dq, micros}.check()
}

// key orders intervals: a month is 30 days and a day 24 hours.
func (v ival) key() int64 { return (v.months*daysPerMonth+v.days)*microsPerDay + v.micros }

func (v ival) justifyHours() ival {
	v.days += v.micros / microsPerDay
	v.micros %= microsPerDay

	return v
}

func (v ival) justifyDays() ival {
	v.months += v.days / daysPerMonth
	v.days %= daysPerMonth

	return v
}

func (v ival) justify() ival {
	v = v.justifyHours().justifyDays()

	switch {
	case v.months > 0 && (v.days < 0 || v.days == 0 && v.micros < 0):
		v.months--
		v.days += daysPerMonth
	case v.months < 0 && (v.days > 0 || v.days == 0 && v.micros > 0):
		v.months++
		v.days -= daysPerMonth
	}

	switch {
	case v.days > 0 && v.micros < 0:
		v.days--
		v.micros += microsPerDay
	case v.days < 0 && v.micros > 0:
		v.days++
		v.micros -= microsPerDay
	}

	return v
}

func (v ival) extract(part string) (int64, error) {
	switch strings.ToUpper(part) {
	case partYear:
		return v.months / monthsPerYear, nil
	case partMonth:
		return v.months % monthsPerYear, nil
	case partDay:
		return v.days, nil
	case partHour:
		return v.micros / microsPerHour, nil
	case partMinute:
		return v.micros % microsPerHour / microsPerMinute, nil
	case partSecond:
		return v.micros % microsPerMinute / ivMicrosPerSec, nil
	case "MILLISECOND":
		return v.micros % ivMicrosPerSec / 1000, nil //nolint:mnd // microseconds per millisecond
	case "MICROSECOND":
		return v.micros % ivMicrosPerSec, nil
	}

	return 0, errBQ("EXTRACT does not support the %s date part for INTERVAL", part)
}

func ivalArg(v driver.Value) (ival, error) { return parseInterval(toStr(v)) }

func ivalFn(f func(ival) (driver.Value, error)) udfDef {
	return udfDef{n: 1, fn: func(a []driver.Value) (driver.Value, error) {
		if a[0] == nil {
			return nil, nil
		}

		v, err := ivalArg(a[0])
		if err != nil {
			return nil, err
		}

		return f(v)
	}}
}

func ivalBinary(f func(a, b ival) (ival, error)) udfDef {
	return udfDef{n: argc2, fn: func(a []driver.Value) (driver.Value, error) {
		if anyNull(a) {
			return nil, nil
		}

		x, err := ivalArg(a[0])
		if err != nil {
			return nil, err
		}

		y, err := ivalArg(a[1])
		if err != nil {
			return nil, err
		}

		out, err := f(x, y)
		if err != nil {
			return nil, err
		}

		return out.String(), nil
	}}
}

func ivalScale(f func(v ival, k int64) (ival, error)) udfDef {
	return udfDef{n: argc2, fn: func(a []driver.Value) (driver.Value, error) {
		if anyNull(a) {
			return nil, nil
		}

		v, err := ivalArg(a[0])
		if err != nil {
			return nil, err
		}

		k, err := toInt(a[1])
		if err != nil {
			return nil, err
		}

		out, err := f(v, k)
		if err != nil {
			return nil, err
		}

		return out.String(), nil
	}}
}

func addIntervalUDFs(d map[string]udfDef) {
	d["interval_make"] = udfDef{n: argc2, fn: udfIntervalMake}
	d["interval_parse"] = udfDef{n: argc3, fn: udfIntervalParse}
	d["interval_cast"] = udfDef{n: 1, fn: udfIntervalCast}
	d["interval_add"] = ivalBinary(func(a, b ival) (ival, error) { return a.add(b) })
	d["interval_sub"] = ivalBinary(func(a, b ival) (ival, error) { return a.add(b.neg()) })
	d["interval_mul"] = ivalScale(ival.mul)
	d["interval_div"] = ivalScale(ival.div)
	d["interval_neg"] = ivalFn(func(v ival) (driver.Value, error) { return v.neg().String(), nil })
	d["interval_key"] = ivalFn(func(v ival) (driver.Value, error) { return v.key(), nil })
	d["justify_days"] = ivalFn(func(v ival) (driver.Value, error) { return v.justifyDays().String(), nil })
	d["justify_hours"] = ivalFn(func(v ival) (driver.Value, error) { return v.justifyHours().String(), nil })
	d["justify_interval"] = ivalFn(func(v ival) (driver.Value, error) { return v.justify().String(), nil })
	d["interval_extract"] = udfDef{n: argc2, fn: udfIntervalExtract}
	d["make_interval"] = udfDef{n: makeIntervalArgs, fn: udfMakeInterval}
	d["interval_shift"] = udfDef{n: argc3, fn: udfIntervalShift}
	d["interval_diff"] = udfDef{n: argc2, fn: udfIntervalDiff}
}

func udfIntervalMake(a []driver.Value) (driver.Value, error) {
	if anyNull(a) {
		return nil, nil //nolint:nilnil // NULL in, NULL out
	}

	if f, isFloat := a[0].(float64); isFloat && f != math.Trunc(f) {
		return nil, errBQ("INTERVAL amounts must be integers")
	}

	n, err := toInt(a[0])
	if err != nil {
		return nil, err
	}

	v, err := intervalOf(n, toStr(a[1]))
	if err != nil {
		return nil, err
	}

	return v.String(), nil
}

func udfIntervalParse(a []driver.Value) (driver.Value, error) {
	if a[0] == nil {
		return nil, nil //nolint:nilnil // NULL in, NULL out
	}

	v, err := intervalFromText(toStr(a[0]), toStr(a[1]), toStr(a[2]))
	if err != nil {
		return nil, err
	}

	return v.String(), nil
}

func udfIntervalCast(a []driver.Value) (driver.Value, error) {
	if a[0] == nil {
		return nil, nil //nolint:nilnil // NULL in, NULL out
	}

	v, err := castInterval(toStr(a[0]))
	if err != nil {
		return nil, err
	}

	return v.String(), nil
}

func udfIntervalExtract(a []driver.Value) (driver.Value, error) {
	if anyNull(a) {
		return nil, nil //nolint:nilnil // NULL in, NULL out
	}

	v, err := ivalArg(a[0])
	if err != nil {
		return nil, err
	}

	return v.extract(toStr(a[1]))
}

func udfMakeInterval(a []driver.Value) (driver.Value, error) {
	if anyNull(a) {
		return nil, nil //nolint:nilnil // NULL in, NULL out
	}

	var v ival

	for i, part := range intervalRange() {
		n, err := toInt(a[i])
		if err != nil {
			return nil, err
		}

		piece, err := intervalOf(n, part)
		if err != nil {
			return nil, err
		}

		if v, err = v.add(piece); err != nil {
			return nil, err
		}
	}

	return v.String(), nil
}

// udfIntervalShift is value + interval (sign 1) or value - interval (sign -1); a DATE becomes a DATETIME.
func udfIntervalShift(a []driver.Value) (driver.Value, error) {
	if anyNull(a) {
		return nil, nil //nolint:nilnil // NULL in, NULL out
	}

	t, k, err := parseAny(a[0])
	if err != nil {
		return nil, err
	}

	v, err := ivalArg(a[1])
	if err != nil {
		return nil, err
	}

	sign, err := toInt(a[argc2])
	if err != nil {
		return nil, err
	}

	if sign < 0 {
		v = v.neg()
	}

	if k == KindDate {
		k = KindDatetime
	}

	if k != KindTime {
		t = addMonths(t, int(v.months)).AddDate(0, 0, int(v.days))
	}

	return FormatTemporal(k, t.Add(time.Duration(v.micros)*time.Microsecond)), nil
}

// udfIntervalDiff is a - b for two values of the same temporal family.
func udfIntervalDiff(a []driver.Value) (driver.Value, error) {
	if anyNull(a) {
		return nil, nil //nolint:nilnil // NULL in, NULL out
	}

	x, kx, err := parseAny(a[0])
	if err != nil {
		return nil, err
	}

	y, ky, err := parseAny(a[1])
	if err != nil {
		return nil, err
	}

	if kx != ky {
		return nil, errBQ("Cannot subtract values of different temporal types")
	}

	if kx == KindDate {
		return ival{days: int64(x.Sub(y) / (ivHoursPerDay * time.Hour))}.String(), nil
	}

	return ival{micros: x.Sub(y).Microseconds()}.check2()
}

func (v ival) check2() (driver.Value, error) {
	out, err := v.check()
	if err != nil {
		return nil, err
	}

	return out.String(), nil
}

func addIntervalFuncs(t map[string]*fnDef) {
	t["JUSTIFY_DAYS"] = def("justify_days", 1, 1, fixed(KindInterval))
	t["JUSTIFY_HOURS"] = def("justify_hours", 1, 1, fixed(KindInterval))
	t["JUSTIFY_INTERVAL"] = def("justify_interval", 1, 1, fixed(KindInterval))
	t["MAKE_INTERVAL"] = custom(0, aritySix, genNamed("make_interval",
		[]string{"year", "month", "day", "hour", "minute", "second"},
		[]string{"0", "0", "0", "0", "0", "0"}, scalar(KindInterval)))
}

// intervalValue compiles INTERVAL n PART and INTERVAL 'text' PART [TO PART].
func (c *compiler) intervalValue(n *node, sc *scope) (*gx, error) {
	iv := n.args[0]
	typ := scalar(KindInterval)

	if iv.kind == nString {
		sql := "bq_interval_parse(" + quoteStr(iv.str) + ", " + quoteStr(n.name) + ", " + quoteStr(n.str) + ")"

		return &gx{sql: sql, typ: typ}, nil
	}

	if n.str != "" {
		return nil, invalid("INTERVAL %s TO %s needs a string literal", n.name, n.str)
	}

	amount, err := c.expr(iv, sc)
	if err != nil {
		return nil, err
	}

	return &gx{sql: "bq_interval_make(" + amount.sql + ", " + quoteStr(n.name) + ")", typ: typ}, nil
}

func isIntervalTemporal(t Type) bool {
	return t.Kind == KindDate || t.Kind == KindDatetime || t.Kind == KindTimestamp || t.Kind == KindTime
}

// intervalBinary compiles the operators that involve INTERVAL operands; ok is false when none applies.
func intervalBinary(op string, l, r *gx) (*gx, bool, error) {
	li, ri := l.typ.Kind == KindInterval, r.typ.Kind == KindInterval

	switch {
	case !li && !ri:
		return temporalDiff(op, l, r)
	case (op == "+" || op == "-") && ri && isIntervalTemporal(l.typ):
		sign := "1"
		if op == "-" {
			sign = "-1"
		}

		t := l.typ
		if t.Kind == KindDate {
			t = scalar(KindDatetime)
		}

		return &gx{sql: "bq_interval_shift(" + l.sql + ", " + r.sql + ", " + sign + ")", typ: t}, true, nil
	case op == "+" && li && isIntervalTemporal(r.typ):
		return intervalBinary(op, r, l)
	case isComparison(op):
		if !li || !ri {
			return nil, true, invalid("Cannot compare INTERVAL with a value of another type")
		}

		return &gx{
			sql: "(bq_interval_key(" + l.sql + ") " + op + " bq_interval_key(" + r.sql + "))", typ: scalar(KindBool),
		}, true, nil
	}

	return intervalArithmetic(op, l, r)
}

// temporalDiff is date - date, datetime - datetime and timestamp - timestamp.
func temporalDiff(op string, l, r *gx) (*gx, bool, error) {
	if op == "-" && isIntervalTemporal(l.typ) && l.typ.Kind == r.typ.Kind && l.typ.Kind != KindTime {
		return &gx{sql: "bq_interval_diff(" + l.sql + ", " + r.sql + ")", typ: scalar(KindInterval)}, true, nil
	}

	return nil, false, nil
}

func intervalArithmetic(op string, l, r *gx) (*gx, bool, error) {
	li, ri := l.typ.Kind == KindInterval, r.typ.Kind == KindInterval
	fn, known := map[string]string{"+": decAdd, "-": decSub, "*": decMul, "/": decDiv}[op]

	if !known || li && ri && op != "+" && op != "-" || (li != ri) && (op == "+" || op == "-") || !li && op == "/" {
		return nil, true, invalid("Operator %s is not supported for INTERVAL operands", op)
	}

	if ri && !li {
		l, r = r, l
	}

	return &gx{sql: "bq_interval_" + fn + "(" + l.sql + ", " + r.sql + ")", typ: scalar(KindInterval)}, true, nil
}

func isComparison(op string) bool {
	switch op {
	case "=", "!=", "<>", "<", ">", "<=", ">=":
		return true
	}

	return false
}
