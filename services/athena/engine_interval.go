package athena

import (
	"bytes"
	"database/sql/driver"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const (
	typeIntervalYM = "interval year to month"
	typeIntervalDT = "interval day to second"

	ivKindYM byte = 'Y'
	ivKindDT byte = 'D'

	ivLen        = 13
	ivKindOffset = 4
	ivSignFlip   = uint64(1) << 63

	millisPerSecond = 1000
	millisPerMinute = 60 * millisPerSecond
	millisPerHour   = 60 * millisPerMinute
	millisPerDay    = 24 * millisPerHour

	nanosPerMilli  = 1e6
	microsPerMilli = 1e3
)

// ivMagic heads every interval blob so varbinary data is never mistaken for one; the offset-binary
// payload makes SQLite's memcmp ordering of the blob the interval ordering.
var ivMagic = [ivKindOffset]byte{0xF3, 'i', 'v', 0x1D} //nolint:gochecknoglobals // immutable marker

var errTypeMismatch = errors.New("TYPE_MISMATCH")

func errIntervalType(format string, args ...any) error {
	return fmt.Errorf("%w: %s", errTypeMismatch, fmt.Sprintf(format, args...))
}

func encodeInterval(kind byte, n int64) []byte {
	out := make([]byte, ivLen)
	copy(out, ivMagic[:])
	out[ivKindOffset] = kind
	binary.BigEndian.PutUint64(out[ivKindOffset+1:], toU64(n)^ivSignFlip)

	return out
}

func decodeInterval(v any) (byte, int64, bool) {
	b, ok := v.([]byte)
	if !ok || len(b) != ivLen || !bytes.HasPrefix(b, ivMagic[:]) {
		return 0, 0, false
	}

	return b[ivKindOffset], toI64(binary.BigEndian.Uint64(b[ivKindOffset+1:]) ^ ivSignFlip), true
}

func intervalType(kind byte) string {
	if kind == ivKindYM {
		return typeIntervalYM
	}

	return typeIntervalDT
}

// formatInterval renders an interval the way Trino prints it: Y-M and [-]D HH:MM:SS.mmm.
func formatInterval(kind byte, n int64) string {
	sign := ""
	u := toU64(n)

	if n < 0 {
		sign, u = "-", -u
	}

	if kind == ivKindYM {
		return fmt.Sprintf("%s%d-%d", sign, u/monthsPerYear, u%monthsPerYear)
	}

	const perDay, perHour, perMinute, perSecond = millisPerDay, millisPerHour, millisPerMinute, millisPerSecond

	return fmt.Sprintf("%s%d %02d:%02d:%02d.%03d", sign, u/perDay, u%perDay/perHour, u%perHour/perMinute,
		u%perMinute/perSecond, u%perSecond)
}

func ivFieldOrder() []string { return []string{unitDay, unitHour, unitMinute, unitSecond} }

func ivFieldMillis(unit string) int64 {
	switch unit {
	case unitDay:
		return millisPerDay
	case unitHour:
		return millisPerHour
	case unitMinute:
		return millisPerMinute
	default:
		return millisPerSecond
	}
}

func secondsToMillis(s string) (int64, bool) {
	whole, frac, _ := strings.Cut(s, ".")

	n, err := strconv.ParseInt(whole, 10, 64)
	if err != nil {
		return 0, false
	}

	ms := n * millisPerSecond

	if frac != "" {
		f, ferr := strconv.ParseInt((frac + "00")[:3], 10, 64)
		if ferr != nil {
			return 0, false
		}

		ms += f
	}

	return ms, true
}

func ivDayTimeRegexp(from, to string) *regexp.Regexp {
	var sb strings.Builder

	sb.WriteString("^")

	started := false

	for _, f := range ivFieldOrder() {
		if f == from {
			started = true
		}

		if !started {
			continue
		}

		if f != from {
			sb.WriteString(map[string]string{unitHour: " ", unitMinute: ":", unitSecond: ":"}[f])
		}

		if f == unitSecond {
			sb.WriteString(`(\d+(?:\.\d+)?)`)
		} else {
			sb.WriteString(`(\d+)`)
		}

		if f == to {
			break
		}
	}

	sb.WriteString("$")

	return regexp.MustCompile(sb.String())
}

func ivFieldRange(from, to string) []string {
	var out []string

	started := false

	for _, f := range ivFieldOrder() {
		started = started || f == from
		if started {
			out = append(out, f)
		}

		if f == to {
			break
		}
	}

	return out
}

func splitSign(lit string) (int64, string) {
	lit = strings.TrimSpace(lit)

	switch {
	case strings.HasPrefix(lit, "-"):
		return -1, strings.TrimSpace(lit[1:])
	case strings.HasPrefix(lit, "+"):
		return 1, strings.TrimSpace(lit[1:])
	default:
		return 1, lit
	}
}

func parseDayTime(lit, from, to string) (int64, error) {
	if to == "" {
		to = from
	}

	fields := ivFieldRange(from, to)
	sign, body := splitSign(lit)

	m := ivDayTimeRegexp(from, to).FindStringSubmatch(body)
	if m == nil || len(fields) == 0 || fields[len(fields)-1] != to {
		if from == to {
			return 0, badInterval("Invalid INTERVAL value '%s'", lit)
		}

		return 0, badInterval("Invalid INTERVAL %s TO %s value '%s'", strings.ToUpper(from), strings.ToUpper(to), lit)
	}

	var total int64

	for i, f := range fields {
		if f == unitSecond {
			ms, ok := secondsToMillis(m[i+1])
			if !ok {
				return 0, badInterval("Invalid INTERVAL value '%s'", lit)
			}

			total += ms

			continue
		}

		n, err := strconv.ParseInt(m[i+1], 10, 64)
		if err != nil {
			return 0, badInterval("Invalid INTERVAL value '%s'", lit)
		}

		total += n * ivFieldMillis(f)
	}

	return sign * total, nil
}

var ivYearMonth = regexp.MustCompile(`^(\d+)-(\d+)$`)

func parseYearMonth(lit, from, to string) (int64, error) {
	sign, body := splitSign(lit)

	switch {
	case from == unitYear && to == unitMonth:
		m := ivYearMonth.FindStringSubmatch(body)
		if m == nil {
			return 0, badInterval("Invalid INTERVAL YEAR TO MONTH value '%s'", lit)
		}

		y, _ := strconv.ParseInt(m[1], 10, 64)
		mo, _ := strconv.ParseInt(m[2], 10, 64)

		return sign * (y*monthsPerYear + mo), nil
	case to == "" || to == from:
		n, err := strconv.ParseInt(body, 10, 64)
		if err != nil {
			return 0, badInterval("Invalid INTERVAL value '%s'", lit)
		}

		return sign * n * map[string]int64{unitYear: monthsPerYear, unitMonth: 1, unitQuarter: monthsPerQuart}[from], nil
	default:
		return 0, badInterval("INTERVAL %s TO %s is not supported", strings.ToUpper(from), strings.ToUpper(to))
	}
}

// parseIntervalLiteral converts an INTERVAL literal body into a kind and a month or millisecond count.
func parseIntervalLiteral(lit, from, to string) (byte, int64, error) {
	switch from {
	case unitYear, unitMonth, unitQuarter:
		n, err := parseYearMonth(lit, from, to)

		return ivKindYM, n, err
	case unitWeek:
		n, err := parseDayTime(lit, unitDay, unitDay)

		return ivKindDT, n * daysPerWeek, err
	case unitDay, unitHour, unitMinute, unitSecond:
		n, err := parseDayTime(lit, from, to)

		return ivKindDT, n, err
	default:
		return 0, 0, badInterval("Invalid INTERVAL unit '%s'", from)
	}
}

func ivArg(v driver.Value) (byte, int64, bool) { return decodeInterval(v) }

func ivValue(kind byte, n int64) driver.Value { return encodeInterval(kind, n) }

func udfIntervalYM(_ *fnCtx, args []driver.Value) (driver.Value, error) {
	n, _ := toInt(args[0])

	return ivValue(ivKindYM, n), nil
}

func udfIntervalDT(_ *fnCtx, args []driver.Value) (driver.Value, error) {
	n, _ := toInt(args[0])

	return ivValue(ivKindDT, n), nil
}

func udfIntervalStr(_ *fnCtx, args []driver.Value) (driver.Value, error) {
	if kind, n, ok := ivArg(args[0]); ok {
		return formatInterval(kind, n), nil
	}

	return args[0], nil
}

func udfIntervalCast(_ *fnCtx, args []driver.Value) (driver.Value, error) {
	kindName, _ := asString(args[0])
	want := ivKindDT

	if kindName == "ym" {
		want = ivKindYM
	}

	if kind, _, ok := ivArg(args[1]); ok {
		if kind != want {
			return nil, errIntervalType("Cannot cast %s to %s", intervalType(kind), intervalType(want))
		}

		return args[1], nil
	}

	s, ok := asString(args[1])
	if !ok {
		return sqlNull(), nil
	}

	if want == ivKindYM {
		n, err := parseYearMonth(s, unitYear, unitMonth)

		return ivValue(want, n), err
	}

	n, err := parseDayTime(strings.Replace(strings.TrimSpace(s), "  ", " ", 1), unitDay, unitSecond)

	return ivValue(want, n), err
}

func udfToMilliseconds(_ *fnCtx, args []driver.Value) (driver.Value, error) {
	if args[0] == nil {
		return sqlNull(), nil
	}

	kind, n, ok := ivArg(args[0])
	if !ok || kind != ivKindDT {
		return nil, errIntervalType("Unexpected parameters for function to_milliseconds")
	}

	return n, nil
}

func durationMillis(unit string) (float64, bool) {
	switch unit {
	case "ns":
		return 1 / nanosPerMilli, true
	case "us":
		return 1 / microsPerMilli, true
	case "ms":
		return 1, true
	case "s":
		return millisPerSecond, true
	case "m":
		return millisPerMinute, true
	case "h":
		return millisPerHour, true
	case "d":
		return millisPerDay, true
	default:
		return 0, false
	}
}

var durationPattern = regexp.MustCompile(`^\s*(\d+(?:\.\d+)?)\s*([a-zA-Z]+)\s*$`)

func udfParseDuration(_ *fnCtx, args []driver.Value) (driver.Value, error) {
	s, ok := asString(args[0])
	if !ok {
		return sqlNull(), nil
	}

	m := durationPattern.FindStringSubmatch(s)
	if m == nil {
		return nil, errArg("Invalid duration: %s", s)
	}

	mult, known := durationMillis(m[2])
	if !known {
		return nil, errArg("Unknown time unit: %s", m[2])
	}

	v, _ := strconv.ParseFloat(m[1], 64)

	return ivValue(ivKindDT, int64(math.Round(v*mult))), nil
}

// ivPart extracts a field from an interval; ok=false when the value is not an interval.
func ivPart(field string, v driver.Value) (driver.Value, bool, error) {
	kind, n, ok := ivArg(v)
	if !ok {
		return nil, false, nil
	}

	if kind == ivKindYM {
		switch field {
		case unitYear:
			return n / monthsPerYear, true, nil
		case unitMonth:
			return n % monthsPerYear, true, nil
		}
	} else {
		switch field {
		case unitDay:
			return n / millisPerDay, true, nil
		case unitHour:
			return n % millisPerDay / millisPerHour, true, nil
		case unitMinute:
			return n % millisPerHour / millisPerMinute, true, nil
		case unitSecond:
			return n % millisPerMinute / millisPerSecond, true, nil
		case unitMillisecond:
			return n % millisPerSecond, true, nil
		}
	}

	return nil, true, errIntervalType("Unexpected parameters (%s) for function %s", intervalType(kind), field)
}

func udfIntervalPart(_ *fnCtx, args []driver.Value) (driver.Value, error) {
	field, _ := asString(args[0])

	if args[1] == nil {
		return sqlNull(), nil
	}

	if out, ok, err := ivPart(field, args[1]); ok || err != nil {
		return out, err
	}

	return udfTimestampPart(field, args[1])
}

func udfTimestampPart(field string, v driver.Value) (driver.Value, error) {
	t, ok := parseTS(v)
	if !ok {
		return sqlNull(), nil
	}

	switch field {
	case unitYear:
		return int64(t.Year()), nil
	case unitMonth:
		return int64(t.Month()), nil
	case unitDay:
		return int64(t.Day()), nil
	case unitHour:
		return int64(t.Hour()), nil
	case unitMinute:
		return int64(t.Minute()), nil
	default:
		return int64(t.Second()), nil
	}
}

// intervalAware makes a date-part function accept interval values too.
func intervalAware(field string, fn scalarFn) scalarFn {
	return func(c *fnCtx, args []driver.Value) (driver.Value, error) {
		if out, ok, err := ivPart(field, args[0]); ok || err != nil {
			return out, err
		}

		return fn(c, args)
	}
}

func shiftTemporal(v driver.Value, kind byte, n int64, add bool) (driver.Value, error) {
	t, ok := parseTS(v)
	if !ok {
		return nil, errIntervalType("Cannot apply operator to %v and %s", v, intervalType(kind))
	}

	if !add {
		n = -n
	}

	dateOnly := isDateOnly(v)

	if kind == ivKindYM {
		return formatLike(addMonthsClamped(t, int(n)), dateOnly), nil
	}

	if dateOnly {
		if n%millisPerDay != 0 {
			verb := "add hour, minutes or seconds to"
			if !add {
				verb = "subtract hour, minutes or seconds from"
			}

			return nil, errArg("Cannot %s a date", verb)
		}

		return t.AddDate(0, 0, int(n/millisPerDay)).Format(dateLayout), nil
	}

	return t.Add(time.Duration(n) * time.Millisecond).Format(tsLayout), nil
}

func scaleInterval(kind byte, n int64, factor driver.Value, divide bool) (driver.Value, error) {
	f, ok := numericOf(factor)
	if !ok {
		return nil, errIntervalType("Cannot apply operator to %s and %v", intervalType(kind), factor)
	}

	if divide {
		if f == 0 {
			return nil, errArg("Division by zero")
		}

		return ivValue(kind, int64(float64(n)/f)), nil
	}

	return ivValue(kind, int64(float64(n)*f)), nil
}

func intervalOp(op string, a, b driver.Value) (driver.Value, error) {
	ka, na, aiv := ivArg(a)
	kb, nb, biv := ivArg(b)

	switch {
	case aiv && biv:
		return intervalPair(op, ka, na, kb, nb)
	case aiv:
		switch op {
		case "+":
			return shiftTemporal(b, ka, na, true)
		case "*", "/":
			return scaleInterval(ka, na, b, op == "/")
		}
	case biv:
		switch op {
		case "+", "-":
			return shiftTemporal(a, kb, nb, op == "+")
		case "*":
			return scaleInterval(kb, nb, a, false)
		}
	}

	return nil, errIntervalType("Cannot apply operator: %s", op)
}

func intervalPair(op string, ka byte, na int64, kb byte, nb int64) (driver.Value, error) {
	if ka != kb || (op != "+" && op != "-") {
		return nil, errIntervalType("Cannot apply operator: %s %s %s", intervalType(ka), op, intervalType(kb))
	}

	if op == "-" {
		nb = -nb
	}

	return ivValue(ka, na+nb), nil
}

func udfIntervalOp(_ *fnCtx, args []driver.Value) (driver.Value, error) {
	op, _ := asString(args[0])
	a, b := args[1], args[2]

	if a == nil || b == nil {
		return sqlNull(), nil
	}

	_, _, aiv := ivArg(a)
	_, _, biv := ivArg(b)

	if aiv || biv {
		return intervalOp(op, a, b)
	}

	return numericOp(op, a, b)
}

func registerIntervalFuncs() {
	reg := registerScalar
	reg("athena_iv_ym", 1, udfIntervalYM)
	reg("athena_iv_dt", 1, udfIntervalDT)
	reg("athena_iv_str", 1, udfIntervalStr)
	reg("athena_iv_cast", argsTwo, udfIntervalCast)
	reg("athena_iv_op", argsThree, udfIntervalOp)
	reg("athena_iv_part", argsTwo, udfIntervalPart)
	reg("to_milliseconds", 1, udfToMilliseconds)
	reg("parse_duration", 1, udfParseDuration)
}

func toU64(n int64) uint64 { return uint64(n) } //nolint:gosec // two's-complement reinterpretation

func toI64(n uint64) int64 { return int64(n) } //nolint:gosec // two's-complement reinterpretation
