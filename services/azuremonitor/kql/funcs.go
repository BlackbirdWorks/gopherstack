package kql

import (
	"encoding/base64"
	"encoding/json"
	"math"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

type callCtx struct {
	now time.Time
}

type fnDef struct {
	ret      func(args []Type) (Type, error)
	call     func(c *callCtx, a []any) (any, error)
	min, max int
	nullable bool
}

func fixed(t Type) func([]Type) (Type, error) {
	return func([]Type) (Type, error) { return t, nil }
}

func sameAsFirst(a []Type) (Type, error) { return a[0], nil }

func parseJSONValue(s string) (any, error) {
	var v any

	dec := json.NewDecoder(strings.NewReader(s))
	dec.UseNumber()

	if err := dec.Decode(&v); err != nil {
		return nil, err
	}

	return NormalizeJSON(v), nil
}

func str(v any) string { return ToString(v) }

func intArg(v any) int64 {
	n, _ := toInt(v)

	return n
}

func floatArg(v any) float64 {
	f, _ := toFloat(v)

	return f
}

func mathFn(f func(float64) float64) fnDef {
	return fnDef{min: 1, max: 1, ret: fixed(TReal), call: func(_ *callCtx, a []any) (any, error) {
		x, ok := toFloat(a[0])
		if !ok {
			return nil, semErr("a numeric argument was expected")
		}

		r := f(x)
		if math.IsNaN(r) {
			return nil, nil //nolint:nilnil // domain errors yield null
		}

		return r, nil
	}}
}

func keepNumeric(f func(float64) float64) fnDef {
	return fnDef{min: 1, max: 1, ret: sameAsFirst, call: func(_ *callCtx, a []any) (any, error) {
		switch x := a[0].(type) {
		case int64:
			return int64(f(float64(x))), nil
		case float64:
			return f(x), nil
		}

		return nil, semErr("a numeric argument was expected")
	}}
}

func datePart(f func(time.Time) int64, t Type) fnDef {
	return fnDef{min: 1, max: 1, ret: fixed(t), call: func(_ *callCtx, a []any) (any, error) {
		d, ok := a[0].(time.Time)
		if !ok {
			return nil, semErr("a datetime argument was expected")
		}

		return f(d), nil
	}}
}

func startOf(f func(time.Time) time.Time) fnDef {
	return fnDef{min: 1, max: 2, ret: fixed(TDatetime), call: func(_ *callCtx, a []any) (any, error) {
		d, ok := a[0].(time.Time)
		if !ok {
			return nil, semErr("a datetime argument was expected")
		}

		off := int64(0)
		if len(a) > 1 {
			off = intArg(a[1])
		}

		return f(d), offsetNote(off)
	}}
}

func offsetNote(off int64) error {
	if off != 0 {
		return unsupported("a non-zero offset argument")
	}

	return nil
}

func dayStart(t time.Time) time.Time {
	y, m, d := t.UTC().Date()

	return time.Date(y, m, d, 0, 0, 0, 0, time.UTC)
}

func weekStart(t time.Time) time.Time {
	d := dayStart(t)

	return d.AddDate(0, 0, -int(d.Weekday()))
}

func monthStart(t time.Time) time.Time {
	y, m, _ := t.UTC().Date()

	return time.Date(y, m, 1, 0, 0, 0, 0, time.UTC)
}

func yearStart(t time.Time) time.Time { return time.Date(t.UTC().Year(), 1, 1, 0, 0, 0, 0, time.UTC) }

func endOf(f func(time.Time) time.Time, add func(time.Time) time.Time) fnDef {
	return startOf(func(t time.Time) time.Time { return add(f(t)).Add(-100 * time.Nanosecond) })
}

//nolint:gochecknoglobals // static function table
var scalarFuncs map[string]fnDef

func init() { //nolint:gochecknoinits // table references itself via helpers
	scalarFuncs = map[string]fnDef{
		"tostring": {min: 1, max: 1, nullable: true, ret: fixed(TString), call: func(_ *callCtx, a []any) (any, error) {
			return str(a[0]), nil
		}},
		"toint":      convFn(TInt, toLongValue),
		"tolong":     convFn(TLong, toLongValue),
		"toreal":     convFn(TReal, toRealValue),
		"todouble":   convFn(TReal, toRealValue),
		"todecimal":  convFn(TReal, toRealValue),
		"tobool":     convFn(TBool, toBoolValue),
		"toboolean":  convFn(TBool, toBoolValue),
		"todatetime": convFn(TDatetime, toDatetimeValue),
		"totimespan": convFn(TTimespan, toTimespanValue),
		"todynamic":  convFn(TDynamic, toDynamicValue),
		"parse_json": convFn(TDynamic, toDynamicValue),
		"tolower": {min: 1, max: 1, ret: fixed(TString), call: func(_ *callCtx, a []any) (any, error) {
			return strings.ToLower(str(a[0])), nil
		}},
		"toupper": {min: 1, max: 1, ret: fixed(TString), call: func(_ *callCtx, a []any) (any, error) {
			return strings.ToUpper(str(a[0])), nil
		}},
		"strlen": {min: 1, max: 1, ret: fixed(TLong), call: func(_ *callCtx, a []any) (any, error) {
			return int64(utf8.RuneCountInString(str(a[0]))), nil
		}},
		"substring": {min: 2, max: 3, ret: fixed(TString), call: fnSubstring},
		"strcat":    {min: 1, max: -1, nullable: true, ret: fixed(TString), call: fnStrcat},
		"strcat_delim": {min: 3, max: -1, nullable: true, ret: fixed(TString), call: func(_ *callCtx, a []any) (any, error) {
			parts := make([]string, 0, len(a)-1)
			for _, v := range a[1:] {
				parts = append(parts, str(v))
			}

			return strings.Join(parts, str(a[0])), nil
		}},
		"replace_string": {min: 3, max: 3, ret: fixed(TString), call: func(_ *callCtx, a []any) (any, error) {
			return strings.ReplaceAll(str(a[0]), str(a[1]), str(a[2])), nil
		}},
		"replace_regex": {min: 3, max: 3, ret: fixed(TString), call: fnReplaceRegex},
		"split":         {min: 2, max: 2, ret: fixed(TDynamic), call: fnSplit},
		"indexof":       {min: 2, max: 4, ret: fixed(TLong), call: fnIndexOf},
		"trim":          {min: 2, max: 2, ret: fixed(TString), call: trimFn(true, true)},
		"trim_start":    {min: 2, max: 2, ret: fixed(TString), call: trimFn(true, false)},
		"trim_end":      {min: 2, max: 2, ret: fixed(TString), call: trimFn(false, true)},
		"reverse": {min: 1, max: 1, ret: fixed(TString), call: func(_ *callCtx, a []any) (any, error) {
			rs := []rune(str(a[0]))
			for i, j := 0, len(rs)-1; i < j; i, j = i+1, j-1 {
				rs[i], rs[j] = rs[j], rs[i]
			}

			return string(rs), nil
		}},
		"strrep":  {min: 2, max: 3, ret: fixed(TString), call: fnStrrep},
		"extract": {min: 3, max: 3, ret: fixed(TString), call: fnExtract},
		"countof": {min: 2, max: 3, ret: fixed(TLong), call: func(_ *callCtx, a []any) (any, error) {
			if str(a[1]) == "" {
				return int64(0), nil
			}

			return int64(strings.Count(str(a[0]), str(a[1]))), nil
		}},
		"isempty": {min: 1, max: 1, nullable: true, ret: fixed(TBool), call: func(_ *callCtx, a []any) (any, error) {
			return a[0] == nil || a[0] == "", nil
		}},
		"isnotempty": {min: 1, max: 1, nullable: true, ret: fixed(TBool), call: func(_ *callCtx, a []any) (any, error) {
			return !(a[0] == nil || a[0] == ""), nil
		}},
		"isnull": {min: 1, max: 1, nullable: true, ret: fixed(TBool), call: func(_ *callCtx, a []any) (any, error) {
			return a[0] == nil, nil
		}},
		"isnotnull": {min: 1, max: 1, nullable: true, ret: fixed(TBool), call: func(_ *callCtx, a []any) (any, error) {
			return a[0] != nil, nil
		}},
		"isnan": {min: 1, max: 1, ret: fixed(TBool), call: func(_ *callCtx, a []any) (any, error) {
			return math.IsNaN(floatArg(a[0])), nil
		}},
		"isfinite": {min: 1, max: 1, ret: fixed(TBool), call: func(_ *callCtx, a []any) (any, error) {
			f := floatArg(a[0])

			return !math.IsNaN(f) && !math.IsInf(f, 0), nil
		}},
		"isinf": {min: 1, max: 1, ret: fixed(TBool), call: func(_ *callCtx, a []any) (any, error) {
			return math.IsInf(floatArg(a[0]), 0), nil
		}},
		"coalesce": {min: 1, max: -1, nullable: true, ret: firstNonDynamic, call: func(_ *callCtx, a []any) (any, error) {
			for _, v := range a {
				if v != nil && v != "" {
					return v, nil
				}
			}

			return nil, nil //nolint:nilnil // all null
		}},
		"iff":  {min: 3, max: 3, nullable: true, ret: branchType(1, 2), call: fnIff},
		"iif":  {min: 3, max: 3, nullable: true, ret: branchType(1, 2), call: fnIff},
		"case": {min: 3, max: -1, nullable: true, ret: caseType, call: fnCase},
		"not": {min: 1, max: 1, nullable: true, ret: fixed(TBool), call: func(_ *callCtx, a []any) (any, error) {
			if a[0] == nil {
				return nil, nil //nolint:nilnil // not(null) is null
			}

			b, ok := a[0].(bool)
			if !ok {
				return nil, semErr("not() expects a boolean argument")
			}

			return !b, nil
		}},
		"ago": {min: 1, max: 1, ret: fixed(TDatetime), call: func(c *callCtx, a []any) (any, error) {
			d, ok := a[0].(time.Duration)
			if !ok {
				return nil, semErr("ago() expects a timespan")
			}

			return c.now.Add(-d), nil
		}},
		"now": {min: 0, max: 1, ret: fixed(TDatetime), call: func(c *callCtx, a []any) (any, error) {
			if len(a) == 1 {
				if d, ok := a[0].(time.Duration); ok {
					return c.now.Add(d), nil
				}
			}

			return c.now, nil
		}},
		"bin":   {min: 2, max: 2, ret: sameAsFirst, call: fnBin},
		"floor": {min: 2, max: 2, ret: sameAsFirst, call: fnBin},
		"abs": {min: 1, max: 1, ret: sameAsFirst, call: func(_ *callCtx, a []any) (any, error) {
			switch x := a[0].(type) {
			case int64:
				return max(x, -x), nil
			case float64:
				return math.Abs(x), nil
			}

			return nil, semErr("abs() expects a number")
		}},
		"ceiling": keepNumeric(math.Ceil),
		"round":   {min: 1, max: 2, ret: sameAsFirst, call: fnRound},
		"sqrt":    mathFn(math.Sqrt),
		"exp":     mathFn(math.Exp),
		"exp2":    mathFn(math.Exp2),
		"exp10":   mathFn(func(x float64) float64 { return math.Pow(10, x) }),
		"log":     mathFn(math.Log),
		"log2":    mathFn(math.Log2),
		"log10":   mathFn(math.Log10),
		"pow": {min: 2, max: 2, ret: fixed(TReal), call: func(_ *callCtx, a []any) (any, error) {
			return math.Pow(floatArg(a[0]), floatArg(a[1])), nil
		}},
		"sign": {min: 1, max: 1, ret: fixed(TLong), call: func(_ *callCtx, a []any) (any, error) {
			f := floatArg(a[0])

			switch {
			case f > 0:
				return int64(1), nil
			case f < 0:
				return int64(-1), nil
			}

			return int64(0), nil
		}},
		"min_of": {min: 1, max: -1, ret: sameAsFirst, call: extremum(-1)},
		"max_of": {min: 1, max: -1, ret: sameAsFirst, call: extremum(1)},
		"datetime_add": {min: 3, max: 3, ret: fixed(TDatetime), call: fnDatetimeAdd},
		"datetime_diff": {min: 3, max: 3, ret: fixed(TLong), call: fnDatetimeDiff},
		"startofday":   startOf(dayStart),
		"startofweek":  startOf(weekStart),
		"startofmonth": startOf(monthStart),
		"startofyear":  startOf(yearStart),
		"endofday": endOf(dayStart, func(t time.Time) time.Time { return t.AddDate(0, 0, 1) }),
		"endofweek": endOf(weekStart, func(t time.Time) time.Time { return t.AddDate(0, 0, 7) }),
		"endofmonth": endOf(monthStart, func(t time.Time) time.Time { return t.AddDate(0, 1, 0) }),
		"endofyear": endOf(yearStart, func(t time.Time) time.Time { return t.AddDate(1, 0, 0) }),
		"dayofweek": {min: 1, max: 1, ret: fixed(TTimespan), call: func(_ *callCtx, a []any) (any, error) {
			d, ok := a[0].(time.Time)
			if !ok {
				return nil, semErr("a datetime argument was expected")
			}

			return time.Duration(d.UTC().Weekday()) * 24 * time.Hour, nil
		}},
		"dayofmonth":  datePart(func(t time.Time) int64 { return int64(t.UTC().Day()) }, TInt),
		"dayofyear":   datePart(func(t time.Time) int64 { return int64(t.UTC().YearDay()) }, TInt),
		"hourofday":   datePart(func(t time.Time) int64 { return int64(t.UTC().Hour()) }, TInt),
		"getyear":     datePart(func(t time.Time) int64 { return int64(t.UTC().Year()) }, TInt),
		"getmonth":    datePart(func(t time.Time) int64 { return int64(t.UTC().Month()) }, TInt),
		"monthofyear": datePart(func(t time.Time) int64 { return int64(t.UTC().Month()) }, TInt),
		"week_of_year": datePart(func(t time.Time) int64 {
			_, w := t.UTC().ISOWeek()

			return int64(w)
		}, TInt),
		"format_datetime": {min: 2, max: 2, ret: fixed(TString), call: fnFormatDatetime},
		"make_datetime":   {min: 3, max: 7, ret: fixed(TDatetime), call: fnMakeDatetime},
		"unixtime_seconds_todatetime": {min: 1, max: 1, ret: fixed(TDatetime), call: func(_ *callCtx, a []any) (any, error) {
			return time.Unix(0, int64(floatArg(a[0])*1e9)).UTC(), nil
		}},
		"array_length": {min: 1, max: 1, ret: fixed(TLong), call: func(_ *callCtx, a []any) (any, error) {
			if arr, ok := asArray(a[0]); ok {
				return int64(len(arr)), nil
			}

			return nil, nil //nolint:nilnil // non-arrays yield null
		}},
		"set_has_element": {min: 2, max: 2, ret: fixed(TBool), call: func(_ *callCtx, a []any) (any, error) {
			arr, _ := asArray(a[0])

			return indexIn(arr, a[1]) >= 0, nil
		}},
		"array_index_of": {min: 2, max: 2, ret: fixed(TLong), call: func(_ *callCtx, a []any) (any, error) {
			arr, _ := asArray(a[0])

			return int64(indexIn(arr, a[1])), nil
		}},
		"gettype": {min: 1, max: 1, nullable: true, ret: fixed(TString), call: func(_ *callCtx, a []any) (any, error) {
			return string(getType(a[0])), nil
		}},
		"url_encode": {min: 1, max: 1, ret: fixed(TString), call: func(_ *callCtx, a []any) (any, error) {
			return url.QueryEscape(str(a[0])), nil
		}},
		"url_decode": {min: 1, max: 1, ret: fixed(TString), call: func(_ *callCtx, a []any) (any, error) {
			s, err := url.QueryUnescape(str(a[0]))
			if err != nil {
				return nil, nil //nolint:nilnil // invalid escape yields null
			}

			return s, nil
		}},
		"base64_encode_tostring": {min: 1, max: 1, ret: fixed(TString), call: func(_ *callCtx, a []any) (any, error) {
			return base64.StdEncoding.EncodeToString([]byte(str(a[0]))), nil
		}},
		"base64_decode_tostring": {min: 1, max: 1, ret: fixed(TString), call: func(_ *callCtx, a []any) (any, error) {
			b, err := base64.StdEncoding.DecodeString(str(a[0]))
			if err != nil {
				return nil, nil //nolint:nilnil // invalid base64 yields null
			}

			return string(b), nil
		}},
		"__has_any": {min: 2, max: -1, ret: fixed(TBool), call: hasAnyAll(false)},
		"__has_all": {min: 2, max: -1, ret: fixed(TBool), call: hasAnyAll(true)},
	}
}

func getType(v any) Type {
	if v == nil {
		return "null"
	}

	if _, ok := v.([]any); ok {
		return "array"
	}

	if _, ok := v.(map[string]any); ok {
		return "dictionary"
	}

	return typeOfValue(v)
}

func asArray(v any) ([]any, bool) {
	if s, ok := v.(string); ok {
		parsed, err := parseJSONValue(s)
		if err != nil {
			return nil, false
		}

		v = parsed
	}

	arr, ok := v.([]any)

	return arr, ok
}

func indexIn(arr []any, v any) int {
	for i, e := range arr {
		if c, ok := compareValues(e, v); ok && c == 0 {
			return i
		}
	}

	return -1
}

func hasAnyAll(all bool) func(*callCtx, []any) (any, error) {
	return func(_ *callCtx, a []any) (any, error) {
		hay := strings.ToLower(str(a[0]))
		matched := 0

		for _, n := range a[1:] {
			if hasTerm(hay, strings.ToLower(str(n)), true, true) {
				matched++
			}
		}

		if all {
			return matched == len(a)-1, nil
		}

		return matched > 0, nil
	}
}

func firstNonDynamic(a []Type) (Type, error) {
	for _, t := range a {
		if t != TDynamic && t != "" {
			return t, nil
		}
	}

	return TDynamic, nil
}

func branchType(i, j int) func([]Type) (Type, error) {
	return func(a []Type) (Type, error) {
		switch {
		case a[i] == a[j]:
			return a[i], nil
		case isNumType(a[i]) && isNumType(a[j]):
			return TReal, nil
		}

		return TDynamic, nil
	}
}

func caseType(a []Type) (Type, error) {
	t := a[1]

	for i := 3; i < len(a); i += 2 {
		if a[i] != t {
			return TDynamic, nil
		}
	}

	if len(a)%2 == 1 && a[len(a)-1] != t {
		return TDynamic, nil
	}

	return t, nil
}

func fnIff(_ *callCtx, a []any) (any, error) {
	b, _ := a[0].(bool)
	if b {
		return a[1], nil
	}

	return a[2], nil
}

func fnCase(_ *callCtx, a []any) (any, error) {
	i := 0
	for ; i+1 < len(a); i += 2 {
		if b, _ := a[i].(bool); b {
			return a[i+1], nil
		}
	}

	if i < len(a) {
		return a[i], nil
	}

	return nil, nil //nolint:nilnil // no branch matched
}

func convFn(t Type, f func(any) any) fnDef {
	return fnDef{min: 1, max: 1, ret: fixed(t), call: func(_ *callCtx, a []any) (any, error) {
		return f(a[0]), nil
	}}
}

func toLongValue(v any) any {
	switch x := v.(type) {
	case string:
		if n, err := strconv.ParseInt(strings.TrimSpace(x), 10, 64); err == nil {
			return n
		}

		if f, err := strconv.ParseFloat(strings.TrimSpace(x), 64); err == nil {
			return int64(f)
		}

		return nil
	case time.Duration:
		return int64(x) / nanosPerTick
	case time.Time:
		return nil
	}

	if n, ok := toInt(v); ok {
		return n
	}

	return nil
}

func toRealValue(v any) any {
	if s, ok := v.(string); ok {
		f, err := strconv.ParseFloat(strings.TrimSpace(s), 64)
		if err != nil {
			return nil
		}

		return f
	}

	if f, ok := toFloat(v); ok {
		return f
	}

	return nil
}

func toBoolValue(v any) any {
	switch x := v.(type) {
	case bool:
		return x
	case string:
		switch strings.ToLower(strings.TrimSpace(x)) {
		case "true":
			return true
		case "false":
			return false
		}
	case int64:
		return x != 0
	}

	return nil
}

func toDatetimeValue(v any) any {
	switch x := v.(type) {
	case time.Time:
		return x
	case string:
		if t, ok := ParseDatetime(x); ok {
			return t
		}
	}

	return nil
}

func toTimespanValue(v any) any {
	switch x := v.(type) {
	case time.Duration:
		return x
	case string:
		if d, ok := ParseTimespan(x); ok {
			return d
		}
	case int64:
		return time.Duration(x) * nanosPerTick
	}

	return nil
}

func toDynamicValue(v any) any {
	if s, ok := v.(string); ok {
		parsed, err := parseJSONValue(s)
		if err != nil {
			return s
		}

		return parsed
	}

	return v
}

func fnSubstring(_ *callCtx, a []any) (any, error) {
	rs := []rune(str(a[0]))
	start := int(intArg(a[1]))

	if start < 0 {
		start = 0
	}

	if start >= len(rs) {
		return "", nil
	}

	end := len(rs)

	if len(a) > 2 {
		n := int(intArg(a[2]))
		if n < 0 {
			return "", nil
		}

		end = min(len(rs), start+n)
	}

	return string(rs[start:end]), nil
}

func fnStrcat(_ *callCtx, a []any) (any, error) {
	var sb strings.Builder

	for _, v := range a {
		sb.WriteString(str(v))
	}

	return sb.String(), nil
}

var backrefRe = regexp.MustCompile(`\\(\d)`)

func fnReplaceRegex(_ *callCtx, a []any) (any, error) {
	re, err := compileRegex(str(a[1]))
	if err != nil {
		return nil, err
	}

	rep := backrefRe.ReplaceAllString(strings.ReplaceAll(str(a[2]), "$", "$$"), "$${$1}")

	return re.ReplaceAllString(str(a[0]), rep), nil
}

func fnSplit(_ *callCtx, a []any) (any, error) {
	parts := strings.Split(str(a[0]), str(a[1]))
	out := make([]any, len(parts))

	for i, p := range parts {
		out[i] = p
	}

	return out, nil
}

func fnIndexOf(_ *callCtx, a []any) (any, error) {
	rs := []rune(str(a[0]))
	needle := []rune(str(a[1]))
	start := 0

	if len(a) > 2 {
		start = int(intArg(a[2]))
	}

	end := len(rs)

	if len(a) > 3 {
		if l := int(intArg(a[3])); l >= 0 {
			end = min(end, start+l)
		}
	}

	if start < 0 || start > len(rs) {
		return int64(-1), nil
	}

	idx := strings.Index(string(rs[start:end]), string(needle))
	if idx < 0 {
		return int64(-1), nil
	}

	return int64(start + utf8.RuneCountInString(string(rs[start:end])[:idx])), nil
}

func trimFn(front, back bool) func(*callCtx, []any) (any, error) {
	return func(_ *callCtx, a []any) (any, error) {
		s := str(a[1])

		pat := "(?:" + str(a[0]) + ")"
		if front {
			re, err := compileRegex("^" + pat + "+")
			if err != nil {
				return nil, err
			}

			s = re.ReplaceAllString(s, "")
		}

		if back {
			re, err := compileRegex(pat + "+$")
			if err != nil {
				return nil, err
			}

			s = re.ReplaceAllString(s, "")
		}

		return s, nil
	}
}

func fnStrrep(_ *callCtx, a []any) (any, error) {
	n := int(intArg(a[1]))
	if n <= 0 || n > 1024 {
		return "", nil
	}

	delim := ""
	if len(a) > 2 {
		delim = str(a[2])
	}

	parts := make([]string, n)
	for i := range parts {
		parts[i] = str(a[0])
	}

	return strings.Join(parts, delim), nil
}

func fnExtract(_ *callCtx, a []any) (any, error) {
	re, err := compileRegex(str(a[0]))
	if err != nil {
		return nil, err
	}

	g := int(intArg(a[1]))

	m := re.FindStringSubmatch(str(a[2]))
	if m == nil || g < 0 || g >= len(m) {
		return nil, nil //nolint:nilnil // no match
	}

	return m[g], nil
}

func fnBin(_ *callCtx, a []any) (any, error) {
	switch x := a[0].(type) {
	case int64:
		if r, ok := a[1].(int64); ok {
			if r == 0 {
				return nil, nil //nolint:nilnil // zero bin size
			}

			q := x / r
			if x%r != 0 && (x < 0) != (r < 0) {
				q--
			}

			return q * r, nil
		}

		if r, ok := a[1].(float64); ok && r != 0 {
			return math.Floor(float64(x)/r) * r, nil
		}
	case float64:
		if r, ok := toFloat(a[1]); ok && r != 0 {
			return math.Floor(x/r) * r, nil
		}
	case time.Duration:
		if r, ok := a[1].(time.Duration); ok && r != 0 {
			return time.Duration(floorDiv(int64(x), int64(r))) * r, nil
		}
	case time.Time:
		if r, ok := a[1].(time.Duration); ok && r >= 100*time.Nanosecond {
			ticks := x.UnixNano()/nanosPerTick + ticksAtUnixEpoch
			rt := int64(r) / nanosPerTick
			b := floorDiv(ticks, rt) * rt

			return time.Unix(0, (b-ticksAtUnixEpoch)*nanosPerTick).UTC(), nil
		}
	}

	return nil, semErr("bin() arguments are not valid")
}

func floorDiv(a, b int64) int64 {
	q := a / b
	if a%b != 0 && (a < 0) != (b < 0) {
		q--
	}

	return q
}

func fnRound(_ *callCtx, a []any) (any, error) {
	digits := 0
	if len(a) > 1 {
		digits = int(intArg(a[1]))
	}

	switch x := a[0].(type) {
	case int64:
		return x, nil
	case float64:
		p := math.Pow(10, float64(digits))

		return math.Round(x*p) / p, nil
	}

	return nil, semErr("round() expects a number")
}

func extremum(sign int) func(*callCtx, []any) (any, error) {
	return func(_ *callCtx, a []any) (any, error) {
		best := a[0]

		for _, v := range a[1:] {
			if c, ok := compareValues(v, best); ok && c*sign > 0 {
				best = v
			}
		}

		return best, nil
	}
}

func fnDatetimeAdd(_ *callCtx, a []any) (any, error) {
	t, ok := a[2].(time.Time)
	if !ok {
		return nil, semErr("datetime_add() expects a datetime")
	}

	n := int(intArg(a[1]))

	switch strings.ToLower(str(a[0])) {
	case "year":
		return t.AddDate(n, 0, 0), nil
	case "quarter":
		return t.AddDate(0, 3*n, 0), nil
	case "month":
		return t.AddDate(0, n, 0), nil
	case "week":
		return t.AddDate(0, 0, 7*n), nil
	case "day":
		return t.AddDate(0, 0, n), nil
	case "hour":
		return t.Add(time.Duration(n) * time.Hour), nil
	case "minute":
		return t.Add(time.Duration(n) * time.Minute), nil
	case "second":
		return t.Add(time.Duration(n) * time.Second), nil
	case "millisecond":
		return t.Add(time.Duration(n) * time.Millisecond), nil
	case "microsecond":
		return t.Add(time.Duration(n) * time.Microsecond), nil
	}

	return nil, semErr("datetime_add(): unknown part %q", str(a[0]))
}

func fnDatetimeDiff(_ *callCtx, a []any) (any, error) {
	t1, ok1 := a[1].(time.Time)
	t2, ok2 := a[2].(time.Time)

	if !ok1 || !ok2 {
		return nil, semErr("datetime_diff() expects datetimes")
	}

	t1, t2 = t1.UTC(), t2.UTC()

	switch strings.ToLower(str(a[0])) {
	case "year":
		return int64(t1.Year() - t2.Year()), nil
	case "quarter":
		return (int64(t1.Year())*12 + int64(t1.Month()) - 1) / 3 - (int64(t2.Year())*12+int64(t2.Month())-1)/3, nil
	case "month":
		return int64(t1.Year()*12+int(t1.Month())) - int64(t2.Year()*12+int(t2.Month())), nil
	case "week":
		return int64(weekStart(t1).Sub(weekStart(t2)) / (7 * 24 * time.Hour)), nil
	case "day":
		return int64(dayStart(t1).Sub(dayStart(t2)) / (24 * time.Hour)), nil
	case "hour":
		return int64(t1.Truncate(time.Hour).Sub(t2.Truncate(time.Hour)) / time.Hour), nil
	case "minute":
		return int64(t1.Truncate(time.Minute).Sub(t2.Truncate(time.Minute)) / time.Minute), nil
	case "second":
		return int64(t1.Truncate(time.Second).Sub(t2.Truncate(time.Second)) / time.Second), nil
	case "millisecond":
		return t1.Sub(t2).Milliseconds(), nil
	case "microsecond":
		return t1.Sub(t2).Microseconds(), nil
	}

	return nil, semErr("datetime_diff(): unknown part %q", str(a[0]))
}

var fmtTokens = []struct{ tok, layout string }{ //nolint:gochecknoglobals // static lookup
	{"yyyy", "2006"}, {"yy", "06"}, {"MM", "01"}, {"dd", "02"}, {"HH", "15"}, {"hh", "03"}, {"mm", "04"},
	{"ss", "05"}, {"fff", ".000"}, {"tt", "PM"},
}

func fnFormatDatetime(_ *callCtx, a []any) (any, error) {
	t, ok := a[0].(time.Time)
	if !ok {
		return nil, semErr("format_datetime() expects a datetime")
	}

	f := str(a[1])

	var sb strings.Builder

	for i := 0; i < len(f); {
		matched := false

		for _, tk := range fmtTokens {
			if strings.HasPrefix(f[i:], tk.tok) {
				out := t.UTC().Format(tk.layout)
				if tk.tok == "fff" {
					out = out[1:]
				}

				sb.WriteString(out)

				i += len(tk.tok)
				matched = true

				break
			}
		}

		if !matched {
			sb.WriteByte(f[i])

			i++
		}
	}

	return sb.String(), nil
}

func fnMakeDatetime(_ *callCtx, a []any) (any, error) {
	parts := [6]int{0, 1, 1, 0, 0, 0}
	for i := 0; i < len(a) && i < 6; i++ {
		parts[i] = int(intArg(a[i]))
	}

	return time.Date(parts[0], time.Month(parts[1]), parts[2], parts[3], parts[4], parts[5], 0, time.UTC), nil
}
