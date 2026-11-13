package azurearm

import (
	"strconv"
	"strings"
	"time"
)

const (
	logicLocaleEnUS = "en-us"
	logicRoundTrip  = "o"
	widthTwo        = 2
	widthThree      = 3
	widthFour       = 4
	twelve          = 12
)

var logicTimeLayouts = []string{ //nolint:gochecknoglobals // static parse layouts
	time.RFC3339Nano, "2006-01-02T15:04:05.9999999", "2006-01-02T15:04:05", "2006-01-02T15:04",
	"2006-01-02 15:04:05", "2006-01-02", "01/02/2006 15:04:05", "01/02/2006 15:04", "01/02/2006", "1/2/2006",
}

type logicTS struct {
	t       time.Time
	hasZone bool
}

func parseLogicTime(s string) (logicTS, error) {
	for _, l := range logicTimeLayouts {
		if t, err := time.Parse(l, s); err == nil {
			zoned := l == time.RFC3339Nano

			return logicTS{t: t.UTC(), hasZone: zoned}, nil
		}
	}

	return logicTS{}, exprErr("the timestamp %q is not valid", s)
}

var logicStandardFormats = map[string]string{ //nolint:gochecknoglobals // static .NET standard specifiers
	"d": "M/d/yyyy", "D": "dddd, MMMM d, yyyy", "f": "dddd, MMMM d, yyyy h:mm tt",
	"F": "dddd, MMMM d, yyyy h:mm:ss tt", "g": "M/d/yyyy h:mm tt", "G": "M/d/yyyy h:mm:ss tt",
	"m": "MMMM d", "M": "MMMM d", "s": "yyyy-MM-ddTHH:mm:ss", "t": "h:mm tt", "T": "h:mm:ss tt",
	"u": "yyyy-MM-dd HH:mm:ssZ", "U": "dddd, MMMM d, yyyy h:mm:ss tt", "y": "MMMM yyyy", "Y": "MMMM yyyy",
}

type logicLocale struct {
	months [12]string
	days   [7]string
}

//nolint:gochecknoglobals // static locale name tables
var logicLocales = map[string]logicLocale{
	logicLocaleEnUS: {
		months: [12]string{
			"January", "February", "March", "April", "May", "June", "July", "August", "September", "October",
			"November", "December",
		},
		days: [7]string{"Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"},
	},
	"fr-fr": {
		months: [12]string{
			"janvier", "février", "mars", "avril", "mai", "juin", "juillet", "août", "septembre", "octobre",
			"novembre", "décembre",
		},
		days: [7]string{"dimanche", "lundi", "mardi", "mercredi", "jeudi", "vendredi", "samedi"},
	},
	"es-es": {
		months: [12]string{
			"enero", "febrero", "marzo", "abril", "mayo", "junio", "julio", "agosto", "septiembre", "octubre",
			"noviembre", "diciembre",
		},
		days: [7]string{"domingo", "lunes", "martes", "miércoles", "jueves", "viernes", "sábado"},
	},
}

// formatLogicTime renders ts using a .NET standard or custom format string.
func formatLogicTime(ts logicTS, format, locale string) (string, error) {
	loc, ok := logicLocales[strings.ToLower(locale)]
	if !ok {
		if locale != "" {
			return "", exprErr("the locale %q is not supported", locale)
		}

		loc = logicLocales[logicLocaleEnUS]
	}

	if format == logicRoundTrip || format == "O" {
		out := ts.t.Format("2006-01-02T15:04:05.0000000")
		if ts.hasZone {
			out += "Z"
		}

		return out, nil
	}

	if f, std := logicStandardFormats[format]; std {
		format = f
	} else if len(format) == 1 {
		return "", exprErr("the format specifier %q is not valid", format)
	}

	return formatLogicCustom(ts.t, format, loc), nil
}

func countRun(f string, i int) int {
	n := 0
	for i+n < len(f) && f[i+n] == f[i] {
		n++
	}

	return n
}

func formatLogicCustom(t time.Time, f string, loc logicLocale) string {
	var sb strings.Builder

	for i := 0; i < len(f); {
		c := f[i]
		n := countRun(f, i)

		if lit, skip, ok := formatLiteral(f, i, c); ok {
			sb.WriteString(lit)

			i += skip

			continue
		}

		sb.WriteString(formatField(t, c, n, loc))

		i += n
	}

	return sb.String()
}

// formatLiteral handles escapes and quoted runs; skip is how many format characters it consumed.
func formatLiteral(f string, i int, c byte) (string, int, bool) {
	switch c {
	case '\\':
		if i+1 < len(f) {
			return string(f[i+1]), widthTwo, true
		}
	case '\'', '"':
		if end := strings.IndexByte(f[i+1:], c); end >= 0 {
			return f[i+1 : i+1+end], end + widthTwo, true
		}
	}

	return "", 0, false
}

func formatField(t time.Time, c byte, n int, loc logicLocale) string {
	switch c {
	case 'y':
		return fmtYear(t.Year(), n)
	case 'M':
		return fmtMonth(t, n, loc)
	case 'd':
		return fmtDay(t, n, loc)
	case 'H':
		return padNum(t.Hour(), n)
	case 'h':
		h := t.Hour() % twelve
		if h == 0 {
			h = twelve
		}

		return padNum(h, n)
	case 'm':
		return padNum(t.Minute(), n)
	case 's':
		return padNum(t.Second(), n)
	case 'f', 'F':
		return fmtFraction(t, n, c == 'F')
	case 't':
		return fmtMeridiem(t, n)
	case 'K', 'Z':
		return "Z"
	}

	return strings.Repeat(string(c), n)
}

func padNum(v, width int) string {
	s := strconv.Itoa(v)
	for len(s) < width {
		s = "0" + s
	}

	return s
}

func fmtYear(y, n int) string {
	switch n {
	case 1:
		return strconv.Itoa(y % yearMod)
	case widthTwo:
		return padNum(y%yearMod, widthTwo)
	}

	return padNum(y, n)
}

func fmtMonth(t time.Time, n int, loc logicLocale) string {
	switch {
	case n >= widthFour:
		return loc.months[t.Month()-1]
	case n == widthThree:
		return string([]rune(loc.months[t.Month()-1])[:widthThree])
	}

	return padNum(int(t.Month()), n)
}

func fmtDay(t time.Time, n int, loc logicLocale) string {
	switch {
	case n >= widthFour:
		return loc.days[t.Weekday()]
	case n == widthThree:
		return string([]rune(loc.days[t.Weekday()])[:widthThree])
	}

	return padNum(t.Day(), n)
}

func fmtFraction(t time.Time, n int, trim bool) string {
	const maxDigits = 7

	digits := padNum(t.Nanosecond()/100, maxDigits) //nolint:mnd // .NET ticks are 100ns

	n = min(n, maxDigits)
	out := digits[:n]

	if trim {
		out = strings.TrimRight(out, "0")
	}

	return out
}

func fmtMeridiem(t time.Time, n int) string {
	s := "AM"
	if t.Hour() >= twelve {
		s = "PM"
	}

	if n == 1 {
		return s[:1]
	}

	return s
}

func addLogicTimeFuncs(m map[string]logicFunc) {
	m["utcnow"] = func(c *logicEvalCtx, a []*logicExpr) (any, error) {
		format := logicRoundTrip

		if len(a) == 1 {
			v, err := c.eval(a[0])
			if err != nil {
				return nil, err
			}

			format = logicString(v)
		}

		return formatLogicTime(logicTS{t: c.now().UTC(), hasZone: true}, format, "")
	}
	m["formatdatetime"] = eagerLogic("formatDateTime", 1, argsThree, func(a []any) (any, error) {
		ts, err := parseLogicTime(logicString(a[0]))
		if err != nil {
			return nil, err
		}

		format, locale := logicRoundTrip, ""
		if len(a) >= argsTwo {
			format = logicString(a[1])
		}

		if len(a) == argsThree {
			locale = logicString(a[2])
		}

		return formatLogicTime(ts, format, locale)
	})

	for name, unit := range map[string]string{
		"adddays": logicUnitDay, "addhours": logicUnitHour, "addminutes": logicUnitMinute, "addseconds": logicUnitSecond,
	} {
		m[name] = eagerLogic(name, argsTwo, argsThree, func(a []any) (any, error) {
			return logicAddTime(a[0], a[1], unit, 1, optFormat(a, argsTwo))
		})
	}

	m["addtotime"] = eagerLogic("addToTime", argsThree, argsFour, func(a []any) (any, error) {
		return logicAddTime(a[0], a[1], strings.ToLower(logicString(a[2])), 1, optFormat(a, argsThree))
	})
	m["subtractfromtime"] = eagerLogic("subtractFromTime", argsThree, argsFour, func(a []any) (any, error) {
		return logicAddTime(a[0], a[1], strings.ToLower(logicString(a[2])), -1, optFormat(a, argsThree))
	})
	m["startofday"] = startOfFunc("startOfDay", func(t time.Time) time.Time {
		return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
	})
	m["startofhour"] = startOfFunc("startOfHour", func(t time.Time) time.Time {
		return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), 0, 0, 0, time.UTC)
	})
	m["startofmonth"] = startOfFunc("startOfMonth", func(t time.Time) time.Time {
		return time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, time.UTC)
	})
	m["ticks"] = eagerLogic("ticks", 1, 1, func(a []any) (any, error) {
		ts, err := parseLogicTime(logicString(a[0]))
		if err != nil {
			return nil, err
		}

		const ticksPerSecond, epochTicks = int64(10_000_000), int64(621355968000000000)

		return ts.t.Unix()*ticksPerSecond + int64(ts.t.Nanosecond())/100 + epochTicks, nil
	})
}

func optFormat(a []any, i int) string {
	if len(a) > i {
		return logicString(a[i])
	}

	return logicRoundTrip
}

func startOfFunc(name string, f func(time.Time) time.Time) logicFunc {
	return eagerLogic(name, 1, argsTwo, func(a []any) (any, error) {
		ts, err := parseLogicTime(logicString(a[0]))
		if err != nil {
			return nil, err
		}

		return formatLogicTime(logicTS{t: f(ts.t), hasZone: ts.hasZone}, optFormat(a, 1), "")
	})
}

func logicAddTime(tsv, nv any, unit string, sign int64, format string) (any, error) {
	ts, err := parseLogicTime(logicString(tsv))
	if err != nil {
		return nil, err
	}

	n, ok := logicInt(nv)
	if !ok {
		return nil, exprErr("the interval must be an integer")
	}

	n *= sign

	switch unit {
	case logicUnitSecond:
		ts.t = ts.t.Add(time.Duration(n) * time.Second)
	case logicUnitMinute:
		ts.t = ts.t.Add(time.Duration(n) * time.Minute)
	case logicUnitHour:
		ts.t = ts.t.Add(time.Duration(n) * time.Hour)
	case logicUnitDay:
		ts.t = ts.t.AddDate(0, 0, int(n))
	case logicUnitWeek:
		ts.t = ts.t.AddDate(0, 0, int(n)*7) //nolint:mnd // days per week
	case logicUnitMonth:
		ts.t = ts.t.AddDate(0, int(n), 0)
	case "year":
		ts.t = ts.t.AddDate(int(n), 0, 0)
	default:
		return nil, exprErr("the time unit %q is not valid", unit)
	}

	return formatLogicTime(ts, format, "")
}
