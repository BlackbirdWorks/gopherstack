package iot

import (
	"strconv"
	"strings"
	"time"
)

func timeFuncs() map[string]funcDef {
	return map[string]funcDef{
		"parse_time":    {since2016: true, impl: parseTimeFunc},
		"time_to_epoch": {since2016: true, impl: timeToEpochFunc},
	}
}

// parseTimeFunc formats epoch milliseconds with a Joda-Time pattern (iot-sql-function-parse-time).
func parseTimeFunc(_ *sqlCtx, args []any) any {
	pattern, ok := strArg(args, 0)
	ms, ok2 := toIntConv(arg(args, 1))

	if !ok || !ok2 {
		return sqlUndefined{}
	}

	loc := time.UTC

	if len(args) > zoneArgPos {
		zone, zok := strArg(args, zoneArgPos)
		if !zok {
			return sqlUndefined{}
		}

		l, err := time.LoadLocation(zone)
		if err != nil {
			return sqlUndefined{}
		}

		loc = l
	}

	out, fok := jodaFormat(time.UnixMilli(ms).In(loc), pattern)
	if !fok {
		return sqlUndefined{}
	}

	return out
}

func jodaFormat(t time.Time, pattern string) (string, bool) {
	var sb strings.Builder

	for i := 0; i < len(pattern); {
		ch := pattern[i]

		switch {
		case ch == '\'':
			lit, next := quotedLiteral(pattern, i)
			sb.WriteString(lit)

			i = next
		case isLetter(ch):
			n := runLen(pattern, i)

			part, ok := jodaField(t, ch, n)
			if !ok {
				return "", false
			}

			sb.WriteString(part)

			i += n
		default:
			sb.WriteByte(ch)

			i++
		}
	}

	return sb.String(), true
}

func isLetter(c byte) bool { return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') }

func runLen(s string, i int) int {
	n := 1
	for i+n < len(s) && s[i+n] == s[i] {
		n++
	}

	return n
}

// quotedLiteral reads a '...' literal (two quotes make one) starting at i and returns it with the next index.
func quotedLiteral(s string, i int) (string, int) {
	if i+1 < len(s) && s[i+1] == '\'' {
		return "'", i + doubledQuote
	}

	var sb strings.Builder

	for j := i + 1; j < len(s); j++ {
		if s[j] == '\'' {
			if j+1 < len(s) && s[j+1] == '\'' {
				sb.WriteByte('\'')

				j++

				continue
			}

			return sb.String(), j + 1
		}

		sb.WriteByte(s[j])
	}

	return sb.String(), len(s)
}

func pad(v, n int) string {
	s := strconv.Itoa(v)
	for len(s) < n {
		s = "0" + s
	}

	return s
}

const (
	hoursHalfDay  = 12
	hoursDay      = 24
	shortName     = 3
	fullName      = 4
	yearTwoDigit  = 2
	milliDigits   = 3
	doubledQuote  = 2
	zoneArgPos    = 2
	centuryYears  = 100
	weekdayShift  = 6
	daysInWeek    = 7
	nanosPerMilli = 1_000_000
)

func jodaField(t time.Time, ch byte, n int) (string, bool) {
	switch ch {
	case 'y', 'Y':
		if n == yearTwoDigit {
			return pad(t.Year()%centuryYears, yearTwoDigit), true
		}

		return pad(t.Year(), n), true
	case 'M':
		return monthField(t, n), true
	case 'd':
		return pad(t.Day(), n), true
	case 'D':
		return pad(t.YearDay(), n), true
	case 'E':
		return dayNameField(t, n), true
	case 'e':
		return strconv.Itoa((int(t.Weekday())+weekdayShift)%daysInWeek + 1), true
	case 'G':
		return eraField(t), true
	}

	return jodaTimeField(t, ch, n)
}

func monthField(t time.Time, n int) string {
	switch {
	case n >= fullName:
		return t.Month().String()
	case n == shortName:
		return t.Month().String()[:shortName]
	}

	return pad(int(t.Month()), n)
}

func dayNameField(t time.Time, n int) string {
	if n >= fullName {
		return t.Weekday().String()
	}

	return t.Weekday().String()[:shortName]
}

func eraField(t time.Time) string {
	if t.Year() >= 1 {
		return "AD"
	}

	return "BC"
}

func jodaTimeField(t time.Time, ch byte, n int) (string, bool) {
	switch ch {
	case 'H':
		return pad(t.Hour(), n), true
	case 'h':
		return pad((t.Hour()+hoursHalfDay-1)%hoursHalfDay+1, n), true
	case 'k':
		return pad((t.Hour()+hoursDay-1)%hoursDay+1, n), true
	case 'K':
		return pad(t.Hour()%hoursHalfDay, n), true
	case 'm':
		return pad(t.Minute(), n), true
	case 's':
		return pad(t.Second(), n), true
	case 'S':
		frac := pad(t.Nanosecond()/nanosPerMilli, milliDigits)
		for len(frac) < n {
			frac += "0"
		}

		return frac[:n], true
	case 'a':
		return t.Format("PM"), true
	}

	return zoneField(t, ch, n)
}

func zoneField(t time.Time, ch byte, n int) (string, bool) {
	switch ch {
	case 'z':
		if n >= fullName {
			return t.Location().String(), true
		}

		return t.Format("MST"), true
	case 'Z':
		switch n {
		case 1:
			return t.Format("-0700"), true
		case yearTwoDigit:
			return t.Format("-07:00"), true
		}

		return t.Location().String(), true
	}

	return "", false
}
