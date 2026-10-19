package athena

import (
	"database/sql/driver"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const (
	unixNanosPerSecond = 1e9
	nanosPerMicro      = 1000
	jodaQuotePair      = 2
)

var badMySQLSpecifier = regexp.MustCompile(`%[^YymcdeHkhIisSfpMbWaTj%]`)

func mysqlToGo(format string) (string, error) {
	if m := badMySQLSpecifier.FindString(format); m != "" {
		return "", fmt.Errorf("%w: unsupported date format specifier %s", errBadValue, m)
	}

	return strings.NewReplacer(
		"%Y", "2006", "%y", "06", "%m", "1", "%c", "1", "%d", "2", "%e", "2", "%H", "15", "%k", "15",
		"%h", "3", "%I", "3", "%i", "4", "%s", "5", "%S", "5", "%f", ".000000", "%p", "PM",
		"%M", "January", "%b", "Jan", "%W", "Monday", "%a", "Mon", "%T", "15:04:05", "%j", "002", "%%", "%",
	).Replace(format), nil
}

func mysqlFormatToken(t time.Time, c byte) (string, bool) {
	switch c {
	case 'Y', 'y', 'H', 'p', 'M', 'b', 'W', 'a', 'T', 'j', 'h', 'I':
		return t.Format(mysqlLayout(c)), true
	case 'm':
		return t.Format("01"), true
	case 'c':
		return t.Format("1"), true
	case 'd':
		return t.Format("02"), true
	case 'e':
		return t.Format("2"), true
	case 'f':
		return fmt.Sprintf("%06d", t.Nanosecond()/nanosPerMicro), true
	case 'k':
		return strconv.Itoa(t.Hour()), true
	case 'i':
		return t.Format("04"), true
	case 's', 'S':
		return t.Format("05"), true
	case '%':
		return "%", true
	default:
		return "", false
	}
}

func mysqlLayout(c byte) string {
	switch c {
	case 'Y':
		return "2006"
	case 'y':
		return "06"
	case 'H':
		return "15"
	case 'h', 'I':
		return "03"
	case 'p':
		return "PM"
	case 'M':
		return "January"
	case 'b':
		return "Jan"
	case 'W':
		return "Monday"
	case 'a':
		return "Mon"
	case 'T':
		return "15:04:05"
	default:
		return "002"
	}
}

func formatMySQL(t time.Time, format string) (string, error) {
	var sb strings.Builder

	for i := 0; i < len(format); i++ {
		if format[i] != '%' || i+1 >= len(format) {
			sb.WriteByte(format[i])

			continue
		}

		i++

		s, ok := mysqlFormatToken(t, format[i])
		if !ok {
			return "", fmt.Errorf("%w: unsupported date format specifier %%%c", errBadValue, format[i])
		}

		sb.WriteString(s)
	}

	return sb.String(), nil
}

func fmtTS(t time.Time) string { return t.UTC().Format(tsLayout) }

func udfDateFormat(_ *fnCtx, args []driver.Value) (driver.Value, error) {
	t, ok := parseTS(args[0])
	f, fok := asString(args[1])

	if !ok || !fok {
		return sqlNull(), nil
	}

	return formatMySQL(t, f)
}

func udfDateParse(_ *fnCtx, args []driver.Value) (driver.Value, error) {
	s, ok := asString(args[0])
	f, fok := asString(args[1])

	if !ok || !fok {
		return sqlNull(), nil
	}

	layout, err := mysqlToGo(f)
	if err != nil {
		return nil, err
	}

	t, perr := time.Parse(layout, strings.TrimSpace(s))
	if perr != nil {
		return nil, fmt.Errorf("%w: Invalid format: %q", errBadValue, s)
	}

	return fmtTS(t), nil
}

var jodaTokens = []struct{ joda, goFmt string }{ //nolint:gochecknoglobals // immutable lookup
	{"yyyy", "2006"}, {"yy", "06"}, {"MMMM", "January"}, {"MMM", "Jan"}, {"MM", "01"}, {"M", "1"},
	{"dd", "02"}, {"d", "2"}, {"HH", "15"}, {"H", "15"}, {"hh", "03"}, {"h", "3"}, {"mm", "04"},
	{"ss", "05"}, {"SSS", ".000"}, {"EEEE", "Monday"}, {"EEE", "Mon"}, {"a", "PM"},
	{"ZZ", "-07:00"}, {"Z", "-0700"}, {"z", "MST"},
}

func jodaToGo(format string) string {
	var sb strings.Builder

	for i := 0; i < len(format); {
		if format[i] == '\'' {
			end := strings.IndexByte(format[i+1:], '\'')
			if end < 0 {
				end = len(format) - i - 1
			}

			sb.WriteString(format[i+1 : i+1+end])
			i += end + jodaQuotePair

			continue
		}

		matched := false

		for _, tk := range jodaTokens {
			if strings.HasPrefix(format[i:], tk.joda) {
				sb.WriteString(tk.goFmt)

				i += len(tk.joda)
				matched = true

				break
			}
		}

		if !matched {
			sb.WriteByte(format[i])

			i++
		}
	}

	return sb.String()
}

func udfFormatDatetime(_ *fnCtx, args []driver.Value) (driver.Value, error) {
	t, ok := parseTS(args[0])
	f, fok := asString(args[1])

	if !ok || !fok {
		return sqlNull(), nil
	}

	return t.Format(jodaToGo(f)), nil
}

func udfParseDatetime(_ *fnCtx, args []driver.Value) (driver.Value, error) {
	s, ok := asString(args[0])
	f, fok := asString(args[1])

	if !ok || !fok {
		return sqlNull(), nil
	}

	t, err := time.Parse(jodaToGo(f), strings.TrimSpace(s))
	if err != nil {
		return nil, fmt.Errorf("%w: Invalid format: %q", errBadValue, s)
	}

	return fmtTS(t), nil
}

func udfFromISO8601Timestamp(_ *fnCtx, args []driver.Value) (driver.Value, error) {
	s, ok := asString(args[0])
	if !ok {
		return sqlNull(), nil
	}

	t, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(s))
	if err != nil {
		t2, perr := parseISOLoose(s)
		if perr != nil {
			return nil, fmt.Errorf("%w: Invalid format: %q", errBadValue, s)
		}

		t = t2
	}

	return fmtTS(t), nil
}

func parseISOLoose(s string) (time.Time, error) {
	for _, l := range []string{"2006-01-02T15:04:05.999999999", "2006-01-02T15:04:05", "2006-01-02"} {
		if t, err := time.Parse(l, strings.TrimSpace(s)); err == nil {
			return t, nil
		}
	}

	return time.Time{}, errBadValue
}

func udfFromISO8601Date(_ *fnCtx, args []driver.Value) (driver.Value, error) {
	s, ok := asString(args[0])
	if !ok {
		return sqlNull(), nil
	}

	t, err := parseISOLoose(s)
	if err != nil {
		return nil, fmt.Errorf("%w: Invalid format: %q", errBadValue, s)
	}

	return t.Format(dateLayout), nil
}

func udfToISO8601(_ *fnCtx, args []driver.Value) (driver.Value, error) {
	t, ok := parseTS(args[0])
	if !ok {
		return sqlNull(), nil
	}

	if isDateOnly(args[0]) {
		return t.Format(dateLayout), nil
	}

	return t.Format("2006-01-02T15:04:05.000Z"), nil
}

func udfFromUnixtime(_ *fnCtx, args []driver.Value) (driver.Value, error) {
	f, ok := toFloatArg(args[0])
	if !ok {
		return sqlNull(), nil
	}

	sec, frac := math.Modf(f)

	return fmtTS(time.Unix(int64(sec), int64(frac*unixNanosPerSecond))), nil
}

func udfToUnixtime(_ *fnCtx, args []driver.Value) (driver.Value, error) {
	t, ok := parseTS(args[0])
	if !ok {
		return sqlNull(), nil
	}

	return float64(t.UnixNano()) / unixNanosPerSecond, nil
}

func tsPart(f func(time.Time) int64) func(*fnCtx, []driver.Value) (driver.Value, error) {
	return datePart(f)
}

func isoDayOfWeek(t time.Time) int64 {
	if t.Weekday() == time.Sunday {
		return daysPerWeek
	}

	return int64(t.Weekday())
}

func isoWeek(t time.Time) int64 {
	_, w := t.ISOWeek()

	return int64(w)
}

func quarterOf(t time.Time) int64 { return int64((int(t.Month())-1)/monthsPerQuart + 1) }

func udfLastDayOfMonth(_ *fnCtx, args []driver.Value) (driver.Value, error) {
	t, ok := parseTS(args[0])
	if !ok {
		return sqlNull(), nil
	}

	y, m, _ := t.Date()

	return time.Date(y, m+1, 0, 0, 0, 0, 0, time.UTC).Format(dateLayout), nil
}

func udfToDate(_ *fnCtx, args []driver.Value) (driver.Value, error) {
	t, ok := parseTS(args[0])
	if !ok {
		return sqlNull(), nil
	}

	return t.Format(dateLayout), nil
}
