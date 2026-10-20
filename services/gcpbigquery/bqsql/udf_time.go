package bqsql

import (
	"database/sql/driver"
	"encoding/json"
	"strconv"
	"strings"
	"time"
)

// parseAny parses a stored temporal text value and reports which family it belongs to.
func parseAny(v driver.Value) (time.Time, Kind, error) {
	s := strings.TrimSpace(toStr(v))

	switch {
	case len(s) == len(DateLayout) && strings.Count(s, "-") == 2:
		t, err := ParseTemporal(KindDate, s)

		return t, KindDate, err
	case isDatetimeText(s):
		t, err := ParseTemporal(KindDatetime, s)

		return t, KindDatetime, err
	case strings.Contains(s, "-") || strings.Contains(s, " "):
		t, err := ParseTemporal(KindTimestamp, s)

		return t, KindTimestamp, err
	case strings.Contains(s, ":"):
		t, err := parseClock(s)

		return t, KindTime, err
	}

	return time.Time{}, KindUnknown, errBQ("invalid date/time value %q", s)
}

func nullable(a []driver.Value, f udfFn) udfFn {
	return func(args []driver.Value) (driver.Value, error) {
		if anyNull(args[:min(len(args), len(a))]) {
			return nil, nil
		}

		return f(args)
	}
}

func addTimeUDFs(d map[string]udfDef) {
	now := func(k Kind) udfFn {
		return func([]driver.Value) (driver.Value, error) { return FormatTemporal(k, time.Now().UTC()), nil }
	}

	d["current_date"] = udfDef{n: -1, rand: true, fn: now(KindDate)}
	d["current_timestamp"] = udfDef{n: -1, rand: true, fn: now(KindTimestamp)}
	d["current_datetime"] = udfDef{n: -1, rand: true, fn: now(KindDatetime)}
	d["current_time"] = udfDef{n: -1, rand: true, fn: now(KindTime)}
	d["date"] = udfDef{n: -1, fn: udfDate}
	d["timestamp"] = udfDef{n: -1, fn: convertTo(KindTimestamp)}
	d["datetime"] = udfDef{n: -1, fn: udfDatetime}
	d["time"] = udfDef{n: -1, fn: udfTime}
	d["date_from_unix_date"] = udfDef{
		n: 1,
		fn: nullable([]driver.Value{nil}, func(a []driver.Value) (driver.Value, error) {
			n, err := toInt(a[0])

			return FormatTemporal(KindDate, time.Unix(n*secPerDay, 0)), err
		}),
	}
	d["unix_date"] = udfDef{n: 1, fn: unixFn(secPerDay, 1)}
	d["timestamp_seconds"] = udfDef{n: 1, fn: fromUnix(time.Second)}
	d["timestamp_millis"] = udfDef{n: 1, fn: fromUnix(time.Millisecond)}
	d["timestamp_micros"] = udfDef{n: 1, fn: fromUnix(time.Microsecond)}
	d["unix_seconds"] = udfDef{n: 1, fn: unixFn(1, 1)}
	d["unix_millis"] = udfDef{n: 1, fn: unixFn(1, perMilli)}
	d["unix_micros"] = udfDef{n: 1, fn: unixFn(1, perMicro)}
	d["last_day"] = udfDef{n: -1, fn: udfLastDay}
	d["date_diff"] = udfDef{n: argc3, fn: udfDateDiff}
	d["date_trunc"] = udfDef{n: -1, fn: udfDateTrunc}
	d["date_add"] = udfDef{n: argc3, fn: udfDateAdd}
	d["extract"] = udfDef{n: argc2, fn: udfExtract}
	d["format_time"] = udfDef{n: -1, fn: udfFormatTime}
	d["parse_date"] = udfDef{n: -1, fn: parseFn(KindDate)}
	d["parse_timestamp"] = udfDef{n: -1, fn: parseFn(KindTimestamp)}
	d["parse_datetime"] = udfDef{n: -1, fn: parseFn(KindDatetime)}
	d["parse_time"] = udfDef{n: -1, fn: parseFn(KindTime)}
	d["generate_dates"] = udfDef{n: argc4, fn: udfGenerateDates}
}

const (
	secPerDay   = 86400
	daysPerWeek = 7
	extendedLen = 2
)

func zoneOf(a []driver.Value, idx int) *time.Location {
	if len(a) > idx && a[idx] != nil {
		if loc, err := time.LoadLocation(toStr(a[idx])); err == nil {
			return loc
		}
	}

	return time.UTC
}

func convertTo(k Kind) udfFn {
	return func(a []driver.Value) (driver.Value, error) {
		if a[0] == nil {
			return nil, nil
		}

		t, from, err := parseAny(a[0])
		if err != nil {
			return nil, err
		}

		if (from == KindDate || from == KindDatetime) && k == KindTimestamp {
			y, m, d := t.Date()
			t = time.Date(y, m, d, t.Hour(), t.Minute(), t.Second(), t.Nanosecond(), zoneOf(a, 1)).UTC()
		}

		return FormatTemporal(k, t), nil
	}
}

func udfDate(a []driver.Value) (driver.Value, error) {
	if anyNull(a) {
		return nil, nil //nolint:nilnil // NULL in, NULL out
	}

	if len(a) == 3 { //nolint:mnd // DATE(year, month, day)
		y, e1 := toInt(a[0])
		m, e2 := toInt(a[1])
		d, e3 := toInt(a[2])

		if e1 != nil || e2 != nil || e3 != nil {
			return nil, errBQ("invalid DATE arguments")
		}

		t := time.Date(int(y), time.Month(m), int(d), 0, 0, 0, 0, time.UTC)

		return FormatTemporal(KindDate, t), nil
	}

	t, k, err := parseAny(a[0])
	if err != nil {
		return nil, err
	}

	if k == KindTimestamp {
		t = t.In(zoneOf(a, 1))
	}

	y, m, d := t.Date()

	return FormatTemporal(KindDate, time.Date(y, m, d, 0, 0, 0, 0, time.UTC)), nil
}

func udfDatetime(a []driver.Value) (driver.Value, error) {
	if anyNull(a) {
		return nil, nil //nolint:nilnil // NULL in, NULL out
	}

	if len(a) == 6 { //nolint:mnd // DATETIME(y, m, d, h, mi, s)
		n := make([]int, len(a))

		for i, v := range a {
			x, err := toInt(v)
			if err != nil {
				return nil, err
			}

			n[i] = int(x)
		}

		return FormatTemporal(KindDatetime, time.Date(n[0], time.Month(n[1]), n[2], n[3], n[4], n[5], 0, time.UTC)), nil
	}

	t, k, err := parseAny(a[0])
	if err != nil {
		return nil, err
	}

	switch {
	case k == KindTimestamp:
		t = wallClock(t.In(zoneOf(a, 1)))
	case k == KindDate && len(a) == 2:
		tm, _, terr := parseAny(a[1])
		if terr != nil {
			return nil, terr
		}

		y, m, d := t.Date()
		t = time.Date(y, m, d, tm.Hour(), tm.Minute(), tm.Second(), tm.Nanosecond(), time.UTC)
	}

	return FormatTemporal(KindDatetime, t), nil
}

func wallClock(t time.Time) time.Time {
	y, m, d := t.Date()

	return time.Date(y, m, d, t.Hour(), t.Minute(), t.Second(), t.Nanosecond(), time.UTC)
}

func udfTime(a []driver.Value) (driver.Value, error) {
	if anyNull(a) {
		return nil, nil //nolint:nilnil // NULL in, NULL out
	}

	if len(a) == 3 { //nolint:mnd // TIME(h, m, s)
		h, _ := toInt(a[0])
		m, _ := toInt(a[1])
		s, _ := toInt(a[2])

		return FormatTemporal(KindTime, time.Date(1970, 1, 1, int(h), int(m), int(s), 0, time.UTC)), nil
	}

	t, k, err := parseAny(a[0])
	if err != nil {
		return nil, err
	}

	if k == KindTimestamp {
		t = t.In(zoneOf(a, 1))
	}

	return FormatTemporal(KindTime, t), nil
}

func unixFn(unitSecs int64, perSecond int64) udfFn {
	return func(a []driver.Value) (driver.Value, error) {
		if a[0] == nil {
			return nil, nil
		}

		t, _, err := parseAny(a[0])
		if err != nil {
			return nil, err
		}

		if unitSecs == secPerDay {
			return floorDiv(t.Unix(), secPerDay), nil
		}

		return t.Unix()*perSecond + int64(t.Nanosecond())/(nanosPerSecond/perSecond), nil
	}
}

func floorDiv(a, b int64) int64 {
	q := a / b
	if a%b != 0 && (a < 0) != (b < 0) {
		q--
	}

	return q
}

func fromUnix(unit time.Duration) udfFn {
	return func(a []driver.Value) (driver.Value, error) {
		if a[0] == nil {
			return nil, nil
		}

		n, err := toInt(a[0])
		if err != nil {
			return nil, err
		}

		return FormatTemporal(KindTimestamp, time.Unix(0, 0).Add(time.Duration(n)*unit)), nil
	}
}

func weekStart(part string) (time.Weekday, bool) {
	switch part {
	case litWeek, "WEEK(SUNDAY)":
		return time.Sunday, true
	case "WEEK(MONDAY)":
		return time.Monday, true
	case "WEEK(TUESDAY)":
		return time.Tuesday, true
	case "WEEK(WEDNESDAY)":
		return time.Wednesday, true
	case "WEEK(THURSDAY)":
		return time.Thursday, true
	case "WEEK(FRIDAY)":
		return time.Friday, true
	case "WEEK(SATURDAY)":
		return time.Saturday, true
	}

	return time.Sunday, false
}

func truncateTime(t time.Time, part string) (time.Time, error) {
	y, mo, d := t.Date()

	if wd, ok := weekStart(part); ok {
		off := (int(t.Weekday()) - int(wd) + 7) % 7 //nolint:mnd // days per week

		return time.Date(y, mo, d-off, 0, 0, 0, 0, time.UTC), nil
	}

	switch part {
	case litYear:
		return time.Date(y, 1, 1, 0, 0, 0, 0, time.UTC), nil
	case "ISOYEAR":
		iy, _ := t.ISOWeek()
		jan4 := time.Date(iy, 1, 4, 0, 0, 0, 0, time.UTC)

		return jan4.AddDate(0, 0, -((int(jan4.Weekday()) + 6) % 7)), nil //nolint:mnd // Monday-based weekday
	case litQuarter:
		return time.Date(
			y,
			time.Month((int(mo)-1)/3*3+1),
			1,
			0,
			0,
			0,
			0,
			time.UTC,
		), nil
	case litMonth:
		return time.Date(y, mo, 1, 0, 0, 0, 0, time.UTC), nil
	case "ISOWEEK":
		return time.Date(
			y,
			mo,
			d-((int(t.Weekday())+daysPerWeek-1)%daysPerWeek),
			0,
			0,
			0,
			0,
			time.UTC,
		), nil
	case litDay:
		return time.Date(y, mo, d, 0, 0, 0, 0, time.UTC), nil
	case litHour:
		return t.Truncate(time.Hour), nil
	case litMinute:
		return t.Truncate(time.Minute), nil
	case litSecond:
		return t.Truncate(time.Second), nil
	case litMillisecond:
		return t.Truncate(time.Millisecond), nil
	case litMicrosecond:
		return t.Truncate(time.Microsecond), nil
	}

	return t, errBQ("invalid date part %s", part)
}

func udfDateTrunc(a []driver.Value) (driver.Value, error) {
	if a[0] == nil {
		return nil, nil //nolint:nilnil // NULL in, NULL out
	}

	t, k, err := parseAny(a[0])
	if err != nil {
		return nil, err
	}

	t, err = truncateTime(t, strings.ToUpper(toStr(a[1])))
	if err != nil {
		return nil, err
	}

	return FormatTemporal(k, t), nil
}

func addMonths(t time.Time, n int) time.Time {
	y, m, d := t.Date()
	first := time.Date(y, m, 1, t.Hour(), t.Minute(), t.Second(), t.Nanosecond(), time.UTC).AddDate(0, n, 0)
	last := time.Date(first.Year(), first.Month()+1, 0, 0, 0, 0, 0, time.UTC).Day()

	return time.Date(
		first.Year(),
		first.Month(),
		min(d, last),
		t.Hour(),
		t.Minute(),
		t.Second(),
		t.Nanosecond(),
		time.UTC,
	)
}

func udfDateAdd(a []driver.Value) (driver.Value, error) {
	if anyNull(a) {
		return nil, nil //nolint:nilnil // NULL in, NULL out
	}

	t, k, err := parseAny(a[0])
	if err != nil {
		return nil, err
	}

	n, err := toInt(a[1])
	if err != nil {
		return nil, err
	}

	switch strings.ToUpper(toStr(a[2])) {
	case litYear:
		t = addMonths(t, int(n)*12) //nolint:mnd // months per year
	case litQuarter:
		t = addMonths(t, int(n)*3) //nolint:mnd // months per quarter
	case litMonth:
		t = addMonths(t, int(n))
	case litWeek:
		t = t.AddDate(0, 0, int(n)*7) //nolint:mnd // days per week
	case litDay:
		t = t.AddDate(0, 0, int(n))
	case litHour:
		t = t.Add(time.Duration(n) * time.Hour)
	case litMinute:
		t = t.Add(time.Duration(n) * time.Minute)
	case litSecond:
		t = t.Add(time.Duration(n) * time.Second)
	case litMillisecond:
		t = t.Add(time.Duration(n) * time.Millisecond)
	case litMicrosecond:
		t = t.Add(time.Duration(n) * time.Microsecond)
	default:
		return nil, errBQ("invalid date part %s", toStr(a[2]))
	}

	return FormatTemporal(k, t), nil
}

func udfDateDiff(a []driver.Value) (driver.Value, error) {
	if anyNull(a) {
		return nil, nil //nolint:nilnil // NULL in, NULL out
	}

	x, _, err := parseAny(a[0])
	if err != nil {
		return nil, err
	}

	y, _, err := parseAny(a[1])
	if err != nil {
		return nil, err
	}

	part := strings.ToUpper(toStr(a[2]))

	switch part {
	case litYear:
		return int64(x.Year() - y.Year()), nil
	case litQuarter:
		return int64((x.Year()-y.Year())*4 + (int(x.Month())-1)/3 - (int(y.Month())-1)/3), nil
	case litMonth:
		return int64((x.Year()-y.Year())*12 + int(x.Month()) - int(y.Month())), nil
	}

	if wd, ok := weekStart(part); ok {
		xs, _ := truncateTime(x, litDay)
		ys, _ := truncateTime(y, litDay)
		xw, _ := truncateTime(xs, "WEEK("+strings.ToUpper(wd.String())+")")
		yw, _ := truncateTime(ys, "WEEK("+strings.ToUpper(wd.String())+")")

		return int64(xw.Sub(yw) / (7 * 24 * time.Hour)), nil
	}

	return elapsed(x.Sub(y), part)
}

func elapsed(d time.Duration, part string) (driver.Value, error) {
	units := map[string]time.Duration{
		litDay: hoursPerDay * time.Hour, litHour: time.Hour, litMinute: time.Minute, litSecond: time.Second,
		litMillisecond: time.Millisecond, litMicrosecond: time.Microsecond,
	}

	u, ok := units[part]
	if !ok {
		return nil, errBQ("invalid date part %s", part)
	}

	return int64(d / u), nil
}

func udfLastDay(a []driver.Value) (driver.Value, error) {
	if a[0] == nil {
		return nil, nil //nolint:nilnil // NULL in, NULL out
	}

	t, _, err := parseAny(a[0])
	if err != nil {
		return nil, err
	}

	part := litMonth
	if len(a) > 1 {
		part = strings.ToUpper(toStr(a[1]))
	}

	var end time.Time

	switch part {
	case litMonth:
		end = time.Date(t.Year(), t.Month()+1, 0, 0, 0, 0, 0, time.UTC)
	case litQuarter:
		end = time.Date(
			t.Year(),
			time.Month((int(t.Month())-1)/monthsPerQtr*monthsPerQtr+monthsPerQtr+1),
			0,
			0,
			0,
			0,
			0,
			time.UTC,
		)
	case litYear:
		end = time.Date(t.Year(), 12, 31, 0, 0, 0, 0, time.UTC)
	default:
		start, terr := truncateTime(t, part)
		if terr != nil {
			return nil, terr
		}

		end = start.AddDate(0, 0, 6) //nolint:mnd // week length
	}

	return FormatTemporal(KindDate, end), nil
}

func udfExtract(a []driver.Value) (driver.Value, error) {
	if a[0] == nil {
		return nil, nil //nolint:nilnil // NULL in, NULL out
	}

	t, _, err := parseAny(a[0])
	if err != nil {
		return nil, err
	}

	part := strings.ToUpper(toStr(a[1]))

	if wd, ok := weekStart(part); ok {
		start, _ := truncateTime(
			time.Date(t.Year(), 1, 1, 0, 0, 0, 0, time.UTC),
			"WEEK("+strings.ToUpper(wd.String())+")",
		)
		cur, _ := truncateTime(t, "WEEK("+strings.ToUpper(wd.String())+")")

		return int64(
			cur.Sub(start)/(7*24*time.Hour),
		) + boolInt(
			start.Year() == t.Year() && start.YearDay() == 1,
		), nil
	}

	return extractSimple(t, part)
}

func boolInt(b bool) int64 {
	if b {
		return 1
	}

	return 0
}

func extractParts() map[string]func(time.Time) int64 {
	return map[string]func(time.Time) int64{
		litYear:        func(t time.Time) int64 { return int64(t.Year()) },
		"ISOYEAR":      isoYear,
		litQuarter:     quarterOf,
		litMonth:       func(t time.Time) int64 { return int64(t.Month()) },
		"ISOWEEK":      isoWeek,
		litDay:         func(t time.Time) int64 { return int64(t.Day()) },
		"DAYOFWEEK":    func(t time.Time) int64 { return int64(t.Weekday()) + 1 },
		"DAYOFYEAR":    func(t time.Time) int64 { return int64(t.YearDay()) },
		litHour:        func(t time.Time) int64 { return int64(t.Hour()) },
		litMinute:      func(t time.Time) int64 { return int64(t.Minute()) },
		litSecond:      func(t time.Time) int64 { return int64(t.Second()) },
		litMillisecond: func(t time.Time) int64 { return int64(t.Nanosecond() / nanosPerMilli) },
		litMicrosecond: func(t time.Time) int64 { return int64(t.Nanosecond() / nanosPerMicro) },
	}
}

func isoYear(t time.Time) int64 {
	y, _ := t.ISOWeek()

	return int64(y)
}

func isoWeek(t time.Time) int64 {
	_, w := t.ISOWeek()

	return int64(w)
}

const (
	perMilli       = 1_000
	perMicro       = 1_000_000
	nanosPerSecond = 1_000_000_000
	hoursPerDay    = 24
	nanosPerMilli  = 1_000_000
	nanosPerMicro  = 1_000
	monthsPerQtr   = 3
)

func quarterOf(t time.Time) int64 { return int64((int(t.Month())-1)/monthsPerQtr + 1) }

func extractSimple(t time.Time, part string) (driver.Value, error) {
	if f, ok := extractParts()[part]; ok {
		return f(t), nil
	}

	switch part {
	case litDate:
		return FormatTemporal(KindDate, t), nil
	case litTime:
		return FormatTemporal(KindTime, t), nil
	case litDatetime:
		return FormatTemporal(KindDatetime, t), nil
	}

	return nil, errBQ("invalid extract part %s", part)
}

func udfGenerateDates(a []driver.Value) (driver.Value, error) {
	if anyNull(a) {
		return nil, nil //nolint:nilnil // NULL in, NULL out
	}

	start, k, err := parseAny(a[0])
	if err != nil {
		return nil, err
	}

	end, _, err := parseAny(a[1])
	if err != nil {
		return nil, err
	}

	step, err := toInt(a[2])
	if err != nil || step == 0 {
		return nil, errBQ("step must not be zero")
	}

	const maxElems = 1_000_000

	out := []string{}

	for t := start; (step > 0 && !t.After(end)) || (step < 0 && !t.Before(end)); {
		out = append(out, FormatTemporal(k, t))
		if len(out) > maxElems {
			return nil, errBQ("generated array is too large")
		}

		next, aerr := udfDateAdd([]driver.Value{FormatTemporal(k, t), step, a[3]})
		if aerr != nil {
			return nil, aerr
		}

		t, _, _ = parseAny(next)
	}

	b, _ := json.Marshal(out)

	return string(b), nil
}

// strftimeLayouts maps strftime specifiers to the Go layout fragment that renders and parses them.
func strftimeLayouts() map[byte]string {
	return map[byte]string{
		'Y': "2006", 'y': "06", 'm': "01", 'd': "02", 'e': "_2", 'H': "15", 'I': "03", 'M': "04", 'S': "05",
		'p': "PM", 'b': "Jan", 'h': "Jan", 'B': "January", 'a': "Mon", 'A': "Monday", 'j': "002", 'Z': "MST",
		'z': "-0700", 'F': "2006-01-02", 'T': "15:04:05", 'D': "01/02/06", 'R': "15:04", '%': "%",
		'n': "\n", 't': "\t",
	}
}

// strftimeSpec renders one strftime specifier.
func strftimeSpec(t time.Time, c byte) (string, bool) {
	if layout, ok := strftimeLayouts()[c]; ok {
		return t.Format(layout), true
	}

	switch c {
	case 's':
		return strconv.FormatInt(t.Unix(), decimalBase), true
	case 'u':
		return strconv.Itoa((int(t.Weekday())+daysPerWeek-1)%daysPerWeek + 1), true
	case 'w':
		return strconv.Itoa(int(t.Weekday())), true
	case 'Q':
		return strconv.FormatInt(quarterOf(t), decimalBase), true
	}

	return "", false
}

func formatStrftime(t time.Time, f string) (string, error) {
	var sb strings.Builder

	for i := 0; i < len(f); i++ {
		if f[i] != '%' || i+1 >= len(f) {
			sb.WriteByte(f[i])

			continue
		}

		i++

		if f[i] == 'E' && i+2 < len(f)+1 {
			out, n := extendedSpec(t, f[i+1:])
			sb.WriteString(out)

			i += n

			continue
		}

		out, ok := strftimeSpec(t, f[i])
		if !ok {
			return "", errBQ("unsupported format specifier %%%c", f[i])
		}

		sb.WriteString(out)
	}

	return sb.String(), nil
}

// extendedSpec handles %E*S, %E3S, %E6S and similar; it returns the text and bytes consumed after 'E'.
func extendedSpec(t time.Time, rest string) (string, int) {
	switch {
	case strings.HasPrefix(rest, "*S"):
		return t.Format("05.999999999"), 2 //nolint:mnd // consumed *S
	case strings.HasPrefix(rest, "3S"):
		return t.Format("05.000"), 2 //nolint:mnd // consumed 3S
	case strings.HasPrefix(rest, "6S"):
		return t.Format("05.000000"), 2 //nolint:mnd // consumed 6S
	}

	return "", 0
}

func udfFormatTime(a []driver.Value) (driver.Value, error) {
	if a[0] == nil || a[1] == nil {
		return nil, nil //nolint:nilnil // NULL in, NULL out
	}

	t, k, err := parseAny(a[1])
	if err != nil {
		return nil, err
	}

	if k == KindTimestamp {
		t = t.In(zoneOf(a, argc2))
	}

	return formatStrftime(t, toStr(a[0]))
}

func strftimeLayout(f string) (string, error) {
	repl := strftimeLayouts()

	var sb strings.Builder

	for i := 0; i < len(f); i++ {
		if f[i] != '%' || i+1 >= len(f) {
			sb.WriteByte(f[i])

			continue
		}

		i++

		if f[i] == 'E' && i+extendedLen < len(f) {
			sb.WriteString("05.999999999")

			i += extendedLen

			continue
		}

		s, ok := repl[f[i]]
		if !ok {
			return "", errBQ("unsupported format specifier %%%c", f[i])
		}

		sb.WriteString(s)
	}

	return sb.String(), nil
}

func parseFn(k Kind) udfFn {
	return func(a []driver.Value) (driver.Value, error) {
		if a[0] == nil || a[1] == nil {
			return nil, nil
		}

		layout, err := strftimeLayout(toStr(a[0]))
		if err != nil {
			return nil, err
		}

		t, err := time.Parse(layout, strings.TrimSpace(toStr(a[1])))
		if err != nil {
			return nil, errBQ("Failed to parse input string %q", toStr(a[1]))
		}

		return FormatTemporal(k, t), nil
	}
}

// isDatetimeText reports a zone-less YYYY-MM-DDTHH:MM:SS text.
func isDatetimeText(s string) bool {
	_, tail, found := strings.Cut(s, "T")

	return found && !strings.HasSuffix(s, "Z") && !strings.ContainsAny(tail, "+")
}
