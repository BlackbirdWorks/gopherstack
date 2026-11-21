package bqsql

import (
	"database/sql/driver"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode"
)

// Format elements of CAST ... FORMAT for date and time types, from
// https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/format-elements#format_elements_date_time.
const (
	elYYYY  = "YYYY"
	elYYY   = "YYY"
	elRRRR  = "RRRR"
	elMON   = "MON"
	elMONTH = "MONTH"
	elDDD   = "DDD"
	elHH24  = "HH24"
	elHH12  = "HH12"
	elSSSSS = "SSSSS"
	elTZH   = "TZH"
	elTZM   = "TZM"
	elPM    = "P.M."
	elThouY = "Y,YYY"
)

const meridianDots = "A.M."

const (
	fmtMaxModel      = 256
	fmtMaxDigits     = 9
	fmtYearDigits    = 4
	fmtSecsPerDay    = 86400
	fmtDayWidth      = 9
	fmtThousand      = 1000
	fmtHundred       = 100
	fmtMinutes       = 60
	fmtHalfDay       = 12
	fmtDayOfYear     = 3
	fmtSecOfDayLen   = 5
	fmtMonthAbbr     = 3
	fmtTwo           = 2
	fmtMonths        = 12
	fmtSecsPerHour   = 3600
	fmtMaxDay        = 31
	fmtMaxHour       = 24
	fmtMaxOffsetHour = 14
)

type fmtTok struct {
	text string
	elem string
	lit  bool
}

func fmtElements() []string {
	return []string{
		elThouY,
		elHH24,
		elHH12,
		elSSSSS,
		elYYYY,
		elRRRR,
		elMONTH,
		meridianDots,
		elPM,
		"FF1",
		"FF2",
		"FF3",
		"FF4",
		"FF5",
		"FF6",
		"FF7",
		"FF8",
		"FF9",
		elTZH,
		elTZM,
		elDDD,
		elYYY,
		elMON,
		litDay,
		"HH",
		"MI",
		"SS",
		"YY",
		"RR",
		"MM",
		"DD",
		"DY",
		"AM",
		"PM",
		"Y",
		"D",
	}
}

func tokenizeFormat(model string) ([]fmtTok, error) {
	if len(model) > fmtMaxModel {
		return nil, errBQ("The format string is too long")
	}

	var toks []fmtTok

	for i := 0; i < len(model); {
		c := model[i]

		switch {
		case c == '"':
			var sb strings.Builder

			i++

			for ; i < len(model) && model[i] != '"'; i++ {
				if model[i] == '\\' && i+1 < len(model) && (model[i+1] == '"' || model[i+1] == '\\') {
					i++
				}

				sb.WriteByte(model[i])
			}

			if i >= len(model) {
				return nil, errBQ("Unterminated quoted text in the format string")
			}

			i++

			toks = append(toks, fmtTok{text: sb.String(), lit: true})
		case strings.ContainsRune("-./,';: ", rune(c)):
			toks = append(toks, fmtTok{text: string(c), lit: true})
			i++
		default:
			el, ok := matchElement(model[i:])
			if !ok {
				return nil, errBQ("Invalid format element or character %q in the format string", string(c))
			}

			toks = append(toks, fmtTok{text: model[i : i+len(el)], elem: el})
			i += len(el)
		}
	}

	return toks, nil
}

func matchElement(s string) (string, bool) {
	up := strings.ToUpper(s)

	for _, e := range fmtElements() {
		if strings.HasPrefix(up, e) {
			return e, true
		}
	}

	return "", false
}

func fmtHasDate(k Kind) bool { return k != KindTime }
func fmtHasTime(k Kind) bool { return k != KindDate }

func applyCase(tok fmtTok, s string) string {
	switch {
	case len(tok.text) > 1 && unicode.IsLower(rune(tok.text[0])):
		return strings.ToLower(s)
	case len(tok.text) > 1 && unicode.IsLower(rune(tok.text[1])):
		return capitalizeWords(strings.ToLower(s))
	case unicode.IsLower(rune(tok.text[0])):
		return strings.ToLower(s)
	}

	return strings.ToUpper(s)
}

func capitalizeWords(s string) string {
	b := []byte(s)
	start := true

	for i, c := range b {
		if start && c >= 'a' && c <= 'z' {
			b[i] = c - 'a' + 'A'
		}

		start = c == ' '
	}

	return string(b)
}

func zoneOffset(t time.Time) (string, string) {
	_, off := t.Zone()
	sign := "+"

	if off < 0 {
		sign, off = "-", -off
	}

	return fmt.Sprintf("%s%02d", sign, off/fmtSecsPerHour), fmt.Sprintf("%02d", off%fmtSecsPerHour/fmtMinutes)
}

func formatWithModel(t time.Time, k Kind, model string) (string, error) {
	toks, err := tokenizeFormat(model)
	if err != nil {
		return "", err
	}

	var sb strings.Builder

	for _, tok := range toks {
		if tok.lit {
			sb.WriteString(tok.text)

			continue
		}

		s, ferr := formatElement(t, k, tok)
		if ferr != nil {
			return "", ferr
		}

		sb.WriteString(s)
	}

	return sb.String(), nil
}

func dateElements() map[string]bool {
	return map[string]bool{
		elYYYY: true, elYYY: true, "YY": true, "Y": true, elRRRR: true, "RR": true, "MM": true, elMON: true,
		elMONTH: true, litDay: true, "DY": true, "D": true, "DD": true, elDDD: true,
	}
}

func formatElement(t time.Time, k Kind, tok fmtTok) (string, error) {
	isDate := dateElements()[tok.elem]
	zone := tok.elem == elTZH || tok.elem == elTZM

	if isDate && !fmtHasDate(k) || !isDate && !zone && !fmtHasTime(k) {
		return "", errBQ("Format element %s is not valid for %s", tok.elem, k.RESTName())
	}

	if f, ok := elementFormatters()[tok.elem]; ok {
		return f(t, tok), nil
	}

	n, err := strconv.Atoi(strings.TrimPrefix(tok.elem, "FF"))
	if err != nil || n < 1 || n > fmtMaxDigits {
		return "", errBQ("Unsupported format element %s", tok.elem)
	}

	return fractionDigits(t, n), nil
}

func lastDigits(y, n int) string {
	s := fmt.Sprintf("%0*d", n, y)

	return s[len(s)-n:]
}

func twoDigits(v int) string { return fmt.Sprintf("%02d", v) }

func elementFormatters() map[string]func(time.Time, fmtTok) string {
	year4 := func(t time.Time, _ fmtTok) string { return fmt.Sprintf("%0*d", fmtYearDigits, t.Year()) }
	year2 := func(t time.Time, _ fmtTok) string { return lastDigits(t.Year(), fmtTwo) }
	hour12 := func(t time.Time, _ fmtTok) string {
		h := t.Hour() % fmtHalfDay
		if h == 0 {
			h = fmtHalfDay
		}

		return twoDigits(h)
	}
	meridian := func(t time.Time, tok fmtTok) string {
		s := "AM"
		if t.Hour() >= fmtHalfDay {
			s = "PM"
		}

		if strings.Contains(tok.elem, ".") {
			s = s[:1] + "." + s[1:] + "."
		}

		return applyCase(tok, s)
	}

	return map[string]func(time.Time, fmtTok) string{
		elYYYY: year4, elRRRR: year4, "YY": year2, "RR": year2,
		elYYY: func(t time.Time, _ fmtTok) string { return lastDigits(t.Year(), fmtDayOfYear) },
		"Y":   func(t time.Time, _ fmtTok) string { return lastDigits(t.Year(), 1) },
		"MM":  func(t time.Time, _ fmtTok) string { return twoDigits(int(t.Month())) },
		elMON: func(t time.Time, tok fmtTok) string {
			return applyCase(tok, strings.ToUpper(t.Month().String()[:fmtMonthAbbr]))
		},
		elMONTH: func(t time.Time, tok fmtTok) string { return applyCase(tok, strings.ToUpper(t.Month().String())) },
		litDay: func(t time.Time, tok fmtTok) string {
			return fmt.Sprintf("%-*s", fmtDayWidth, applyCase(tok, strings.ToUpper(t.Weekday().String())))
		},
		"DY": func(t time.Time, tok fmtTok) string {
			return applyCase(tok, strings.ToUpper(t.Weekday().String()[:fmtMonthAbbr]))
		},
		"D":    func(t time.Time, _ fmtTok) string { return strconv.Itoa(int(t.Weekday()) + 1) },
		"DD":   func(t time.Time, _ fmtTok) string { return twoDigits(t.Day()) },
		elDDD:  func(t time.Time, _ fmtTok) string { return fmt.Sprintf("%03d", t.YearDay()) },
		"HH":   hour12,
		elHH12: hour12,
		elHH24: func(t time.Time, _ fmtTok) string { return twoDigits(t.Hour()) },
		"MI":   func(t time.Time, _ fmtTok) string { return twoDigits(t.Minute()) },
		"SS":   func(t time.Time, _ fmtTok) string { return twoDigits(t.Second()) },
		elSSSSS: func(t time.Time, _ fmtTok) string {
			return fmt.Sprintf("%0*d", fmtSecOfDayLen, t.Hour()*fmtSecsPerHour+t.Minute()*fmtMinutes+t.Second())
		},
		"AM": meridian, "PM": meridian, meridianDots: meridian, elPM: meridian,
		elTZH: func(t time.Time, _ fmtTok) string {
			h, _ := zoneOffset(t)

			return h
		},
		elTZM: func(t time.Time, _ fmtTok) string {
			_, m := zoneOffset(t)

			return m
		},
	}
}

// fractionDigits renders the fractional second rounded to n digits.
func fractionDigits(t time.Time, n int) string {
	div, limit := int64(1), int64(1)

	for range fmtMaxDigits - n {
		div *= 10
	}

	for range n {
		limit *= 10
	}

	v := min((int64(t.Nanosecond())+div/2)/div, limit-1)

	return fmt.Sprintf("%0*d", n, v)
}

func parseGroup(e string) string {
	switch e {
	case elYYYY, elYYY, "YY", "Y", elThouY, elRRRR, "RR":
		return "year"
	case "MM", elMON, elMONTH:
		return "month"
	case "HH", elHH12, elHH24:
		return "hour"
	case meridianDots, elPM:
		return "meridian"
	}

	return e
}

func modelGroups(toks []fmtTok) (map[string]bool, error) {
	seen := map[string]bool{}

	for _, t := range toks {
		if t.lit {
			continue
		}

		g := parseGroup(t.elem)
		if seen[g] {
			return nil, errBQ("The format string contains %s more than once", g)
		}

		seen[g] = true
	}

	return seen, nil
}

func meridianMismatch(has map[string]bool, h24 bool) bool {
	hh := has["hour"] && !h24

	return h24 && has["meridian"] || hh && !has["meridian"] || !has["hour"] && has["meridian"]
}

func sssssConflict(has map[string]bool) bool {
	return has[elSSSSS] && (has["hour"] || has["MI"] || has["SS"] || has["meridian"])
}

func kindMismatch(has map[string]bool, k Kind) bool {
	clock := has["hour"] || has["MI"] || has["SS"] || has[elSSSSS]
	date := has["year"] || has["month"] || has["DD"] || has[elDDD]

	return clock && !fmtHasTime(k) || date && !fmtHasDate(k)
}

func validateParseModel(toks []fmtTok, k Kind) error {
	has, err := modelGroups(toks)
	if err != nil {
		return err
	}

	h24 := slices.ContainsFunc(toks, func(t fmtTok) bool { return t.elem == elHH24 })

	switch {
	case has[elDDD] && has["month"]:
		return errBQ("DDD cannot be combined with a month element")
	case meridianMismatch(has, h24):
		return errBQ("HH12 and the meridian indicator must be used together; HH24 cannot be combined with them")
	case sssssConflict(has):
		return errBQ("SSSSS cannot be combined with hour, minute, second or meridian elements")
	case kindMismatch(has, k):
		return errBQ("Format element not valid for %s", k.RESTName())
	}

	return nil
}

type fmtParsed struct {
	year, month, day, hour, min, sec, nanos      int
	yearSet, monthSet, daySet, pm, hh12, zoneSet bool
	tzSign                                       int
	tzh, tzm                                     int
	secOfDay                                     int
}

func (p *fmtParsed) time(k Kind, loc *time.Location, now time.Time) time.Time {
	y, m, d := p.year, p.month, p.day

	if !p.yearSet {
		y = now.Year()
	}

	if !p.monthSet {
		m = int(now.Month())
	}

	if d == 0 {
		d = 1
	}

	if k == KindTime {
		y, m, d = 1970, 1, 1
	}

	h, mi, s := p.hour, p.min, p.sec

	if p.secOfDay > 0 {
		h, mi, s = p.secOfDay/fmtSecsPerHour, p.secOfDay%fmtSecsPerHour/fmtMinutes, p.secOfDay%fmtMinutes
	}

	if p.hh12 {
		h %= fmtHalfDay
		if p.pm {
			h += fmtHalfDay
		}
	}

	if p.zoneSet {
		loc = time.FixedZone("", p.tzSign*(p.tzh*3600+p.tzm*fmtMinutes))
	}

	return time.Date(y, time.Month(m), d, h, mi, s, p.nanos, loc)
}

func parseWithModel(s, model string, k Kind, loc *time.Location, now time.Time) (time.Time, error) {
	toks, err := tokenizeFormat(model)
	if err != nil {
		return time.Time{}, err
	}

	if err = validateParseModel(toks, k); err != nil {
		return time.Time{}, err
	}

	var p fmtParsed

	pos := 0

	for i, tok := range toks {
		if pos, err = parseToken(&p, s, pos, tok, i+1 >= len(toks) || toks[i+1].lit); err != nil {
			return time.Time{}, err
		}
	}

	if pos != len(s) {
		return time.Time{}, errBQ("Invalid %s string %q: unparsed trailing text", k.RESTName(), s)
	}

	out := p.time(k, loc, now)
	if !p.inRange(k, out) {
		return time.Time{}, errBQ("Invalid %s string %q", k.RESTName(), s)
	}

	return out, nil
}

func (p *fmtParsed) zoneInRange() bool {
	return !p.zoneSet || p.tzh <= fmtMaxOffsetHour && p.tzm < fmtMinutes
}

// inRange rejects fields that time.Date would silently roll over.
func (p *fmtParsed) inRange(k Kind, out time.Time) bool {
	if p.month > fmtMonths || p.day > fmtMaxDay || p.hour > fmtMaxHour || p.min >= fmtMinutes ||
		p.sec >= fmtMinutes || p.secOfDay >= fmtSecsPerDay {
		return false
	}

	if !p.zoneInRange() {
		return false
	}

	if k == KindTime {
		return true
	}

	if p.yearSet && p.year < 1 {
		return false
	}

	return (!p.daySet || out.Day() == p.day) && (!p.monthSet || int(out.Month()) == p.month)
}

func takeDigits(s string, pos, n int) (int, int, error) {
	if pos+n > len(s) {
		return 0, pos, errBQ("Invalid date/time string: expected %d digits at position %d", n, pos)
	}

	v, err := strconv.Atoi(s[pos : pos+n])
	if err != nil || strings.ContainsAny(s[pos:pos+n], "+- ") {
		return 0, pos, errBQ("Invalid date/time string: expected %d digits at position %d", n, pos)
	}

	return v, pos + n, nil
}

func takeRun(s string, pos, maxN int) (int, int, error) {
	n := 0
	for pos+n < len(s) && n < maxN && s[pos+n] >= '0' && s[pos+n] <= '9' {
		n++
	}

	if n == 0 {
		return 0, pos, errBQ("Invalid date/time string: expected digits at position %d", pos)
	}

	v, _ := strconv.Atoi(s[pos : pos+n])

	return v, pos + n, nil
}

func parseToken(p *fmtParsed, s string, pos int, tok fmtTok, delimited bool) (int, error) {
	if tok.lit {
		return parseLiteral(s, pos, tok)
	}

	var (
		v   int
		err error
	)

	switch tok.elem {
	case elYYYY, elRRRR:
		if tok.elem == elYYYY && delimited {
			v, pos, err = takeRun(s, pos, fmtYearDigits+1)
		} else {
			v, pos, err = takeDigits(s, pos, fmtYearDigits)
		}

		p.year, p.yearSet = v, true
	case elThouY:
		pos, err = parseThousandsYear(p, s, pos)
	case elYYY, "YY", "Y", "RR":
		pos, err = parseShortYear(p, s, pos, tok.elem)
	case "MM":
		v, pos, err = takeDigits(s, pos, fmtTwo)
		p.month, p.monthSet = v, true
	case elMON, elMONTH:
		pos, err = parseMonthName(p, s, pos, tok.elem == elMON)
	case "DD":
		p.day, pos, err = takeDigits(s, pos, fmtTwo)
		p.daySet = true
	default:
		pos, err = parseTimeToken(p, s, pos, tok)
	}

	return pos, err
}

func parseLiteral(s string, pos int, tok fmtTok) (int, error) {
	if tok.text == " " {
		n := 0
		for pos+n < len(s) && unicode.IsSpace(rune(s[pos+n])) {
			n++
		}

		if n == 0 {
			return pos, errBQ("Invalid date/time string: expected whitespace at position %d", pos)
		}

		return pos + n, nil
	}

	if !strings.HasPrefix(s[pos:], tok.text) {
		return pos, errBQ("Invalid date/time string: expected %q at position %d", tok.text, pos)
	}

	return pos + len(tok.text), nil
}

func parseThousandsYear(p *fmtParsed, s string, pos int) (int, error) {
	hi, np, err := takeRun(s, pos, fmtTwo)
	if err != nil {
		return pos, err
	}

	if np >= len(s) || s[np] != ',' {
		return pos, errBQ("Invalid date/time string: expected ',' at position %d", np)
	}

	lo, np, err := takeDigits(s, np+1, fmtDayOfYear)
	p.year, p.yearSet = hi*fmtThousand+lo, true

	return np, err
}

func parseShortYear(p *fmtParsed, s string, pos int, el string) (int, error) {
	n := map[string]int{elYYY: fmtDayOfYear, "YY": fmtTwo, "Y": 1, "RR": fmtTwo}[el]

	v, np, err := takeDigits(s, pos, n)
	if err != nil {
		return pos, err
	}

	now := time.Now().UTC().Year()
	mod := 1

	for range n {
		mod *= 10
	}

	base := now / mod * mod

	if el == "RR" {
		cur := now % fmtHundred

		switch {
		case v < 50 && cur >= 50:
			base += fmtHundred
		case v >= 50 && cur < 50:
			base -= fmtHundred
		}
	}

	p.year, p.yearSet = base+v, true

	return np, nil
}

func parseMonthName(p *fmtParsed, s string, pos int, abbr bool) (int, error) {
	end := pos
	for end < len(s) && (s[end] >= 'A' && s[end] <= 'Z' || s[end] >= 'a' && s[end] <= 'z') {
		end++
	}

	word := strings.ToUpper(s[pos:end])

	for m := time.January; m <= time.December; m++ {
		name := strings.ToUpper(m.String())
		if abbr {
			name = name[:fmtMonthAbbr]
		}

		if word == name {
			p.month, p.monthSet = int(m), true

			return end, nil
		}
	}

	return pos, errBQ("Invalid date/time string: %q is not a month name", word)
}

func parseTimeToken(p *fmtParsed, s string, pos int, tok fmtTok) (int, error) {
	var (
		v   int
		err error
	)

	switch tok.elem {
	case "HH", elHH12:
		p.hour, pos, err = takeDigits(s, pos, fmtTwo)
		p.hh12 = true
	case elHH24:
		p.hour, pos, err = takeDigits(s, pos, fmtTwo)
	case "MI":
		p.min, pos, err = takeDigits(s, pos, fmtTwo)
	case "SS":
		p.sec, pos, err = takeDigits(s, pos, fmtTwo)
	case elSSSSS:
		p.secOfDay, pos, err = takeDigits(s, pos, fmtSecOfDayLen)
	case meridianDots, elPM:
		if pos+4 > len(s) || !strings.EqualFold(s[pos+1:pos+4], ".M.") ||
			!strings.ContainsAny(s[pos:pos+1], "AaPp") {
			return pos, errBQ("Invalid date/time string: expected A.M. or P.M. at position %d", pos)
		}

		p.pm = strings.EqualFold(s[pos:pos+1], "P")
		pos += 4
	case elTZH:
		pos, err = parseTZH(p, s, pos)
	case elTZM:
		v, pos, err = takeDigits(s, pos, fmtTwo)
		p.tzm, p.zoneSet = v, true
	case "AM", "PM", "D", "DY", litDay, elDDD:
		return pos, errBQ("Format element %s is not supported for parsing", tok.elem)
	default:
		n, _ := strconv.Atoi(strings.TrimPrefix(tok.elem, "FF"))
		p.nanos, pos, err = takeDigits(s, pos, n)

		for range fmtMaxDigits - n {
			p.nanos *= 10
		}
	}

	return pos, err
}

func parseTZH(p *fmtParsed, s string, pos int) (int, error) {
	if pos >= len(s) || !strings.ContainsRune("+- ", rune(s[pos])) {
		return pos, errBQ("Invalid date/time string: expected a sign at position %d", pos)
	}

	p.tzSign = 1
	if s[pos] == '-' {
		p.tzSign = -1
	}

	v, np, err := takeDigits(s, pos+1, fmtTwo)
	p.tzh, p.zoneSet = v, true

	return np, err
}

func castFormatZone(v driver.Value) (*time.Location, error) {
	if v == nil {
		return time.UTC, nil
	}

	loc, err := time.LoadLocation(toStr(v))
	if err != nil {
		return nil, errBQ("Invalid time zone: %s", toStr(v))
	}

	return loc, nil
}

func isTemporalKind(k Kind) bool {
	return k == KindDate || k == KindTime || k == KindDatetime || k == KindTimestamp
}

func udfCastFormat(a []driver.Value) (driver.Value, error) {
	if a[0] == nil || a[3] == nil {
		return nil, nil //nolint:nilnil // NULL in, NULL out
	}

	loc, err := castFormatZone(a[4])
	if err != nil {
		return nil, err
	}

	tk, _ := KindOf(toStr(a[1]))
	sk, _ := KindOf(toStr(a[2]))
	model := toStr(a[3])

	switch {
	case tk == KindString && isTemporalKind(sk):
		t, _, perr := parseAny(a[0])
		if perr != nil {
			return nil, perr
		}

		if sk == KindTimestamp {
			t = t.In(loc)
		}

		return formatWithModel(t, sk, model)
	case sk == KindString && isTemporalKind(tk):
		if tk != KindTimestamp {
			loc = time.UTC
		}

		t, perr := parseWithModel(toStr(a[0]), model, tk, loc, time.Now().UTC())
		if perr != nil {
			return nil, perr
		}

		return FormatTemporal(tk, t), nil
	}

	return nil, errBQ("CAST ... FORMAT is supported between STRING and DATE, TIME, DATETIME and TIMESTAMP only")
}
