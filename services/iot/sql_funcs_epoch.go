package iot

import (
	"strconv"
	"strings"
	"time"
	"unicode"
)

const (
	centuryBase   = 2000
	nanoDigits    = 9
	ampmLen       = 2
	hourDigits    = 2
	hourMinDigits = 4
	secsPerHour   = 3600
	secsPerMinute = 60
	greedyDigits  = 9
)

// epochFields collects what a java.time pattern parsed out of the input.
type epochFields struct {
	loc    *time.Location
	year   int
	month  int
	day    int
	hour   int
	minute int
	second int
	nanos  int
	pm     int
	clock  bool
}

// timeToEpochFunc parses a timestamp with a java.time pattern into epoch milliseconds.
func timeToEpochFunc(_ *sqlCtx, args []any) any {
	text, ok := strArg(args, 0)
	pattern, ok2 := strArg(args, 1)

	if !ok || !ok2 {
		return sqlUndefined{}
	}

	f := epochFields{month: 1, day: 1, pm: -1, loc: time.UTC}
	if !f.parse(text, pattern) {
		return sqlUndefined{}
	}

	if f.clock && f.pm == 1 {
		f.hour = f.hour%hoursHalfDay + hoursHalfDay
	}

	t := time.Date(f.year, time.Month(f.month), f.day, f.hour, f.minute, f.second, f.nanos, f.loc)
	if t.Day() != f.day || int(t.Month()) != f.month {
		return sqlUndefined{}
	}

	return t.UnixMilli()
}

func (f *epochFields) parse(text, pattern string) bool {
	pos := 0

	for i := 0; i < len(pattern); {
		ch := pattern[i]

		switch {
		case ch == '\'':
			lit, next := quotedLiteral(pattern, i)
			if !strings.HasPrefix(text[pos:], lit) {
				return false
			}

			pos += len(lit)
			i = next
		case isLetter(ch):
			n := runLen(pattern, i)

			adv, ok := f.field(text[pos:], ch, n)
			if !ok {
				return false
			}

			pos += adv
			i += n
		default:
			if pos >= len(text) || text[pos] != ch {
				return false
			}

			pos++
			i++
		}
	}

	return pos == len(text)
}

func (f *epochFields) field(s string, ch byte, n int) (int, bool) {
	switch ch {
	case 'y', 'u':
		return f.yearField(s, n)
	case 'M', 'L':
		return f.monthField(s, n)
	case 'd':
		return numField(s, n, &f.day)
	case 'H':
		return numField(s, n, &f.hour)
	case 'h', 'K':
		f.clock = true

		return numField(s, n, &f.hour)
	case 'm':
		return numField(s, n, &f.minute)
	case 's':
		return numField(s, n, &f.second)
	case 'S':
		return f.fractionField(s, n)
	case 'a':
		return f.ampmField(s)
	case 'E':
		return letterRun(s)
	case 'V', 'z', 'O', 'X', 'x', 'Z':
		return f.zoneField(s)
	}

	return 0, false
}

func (f *epochFields) yearField(s string, n int) (int, bool) {
	adv, ok := numField(s, max(n, 1), &f.year)
	if ok && n == yearTwoDigit {
		f.year += centuryBase
	}

	return adv, ok
}

func (f *epochFields) monthField(s string, n int) (int, bool) {
	if n < shortName {
		return numField(s, n, &f.month)
	}

	for m := time.January; m <= time.December; m++ {
		name := m.String()
		if n == shortName {
			name = name[:shortName]
		}

		if len(s) >= len(name) && strings.EqualFold(s[:len(name)], name) {
			f.month = int(m)

			return len(name), true
		}
	}

	return 0, false
}

func (f *epochFields) fractionField(s string, n int) (int, bool) {
	digits := digitRun(s, n)
	if digits != n {
		return 0, false
	}

	frac := s[:digits]
	for len(frac) < nanoDigits {
		frac += "0"
	}

	v, err := strconv.Atoi(frac[:nanoDigits])
	f.nanos = v

	return digits, err == nil
}

func (f *epochFields) ampmField(s string) (int, bool) {
	if len(s) < ampmLen {
		return 0, false
	}

	switch strings.ToUpper(s[:ampmLen]) {
	case "AM":
		f.pm = 0
	case "PM":
		f.pm = 1
	default:
		return 0, false
	}

	return ampmLen, true
}

// zoneField reads a zone id, offset or UTC+hh:mm form up to the next space.
func (f *epochFields) zoneField(s string) (int, bool) {
	end := strings.IndexFunc(s, unicode.IsSpace)
	if end < 0 {
		end = len(s)
	}

	loc, ok := parseZone(s[:end])
	f.loc = loc

	return end, ok
}

func parseZone(z string) (*time.Location, bool) {
	z = strings.TrimPrefix(strings.TrimPrefix(z, "UTC"), "GMT")

	switch {
	case z == "" || z == "Z":
		return time.UTC, true
	case z[0] == '+' || z[0] == '-':
		return parseOffset(z)
	}

	loc, err := time.LoadLocation(z)

	return loc, err == nil
}

func parseOffset(z string) (*time.Location, bool) {
	sign := 1
	if z[0] == '-' {
		sign = -1
	}

	digits := strings.ReplaceAll(z[1:], ":", "")
	if len(digits) != hourDigits && len(digits) != hourMinDigits {
		return nil, false
	}

	h, err := strconv.Atoi(digits[:ampmLen])
	if err != nil {
		return nil, false
	}

	m := 0
	if len(digits) == hourMinDigits {
		if m, err = strconv.Atoi(digits[hourDigits:]); err != nil {
			return nil, false
		}
	}

	return time.FixedZone("", sign*(h*secsPerHour+m*secsPerMinute)), true
}

func digitRun(s string, limit int) int {
	n := 0
	for n < len(s) && n < limit && s[n] >= '0' && s[n] <= '9' {
		n++
	}

	return n
}

// numField reads up to n digits (exactly n when n is 2 or more) into dst.
func numField(s string, n int, dst *int) (int, bool) {
	limit := n
	if n == 1 {
		limit = greedyDigits
	}

	d := digitRun(s, limit)
	if d == 0 || (n > 1 && n < fullName && d != n) {
		return 0, false
	}

	v, err := strconv.Atoi(s[:d])
	*dst = v

	return d, err == nil
}

func letterRun(s string) (int, bool) {
	n := 0
	for n < len(s) && unicode.IsLetter(rune(s[n])) {
		n++
	}

	return n, n > 0
}
