package azurearm

import (
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	twoDigitYearCutoff = 30
	fullYearDigits     = 4
	maxFracDigits      = 9
)

type fmtTok struct {
	lit  string
	n    int
	kind byte
}

func tokenizeNetFormat(f string) []fmtTok {
	var toks []fmtTok

	for i := 0; i < len(f); {
		c := f[i]

		if lit, skip, ok := formatLiteral(f, i, c); ok {
			toks = append(toks, fmtTok{kind: 'L', lit: lit})
			i += skip

			continue
		}

		n := countRun(f, i)

		switch c {
		case 'y', 'M', 'd', 'H', 'h', 'm', 's', 'f', 'F', 't', 'K', 'z':
			toks = append(toks, fmtTok{kind: c, n: n})
		default:
			toks = append(toks, fmtTok{kind: 'L', lit: strings.Repeat(string(c), n)})
		}

		i += n
	}

	return toks
}

type parsedTime struct {
	year, month, day, hour, min, sec, nsec int
	pm, hasPM, twelve, zone                bool
}

type timeScanner struct {
	loc logicLocale
	s   string
	pos int
}

func (sc *timeScanner) digits(minN, maxN int) (int, bool) {
	start := sc.pos
	for sc.pos < len(sc.s) && sc.pos-start < maxN && sc.s[sc.pos] >= '0' && sc.s[sc.pos] <= '9' {
		sc.pos++
	}

	if sc.pos-start < minN {
		return 0, false
	}

	v, err := strconv.Atoi(sc.s[start:sc.pos])

	return v, err == nil
}

// name matches one of names (full, or the first three letters when abbrev) ignoring case; it returns its index.
func (sc *timeScanner) name(names []string, abbrev bool) (int, bool) {
	rest := strings.ToLower(sc.s[sc.pos:])
	best, bestLen := -1, 0

	for i, nm := range names {
		cand := strings.ToLower(nm)
		if abbrev {
			cand = string([]rune(cand)[:widthThree])
		}

		if strings.HasPrefix(rest, cand) && len(cand) > bestLen {
			best, bestLen = i, len(cand)
		}
	}

	if best < 0 {
		return 0, false
	}

	sc.pos += bestLen

	return best, true
}

func (sc *timeScanner) literal(lit string) bool {
	if !strings.HasPrefix(sc.s[sc.pos:], lit) {
		return false
	}

	sc.pos += len(lit)

	return true
}

func (sc *timeScanner) field(t fmtTok, pt *parsedTime) bool {
	switch t.kind {
	case 'L':
		return sc.literal(t.lit)
	case 'y':
		return sc.year(t.n, pt)
	case 'M':
		return sc.month(t.n, pt)
	case 'd':
		return sc.day(t.n, pt)
	case 'H', 'h', 'm', 's':
		return sc.clock(t, pt)
	case 'f', 'F':
		return sc.fraction(t, pt)
	case 't':
		return sc.meridiem(pt)
	}

	return sc.offset(pt)
}

func (sc *timeScanner) year(n int, pt *parsedTime) bool {
	switch {
	case n >= widthThree:
		y, ok := sc.digits(widthThree, fullYearDigits)
		pt.year = y

		return ok
	case n == widthTwo:
		y, ok := sc.digits(widthTwo, widthTwo)
		pt.year = y + 2000 //nolint:mnd // two-digit year pivot

		if y >= twoDigitYearCutoff {
			pt.year = y + 1900 //nolint:mnd // two-digit year pivot
		}

		return ok
	}

	y, ok := sc.digits(1, widthTwo)
	pt.year = y

	return ok
}

func (sc *timeScanner) month(n int, pt *parsedTime) bool {
	if n >= widthThree {
		i, ok := sc.name(sc.loc.months[:], n == widthThree)
		pt.month = i + 1

		return ok
	}

	v, ok := sc.digits(1, widthTwo)
	pt.month = v

	return ok
}

func (sc *timeScanner) day(n int, pt *parsedTime) bool {
	if n >= widthThree {
		_, ok := sc.name(sc.loc.days[:], n == widthThree)

		return ok
	}

	v, ok := sc.digits(1, widthTwo)
	pt.day = v

	return ok
}

func (sc *timeScanner) clock(t fmtTok, pt *parsedTime) bool {
	v, ok := sc.digits(1, widthTwo)

	switch t.kind {
	case 'H':
		pt.hour = v
	case 'h':
		pt.hour, pt.twelve = v, true
	case 'm':
		pt.min = v
	default:
		pt.sec = v
	}

	return ok
}

func (sc *timeScanner) fraction(t fmtTok, pt *parsedTime) bool {
	if t.n > maxFracDigits {
		return false
	}

	start := sc.pos
	v, ok := sc.digits(t.n, t.n)

	if !ok && t.kind == 'F' {
		sc.pos = start

		return true
	}

	for i := t.n; i < maxFracDigits; i++ {
		v *= 10
	}

	pt.nsec = v

	return ok
}

func (sc *timeScanner) meridiem(pt *parsedTime) bool {
	rest := strings.ToUpper(sc.s[sc.pos:])

	switch {
	case strings.HasPrefix(rest, "AM"):
		sc.pos += widthTwo
	case strings.HasPrefix(rest, "PM"):
		sc.pos += widthTwo
		pt.pm = true
	default:
		return false
	}

	pt.hasPM = true

	return true
}

func (sc *timeScanner) offset(pt *parsedTime) bool {
	if sc.literal("Z") {
		pt.zone = true

		return true
	}

	if sc.pos < len(sc.s) && (sc.s[sc.pos] == '+' || sc.s[sc.pos] == '-') {
		sc.pos++

		if _, ok := sc.digits(1, widthTwo); !ok {
			return false
		}

		if sc.literal(":") {
			if _, ok := sc.digits(widthTwo, widthTwo); !ok {
				return false
			}
		}

		pt.zone = true

		return true
	}

	return false
}

func (pt *parsedTime) build() (logicTS, bool) {
	h := pt.hour
	if pt.twelve {
		if h < 1 || h > twelve {
			return logicTS{}, false
		}

		h %= twelve
		if pt.pm {
			h += twelve
		}
	}

	if pt.year == 0 {
		pt.year = 1
	}

	if pt.month == 0 {
		pt.month = 1
	}

	if pt.day == 0 {
		pt.day = 1
	}

	t := time.Date(pt.year, time.Month(pt.month), pt.day, h, pt.min, pt.sec, pt.nsec, time.UTC)
	if t.Year() != pt.year || int(t.Month()) != pt.month || t.Day() != pt.day || h > twelve*widthTwo-1 ||
		pt.min > 59 || pt.sec > 59 {
		return logicTS{}, false
	}

	return logicTS{t: t, hasZone: pt.zone}, true
}

func parseNetFormat(s, format string, loc logicLocale) (logicTS, bool) {
	sc := &timeScanner{s: s, loc: loc}
	pt := &parsedTime{}

	for _, tok := range tokenizeNetFormat(format) {
		if !sc.field(tok, pt) {
			return logicTS{}, false
		}
	}

	if sc.pos != len(sc.s) || !utf8.ValidString(s) {
		return logicTS{}, false
	}

	return pt.build()
}

var logicLocaleFormats = map[string][]string{ //nolint:gochecknoglobals // static per-locale date patterns
	logicLocaleEnUS: {"M/d/yyyy", "MMMM d, yyyy", "dddd, MMMM d, yyyy", "MMM d, yyyy"},
	"fr-fr":         {"d/M/yyyy", "d MMMM yyyy", "dddd d MMMM yyyy", "d MMM yyyy"},
	"es-es": {
		"d/M/yyyy", "d MMMM yyyy", "dddd d MMMM yyyy", "d 'de' MMMM 'de' yyyy", "dddd, d 'de' MMMM 'de' yyyy",
	},
}

var logicTimeSuffixes = []string{ //nolint:gochecknoglobals // static
	"", " H:mm", " H:mm:ss", " h:mm tt", " h:mm:ss tt", " H'h'",
}

func parseLocaleDefault(s string, loc logicLocale, key string) (logicTS, bool) {
	if len(s) >= fullYearDigits+1 && s[fullYearDigits] == '-' {
		if ts, err := parseLogicTime(s); err == nil {
			return ts, true
		}
	}

	for _, base := range logicLocaleFormats[key] {
		for _, suf := range logicTimeSuffixes {
			if ts, ok := parseNetFormat(s, base+suf, loc); ok {
				return ts, true
			}
		}
	}

	if ts, err := parseLogicTime(s); err == nil {
		return ts, true
	}

	return logicTS{}, false
}

func logicParseDateTime(a []any) (any, error) {
	s := strings.TrimSpace(logicString(a[0]))
	key := logicLocaleEnUS

	if len(a) >= argsTwo && logicString(a[1]) != "" {
		key = strings.ToLower(logicString(a[1]))
	}

	loc, ok := logicLocales[key]
	if !ok {
		return nil, exprErr("the locale %q is not supported", logicString(a[1]))
	}

	var (
		ts    logicTS
		found bool
	)

	if len(a) == argsThree && logicString(a[2]) != "" {
		ts, found = parseWithFormat(s, logicString(a[2]), loc)
	} else {
		ts, found = parseLocaleDefault(s, loc, key)
	}

	if !found {
		return nil, exprErr("the timestamp %q is not valid for the locale and format", s)
	}

	return formatLogicTime(ts, logicRoundTrip, "")
}

func parseWithFormat(s, format string, loc logicLocale) (logicTS, bool) {
	if format == logicRoundTrip || format == "O" {
		ts, err := parseLogicTime(s)

		return ts, err == nil
	}

	if f, std := logicStandardFormats[format]; std {
		format = f
	} else if len(format) == 1 {
		return logicTS{}, false
	}

	return parseNetFormat(s, format, loc)
}
