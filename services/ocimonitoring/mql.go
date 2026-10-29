package ocimonitoring

import (
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

// Statistics supported by the MQL subset.
const (
	statMean      = "mean"
	statSum       = "sum"
	statCount     = "count"
	statMax       = "max"
	statMin       = "min"
	statRate      = "rate"
	statPercent   = "percentile"
	statFirst     = "first"
	statLast      = "last"
	statIncrement = "increment"
	statAbsent    = "absent"

	hoursPerDay = 24
)

// Dimension filter operators.
const (
	opEq      = "="
	opNotEq   = "!="
	opFuzzy   = "=~"
	opNotFuzz = "!=~"
)

type dimFilter struct {
	key, op, value string
}

type predicate struct {
	op     string
	values []float64
}

// MQL is a parsed Monitoring Query Language expression.
type MQL struct {
	Pred     *predicate
	Metric   string
	Stat     string
	Filters  []dimFilter
	GroupBy  []string
	Interval time.Duration
	Quantile float64
	Grouped  bool
}

var intervalRe = regexp.MustCompile(`^(\d+)([mhd])$`)

// parseInterval parses 1m-60m, 1h-24h or 1d.
func parseInterval(s string) (time.Duration, *APIError) {
	m := intervalRe.FindStringSubmatch(s)
	if m == nil {
		return 0, errParam("interval " + strconv.Quote(s) + " must be 1m-60m, 1h-24h or 1d")
	}

	n, _ := strconv.Atoi(m[1])

	switch {
	case m[2] == "m" && n >= 1 && n <= 60:
		return time.Duration(n) * time.Minute, nil
	case m[2] == "h" && n >= 1 && n <= 24:
		return time.Duration(n) * time.Hour, nil
	case m[2] == "d" && n == 1:
		return hoursPerDay * time.Hour, nil
	}

	return 0, errParam("interval " + strconv.Quote(s) + " must be 1m-60m, 1h-24h or 1d")
}

type mqlScanner struct {
	src []rune
	pos int
}

func (s *mqlScanner) skip() {
	for s.pos < len(s.src) && unicode.IsSpace(s.src[s.pos]) {
		s.pos++
	}
}

func (s *mqlScanner) peek() rune {
	s.skip()

	if s.pos >= len(s.src) {
		return 0
	}

	return s.src[s.pos]
}

func (s *mqlScanner) accept(lit string) bool {
	s.skip()

	if strings.HasPrefix(string(s.src[s.pos:]), lit) {
		s.pos += len([]rune(lit))

		return true
	}

	return false
}

func (s *mqlScanner) word() string {
	s.skip()

	start := s.pos
	for s.pos < len(s.src) && (unicode.IsLetter(s.src[s.pos]) || unicode.IsDigit(s.src[s.pos]) || s.src[s.pos] == '_') {
		s.pos++
	}

	return string(s.src[start:s.pos])
}

func (s *mqlScanner) quoted() (string, bool) {
	s.skip()

	if s.pos >= len(s.src) || s.src[s.pos] != '"' {
		return "", false
	}

	var sb strings.Builder

	for s.pos++; s.pos < len(s.src); s.pos++ {
		switch c := s.src[s.pos]; {
		case c == '\\' && s.pos+1 < len(s.src):
			s.pos++
			sb.WriteRune(s.src[s.pos])
		case c == '"':
			s.pos++

			return sb.String(), true
		default:
			sb.WriteRune(c)
		}
	}

	return "", false
}

func (s *mqlScanner) number() (float64, bool) {
	s.skip()

	start := s.pos
	if s.pos < len(s.src) && s.src[s.pos] == '-' {
		s.pos++
	}

	for s.pos < len(s.src) && (unicode.IsDigit(s.src[s.pos]) || s.src[s.pos] == '.') {
		s.pos++
	}

	f, err := strconv.ParseFloat(string(s.src[start:s.pos]), 64)
	if err != nil {
		s.pos = start

		return 0, false
	}

	return f, true
}

// ParseMQL parses `Metric[interval]{filters}.groupBy(dims).stat() [predicate]`.
func ParseMQL(src string) (*MQL, error) {
	s := &mqlScanner{src: []rune(src)}
	q := &MQL{}

	open := strings.IndexRune(src, '[')
	if open <= 0 {
		return nil, errParam("a query needs a metric name followed by an interval in square brackets")
	}

	q.Metric = strings.TrimSpace(src[:open])
	if q.Metric == "" {
		return nil, errParam("a query needs a metric name")
	}

	s.pos = len([]rune(src[:open])) + 1

	closeIdx := strings.IndexRune(string(s.src[s.pos:]), ']')
	if closeIdx < 0 {
		return nil, errParam("the interval is missing its closing bracket")
	}

	iv, aerr := parseInterval(strings.TrimSpace(string(s.src[s.pos : s.pos+closeIdx])))
	if aerr != nil {
		return nil, aerr
	}

	q.Interval, s.pos = iv, s.pos+closeIdx+1

	if aerr = q.parseFilters(s); aerr != nil {
		return nil, aerr
	}

	if aerr = q.parseChain(s); aerr != nil {
		return nil, aerr
	}

	if aerr = q.parsePredicate(s); aerr != nil {
		return nil, aerr
	}

	if s.peek() != 0 {
		return nil, errParam("unexpected " + strconv.Quote(string(s.src[s.pos:])) +
			" in query; joins (&&, ||) and arithmetic are not supported by this emulator")
	}

	return q, nil
}

func (q *MQL) parseFilters(s *mqlScanner) *APIError {
	if s.peek() != '{' {
		return nil
	}

	s.pos++

	for {
		key := s.word()
		if key == "" {
			return errParam("a dimension filter needs a dimension name")
		}

		var op string

		for _, cand := range []string{opNotFuzz, opFuzzy, opNotEq, opEq} {
			if s.accept(cand) {
				op = cand

				break
			}
		}

		if op == "" {
			return errParam("dimension filter " + key + " needs one of =, !=, =~ or !=~")
		}

		val, ok := s.quoted()
		if !ok {
			return errParam("dimension filter values must be double-quoted")
		}

		q.Filters = append(q.Filters, dimFilter{key: key, op: op, value: val})

		switch {
		case s.accept(","):
		case s.accept("}"):
			return nil
		default:
			return errParam("a dimension filter set must end with }")
		}
	}
}

func (q *MQL) parseChain(s *mqlScanner) *APIError {
	for s.accept(".") {
		fn := s.word()
		if fn == "" || !s.accept("(") {
			return errParam("expected a function call such as .mean() after the metric")
		}

		var aerr *APIError

		switch {
		case strs.Equal(fn, "groupBy"):
			aerr = q.parseGroupBy(s)
		case strs.Equal(fn, "grouping"):
			aerr = errParam("grouping() is not supported by this emulator; use groupBy()")
		default:
			aerr = q.setStat(strings.ToLower(fn), s)
		}

		if aerr != nil {
			return aerr
		}

		if !s.accept(")") {
			return errParam("missing ) after " + fn)
		}
	}

	if q.Stat == "" {
		return errParam("a query needs a statistic such as .mean()")
	}

	return nil
}

func (q *MQL) parseGroupBy(s *mqlScanner) *APIError {
	q.Grouped = true

	for s.peek() != ')' {
		d := s.word()
		if d == "" {
			return errParam("groupBy takes dimension names")
		}

		q.GroupBy = append(q.GroupBy, d)

		if !s.accept(",") {
			break
		}
	}

	return nil
}

func (q *MQL) setStat(fn string, s *mqlScanner) *APIError {
	if q.Stat != "" {
		return errParam("a query can have only one statistic")
	}

	switch fn {
	case "avg":
		fn = statMean
	case statMean, statSum, statCount, statMax, statMin, statRate, statFirst, statLast, statIncrement:
	case statAbsent:
		s.number()
	case statPercent:
		p, ok := s.number()
		if !ok || p <= 0 || p >= 1 {
			return errParam("percentile(p) needs 0 < p < 1")
		}

		q.Quantile = p
	default:
		return errParam("statistic " + fn + "() is not supported by this emulator")
	}

	q.Stat = fn

	return nil
}

func (q *MQL) parsePredicate(s *mqlScanner) *APIError {
	for _, op := range []string{">=", "<=", "==", "!=", ">", "<"} {
		if s.accept(op) {
			v, ok := s.number()
			if !ok {
				return errParam("a threshold number is required after " + op)
			}

			q.Pred = &predicate{op: op, values: []float64{v}}

			return nil
		}
	}

	negate := false

	save := s.pos
	if strs.Equal(s.word(), "not") {
		negate = true
	} else {
		s.pos = save
	}

	if strs.Equal(s.word(), "in") && s.accept("(") {
		lo, ok1 := s.number()
		_ = s.accept(",")
		hi, ok2 := s.number()

		if !ok1 || !ok2 || !s.accept(")") {
			return errParam("in (low, high) needs two numbers")
		}

		op := "in"
		if negate {
			op = "not in"
		}

		q.Pred = &predicate{op: op, values: []float64{lo, hi}}

		return nil
	}

	s.pos = save

	return nil
}

func (p *predicate) holds(v float64) bool {
	switch p.op {
	case ">":
		return v > p.values[0]
	case ">=":
		return v >= p.values[0]
	case "<":
		return v < p.values[0]
	case "<=":
		return v <= p.values[0]
	case "==":
		return v == p.values[0]
	case "!=":
		return v != p.values[0]
	case "in":
		return v >= p.values[0] && v <= p.values[1]
	}

	return v < p.values[0] || v > p.values[1]
}

func fuzzyMatch(pattern, v string) bool {
	for alt := range strings.SplitSeq(pattern, "|") {
		parts := strings.Split(alt, "*")
		for i := range parts {
			parts[i] = regexp.QuoteMeta(parts[i])
		}

		if regexp.MustCompile("^" + strings.Join(parts, ".*") + "$").MatchString(v) {
			return true
		}
	}

	return false
}

// matches reports whether a dimension set satisfies every filter.
func (q *MQL) matches(dims map[string]string) bool {
	for _, f := range q.Filters {
		v, present := dims[f.key]

		var ok bool

		switch f.op {
		case opEq:
			ok = present && v == f.value
		case opNotEq:
			ok = !present || v != f.value
		case opFuzzy:
			ok = present && fuzzyMatch(f.value, v)
		default:
			ok = !present || !fuzzyMatch(f.value, v)
		}

		if !ok {
			return false
		}
	}

	return true
}
