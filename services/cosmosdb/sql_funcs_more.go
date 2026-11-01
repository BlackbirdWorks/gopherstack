package cosmosdb

import (
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"math"
	"math/big"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Built-in functions of the Cosmos DB query language reference (Learn, "Functions") beyond the first set:
// trigonometry, 64-bit integer arithmetic and bit operations, number and date binning, more string and array
// helpers, regular-expression extraction and replacement, and the conditional IIF.
const (
	biAcos              = "ACOS"
	biAsin              = "ASIN"
	biAtan              = "ATAN"
	biAtn2              = "ATN2"
	biCos               = "COS"
	biCot               = "COT"
	biDegrees           = "DEGREES"
	biRadians           = "RADIANS"
	biSin               = "SIN"
	biTan               = "TAN"
	biRand              = "RAND"
	biNumberbin         = "NUMBERBIN"
	biIntadd            = "INTADD"
	biIntsub            = "INTSUB"
	biIntmul            = "INTMUL"
	biIntdiv            = "INTDIV"
	biIntmod            = "INTMOD"
	biIntbitand         = "INTBITAND"
	biIntbitor          = "INTBITOR"
	biIntbitxor         = "INTBITXOR"
	biIntbitnot         = "INTBITNOT"
	biIntbitleftshift   = "INTBITLEFTSHIFT"
	biIntbitrightshift  = "INTBITRIGHTSHIFT"
	biArrayContainsAll  = "ARRAY_CONTAINS_ALL"
	biArrayContainsAny  = "ARRAY_CONTAINS_ANY"
	biChoose            = "CHOOSE"
	biObjecttoarray     = "OBJECTTOARRAY"
	biLastsubafter      = "LASTSUBSTRINGAFTER"
	biLastsubbefore     = "LASTSUBSTRINGBEFORE"
	biSubafter          = "SUBSTRINGAFTER"
	biSubbefore         = "SUBSTRINGBEFORE"
	biRegexextract      = "REGEXEXTRACT"
	biRegexextractall   = "REGEXEXTRACTALL"
	biRegexreplace      = "REGEXREPLACE"
	biRegexreplaceall   = "REGEXREPLACEALL"
	biStringjoin        = "STRINGJOIN"
	biStringsplit       = "STRINGSPLIT"
	biDatetimebin       = "DATETIMEBIN"
	biDocumentid        = "DOCUMENTID"
	biIif               = "IIF"
	biIsDatetime        = "IS_DATETIME"
	biStArea            = "ST_AREA"
	maxShift            = 63
	maxRegexArgs        = 4
	maxBinStartYear     = 1601
	regexOptionsArg     = 2
	regexGroupArg       = 3
	maxFractionalDigits = 7
	fracGroup           = 7
	monthGroup          = 2
	dayGroup            = 3
	hourGroup           = 4
	minuteGroup         = 5
	secondGroup         = 6
	randBits            = 53
	randShift           = 64 - randBits
)

var cosmosDateTime = regexp.MustCompile(
	`^(\d{4})-(\d{2})-(\d{2})(?:[T ](\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,7}))?)?Z?$`,
)

// parseCosmosDateTime accepts the date and time strings IS_DATETIME accepts (Learn, "IS_DATETIME"): a date,
// optionally followed by a T or space and hh:mm:ss with up to seven fractional digits, optionally ending in Z.
func parseCosmosDateTime(s string) (time.Time, bool) {
	m := cosmosDateTime.FindStringSubmatch(s)
	if m == nil {
		return time.Time{}, false
	}

	n := func(i int) int {
		v, _ := strconv.Atoi(m[i])

		return v
	}

	nanos := 0

	if m[fracGroup] != "" {
		frac := m[fracGroup] + strings.Repeat("0", maxFractionalDigits-len(m[fracGroup]))
		ticks, _ := strconv.Atoi(frac)
		nanos = ticks * ticksPerNano
	}

	year, month, day, hour, minute, second := n(
		1,
	), n(
		monthGroup,
	), n(
		dayGroup,
	), n(
		hourGroup,
	), n(
		minuteGroup,
	), n(
		secondGroup,
	)

	t := time.Date(year, time.Month(month), day, hour, minute, second, nanos, time.UTC)
	if t.Year() != year || int(t.Month()) != month || t.Day() != day || hour > 23 || minute > 59 || second > 59 {
		return time.Time{}, false
	}

	return t, true
}

func moreKind(name string) bool {
	switch name {
	case biAcos, biAsin, biAtan, biAtn2, biCos, biCot, biDegrees, biRadians, biSin, biTan, biRand, biNumberbin,
		biIntadd, biIntsub, biIntmul, biIntdiv, biIntmod, biIntbitand, biIntbitor, biIntbitxor, biIntbitnot,
		biIntbitleftshift, biIntbitrightshift, biArrayContainsAll, biArrayContainsAny, biChoose, biObjecttoarray,
		biLastsubafter, biLastsubbefore, biSubafter, biSubbefore, biRegexextract, biRegexextractall, biRegexreplace,
		biRegexreplaceall, biStringjoin, biStringsplit, biDatetimebin, biIif, biIsDatetime, biStArea:
		return true
	}

	return false
}

func moreArity(name string) (int, int, bool) {
	switch name {
	case biRand:
		return 0, 0, true
	case biAcos, biAsin, biAtan, biCos, biCot, biDegrees, biRadians, biSin, biTan, biIntbitnot, biIsDatetime, biStArea:
		return 1, 1, true
	case biAtn2, biIntadd, biIntsub, biIntmul, biIntdiv, biIntmod, biIntbitand, biIntbitor, biIntbitxor,
		biIntbitleftshift, biIntbitrightshift, biLastsubafter, biLastsubbefore, biSubafter, biSubbefore,
		biStringjoin, biStringsplit:
		return arityPair, arityPair, true
	case biNumberbin:
		return 1, arityPair, true
	case biIif:
		return arityTriple, arityTriple, true
	case biArrayContainsAll, biArrayContainsAny, biChoose:
		return arityPair, -1, true
	case biObjecttoarray:
		return 1, arityTriple, true
	case biRegexextract, biRegexextractall:
		return arityPair, maxRegexArgs, true
	case biRegexreplace, biRegexreplaceall:
		return arityTriple, maxRegexArgs, true
	case biDatetimebin:
		return arityPair, arityQuad, true
	}

	return 0, 0, false
}

func (ec *evalCtx) callMore(name string, args []any) any {
	switch name {
	case biAcos, biAsin, biAtan, biAtn2, biCos, biCot, biDegrees, biRadians, biSin, biTan, biRand, biNumberbin:
		return moreMath(name, args)
	case biIntadd, biIntsub, biIntmul, biIntdiv, biIntmod, biIntbitand, biIntbitor, biIntbitxor, biIntbitnot,
		biIntbitleftshift, biIntbitrightshift:
		return intMath(name, args)
	case biArrayContainsAll, biArrayContainsAny, biChoose, biObjecttoarray, biIif:
		return moreCollection(name, args)
	case biRegexextract, biRegexextractall, biRegexreplace, biRegexreplaceall:
		return ec.regexOp(name, args)
	case biDatetimebin:
		return dateTimeBin(args)
	case biIsDatetime:
		s, ok := args[0].(string)
		if !ok {
			return false
		}

		_, valid := parseCosmosDateTime(s)

		return valid
	case biStArea:
		return callSpatialArea(args[0], ec.geometry)
	default:
		return moreString(name, args)
	}
}

func moreMath(name string, args []any) any {
	if name == biRand {
		return floatResult(randomUnit())
	}

	a, ok := toNumber(args[0])
	if !ok {
		return undef{}
	}

	x := numFloat(a)

	switch name {
	case biAtn2:
		b, bok := toNumber(args[1])
		if !bok {
			return undef{}
		}

		return floatResult(math.Atan2(x, numFloat(b)))
	case biNumberbin:
		return numberBin(a, args)
	default:
		return trigonometry(name, x)
	}
}

const degreesPerHalfTurn = 180

func trigonometry(name string, x float64) any {
	switch name {
	case biAcos:
		return floatResult(math.Acos(x))
	case biAsin:
		return floatResult(math.Asin(x))
	case biAtan:
		return floatResult(math.Atan(x))
	case biCos:
		return floatResult(math.Cos(x))
	case biSin:
		return floatResult(math.Sin(x))
	case biTan:
		return floatResult(math.Tan(x))
	case biCot:
		if x == 0 {
			return undef{}
		}

		return floatResult(1 / math.Tan(x))
	case biDegrees:
		return floatResult(x * degreesPerHalfTurn / math.Pi)
	default:
		return floatResult(x * math.Pi / degreesPerHalfTurn)
	}
}

// numberBin rounds a down to a multiple of the bin size (default 1): floor(a / size) * size, so a negative size
// bins upward as the documented example shows. A zero or non-numeric size has no result.
func numberBin(a json.Number, args []any) any {
	size := 1.0

	if len(args) > 1 {
		s, ok := toNumber(args[1])
		if !ok {
			return undef{}
		}

		size = numFloat(s)
	}

	if size == 0 {
		return undef{}
	}

	if ai, aErr := a.Int64(); aErr == nil && size == math.Trunc(size) {
		si := int64(size)
		q := ai / si

		if ai%si != 0 && (ai < 0) != (si < 0) {
			q--
		}

		return json.Number(strconv.FormatInt(q*si, 10))
	}

	return floatResult(math.Floor(numFloat(a)/size) * size)
}

// int64Arg reads an integral number; a fractional value or a non-number has no integer reading.
func int64Arg(v any) (int64, bool) {
	n, ok := toNumber(v)
	if !ok {
		return 0, false
	}

	if i, err := n.Int64(); err == nil {
		return i, true
	}

	f := numFloat(n)
	if f != math.Trunc(f) || math.Abs(f) >= math.MaxInt64 {
		return 0, false
	}

	return int64(f), true
}

// intMath implements the INT* functions: both operands must be integers, else (and on overflow or division by
// zero) there is no result, as the documented examples with decimal and zero operands show.
func intMath(name string, args []any) any {
	a, ok := int64Arg(args[0])
	if !ok {
		return undef{}
	}

	if name == biIntbitnot {
		return json.Number(strconv.FormatInt(^a, 10))
	}

	b, bok := int64Arg(args[1])
	if !bok {
		return undef{}
	}

	r, valid := intBinary(name, a, b)
	if !valid {
		return undef{}
	}

	return json.Number(strconv.FormatInt(r, 10))
}

func intBinary(name string, a, b int64) (int64, bool) {
	switch name {
	case biIntadd, biIntsub, biIntmul, biIntdiv, biIntmod:
		return intArithmetic(name, a, b)
	case biIntbitand:
		return a & b, true
	case biIntbitor:
		return a | b, true
	case biIntbitxor:
		return a ^ b, true
	}

	if b < 0 || b > maxShift {
		return 0, false
	}

	shift := uint(b & maxShift)
	if name == biIntbitleftshift {
		return a << shift, true
	}

	return a >> shift, true
}

func intArithmetic(name string, a, b int64) (int64, bool) {
	switch name {
	case biIntadd:
		return addInt64(a, b)
	case biIntsub:
		return subInt64(a, b)
	case biIntmul:
		return intMultiply(a, b)
	}

	if b == 0 || (a == math.MinInt64 && b == -1) {
		return 0, false
	}

	if name == biIntdiv {
		return a / b, true
	}

	return a % b, true
}

func addInt64(a, b int64) (int64, bool) {
	r := a + b
	overflowPos := a > 0 && b > 0 && r < 0
	overflowNeg := a < 0 && b < 0 && r >= 0

	return r, !overflowPos && !overflowNeg
}

func subInt64(a, b int64) (int64, bool) {
	r := a - b
	overflowPos := a >= 0 && b < 0 && r < 0
	overflowNeg := a < 0 && b > 0 && r >= 0

	return r, !overflowPos && !overflowNeg
}

func intMultiply(a, b int64) (int64, bool) {
	r := a * b
	if a != 0 && (r/a != b || (a == -1 && b == math.MinInt64)) {
		return 0, false
	}

	return r, true
}

func moreCollection(name string, args []any) any {
	switch name {
	case biIif:
		if cond, ok := args[0].(bool); ok && cond {
			return args[1]
		}

		return args[arityPair]
	case biChoose:
		i, ok := int64Arg(args[0])
		if !ok || i < 1 || int(i) >= len(args) {
			return undef{}
		}

		return args[i]
	case biObjecttoarray:
		return objectToArray(args)
	default:
		return arrayContainsMany(name == biArrayContainsAll, args)
	}
}

// arrayContainsMany answers ARRAY_CONTAINS_ALL / ARRAY_CONTAINS_ANY. An undefined value to look for makes the
// answer undefined unless a defined value already decides it, as the documented examples show.
func arrayContainsMany(all bool, args []any) any {
	arr, ok := args[0].([]any)
	if !ok {
		return undef{}
	}

	sawUndef := false

	for _, want := range args[1:] {
		if isUndef(want) {
			sawUndef = true

			continue
		}

		found := slices.ContainsFunc(arr, func(e any) bool { return deepEqual(e, want) })
		if all && !found {
			return false
		}

		if !all && found {
			return true
		}
	}

	if sawUndef {
		return undef{}
	}

	return all
}

func objectToArray(args []any) any {
	obj, ok := args[0].(map[string]any)
	if !ok {
		return undef{}
	}

	kName, vName := "k", "v"

	if len(args) == arityTriple {
		k, kok := args[1].(string)
		v, vok := args[arityPair].(string)

		if !kok || !vok {
			return undef{}
		}

		kName, vName = k, v
	} else if len(args) != 1 {
		return undef{}
	}

	keys := make([]string, 0, len(obj))
	for k := range obj {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	out := make([]any, 0, len(keys))
	for _, k := range keys {
		out = append(out, map[string]any{kName: k, vName: obj[k]})
	}

	return out
}

// moreString implements the delimiter helpers, STRINGJOIN and STRINGSPLIT. The pages do not say what the
// substring helpers return when the delimiter is absent; here that has no result.
func moreString(name string, args []any) any {
	if name == biStringjoin {
		return stringJoin(args)
	}

	ss, ok := strArgs(args, arityPair)
	if !ok {
		return undef{}
	}

	s, delim := ss[0], ss[1]

	switch name {
	case biStringsplit:
		if delim == "" {
			return []any{s}
		}

		parts := strings.Split(s, delim)
		out := make([]any, len(parts))

		for i, p := range parts {
			out[i] = p
		}

		return out
	case biSubafter:
		if _, after, found := strings.Cut(s, delim); found {
			return after
		}
	case biSubbefore:
		if before, _, found := strings.Cut(s, delim); found {
			return before
		}
	case biLastsubafter:
		if _, after, found := strings.CutLast(s, delim); found {
			return after
		}
	default:
		if before, _, found := strings.CutLast(s, delim); found {
			return before
		}
	}

	return undef{}
}

func stringJoin(args []any) any {
	arr, ok := args[0].([]any)
	sep, sok := args[1].(string)

	if !ok || !sok {
		return undef{}
	}

	parts := make([]string, len(arr))

	for i, e := range arr {
		s, isStr := e.(string)
		if !isStr {
			return undef{}
		}

		parts[i] = s
	}

	return strings.Join(parts, sep)
}

// regexOp implements REGEXEXTRACT, REGEXEXTRACTALL, REGEXREPLACE and REGEXREPLACEALL with the i, m, s and x
// options of REGEXMATCH.
func (ec *evalCtx) regexOp(name string, args []any) any {
	subject, ok := args[0].(string)
	pattern, pok := args[1].(string)

	if !ok || !pok {
		return undef{}
	}

	replace := name == biRegexreplace || name == biRegexreplaceall

	optIdx := regexOptionsArg
	if replace {
		optIdx = arityTriple
	}

	opts := ""

	if len(args) > optIdx {
		o, oOK := args[optIdx].(string)
		if !oOK {
			return undef{}
		}

		opts = o
	}

	re := ec.compileRegex(pattern, opts)
	if re == nil {
		return undef{}
	}

	if replace {
		repl, rok := args[regexOptionsArg].(string)
		if !rok {
			return undef{}
		}

		return replaceRegex(re, subject, repl, name == biRegexreplaceall)
	}

	group := 0

	if len(args) > regexGroupArg {
		g, gok := int64Arg(args[regexGroupArg])
		if !gok || g < 0 || int(g) > re.NumSubexp() {
			return undef{}
		}

		group = int(g)
	}

	return extractRegex(re, subject, group, name == biRegexextractall)
}

func (ec *evalCtx) compileRegex(pattern, opts string) *regexp.Regexp {
	pat := pattern

	if strings.ContainsRune(opts, 'x') {
		pat = stripFreeSpacing(pat)
	}

	if flags := strings.Map(func(r rune) rune {
		if r == 'i' || r == 'm' || r == 's' {
			return r
		}

		return -1
	}, opts); flags != "" {
		pat = "(?" + flags + ")" + pat
	}

	if re, ok := ec.regex[pat]; ok {
		return re
	}

	re, err := regexp.Compile(pat)
	if err != nil {
		return nil
	}

	ec.regex[pat] = re

	return re
}

func replaceRegex(re *regexp.Regexp, subject, repl string, all bool) any {
	if all {
		return re.ReplaceAllLiteralString(subject, repl)
	}

	loc := re.FindStringIndex(subject)
	if loc == nil {
		return subject
	}

	return subject[:loc[0]] + repl + subject[loc[1]:]
}

func extractRegex(re *regexp.Regexp, subject string, group int, all bool) any {
	if !all {
		m := re.FindStringSubmatch(subject)
		if m == nil || m[group] == "" && group > 0 && !matchedGroup(re, subject, group) {
			return undef{}
		}

		return m[group]
	}

	out := []any{}

	for _, m := range re.FindAllStringSubmatch(subject, -1) {
		out = append(out, m[group])
	}

	return out
}

func matchedGroup(re *regexp.Regexp, subject string, group int) bool {
	idx := re.FindStringSubmatchIndex(subject)

	return idx != nil && idx[2*group] >= 0
}

// dateTimeBin rounds a date and time down to a multiple of a bin of the given part, counted from a start date
// (default the Unix epoch): DATETIMEBIN(date, part [, size] [, start]).
func dateTimeBin(args []any) any {
	s, ok := args[0].(string)
	partName, pok := args[1].(string)

	if !ok || !pok {
		return undef{}
	}

	t, tok := parseCosmosDateTime(s)
	part := parseDatePart(partName)

	size := int64(1)

	if len(args) > arityPair {
		v, vok := int64Arg(args[arityPair])
		if !vok || v <= 0 {
			return undef{}
		}

		size = v
	}

	origin := time.Unix(0, 0).UTC()

	if len(args) > arityTriple {
		os, osOK := args[arityTriple].(string)
		if !osOK {
			return undef{}
		}

		o, oOK := parseCosmosDateTime(os)
		if !oOK || o.Year() < maxBinStartYear {
			return undef{}
		}

		origin = o
	}

	if !tok || part == dpInvalid {
		return undef{}
	}

	out, valid := binTime(t, part, size, origin)
	if !valid {
		return undef{}
	}

	return out.Format(dateTimeLayout)
}

func binTime(t time.Time, part datePartKind, size int64, origin time.Time) (time.Time, bool) {
	if part == dpYear {
		months := int64(t.Year()-origin.Year()) * monthsPerYear
		step := size * monthsPerYear
		binned := floorDiv(months, step) * step

		return time.Date(origin.Year()+int(binned/monthsPerYear), origin.Month(), 1, 0, 0, 0, 0, time.UTC), true
	}

	if part == dpMonth {
		months := int64(t.Year()-origin.Year())*monthsPerYear + int64(t.Month()) - int64(origin.Month())
		binned := floorDiv(months, size) * size

		return time.Date(origin.Year(), origin.Month()+time.Month(binned), 1, 0, 0, 0, 0, time.UTC), true
	}

	unit := part.duration()
	if part == dpDay {
		unit = hoursPerDay * time.Hour
	}

	if unit == 0 {
		return time.Time{}, false
	}

	return binByUnit(t, origin, unit, size), true
}

// binByUnit floors t to a multiple of size units since origin using integer nanoseconds wide enough for the
// 1601 bin start the function allows (a time.Duration only spans 292 years).
func binByUnit(t, origin time.Time, unit time.Duration, size int64) time.Time {
	const nanosPerSecond = int64(time.Second)

	ns := func(x time.Time) *big.Int {
		v := new(big.Int).Mul(big.NewInt(x.Unix()), big.NewInt(nanosPerSecond))

		return v.Add(v, big.NewInt(int64(x.Nanosecond())))
	}

	diff := new(big.Int).Sub(ns(t), ns(origin))
	step := new(big.Int).Mul(big.NewInt(size), big.NewInt(int64(unit)))
	q, _ := new(big.Int).DivMod(diff, step, new(big.Int))

	binned := new(big.Int).Mul(q, step)
	binned.Add(binned, ns(origin))

	sec, nano := new(big.Int).DivMod(binned, big.NewInt(nanosPerSecond), new(big.Int))

	return time.Unix(sec.Int64(), nano.Int64()).UTC()
}

func floorDiv(a, b int64) int64 {
	q := a / b
	if a%b != 0 && (a < 0) != (b < 0) {
		q--
	}

	return q
}

// documentIDOf is DOCUMENTID(alias): the internal numeric id of the item, here its change-feed sequence number
// (unique within the container, which is stronger than the per-physical-partition uniqueness the page gives).
func documentIDOf(n *exprCall, env []any) any {
	v, ok := n.args[0].(*exprVar)
	if !ok || v.idx < 0 || v.idx >= len(env) {
		return undef{}
	}

	row, isRow := env[v.idx].(*sqlRow)
	if !isRow || row.doc == nil {
		return undef{}
	}

	return json.Number(strconv.FormatInt(row.doc.Seq, 10))
}

// randomUnit is a uniform number in [0, 1).
func randomUnit() float64 {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return 0
	}

	return float64(binary.BigEndian.Uint64(b[:])>>randShift) / (1 << randBits)
}
