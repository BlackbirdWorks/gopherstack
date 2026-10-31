package cosmosdb

import (
	"encoding/json"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	trueText         = "true"
	nullText         = "null"
	arityPair        = 2
	arityTriple      = 3
	arityQuad        = 4
	maxDateTimeParts = 7
	datePartYearLong = "yyyy"
	datePartYear     = "year"
	datePartMonth    = "month"
	datePartDay      = "day"
)

const (
	biAbs                       = "ABS"
	biArrayConcat               = "ARRAY_CONCAT"
	biArrayContains             = "ARRAY_CONTAINS"
	biArrayLength               = "ARRAY_LENGTH"
	biArraySlice                = "ARRAY_SLICE"
	biCeiling                   = "CEILING"
	biConcat                    = "CONCAT"
	biContains                  = "CONTAINS"
	biDatetimeadd               = "DATETIMEADD"
	biDatetimediff              = "DATETIMEDIFF"
	biDatetimepart              = "DATETIMEPART"
	biDatetimetotimestamp       = "DATETIMETOTIMESTAMP"
	biEndswith                  = "ENDSWITH"
	biExp                       = "EXP"
	biFloor                     = "FLOOR"
	biGetcurrentdatetime        = "GETCURRENTDATETIME"
	biGetcurrentdatetimestatic  = "GETCURRENTDATETIMESTATIC"
	biGetcurrentticks           = "GETCURRENTTICKS"
	biGetcurrentticksstatic     = "GETCURRENTTICKSSTATIC"
	biGetcurrenttimestamp       = "GETCURRENTTIMESTAMP"
	biGetcurrenttimestampstatic = "GETCURRENTTIMESTAMPSTATIC"
	biIndexOf                   = "INDEX_OF"
	biIsArray                   = "IS_ARRAY"
	biIsBool                    = "IS_BOOL"
	biIsDefined                 = "IS_DEFINED"
	biIsFiniteNumber            = "IS_FINITE_NUMBER"
	biIsInteger                 = "IS_INTEGER"
	biIsNull                    = "IS_NULL"
	biIsNumber                  = "IS_NUMBER"
	biIsObject                  = "IS_OBJECT"
	biIsPrimitive               = "IS_PRIMITIVE"
	biIsString                  = "IS_STRING"
	biLeft                      = "LEFT"
	biLength                    = "LENGTH"
	biLog                       = "LOG"
	biLog10                     = "LOG10"
	biLower                     = "LOWER"
	biLtrim                     = "LTRIM"
	biPi                        = "PI"
	biPower                     = "POWER"
	biRegexmatch                = "REGEXMATCH"
	biReplace                   = "REPLACE"
	biReplicate                 = "REPLICATE"
	biReverse                   = "REVERSE"
	biRight                     = "RIGHT"
	biRound                     = "ROUND"
	biRtrim                     = "RTRIM"
	biSetintersect              = "SETINTERSECT"
	biSetunion                  = "SETUNION"
	biSign                      = "SIGN"
	biSqrt                      = "SQRT"
	biSquare                    = "SQUARE"
	biStartswith                = "STARTSWITH"
	biStringequals              = "STRINGEQUALS"
	biStringtoarray             = "STRINGTOARRAY"
	biStringtoobject            = "STRINGTOOBJECT"
	biStDistance                = "ST_DISTANCE"
	biStWithin                  = "ST_WITHIN"
	biStIntersects              = "ST_INTERSECTS"
	biStIsValid                 = "ST_ISVALID"
	biStIsValidDetailed         = "ST_ISVALIDDETAILED"
	biDatetimefromparts         = "DATETIMEFROMPARTS"
	biDatetimetoticks           = "DATETIMETOTICKS"
	biTickstodatetime           = "TICKSTODATETIME"
	biStringtoboolean           = "STRINGTOBOOLEAN"
	biStringtonull              = "STRINGTONULL"
	biStringtonumber            = "STRINGTONUMBER"
	biSubstring                 = "SUBSTRING"
	biTimestamptodatetime       = "TIMESTAMPTODATETIME"
	biTostring                  = "TOSTRING"
	biTrim                      = "TRIM"
	biTrunc                     = "TRUNC"
	biUpper                     = "UPPER"
	biVectorDistance            = "VECTORDISTANCE"
	biFullTextContains          = "FULLTEXTCONTAINS"
	biFullTextContainsAll       = "FULLTEXTCONTAINSALL"
	biFullTextContainsAny       = "FULLTEXTCONTAINSANY"
	biFullTextScore             = "FULLTEXTSCORE"
	biRRF                       = "RRF"
)

// fnKind groups built-in functions by implementation family.
type fnKind int

const (
	fnUnknown fnKind = iota
	fnString
	fnMath
	fnType
	fnArray
	fnDate
	fnAggregate
	fnUDF
	fnSpatial
	fnVector
	fnFullText
	fnRank
	fnMore
	fnDocumentID
)

// udfNamespace is the prefix of a user-defined function call: udf.name(args).
const udfNamespace = "udf"

func builtinKind(name string) fnKind {
	switch name {
	case biConcat, biContains, biStartswith, biEndswith, biLower, biUpper, biLength, biSubstring, biReplace,
		biTrim, biLtrim, biRtrim, biLeft, biRight, biReverse, biReplicate, biIndexOf, biTostring, biStringequals,
		biRegexmatch, biStringtonumber, biStringtoboolean, biStringtonull, biStringtoarray, biStringtoobject:
		return fnString
	case biStDistance, biStWithin, biStIntersects, biStIsValid, biStIsValidDetailed:
		return fnSpatial
	case biVectorDistance:
		return fnVector
	case biFullTextContains, biFullTextContainsAll, biFullTextContainsAny, biFullTextScore:
		return fnFullText
	case biRRF:
		return fnRank
	case biDocumentid:
		return fnDocumentID
	case biAbs,
		biCeiling,
		biFloor,
		biRound,
		biTrunc,
		biSqrt,
		biSquare,
		biPower,
		biSign,
		biExp,
		biLog,
		biLog10,
		biPi:
		return fnMath
	case biIsDefined, biIsNull, biIsNumber, biIsString, biIsBool, biIsArray, biIsObject, biIsPrimitive,
		biIsFiniteNumber, biIsInteger:
		return fnType
	case biArrayContains, biArrayLength, biArraySlice, biArrayConcat, biSetintersect, biSetunion:
		return fnArray
	case biGetcurrentdatetime, biGetcurrentdatetimestatic, biGetcurrenttimestamp, biGetcurrenttimestampstatic,
		biGetcurrentticks, biGetcurrentticksstatic, biDatetimeadd, biDatetimediff, biDatetimepart,
		biDatetimetotimestamp, biTimestamptodatetime, biDatetimefromparts, biDatetimetoticks, biTickstodatetime:
		return fnDate
	case aggCount, aggCountIf, aggSum, aggAvg, aggMin, aggMax:
		return fnAggregate
	default:
		if moreKind(name) {
			return fnMore
		}

		return fnUnknown
	}
}

// builtinArity returns the inclusive argument-count range; hi < 0 means unbounded.
func builtinArity(name string) (int, int) {
	switch name {
	case biPi, biGetcurrentdatetime, biGetcurrentdatetimestatic, biGetcurrenttimestamp,
		biGetcurrenttimestampstatic, biGetcurrentticks, biGetcurrentticksstatic:
		return 0, 0
	case biConcat, biArrayConcat, biFullTextContainsAll, biFullTextContainsAny, biFullTextScore, biRRF:
		return arityPair, -1
	case biVectorDistance:
		return arityPair, arityQuad
	case biFullTextContains:
		return arityPair, arityPair
	case biDatetimefromparts:
		return arityTriple, maxDateTimeParts
	case biStDistance, biStWithin, biStIntersects:
		return arityPair, arityPair
	case biContains, biStartswith, biEndswith, biStringequals, biRegexmatch, biArrayContains, biArraySlice:
		return arityPair, arityTriple
	case biLeft, biRight, biReplicate, biIndexOf, biPower, biSetintersect, biSetunion, biDatetimepart:
		return arityPair, arityPair
	case biSubstring, biReplace, biDatetimeadd, biDatetimediff:
		return arityTriple, arityTriple
	case biLog:
		return 1, arityPair
	default:
		if lo, hi, ok := moreArity(name); ok {
			return lo, hi
		}

		return 1, 1
	}
}

func (ec *evalCtx) callBuiltin(kind fnKind, name string, args []any) any {
	switch kind {
	case fnString:
		return ec.callString(name, args)
	case fnMath:
		return callMath(name, args)
	case fnType:
		return callType(name, args[0])
	case fnArray:
		return callArray(name, args)
	case fnDate:
		return ec.callDate(name, args)
	case fnSpatial:
		return callSpatial(name, args, ec.geometry)
	case fnMore:
		return ec.callMore(name, args)
	default:
		return undef{}
	}
}

func strArgs(args []any, n int) ([]string, bool) {
	out := make([]string, n)

	for i := range n {
		s, ok := args[i].(string)
		if !ok {
			return nil, false
		}

		out[i] = s
	}

	return out, true
}

func intArg(v any) (int, bool) {
	n, ok := toNumber(v)
	if !ok {
		return 0, false
	}

	i, err := n.Int64()
	if err != nil {
		f := numFloat(n)
		if f != math.Trunc(f) || math.Abs(f) > math.MaxInt32 {
			return 0, false
		}

		return int(f), true
	}

	if i > math.MaxInt32 || i < math.MinInt32 {
		return 0, false
	}

	return int(i), true
}

func intValue(i int) any { return json.Number(strconv.Itoa(i)) }

func boolArg(args []any, i int) bool {
	if i >= len(args) {
		return false
	}

	b, _ := args[i].(bool)

	return b
}

func (ec *evalCtx) callString(name string, args []any) any {
	switch name {
	case biConcat:
		var sb strings.Builder

		for _, a := range args {
			s, ok := a.(string)
			if !ok {
				return undef{}
			}

			sb.WriteString(s)
		}

		return sb.String()
	case biContains, biStartswith, biEndswith, biStringequals:
		return stringPredicate(name, args)
	case biRegexmatch:
		return ec.regexMatch(args)
	case biTostring:
		return toStringValue(args[0])
	case biStringtonumber, biStringtoboolean, biStringtonull, biStringtoarray, biStringtoobject:
		return parseStringValue(name, args[0])
	default:
		return stringTransform(name, args)
	}
}

func stringPredicate(name string, args []any) any {
	ss, ok := strArgs(args, arityPair)
	if !ok {
		return undef{}
	}

	a, b := ss[0], ss[1]
	if boolArg(args, arityPair) {
		a, b = strings.ToLower(a), strings.ToLower(b)
	}

	switch name {
	case biContains:
		return strings.Contains(a, b)
	case biStartswith:
		return strings.HasPrefix(a, b)
	case biEndswith:
		return strings.HasSuffix(a, b)
	default:
		return a == b
	}
}

func stringTransform(name string, args []any) any {
	s, ok := args[0].(string)
	if !ok {
		return undef{}
	}

	switch name {
	case biLower:
		return strings.ToLower(s)
	case biUpper:
		return strings.ToUpper(s)
	case biLength:
		return intValue(utf8.RuneCountInString(s))
	case biTrim:
		return strings.TrimSpace(s)
	case biLtrim:
		return strings.TrimLeft(s, " \t\r\n")
	case biRtrim:
		return strings.TrimRight(s, " \t\r\n")
	case biReverse:
		r := []rune(s)
		for i, j := 0, len(r)-1; i < j; i, j = i+1, j-1 {
			r[i], r[j] = r[j], r[i]
		}

		return string(r)
	default:
		return stringTransformArgs(name, s, args)
	}
}

func stringTransformArgs(name, s string, args []any) any {
	switch name {
	case biSubstring:
		start, sok := intArg(args[1])
		length, lok := intArg(args[2])

		if !sok || !lok || start < 0 || length < 0 {
			return undef{}
		}

		r := []rune(s)
		start = min(start, len(r))

		return string(r[start:min(start+length, len(r))])
	case biReplace:
		ss, ok := strArgs(args, arityTriple)
		if !ok {
			return undef{}
		}

		return strings.ReplaceAll(ss[0], ss[1], ss[2])
	case biLeft, biRight, biReplicate:
		return leftRightReplicate(name, s, args[1])
	case biIndexOf:
		sub, ok := args[1].(string)
		if !ok {
			return undef{}
		}

		idx := strings.Index(s, sub)
		if idx < 0 {
			return intValue(-1)
		}

		return intValue(utf8.RuneCountInString(s[:idx]))
	default:
		return undef{}
	}
}

const maxReplicate = 1 << 20

func leftRightReplicate(name, s string, nArg any) any {
	n, ok := intArg(nArg)
	if !ok || n < 0 {
		return undef{}
	}

	r := []rune(s)

	switch name {
	case biLeft:
		return string(r[:min(n, len(r))])
	case biRight:
		return string(r[len(r)-min(n, len(r)):])
	default:
		if len(s)*n > maxReplicate {
			return undef{}
		}

		return strings.Repeat(s, n)
	}
}

func toStringValue(v any) any {
	switch t := v.(type) {
	case undef:
		return t
	case string:
		return t
	case nil:
		return nullText
	default:
		return canonicalJSON(t)
	}
}

func parseStringValue(name string, v any) any {
	s, ok := v.(string)
	if !ok {
		return undef{}
	}

	s = strings.TrimSpace(s)

	switch name {
	case biStringtoarray, biStringtoobject:
		return parseJSONContainer(name, s)
	case biStringtonumber:
		f, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return undef{}
		}

		if i, iErr := strconv.ParseInt(s, 10, 64); iErr == nil {
			return json.Number(strconv.FormatInt(i, 10))
		}

		return floatResult(f)
	case biStringtoboolean:
		switch s {
		case trueText:
			return true
		case "false":
			return false
		}
	default:
		if s == nullText {
			return nil
		}
	}

	return undef{}
}

func (ec *evalCtx) regexMatch(args []any) any {
	ss, ok := strArgs(args, arityPair)
	if !ok {
		return undef{}
	}

	pat := ss[1]

	if len(args) > arityPair {
		mods, mok := args[2].(string)
		if !mok {
			return undef{}
		}

		flags := strings.Map(func(r rune) rune {
			if r == 'i' || r == 'm' || r == 's' {
				return r
			}

			return -1
		}, mods)
		if strings.ContainsRune(mods, 'x') {
			pat = stripFreeSpacing(pat)
		}
		if flags != "" {
			pat = "(?" + flags + ")" + pat
		}
	}

	re, cached := ec.regex[pat]
	if !cached {
		var err error

		if re, err = regexp.Compile(pat); err != nil {
			return undef{}
		}

		ec.regex[pat] = re
	}

	return re.MatchString(ss[0])
}

func callMath(name string, args []any) any {
	if name == biPi {
		return floatResult(math.Pi)
	}

	n, ok := toNumber(args[0])
	if !ok {
		return undef{}
	}

	switch name {
	case biAbs, biCeiling, biFloor, biRound, biTrunc, biSign:
		return mathRounding(name, n)
	case biPower:
		e, eok := toNumber(args[1])
		if !eok {
			return undef{}
		}

		return floatResult(math.Pow(numFloat(n), numFloat(e)))
	case biLog:
		return mathLog(n, args)
	default:
		return mathFloat(name, numFloat(n))
	}
}

func mathRounding(name string, n json.Number) any {
	if i, err := n.Int64(); err == nil {
		switch {
		case name == biSign:
			return intValue(cmpSign(i))
		case name == biAbs && i < 0 && i != math.MinInt64:
			return json.Number(strconv.FormatInt(-i, 10))
		default:
			return n
		}
	}

	f := numFloat(n)

	switch name {
	case biAbs:
		return floatResult(math.Abs(f))
	case biCeiling:
		return floatResult(math.Ceil(f))
	case biFloor:
		return floatResult(math.Floor(f))
	case biRound:
		return floatResult(math.Round(f))
	case biTrunc:
		return floatResult(math.Trunc(f))
	default:
		return intValue(cmpSign(int64(math.Copysign(1, f))))
	}
}

func cmpSign(i int64) int {
	switch {
	case i > 0:
		return 1
	case i < 0:
		return -1
	default:
		return 0
	}
}

func mathLog(n json.Number, args []any) any {
	f := numFloat(n)
	if f <= 0 {
		return undef{}
	}

	if len(args) > 1 {
		base, ok := toNumber(args[1])
		if !ok || numFloat(base) <= 0 || numFloat(base) == 1 {
			return undef{}
		}

		return floatResult(math.Log(f) / math.Log(numFloat(base)))
	}

	return floatResult(math.Log(f))
}

func mathFloat(name string, f float64) any {
	switch name {
	case biSqrt:
		if f < 0 {
			return undef{}
		}

		return floatResult(math.Sqrt(f))
	case biSquare:
		return floatResult(f * f)
	case biExp:
		return floatResult(math.Exp(f))
	case biLog10:
		if f <= 0 {
			return undef{}
		}

		return floatResult(math.Log10(f))
	default:
		return undef{}
	}
}

func callType(name string, v any) any {
	_, isNum := toNumber(v)

	switch name {
	case biIsDefined:
		return !isUndef(v)
	case biIsNull:
		return v == nil
	case biIsNumber, biIsFiniteNumber:
		return isNum
	case biIsInteger:
		n, ok := toNumber(v)
		if !ok {
			return false
		}

		f := numFloat(n)

		return f == math.Trunc(f)
	case biIsString:
		_, ok := v.(string)

		return ok
	case biIsBool:
		_, ok := v.(bool)

		return ok
	case biIsArray:
		_, ok := v.([]any)

		return ok
	case biIsObject:
		_, ok := v.(map[string]any)

		return ok
	default:
		_, isStr := v.(string)
		_, isBool := v.(bool)

		return v == nil || isNum || isStr || isBool
	}
}

func callArray(name string, args []any) any {
	arr, ok := args[0].([]any)
	if !ok {
		return undef{}
	}

	switch name {
	case biArrayLength:
		return intValue(len(arr))
	case biArrayContains:
		return arrayContains(arr, args)
	case biArraySlice:
		return arraySlice(arr, args)
	default:
		return arraySetOp(name, arr, args)
	}
}

func arrayContains(arr []any, args []any) any {
	partial := boolArg(args, arityPair)

	for _, e := range arr {
		if deepEqual(e, args[1]) || (partial && partialMatch(e, args[1])) {
			return true
		}
	}

	return false
}

func partialMatch(have, want any) bool {
	hm, hok := have.(map[string]any)
	wm, wok := want.(map[string]any)

	if !hok || !wok {
		return deepEqual(have, want)
	}

	for k, wv := range wm {
		hv, exists := hm[k]
		if !exists || !partialMatch(hv, wv) {
			return false
		}
	}

	return true
}

func arraySlice(arr []any, args []any) any {
	start, ok := intArg(args[1])
	if !ok {
		return undef{}
	}

	if start < 0 {
		start = max(len(arr)+start, 0)
	}

	start = min(start, len(arr))
	end := len(arr)

	if len(args) > arityPair {
		length, lok := intArg(args[2])
		if !lok {
			return undef{}
		}

		end = start + max(length, 0)
		end = min(end, len(arr))
	}

	return append([]any{}, arr[start:end]...)
}

func arraySetOp(name string, arr []any, args []any) any {
	out := []any{}

	switch name {
	case biArrayConcat:
		out = append(out, arr...)

		for _, a := range args[1:] {
			other, ok := a.([]any)
			if !ok {
				return undef{}
			}

			out = append(out, other...)
		}
	case biSetunion, biSetintersect:
		other, ok := args[1].([]any)
		if !ok {
			return undef{}
		}

		out = setCombine(name == biSetunion, arr, other)
	default:
		return undef{}
	}

	return out
}

func setCombine(union bool, a, b []any) []any {
	out := []any{}
	add := func(v any) {
		for _, e := range out {
			if deepEqual(e, v) {
				return
			}
		}

		out = append(out, v)
	}

	contains := func(list []any, v any) bool {
		for _, e := range list {
			if deepEqual(e, v) {
				return true
			}
		}

		return false
	}

	for _, v := range a {
		if union || contains(b, v) {
			add(v)
		}
	}

	if union {
		for _, v := range b {
			add(v)
		}
	}

	return out
}

const dateTimeLayout = "2006-01-02T15:04:05.0000000Z"

const ticksPerNano = 100

func (ec *evalCtx) callDate(name string, args []any) any {
	switch name {
	case biGetcurrentdatetime, biGetcurrentdatetimestatic:
		return ec.now.Format(dateTimeLayout)
	case biGetcurrenttimestamp, biGetcurrenttimestampstatic:
		return json.Number(strconv.FormatInt(ec.now.UnixMilli(), 10))
	case biGetcurrentticks, biGetcurrentticksstatic:
		return json.Number(strconv.FormatInt(ec.now.UnixNano()/ticksPerNano, 10))
	case biTimestamptodatetime:
		ms, ok := toNumber(args[0])
		if !ok {
			return undef{}
		}

		return time.UnixMilli(int64(numFloat(ms))).UTC().Format(dateTimeLayout)
	case biDatetimefromparts, biDatetimetoticks, biTickstodatetime:
		return dateTicksOp(name, args)
	case biDatetimetotimestamp:
		t, ok := parseDateTime(args[0])
		if !ok {
			return undef{}
		}

		return json.Number(strconv.FormatInt(t.UnixMilli(), 10))
	default:
		return dateTimeOp(name, args)
	}
}

func parseDateTime(v any) (time.Time, bool) {
	s, ok := v.(string)
	if !ok {
		return time.Time{}, false
	}

	return parseCosmosDateTime(s)
}

func dateTimeOp(name string, args []any) any {
	partName, ok := args[0].(string)
	if !ok {
		return undef{}
	}

	part := parseDatePart(partName)

	switch name {
	case biDatetimeadd:
		n, nok := intArg(args[1])
		t, tok := parseDateTime(args[2])

		if !nok || !tok {
			return undef{}
		}

		out, valid := addDatePart(part, n, t)
		if !valid {
			return undef{}
		}

		return out.Format(dateTimeLayout)
	case biDatetimediff:
		a, aok := parseDateTime(args[1])
		b, bok := parseDateTime(args[2])

		if !aok || !bok {
			return undef{}
		}

		return diffDatePart(part, a, b)
	default:
		t, tok := parseDateTime(args[1])
		if !tok {
			return undef{}
		}

		return datePart(part, t)
	}
}

type datePartKind int

const (
	dpInvalid datePartKind = iota
	dpYear
	dpMonth
	dpDay
	dpHour
	dpMinute
	dpSecond
	dpMillisecond
	dpMicrosecond
	dpNanosecond
)

// parseDatePart resolves a date part name or its T-SQL style abbreviation.
func parseDatePart(name string) datePartKind {
	switch strings.ToLower(name) {
	case "year", "yyyy", "yy":
		return dpYear
	case "month", "mm", "m":
		return dpMonth
	case "day", "dd", "d":
		return dpDay
	case "hour", "hh":
		return dpHour
	case "minute", "mi", "n":
		return dpMinute
	case "second", "ss", "s":
		return dpSecond
	case "millisecond", "ms":
		return dpMillisecond
	case "microsecond", "mcs":
		return dpMicrosecond
	case "nanosecond", "ns":
		return dpNanosecond
	default:
		return dpInvalid
	}
}

func (k datePartKind) duration() time.Duration {
	switch k {
	case dpHour:
		return time.Hour
	case dpMinute:
		return time.Minute
	case dpSecond:
		return time.Second
	case dpMillisecond:
		return time.Millisecond
	case dpMicrosecond:
		return time.Microsecond
	case dpNanosecond:
		return time.Nanosecond
	default:
		return 0
	}
}

func addDatePart(part datePartKind, n int, t time.Time) (time.Time, bool) {
	switch part {
	case dpYear:
		return t.AddDate(n, 0, 0), true
	case dpMonth:
		return t.AddDate(0, n, 0), true
	case dpDay:
		return t.AddDate(0, 0, n), true
	default:
		d := part.duration()

		return t.Add(time.Duration(n) * d), d != 0
	}
}

const (
	monthsPerYear = 12
	hoursPerDay   = 24
)

func diffDatePart(part datePartKind, a, b time.Time) any {
	switch part {
	case dpYear:
		return intValue(b.Year() - a.Year())
	case dpMonth:
		return intValue((b.Year()-a.Year())*monthsPerYear + int(b.Month()) - int(a.Month()))
	case dpDay:
		const day = hoursPerDay * time.Hour

		return intValue(int(b.Truncate(day).Sub(a.Truncate(day)) / day))
	default:
		d := part.duration()
		if d == 0 {
			return undef{}
		}

		return intValue(int(b.Truncate(d).Sub(a.Truncate(d)) / d))
	}
}

func datePart(part datePartKind, t time.Time) any {
	switch part {
	case dpYear:
		return intValue(t.Year())
	case dpMonth:
		return intValue(int(t.Month()))
	case dpDay:
		return intValue(t.Day())
	case dpHour:
		return intValue(t.Hour())
	case dpMinute:
		return intValue(t.Minute())
	case dpSecond:
		return intValue(t.Second())
	case dpMillisecond:
		return intValue(t.Nanosecond() / int(time.Millisecond))
	case dpMicrosecond:
		return intValue(t.Nanosecond() / int(time.Microsecond))
	case dpNanosecond:
		return intValue(t.Nanosecond())
	default:
		return undef{}
	}
}
