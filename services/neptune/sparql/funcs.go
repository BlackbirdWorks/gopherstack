package sparql

import (
	crand "crypto/rand"
	"encoding/binary"
	"math"
	"math/big"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
)

type builtin func(ev *evaluator, a []Term) (Term, error)

const (
	argsTwo   = 2
	argsThree = 3
	argsFour  = 4

	roundHalf      = 0.5
	mantissaBits   = 53
	mantissaShift  = 64 - mantissaBits
	nanosPerSecond = 1_000_000_000
)

func arity(n int, f builtin) builtin {
	return func(ev *evaluator, a []Term) (Term, error) {
		if len(a) != n {
			return Term{}, errType
		}

		return f(ev, a)
	}
}

func (ev *evaluator) evalCall(c exprCall, en *env) (Term, error) {
	if isAggregate(c.Name) {
		return ev.evalAggregate(c, en)
	}

	switch c.Name {
	case "BOUND":
		if len(c.Args) == 1 {
			if v, ok := c.Args[0].(exprVar); ok {
				_, bound := en.b[v.Name]

				return boolTerm(bound), nil
			}
		}

		return Term{}, errType
	case "IF":
		return ev.evalIf(c, en)
	case "COALESCE":
		for _, a := range c.Args {
			if t, err := ev.eval(a, en); err == nil {
				return t, nil
			}
		}

		return Term{}, errUnbound
	}

	args := make([]Term, len(c.Args))

	for i, a := range c.Args {
		t, err := ev.eval(a, en)
		if err != nil {
			return Term{}, err
		}

		args[i] = t
	}

	if strings.HasPrefix(c.Name, NSXSD) {
		return castTo(c.Name, args)
	}

	f, ok := ev.funcs[c.Name]
	if !ok {
		return Term{}, syntaxf("unknown function %s", c.Name)
	}

	return f(ev, args)
}

func (ev *evaluator) evalIf(c exprCall, en *env) (Term, error) {
	if len(c.Args) != argsThree {
		return Term{}, errType
	}

	cond, err := ev.eval(c.Args[0], en)
	if err != nil {
		return Term{}, err
	}

	b, err := ebv(cond)
	if err != nil {
		return Term{}, err
	}

	if b {
		return ev.eval(c.Args[1], en)
	}

	return ev.eval(c.Args[2], en)
}

func newFuncTable() map[string]builtin {
	t := map[string]builtin{}
	addTermFuncs(t)
	addStringFuncs(t)
	addNumericFuncs(t)
	addDateFuncs(t)
	addHashFuncs(t)

	return t
}

func addTermFuncs(t map[string]builtin) {
	t["STR"] = arity(1, func(_ *evaluator, a []Term) (Term, error) {
		if a[0].Kind == KindBlank {
			return Term{}, errType
		}

		return Literal(a[0].Value), nil
	})
	t["LANG"] = arity(1, func(_ *evaluator, a []Term) (Term, error) {
		if a[0].Kind != KindLiteral {
			return Term{}, errType
		}

		return Literal(a[0].Lang), nil
	})
	t["DATATYPE"] = arity(1, func(_ *evaluator, a []Term) (Term, error) {
		if a[0].Kind != KindLiteral {
			return Term{}, errType
		}

		return IRI(a[0].DT()), nil
	})
	t["IRI"] = arity(1, func(ev *evaluator, a []Term) (Term, error) {
		if a[0].Kind == KindIRI {
			return a[0], nil
		}

		if !isStringLit(a[0]) {
			return Term{}, errType
		}

		return IRI(ev.base(a[0].Value)), nil
	})
	t["URI"] = t["IRI"]
	addPredicateFuncs(t)
	addConstructorFuncs(t)
}

func addPredicateFuncs(t map[string]builtin) {
	t["SAMETERM"] = arity(argsTwo, func(_ *evaluator, a []Term) (Term, error) { return boolTerm(a[0] == a[1]), nil })
	t["ISIRI"] = arity(1, func(_ *evaluator, a []Term) (Term, error) { return boolTerm(a[0].Kind == KindIRI), nil })
	t["ISURI"] = t["ISIRI"]
	t["ISBLANK"] = arity(1, func(_ *evaluator, a []Term) (Term, error) { return boolTerm(a[0].Kind == KindBlank), nil })
	t["ISLITERAL"] = arity(
		1,
		func(_ *evaluator, a []Term) (Term, error) { return boolTerm(a[0].Kind == KindLiteral), nil },
	)
	t["ISNUMERIC"] = arity(1, func(_ *evaluator, a []Term) (Term, error) {
		_, ok := asNumeric(a[0])

		return boolTerm(ok), nil
	})
	t["LANGMATCHES"] = arity(argsTwo, func(_ *evaluator, a []Term) (Term, error) {
		return boolTerm(langMatches(a[0].Value, a[1].Value)), nil
	})
}

func addConstructorFuncs(t map[string]builtin) {
	t["STRLANG"] = arity(argsTwo, func(_ *evaluator, a []Term) (Term, error) {
		if !isStringLit(a[0]) || !isStringLit(a[1]) {
			return Term{}, errType
		}

		return LangLiteral(a[0].Value, a[1].Value), nil
	})
	t["STRDT"] = arity(argsTwo, func(_ *evaluator, a []Term) (Term, error) {
		if !isStringLit(a[0]) || a[1].Kind != KindIRI {
			return Term{}, errType
		}

		return Typed(a[0].Value, a[1].Value), nil
	})
	t["BNODE"] = func(ev *evaluator, a []Term) (Term, error) {
		if len(a) == 0 {
			return ev.freshBlank(), nil
		}

		if len(a) != 1 || !isStringLit(a[0]) {
			return Term{}, errType
		}

		return ev.labeledBlank(a[0].Value), nil
	}
	t["UUID"] = func(_ *evaluator, _ []Term) (Term, error) { return IRI("urn:uuid:" + uuid.NewString()), nil }
	t["STRUUID"] = func(_ *evaluator, _ []Term) (Term, error) { return Literal(uuid.NewString()), nil }
}

func langMatches(tag, rng string) bool {
	tag, rng = strings.ToLower(tag), strings.ToLower(rng)

	switch {
	case rng == "*":
		return tag != ""
	case tag == rng:
		return true
	default:
		return strings.HasPrefix(tag, rng+"-")
	}
}

// strOf returns the lexical form and language of a string literal.
func strOf(t Term) (string, string, bool) {
	if t.Kind != KindLiteral || (t.Datatype != "" && t.Lang == "") {
		return "", "", false
	}

	return t.Value, t.Lang, true
}

func strFn(f func(s string) string) builtin {
	return arity(1, func(_ *evaluator, a []Term) (Term, error) {
		s, lang, ok := strOf(a[0])
		if !ok {
			return Term{}, errType
		}

		out := f(s)
		if lang != "" {
			return LangLiteral(out, lang), nil
		}

		return Literal(out), nil
	})
}

func strPred(f func(a, b string) bool) builtin {
	return arity(argsTwo, func(_ *evaluator, a []Term) (Term, error) {
		x, xl, ok1 := strOf(a[0])
		y, yl, ok2 := strOf(a[1])

		if !ok1 || !ok2 || (yl != "" && xl != yl) {
			return Term{}, errType
		}

		return boolTerm(f(x, y)), nil
	})
}

func addStringFuncs(t map[string]builtin) {
	t["STRLEN"] = arity(1, func(_ *evaluator, a []Term) (Term, error) {
		s, _, ok := strOf(a[0])
		if !ok {
			return Term{}, errType
		}

		return intTerm(int64(utf8.RuneCountInString(s))), nil
	})
	t["UCASE"] = strFn(strings.ToUpper)
	t["LCASE"] = strFn(strings.ToLower)
	t["ENCODE_FOR_URI"] = strFn(func(s string) string { return strings.ReplaceAll(url.QueryEscape(s), "+", "%20") })
	t["CONTAINS"] = strPred(strings.Contains)
	t["STRSTARTS"] = strPred(strings.HasPrefix)
	t["STRENDS"] = strPred(strings.HasSuffix)
	t["STRBEFORE"] = arity(argsTwo, func(_ *evaluator, a []Term) (Term, error) { return strSplit(a, true) })
	t["STRAFTER"] = arity(argsTwo, func(_ *evaluator, a []Term) (Term, error) { return strSplit(a, false) })
	t["SUBSTR"] = substrFn
	t["CONCAT"] = concatFn
	t["REGEX"] = regexFn
	t["REPLACE"] = replaceFn
}

func intTerm(i int64) Term { return Typed(strconv.FormatInt(i, 10), XSDInteger) }

func strSplit(a []Term, before bool) (Term, error) {
	x, xl, ok1 := strOf(a[0])
	y, yl, ok2 := strOf(a[1])

	if !ok1 || !ok2 || (yl != "" && xl != yl) {
		return Term{}, errType
	}

	mk := func(s string) Term {
		if xl != "" {
			return LangLiteral(s, xl)
		}

		return Literal(s)
	}

	before0, after, ok := strings.Cut(x, y)
	if !ok {
		return Literal(""), nil
	}

	if before {
		return mk(before0), nil
	}

	return mk(after), nil
}

func substrFn(_ *evaluator, a []Term) (Term, error) {
	if len(a) < argsTwo || len(a) > argsThree {
		return Term{}, errType
	}

	s, lang, ok := strOf(a[0])
	start, ok2 := asNumeric(a[1])

	if !ok || !ok2 {
		return Term{}, errType
	}

	runes := []rune(s)
	from := int(math.Round(start.float()))
	to := len(runes) + 1

	if len(a) == argsThree {
		ln, ok3 := asNumeric(a[2])
		if !ok3 {
			return Term{}, errType
		}

		to = from + int(math.Round(ln.float()))
	}

	from, to = max(from, 1), min(to, len(runes)+1)

	out := ""
	if from < to {
		out = string(runes[from-1 : to-1])
	}

	if lang != "" {
		return LangLiteral(out, lang), nil
	}

	return Literal(out), nil
}

func concatFn(_ *evaluator, a []Term) (Term, error) {
	var sb strings.Builder

	lang, same := "", true

	for i, t := range a {
		s, l, ok := strOf(t)
		if !ok {
			return Term{}, errType
		}

		sb.WriteString(s)

		if i == 0 {
			lang = l
		} else if l != lang {
			same = false
		}
	}

	if same && lang != "" {
		return LangLiteral(sb.String(), lang), nil
	}

	return Literal(sb.String()), nil
}

func compileRegex(pattern, flags string) (*regexp.Regexp, error) {
	var prefix strings.Builder

	for _, f := range flags {
		switch f {
		case 'i', 's', 'm':
			prefix.WriteRune(f)
		default:
			return nil, errType
		}
	}

	if prefix.String() != "" {
		pattern = "(?" + prefix.String() + ")" + pattern
	}

	re, err := regexp.Compile(pattern)
	if err != nil {
		return nil, errType
	}

	return re, nil
}

func regexFn(_ *evaluator, a []Term) (Term, error) {
	if len(a) < argsTwo || len(a) > argsThree {
		return Term{}, errType
	}

	s, _, ok := strOf(a[0])
	pat, _, ok2 := strOf(a[1])

	flags := ""

	if len(a) == argsThree {
		f, _, ok3 := strOf(a[2])
		if !ok3 {
			return Term{}, errType
		}

		flags = f
	}

	if !ok || !ok2 {
		return Term{}, errType
	}

	re, err := compileRegex(pat, flags)
	if err != nil {
		return Term{}, err
	}

	return boolTerm(re.MatchString(s)), nil
}

func replaceFn(_ *evaluator, a []Term) (Term, error) {
	if len(a) < argsThree || len(a) > argsFour {
		return Term{}, errType
	}

	s, lang, ok := strOf(a[0])
	pat, _, ok2 := strOf(a[1])
	rep, _, ok3 := strOf(a[2])

	flags := ""

	if len(a) == argsFour {
		f, _, ok4 := strOf(a[3])
		if !ok4 {
			return Term{}, errType
		}

		flags = f
	}

	if !ok || !ok2 || !ok3 {
		return Term{}, errType
	}

	re, err := compileRegex(pat, flags)
	if err != nil {
		return Term{}, err
	}

	out := re.ReplaceAllString(s, convertReplacement(rep))
	if lang != "" {
		return LangLiteral(out, lang), nil
	}

	return Literal(out), nil
}

// convertReplacement maps SPARQL's $1 group references onto Go's ${1} form.
func convertReplacement(rep string) string {
	var sb strings.Builder

	for i := 0; i < len(rep); i++ {
		if rep[i] == '$' && i+1 < len(rep) && rep[i+1] >= '0' && rep[i+1] <= '9' {
			j := i + 1
			for j < len(rep) && rep[j] >= '0' && rep[j] <= '9' {
				j++
			}

			sb.WriteString("${" + rep[i+1:j] + "}")

			i = j - 1

			continue
		}

		sb.WriteByte(rep[i])
	}

	return sb.String()
}

func numFn(f func(n numVal) numVal) builtin {
	return arity(1, func(_ *evaluator, a []Term) (Term, error) {
		n, ok := asNumeric(a[0])
		if !ok {
			return Term{}, errType
		}

		return f(n).term(), nil
	})
}

func floatRound(n numVal, f func(float64) float64) numVal {
	switch n.k {
	case nInt:
		return n
	case nDec:
		fv, _ := n.r.Float64()
		r := new(big.Rat)
		r.SetFloat64(f(fv))

		return numVal{k: nDec, r: r}
	default:
		n.f = f(n.f)

		return n
	}
}

func addNumericFuncs(t map[string]builtin) {
	t["ABS"] = numFn(func(n numVal) numVal {
		switch {
		case n.k == nInt && n.i < 0:
			n.i = -n.i
		case n.k == nDec:
			n.r = new(big.Rat).Abs(n.r)
		case n.k >= nFloat:
			n.f = math.Abs(n.f)
		}

		return n
	})
	t["CEIL"] = numFn(func(n numVal) numVal { return floatRound(n, math.Ceil) })
	t["FLOOR"] = numFn(func(n numVal) numVal { return floatRound(n, math.Floor) })
	t["ROUND"] = numFn(func(n numVal) numVal {
		return floatRound(n, func(f float64) float64 { return math.Floor(f + roundHalf) })
	})
	t["RAND"] = func(_ *evaluator, _ []Term) (Term, error) {
		return numVal{k: nDouble, f: randFloat()}.term(), nil
	}
}

// randFloat returns a uniform float in [0, 1) from the system CSPRNG.
func randFloat() float64 {
	var b [8]byte

	_, _ = crand.Read(b[:])

	return float64(binary.BigEndian.Uint64(b[:])>>mantissaShift) / (1 << mantissaBits)
}

func addDateFuncs(t map[string]builtin) {
	t["NOW"] = func(ev *evaluator, _ []Term) (Term, error) {
		return Typed(formatDateTime(ev.now()), XSDDateTime), nil
	}

	part := func(f func(tm timeParts) Term) builtin {
		return arity(1, func(_ *evaluator, a []Term) (Term, error) {
			if a[0].Datatype != XSDDateTime {
				return Term{}, errType
			}

			tm, ok := parseDateTime(a[0].Value)
			if !ok {
				return Term{}, errType
			}

			return f(timeParts{tm}), nil
		})
	}

	t["YEAR"] = part(func(p timeParts) Term { return intTerm(int64(p.Year())) })
	t["MONTH"] = part(func(p timeParts) Term { return intTerm(int64(p.Month())) })
	t["DAY"] = part(func(p timeParts) Term { return intTerm(int64(p.Day())) })
	t["HOURS"] = part(func(p timeParts) Term { return intTerm(int64(p.Hour())) })
	t["MINUTES"] = part(func(p timeParts) Term { return intTerm(int64(p.Minute())) })
	t["SECONDS"] = part(func(p timeParts) Term {
		r := new(big.Rat).SetFrac64(int64(p.Second())*nanosPerSecond+int64(p.Nanosecond()), nanosPerSecond)

		return Typed(formatDecimal(r), XSDDecimal)
	})
	t["TZ"] = part(func(_ timeParts) Term { return Literal("Z") })
	t["TIMEZONE"] = part(func(_ timeParts) Term { return Typed("PT0S", NSXSD+"dayTimeDuration") })
}

// castTo implements the xsd: constructor functions Neptune supports.
func castTo(dt string, a []Term) (Term, error) {
	if len(a) != 1 || a[0].Kind == KindBlank {
		return Term{}, errType
	}

	src := a[0]
	lex := strings.TrimSpace(src.Value)

	switch dt {
	case XSDString:
		return Literal(src.Value), nil
	case XSDBoolean:
		return castBool(src, lex)
	case XSDInteger, NSXSD + "long", NSXSD + "unsignedLong", NSXSD + "int":
		return castInteger(dt, src, lex)
	case XSDDecimal:
		return castDecimal(src, lex)
	case XSDDouble, XSDFloat:
		return castFloating(dt, src, lex)
	case XSDDateTime:
		if tm, ok := parseDateTime(lex); ok && src.Kind == KindLiteral {
			return Typed(formatDateTime(tm), XSDDateTime), nil
		}
	}

	return Term{}, errType
}

func castDecimal(src Term, lex string) (Term, error) {
	if n, ok := asNumeric(src); ok && n.k != nInt && n.k != nDec {
		return numVal{k: nDec, r: new(big.Rat).SetFloat64(n.f)}.term(), nil
	}

	r, ok := new(big.Rat).SetString(lex)
	if !ok || src.Kind != KindLiteral {
		return Term{}, errType
	}

	return numVal{k: nDec, r: r}.term(), nil
}

func castFloating(dt string, src Term, lex string) (Term, error) {
	f, ok := parseFloatLex(lex)
	if n, isNum := asNumeric(src); isNum {
		f, ok = n.float(), true
	}

	if !ok || src.Kind != KindLiteral {
		return Term{}, errType
	}

	k := nDouble
	if dt == XSDFloat {
		k = nFloat
	}

	return numVal{k: k, f: f}.term(), nil
}

func castBool(src Term, lex string) (Term, error) {
	if src.Kind != KindLiteral {
		return Term{}, errType
	}

	if n, ok := asNumeric(src); ok {
		return boolTerm(n.float() != 0 && !math.IsNaN(n.float())), nil
	}

	switch lex {
	case lexTrue, "1":
		return trueTerm(), nil
	case lexFalse, "0":
		return falseTerm(), nil
	}

	return Term{}, errType
}

func castInteger(dt string, src Term, lex string) (Term, error) {
	if src.Kind != KindLiteral {
		return Term{}, errType
	}

	if n, ok := asNumeric(src); ok {
		if n.k == nInt {
			return Typed(strconv.FormatInt(n.i, 10), dt), nil
		}

		f := math.Trunc(n.float())
		if math.IsNaN(f) || math.IsInf(f, 0) || math.Abs(f) > math.MaxInt64 {
			return Term{}, errType
		}

		return Typed(strconv.FormatInt(int64(f), 10), dt), nil
	}

	i, err := strconv.ParseInt(strings.TrimPrefix(lex, "+"), 10, 64)
	if err != nil || src.Datatype != "" && src.Datatype != XSDString {
		return Term{}, errType
	}

	return Typed(strconv.FormatInt(i, 10), dt), nil
}

type timeParts struct{ time.Time }
