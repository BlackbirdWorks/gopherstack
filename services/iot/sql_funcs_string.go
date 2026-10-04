package iot

import (
	"math"
	"regexp"
	"strings"
	"unicode/utf8"
)

const (
	maxPad   = 1000
	argThird = 2
)

func str1(f func(string) any) funcDef {
	return funcDef{impl: func(_ *sqlCtx, args []any) any {
		if s, ok := strArg(args, 0); ok {
			return f(s)
		}

		return sqlUndefined{}
	}}
}

func str2(f func(a, b string) any, since2016 bool) funcDef {
	return funcDef{since2016: since2016, impl: func(_ *sqlCtx, args []any) any {
		a, aok := strArg(args, 0)
		b, bok := strArg(args, 1)

		if !aok || !bok {
			return sqlUndefined{}
		}

		return f(a, b)
	}}
}

func stringFuncs() map[string]funcDef {
	return map[string]funcDef{
		"lower": str1(func(s string) any { return strings.ToLower(s) }),
		"upper": str1(func(s string) any { return strings.ToUpper(s) }),
		"trim":  str1(func(s string) any { return strings.TrimSpace(s) }),
		"ltrim": str1(func(s string) any { return strings.TrimLeft(s, " \t") }),
		"rtrim": str1(func(s string) any { return strings.TrimRight(s, " \t") }),
		"length": {
			since2016: true,
			impl:      str1(func(s string) any { return int64(utf8.RuneCountInString(s)) }).impl,
		},
		"numbytes":       {since2016: true, impl: str1(func(s string) any { return int64(len(s)) }).impl},
		"startswith":     str2(func(a, b string) any { return strings.HasPrefix(a, b) }, false),
		"endswith":       str2(func(a, b string) any { return strings.HasSuffix(a, b) }, false),
		"indexof":        str2(runeIndex, true),
		"regexp_matches": str2(regexpMatches, false),
		"regexp_substr":  str2(regexpSubstr, false),
		"replace":        {impl: replaceFunc},
		"regexp_replace": {impl: regexpReplaceFunc},
		"lpad":           {impl: padFunc(true)},
		"rpad":           {impl: padFunc(false)},
		"substring":      {impl: substringFunc},
		"concat":         {impl: concatFunc},
		"chr":            {impl: chrFunc},
	}
}

func runeIndex(s, sub string) any {
	before, _, found := strings.Cut(s, sub)
	if !found {
		return int64(-1)
	}

	return int64(utf8.RuneCountInString(before))
}

func regexpMatches(s, expr string) any {
	re, err := regexp.Compile(expr)
	if err != nil {
		return sqlUndefined{}
	}

	return re.MatchString(s)
}

func regexpSubstr(s, expr string) any {
	re, err := regexp.Compile(expr)
	if err != nil {
		return sqlUndefined{}
	}

	return re.FindString(s)
}

func replaceFunc(_ *sqlCtx, args []any) any {
	s, sok := strArg(args, 0)
	old, ook := strArg(args, 1)
	repl, rok := strArg(args, argThird)

	if !sok || !ook || !rok {
		return sqlUndefined{}
	}

	return strings.ReplaceAll(s, old, repl)
}

func regexpReplaceFunc(_ *sqlCtx, args []any) any {
	s, sok := strArg(args, 0)
	expr, eok := strArg(args, 1)
	repl, rok := strArg(args, argThird)

	if !sok || !eok || !rok {
		return sqlUndefined{}
	}

	re, err := regexp.Compile(expr)
	if err != nil {
		return sqlUndefined{}
	}

	return re.ReplaceAllString(s, repl)
}

func padFunc(left bool) funcImpl {
	return func(_ *sqlCtx, args []any) any {
		s, sok := strArg(args, 0)
		n, nok := toIntConv(arg(args, 1))

		if !sok || !nok {
			return sqlUndefined{}
		}

		pad := strings.Repeat(" ", int(min(max(n, 0), maxPad)))
		if left {
			return pad + s
		}

		return s + pad
	}
}

// substringFunc truncates fractional indexes and clamps them to the string.
func substringFunc(_ *sqlCtx, args []any) any {
	s, ok := strArg(args, 0)
	if !ok {
		return sqlUndefined{}
	}

	runes := []rune(s)
	start, sok := clampIndex(arg(args, 1), len(runes))

	if !sok {
		return sqlUndefined{}
	}

	end := len(runes)

	if len(args) > argThird {
		if end, ok = clampIndex(args[2], len(runes)); !ok {
			return sqlUndefined{}
		}
	}

	if start >= end {
		return ""
	}

	return string(runes[start:end])
}

func clampIndex(v any, n int) (int, bool) {
	f, ok := toDecimal(v)
	if !ok {
		return 0, false
	}

	return int(math.Min(math.Max(math.Trunc(f), 0), float64(n))), true
}

func chrFunc(_ *sqlCtx, args []any) any {
	i, ok := toIntConv(arg(args, 0))
	if !ok || i < 0 || i > utf8.MaxRune {
		return sqlUndefined{}
	}

	return string(rune(i))
}

// concatFunc joins strings, or flattens when any argument is an array.
func concatFunc(_ *sqlCtx, args []any) any {
	switch len(args) {
	case 0:
		return sqlUndefined{}
	case 1:
		return args[0]
	}

	hasArray := false

	for _, a := range args {
		if _, ok := a.([]any); ok {
			hasArray = true
		}
	}

	if hasArray {
		out := []any{}

		for _, a := range args {
			if arr, ok := a.([]any); ok {
				out = append(out, arr...)
			} else {
				out = append(out, a)
			}
		}

		return out
	}

	var sb strings.Builder

	for _, a := range args {
		s, ok := toStringConv(a)
		if !ok {
			return sqlUndefined{}
		}

		sb.WriteString(s)
	}

	return sb.String()
}
