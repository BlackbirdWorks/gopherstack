package azurearm

import (
	"math"
	"strings"
)

type xfunc struct {
	f   func(c *xctx, a []xval) (xval, error)
	min int
	max int
}

func xNodeArg(c *xctx, a []xval) (*xmlNode, error) {
	if len(a) == 0 {
		return c.node, nil
	}

	ns, ok := a[0].([]*xmlNode)
	if !ok {
		return nil, exprErr("the XPath function expects a node set")
	}

	if len(ns) == 0 {
		return nil, nil //nolint:nilnil // an empty node set yields empty names
	}

	return ns[0], nil
}

func xNameFunc(pick func(n *xmlNode) string) func(c *xctx, a []xval) (xval, error) {
	return func(c *xctx, a []xval) (xval, error) {
		n, err := xNodeArg(c, a)
		if err != nil || n == nil || n.kind != xkElem && n.kind != xkAttr {
			return "", err
		}

		return pick(n), nil
	}
}

func xStrArg(c *xctx, a []xval) string {
	if len(a) == 0 {
		return xStr([]*xmlNode{c.node})
	}

	return xStr(a[0])
}

func xStrFunc(f func(a []string) xval, minArgs int) xfunc {
	return xfunc{min: minArgs, max: minArgs, f: func(_ *xctx, a []xval) (xval, error) {
		s := make([]string, len(a))
		for i, v := range a {
			s[i] = xStr(v)
		}

		return f(s), nil
	}}
}

func xRound(f float64) float64 {
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return f
	}

	return math.Floor(f + 0.5) //nolint:mnd // XPath round
}

func xSubstring(a []xval) xval {
	runes := []rune(xStr(a[0]))
	start := xRound(xNum(a[1]))
	end := math.Inf(1)

	if len(a) == argsThree {
		end = start + xRound(xNum(a[2]))
	}

	var sb strings.Builder

	for i, r := range runes {
		if pos := float64(i + 1); pos >= start && pos < end {
			sb.WriteRune(r)
		}
	}

	return sb.String()
}

func xTranslate(a []string) xval {
	from, to := []rune(a[1]), []rune(a[2])

	var sb strings.Builder

	for _, r := range a[0] {
		idx := -1

		for i, f := range from {
			if f == r {
				idx = i

				break
			}
		}

		switch {
		case idx < 0:
			sb.WriteRune(r)
		case idx < len(to):
			sb.WriteRune(to[idx])
		}
	}

	return sb.String()
}

func xSum(_ *xctx, a []xval) (xval, error) {
	ns, ok := a[0].([]*xmlNode)
	if !ok {
		return nil, exprErr("the XPath function sum expects a node set")
	}

	total := 0.0
	for _, n := range ns {
		total += xNum(n.stringValue())
	}

	return total, nil
}

func xCount(_ *xctx, a []xval) (xval, error) {
	ns, ok := a[0].([]*xmlNode)
	if !ok {
		return nil, exprErr("the XPath function count expects a node set")
	}

	return float64(len(ns)), nil
}

func xNumFunc(f func(float64) float64) xfunc {
	return xfunc{min: 1, max: 1, f: func(_ *xctx, a []xval) (xval, error) { return f(xNum(a[0])), nil }}
}

var xpathFuncs = map[string]xfunc{ //nolint:gochecknoglobals // static XPath 1.0 function library
	"last":          {f: func(c *xctx, _ []xval) (xval, error) { return float64(c.size), nil }},
	"position":      {f: func(c *xctx, _ []xval) (xval, error) { return float64(c.pos), nil }},
	"count":         {f: xCount, min: 1, max: 1},
	"sum":           {f: xSum, min: 1, max: 1},
	fieldName:       {f: xNameFunc(func(n *xmlNode) string { return n.qname() }), max: 1},
	"local-name":    {f: xNameFunc(func(n *xmlNode) string { return n.local }), max: 1},
	"namespace-uri": {f: xNameFunc(func(n *xmlNode) string { return n.space }), max: 1},
	logicVarString:  {f: func(c *xctx, a []xval) (xval, error) { return xStrArg(c, a), nil }, max: 1},
	"string-length": {f: func(c *xctx, a []xval) (xval, error) {
		return float64(len([]rune(xStrArg(c, a)))), nil
	}, max: 1},
	"normalize-space": {f: func(c *xctx, a []xval) (xval, error) {
		return strings.Join(strings.Fields(xStrArg(c, a)), " "), nil
	}, max: 1},
	"concat": {f: func(_ *xctx, a []xval) (xval, error) {
		var sb strings.Builder
		for _, v := range a {
			sb.WriteString(xStr(v))
		}

		return sb.String(), nil
	}, min: argsTwo, max: -1},
	"starts-with": xStrFunc(func(a []string) xval { return strings.HasPrefix(a[0], a[1]) }, argsTwo),
	assocContains: xStrFunc(func(a []string) xval { return strings.Contains(a[0], a[1]) }, argsTwo),
	"substring-before": xStrFunc(func(a []string) xval {
		before, _, _ := strings.Cut(a[0], a[1])
		if !strings.Contains(a[0], a[1]) {
			return ""
		}

		return before
	}, argsTwo),
	"substring-after": xStrFunc(func(a []string) xval {
		_, after, found := strings.Cut(a[0], a[1])
		if !found {
			return ""
		}

		return after
	}, argsTwo),
	"substring": {
		f:   func(_ *xctx, a []xval) (xval, error) { return xSubstring(a), nil },
		min: argsTwo,
		max: argsThree,
	},
	"translate":   xStrFunc(xTranslate, argsThree),
	"boolean":     {f: func(_ *xctx, a []xval) (xval, error) { return xBool(a[0]), nil }, min: 1, max: 1},
	"not":         {f: func(_ *xctx, a []xval) (xval, error) { return !xBool(a[0]), nil }, min: 1, max: 1},
	cmpTrue:       {f: func(*xctx, []xval) (xval, error) { return true, nil }},
	logicLitFalse: {f: func(*xctx, []xval) (xval, error) { return false, nil }},
	"number": {f: func(c *xctx, a []xval) (xval, error) {
		if len(a) == 0 {
			return xNum([]*xmlNode{c.node}), nil
		}

		return xNum(a[0]), nil
	}, max: 1},
	"floor":   xNumFunc(math.Floor),
	"ceiling": xNumFunc(math.Ceil),
	"round":   xNumFunc(xRound),
}
