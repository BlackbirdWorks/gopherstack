package timestreamquery

import (
	"strconv"
	"strings"
)

func isIntegral(t tsType) bool { return t.kind == tyInteger || t.kind == tyBigint }

func intBits(t tsType) int {
	if t.kind == tyInteger {
		return bits32
	}

	return bits64
}

func isFloatClassFunc(lw string) bool {
	return lw == "is_nan" || lw == "is_finite" || lw == "is_infinite"
}

func (inf *inferrer) renameCall(n *node, name string) {
	inf.checkedEdit(name, n.start, n.start+len(n.name))
}

func (inf *inferrer) inTry() bool { return inf.tryDepth > 0 }

func arithFuncName(op string) (string, bool) {
	switch op {
	case "+":
		return "add", true
	case "-":
		return "sub", true
	case "*":
		return "mul", true
	case "/":
		return "div", true
	case "%":
		return "mod", true
	}

	return "", false
}

// rewriteArith routes integral and DOUBLE division through the checked functions once both operand types are known.
func (inf *inferrer) rewriteArith(n *node, a, b, res tsType) {
	base, ok := arithFuncName(n.name)
	if !ok {
		return
	}

	var fn string

	switch {
	case isIntegral(a) && isIntegral(b):
		fn = arithName(base, intBits(res), inf.inTry())
	case res.kind == tyDouble && a.isNumeric() && b.isNumeric() && (n.name == "/" || n.name == "%"):
		fn = "ts_d" + base
		if !inf.nanRoots[n] {
			fn += "n"
		}
	default:
		return
	}

	inf.checkedEdit(fn+"(", n.start, n.start)
	inf.checkedEdit(",", n.opStart, n.opEnd)
	inf.checkedClose(")", n.end)
}

// keepNaN marks a division whose NaN result is rendered or classified, the only places NaN may travel as text.
func (inf *inferrer) keepNaN(n *node) {
	if n.kind != nBinary || (n.name != "/" && n.name != "%") {
		return
	}

	if inf.nanRoots == nil {
		inf.nanRoots = map[*node]bool{}
	}

	inf.nanRoots[n] = true
}

func (inf *inferrer) rewriteNegate(n *node, arg tsType) {
	if n.name != "-" || !isIntegral(arg) || n.args[0].kind == nLiteral {
		return
	}

	inf.checkedEdit(arithName("neg", intBits(arg), inf.inTry())+"(", n.start, n.start+1)
	inf.checkedClose(")", n.end)
}

func (inf *inferrer) rewriteCast(n *node, src tsType) {
	if !castSupported(src.kind, n.typ.kind) || n.headEnd == 0 {
		return
	}

	try := 0
	if n.name == "try_cast" || inf.inTry() {
		try = 1
	}

	inf.checkedEdit("ts_cast(", n.start, n.headEnd)
	tail := ", '" + scalarTypeofName(src.kind) + "', '" + scalarTypeofName(n.typ.kind) + "', " + strconv.Itoa(try) + ")"
	inf.checkedEdit(tail, n.opStart, n.end)
}

// rewriteArrayAgg routes max(x, n), min(x, n) and approx_percentile(x, ARRAY[...]) to their array-returning forms.
func (inf *inferrer) rewriteArrayAgg(lw string, n *node, args []tsType) {
	switch {
	case (lw == "max" || lw == "min") && len(args) == argsTwo && !n.over && !n.distinct:
		inf.edit("ts_"+lw+"_n", n.start, n.start+len(n.name))
	case lw == "approx_percentile" && len(args) == argsTwo && args[1].kind == tyArray && !n.over:
		inf.edit("ts_approx_percentile_array", n.start, n.start+len(n.name))
	}
}

func (inf *inferrer) rewriteCheckedCall(lw string, n *node, args []tsType) {
	inf.rewriteArrayAgg(lw, n, args)

	switch {
	case len(args) == 1 && args[0].kind == tyDouble && isFloatClassFunc(lw):
		inf.renameCall(n, "ts_"+lw)
	case len(args) == 1 && isIntegral(args[0]):
		inf.rewriteUnaryCall(lw, n, args[0])
	case lw == "mod" && len(args) == argsTwo && isIntegral(args[0]) && isIntegral(args[1]):
		inf.renameCall(n, arithName("mod", intBits(supertype(args[0], args[1])), inf.inTry()))
	}
}

func (inf *inferrer) rewriteUnaryCall(lw string, n *node, arg tsType) {
	switch lw {
	case "abs":
		if !n.over {
			inf.renameCall(n, arithName("abs", intBits(arg), inf.inTry()))
		}
	case "sum":
		inf.renameCall(n, "ts_sum")
	case "random", "rand":
		inf.renameCall(n, "ts_random")
	}
}

func (inf *inferrer) rewriteTypedLiteral(n *node) {
	lit := "'" + strings.ReplaceAll(n.name, "'", "''") + "'"

	switch {
	case n.typ.kind == tyTime:
		inf.edit(lit, n.start, n.end)
	case castSupported(tyVarchar, n.typ.kind):
		try := "0"
		if inf.inTry() {
			try = "1"
		}

		inf.checkedEdit("ts_cast("+lit+", 'varchar', '"+scalarTypeofName(n.typ.kind)+"', "+try+")", n.start, n.end)
	}
}

func paramName(name string) string { return strings.TrimPrefix(name, "@") }

func (inf *inferrer) noteParam(name string) {
	pn := paramName(name)

	for _, p := range inf.params {
		if strings.EqualFold(p.name, pn) {
			return
		}
	}

	p := outCol{name: pn}
	if strings.EqualFold(pn, "scheduled_runtime") {
		p.typ = scalar(tyTimestamp)
	}

	inf.params = append(inf.params, p)
}

// bindParams gives each named parameter the type of the first typed sibling it is compared with.
func (inf *inferrer) bindParams(nodes []*node, types []tsType) {
	var anchor tsType

	for i, t := range types {
		if nodes[i].kind != nVar && t.known() && t.kind != tyUnknown {
			anchor = t

			break
		}
	}

	if !anchor.known() {
		return
	}

	for _, n := range nodes {
		if n.kind != nVar {
			continue
		}

		for i, p := range inf.params {
			if strings.EqualFold(p.name, paramName(n.name)) && !p.typ.known() {
				inf.params[i].typ = anchor
			}
		}
	}
}
