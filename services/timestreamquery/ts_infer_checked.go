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

func (inf *inferrer) renameCall(n *node, name string) {
	inf.edit(name, n.start, n.start+len(n.name))
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
	default:
		return
	}

	inf.edit(fn+"(", n.start, n.start)
	inf.edit(",", n.opStart, n.opEnd)
	inf.closeEdit(")", n.end)
}

func (inf *inferrer) rewriteNegate(n *node, arg tsType) {
	if n.name != "-" || !isIntegral(arg) || n.args[0].kind == nLiteral {
		return
	}

	inf.edit(arithName("neg", intBits(arg), inf.inTry())+"(", n.start, n.start+1)
	inf.closeEdit(")", n.end)
}

func (inf *inferrer) rewriteCast(n *node, src tsType) {
	if !castSupported(src.kind, n.typ.kind) || n.headEnd == 0 {
		return
	}

	try := 0
	if n.name == "try_cast" || inf.inTry() {
		try = 1
	}

	inf.edit("ts_cast(", n.start, n.headEnd)
	tail := ", '" + scalarTypeofName(src.kind) + "', '" + scalarTypeofName(n.typ.kind) + "', " + strconv.Itoa(try) + ")"
	inf.edit(tail, n.opStart, n.end)
}

func (inf *inferrer) rewriteCheckedCall(lw string, n *node, args []tsType) {
	switch {
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

		inf.edit("ts_cast("+lit+", 'varchar', '"+scalarTypeofName(n.typ.kind)+"', "+try+")", n.start, n.end)
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
