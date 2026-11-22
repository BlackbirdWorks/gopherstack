package timestreamquery

import (
	"slices"
	"strconv"
	"strings"
)

const (
	fnNaN              = "nan"
	fnApproxPercentile = "approx_percentile"
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
	case res.kind == tyDouble && a.isNumeric() && b.isNumeric() && nanAwareArith(n):
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

func nanAwareArith(n *node) bool { return n.name == "/" || n.name == "%" || hasNonFiniteSource(n) }

// keepNaN marks a NaN expression whose result is rendered or classified, the only places NaN may travel as text.
func (inf *inferrer) keepNaN(n *node) {
	if !isNaNSource(n) {
		return
	}

	if inf.nanRoots == nil {
		inf.nanRoots = map[*node]bool{}
	}

	inf.nanRoots[n] = true
}

func isNaNSource(n *node) bool {
	switch n.kind {
	case nBinary:
		if n.name == "+" || n.name == "-" || n.name == "*" {
			return hasNonFiniteSource(n)
		}

		return n.name == "/" || n.name == "%"
	case nCast:
		return n.typ.kind == tyDouble
	case nTypedLit:
		return n.typ.kind == tyDouble
	case nCall:
		lw := strings.ToLower(n.name)

		return lw == fnNaN || isDomainMath(lw)
	default:
	}

	return false
}

// hasNonFiniteSource reports whether a subtree can yield NaN or an infinity from finite or unparsed inputs.
func hasNonFiniteSource(n *node) bool {
	switch n.kind {
	case nCall:
		lw := strings.ToLower(n.name)
		if lw == "infinity" || lw == fnNaN || isDomainMath(lw) {
			return true
		}
	case nCast, nTypedLit:
		if n.typ.kind == tyDouble {
			return true
		}
	case nBinary:
		if n.name == "/" || n.name == "%" {
			return true
		}
	default:
	}

	return slices.ContainsFunc(n.args, hasNonFiniteSource)
}

func compareFuncName(op string) (string, bool) {
	switch op {
	case "=":
		return "ts_deq", true
	case "<>", "!=":
		return "ts_dne", true
	case "<":
		return "ts_dlt", true
	case "<=":
		return "ts_dle", true
	case ">":
		return "ts_dgt", true
	case ">=":
		return "ts_dge", true
	}

	return "", false
}

// rewriteCompare routes DOUBLE comparisons that may see NaN through functions where NaN is unordered.
func (inf *inferrer) rewriteCompare(n *node, a, b tsType) {
	fn, ok := compareFuncName(n.name)
	if !ok || n.kind != nBinary || !a.isNumeric() || !b.isNumeric() || !hasNonFiniteSource(n) {
		return
	}

	inf.checkedEdit(fn+"(", n.start, n.start)
	inf.checkedEdit(",", n.opStart, n.opEnd)
	inf.checkedClose(")", n.end)
}

// markNaNOperands lets the operands of a NaN-aware arithmetic node pass NaN to it as text.
func (inf *inferrer) markNaNOperands(n *node) {
	if n.kind != nBinary || !hasNonFiniteSource(n) {
		return
	}

	_, isArith := arithFuncName(n.name)
	if _, isCompare := compareFuncName(n.name); !isArith && !isCompare {
		return
	}

	for _, a := range n.args {
		inf.keepNaN(a)
	}
}

// domainMath lists the DOUBLE functions that yield NaN or an infinity outside their domain.
func isDomainMath(lw string) bool {
	switch lw {
	case "sqrt", "ln", "log10", "log2", "acos", "asin", "acosh", "atanh", "power", "pow":
		return true
	}

	return false
}

func (inf *inferrer) rewriteDomainMath(lw string, n *node, args []tsType) {
	if lw == fnNaN && len(args) == 0 && !inf.nanRoots[n] {
		inf.renameCall(n, "ts_nann")

		return
	}

	if !isDomainMath(lw) || len(args) == 0 || !slices.ContainsFunc(args, tsType.isNumeric) {
		return
	}

	name := "ts_" + lw
	if lw == "pow" {
		name = "ts_power"
	}

	if !inf.nanRoots[n] {
		name += "n"
	}

	inf.renameCall(n, name)
}

func isIntervalDS(t tsType) bool { return t.kind == tyIntervalDS }

// intervalFunc names the function that evaluates an interval operator on operands of types a and b.
func intervalFunc(op string, a, b tsType, computed [2]bool) string {
	switch op {
	case "+":
		return intervalAddFunc(a, b, computed)
	case "-":
		return intervalSubFunc(a, b, computed)
	case "*":
		return intervalScaleFunc(a, b)
	case "/":
		if isIntervalDS(a) && b.isNumeric() {
			return "ts_ivdiv"
		}
	}

	return ""
}

func intervalSubFunc(a, b tsType, computed [2]bool) string {
	switch {
	case a.kind == tyTimestamp && isIntervalDS(b) && computed[1]:
		return "ts_tssub"
	case isIntervalDS(a) && isIntervalDS(b):
		return "ts_ivsub"
	}

	return ""
}

func intervalScaleFunc(a, b tsType) string {
	switch {
	case isIntervalDS(a) && b.isNumeric():
		return "ts_ivmul"
	case a.isNumeric() && isIntervalDS(b):
		return "ts_ivmulr"
	}

	return ""
}

func intervalAddFunc(a, b tsType, computed [2]bool) string {
	switch {
	case a.kind == tyTimestamp && isIntervalDS(b) && computed[1]:
		return "ts_tsadd"
	case isIntervalDS(a) && b.kind == tyTimestamp && computed[0]:
		return "ts_tsaddr"
	case isIntervalDS(a) && isIntervalDS(b):
		return "ts_ivadd"
	}

	return ""
}

// rewriteIntervalArith routes interval day to second arithmetic through functions that accept every interval form.
func (inf *inferrer) rewriteIntervalArith(n *node, a, b tsType) {
	isComputed := func(x *node) bool { return x.kind != nLiteral && x.kind != nInterval }

	fn := intervalFunc(n.name, a, b, [2]bool{isComputed(n.args[0]), isComputed(n.args[1])})
	if fn == "" {
		return
	}

	inf.edit(fn+"(", n.start, n.start)
	inf.edit(",", n.opStart, n.opEnd)
	inf.closeEdit(")", n.end)
}

func (inf *inferrer) rewriteNegate(n *node, arg tsType) {
	if n.name == "-" && isIntervalDS(arg) {
		inf.edit("ts_ivneg(", n.start, n.start+1)
		inf.closeEdit(")", n.end)

		return
	}

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

	fn := "ts_cast("
	if n.typ.kind == tyDouble && !inf.nanRoots[n] {
		fn = "ts_castn("
	}

	inf.checkedEdit(fn, n.start, n.headEnd)
	tail := ", '" + scalarTypeofName(src.kind) + "', '" + scalarTypeofName(n.typ.kind) + "', " + strconv.Itoa(try) + ")"
	inf.checkedEdit(tail, n.opStart, n.end)
}

// aggRename names the registered form of an aggregate call the base engine lacks, or "" when it handles the call.
func aggRename(lw string, n *node, args []tsType) string {
	switch lw {
	case "max", "min":
		if len(args) == argsTwo && !n.over && !n.distinct {
			return "ts_" + lw + "_n"
		}
	case "max_by", "min_by":
		if len(args) == argsThree {
			return "ts_" + lw + "_n"
		}
	case fnApproxPercentile:
		return percentileRename(n, args)
	case "approx_distinct":
		if len(args) == argsTwo {
			return "ts_approx_distinct"
		}
	}

	return ""
}

func percentileRename(n *node, args []tsType) string {
	switch len(args) {
	case argsTwo:
		if args[1].kind == tyArray && !n.over {
			return "ts_approx_percentile_array"
		}
	case argsThree:
		return "ts_approx_percentile_w"
	case argsFour:
		return "ts_approx_percentile_wa"
	}

	return ""
}

// rewriteArrayAgg routes the aggregate forms the base engine lacks to their registered implementations.
func (inf *inferrer) rewriteArrayAgg(lw string, n *node, args []tsType) {
	if to := aggRename(lw, n, args); to != "" {
		inf.edit(to, n.start, n.start+len(n.name))
	}
}

// clockRename routes time-of-day and interval inputs of the extraction and date functions to their own forms.
func clockRename(lw string, args []tsType) string {
	switch lw {
	case unitHour, unitMinute, unitSecond, unitMillisecond, unitNanosecond, unitDay, unitDayOfMonth:
		if len(args) != 1 {
			return ""
		}

		part := lw
		if lw == unitDayOfMonth {
			part = unitDay
		}

		switch args[0].kind {
		case tyTime:
			if part != unitDay {
				return "ts_time_" + part
			}
		case tyIntervalDS:
			return "ts_iv_" + part
		default:
		}
	case "date_add":
		if len(args) == argsThree && args[argsTwo].kind == tyTime {
			return "ts_time_add"
		}
	case "date_diff":
		if len(args) == argsThree && args[1].kind == tyTime {
			return "ts_time_diff"
		}
	}

	return ""
}

// rewriteTypedCall routes calls whose behaviour depends on argument types or count to their registered forms.
func (inf *inferrer) rewriteTypedCall(lw string, n *node, args []tsType) {
	rename := func(to string) { inf.edit(to, n.start, n.start+len(n.name)) }

	switch {
	case lw == "reverse" && len(args) == 1 && args[0].kind == tyArray:
		rename("ts_reverse_array")
	case lw == "concat" && slices.ContainsFunc(args, func(t tsType) bool { return t.kind == tyArray }):
		rename("ts_concat_array")
	case lw == "replace" && (len(args) == argsTwo || len(args) == argsThree):
		rename("ts_replace")
	case lw == "strpos" && len(args) == argsThree:
		rename("ts_strpos")
	default:
		if to := clockRename(lw, args); to != "" {
			rename(to)
		}
	}
}

func (inf *inferrer) rewriteCheckedCall(lw string, n *node, args []tsType) {
	inf.rewriteArrayAgg(lw, n, args)
	inf.rewriteTypedCall(lw, n, args)
	inf.rewriteDomainMath(lw, n, args)

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

		fn := "ts_cast("
		if n.typ.kind == tyDouble && !inf.nanRoots[n] {
			fn = "ts_castn("
		}

		inf.checkedEdit(fn+lit+", 'varchar', '"+scalarTypeofName(n.typ.kind)+"', "+try+")", n.start, n.end)
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
