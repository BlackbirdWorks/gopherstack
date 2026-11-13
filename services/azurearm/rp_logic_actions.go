package azurearm

import (
	"encoding/json"
	"fmt"
	"html"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

const (
	logicDefaultUntilCount = 60
	logicDefaultUntilLimit = time.Hour
	logicMaxWaitDays       = 365
	logicHoursPerDay       = 24
	logicDaysPerMonth      = 30
	logicDaysPerYear       = 365
)

func (rs *logicRunState) runCompose(env *logicEnv, name string, def map[string]any) logicOutcome {
	v, err := rs.eval(env, def["inputs"])
	if err != nil {
		return exprFailure(name, err)
	}

	return logicOutcome{status: logicStSucceeded, inputs: v, outputs: v, hasIn: true, hasOut: true}
}

func (rs *logicRunState) runWait(env *logicEnv, name string, def map[string]any) logicOutcome {
	in, err := rs.evalInputs(env, def)
	if err != nil {
		return exprFailure(name, err)
	}

	d, err := logicWaitDuration(in, rs.clock())
	if err != nil {
		return failOutcome(logicCodeInvalidTmpl, err.Error())
	}

	out := logicOutcome{status: logicStSucceeded, inputs: in, hasIn: true}

	if !rs.sleep(d) {
		out.status = logicStCancelled
	}

	return out
}

func (rs *logicRunState) evalInputs(env *logicEnv, def map[string]any) (map[string]any, error) {
	v, err := rs.eval(env, def["inputs"])
	if err != nil {
		return nil, err
	}

	m, _ := objectOf(v)
	if m == nil {
		m = map[string]any{}
	}

	return m, nil
}

func logicWaitDuration(in map[string]any, now time.Time) (time.Duration, error) {
	if iv, ok := objectOf(in["interval"]); ok {
		n, isInt := logicInt(normLogic(iv["count"]))
		if !isInt || n < 0 {
			return 0, inputErr("the Wait interval count must be a non-negative integer")
		}

		return logicUnitDuration(strings.ToLower(stringOf(iv["unit"])), n)
	}

	if u, ok := objectOf(in["until"]); ok {
		ts, err := parseLogicTime(stringOf(u["timestamp"]))
		if err != nil {
			return 0, inputErr("the Wait until timestamp is not valid")
		}

		return ts.t.Sub(now), nil
	}

	return 0, inputErr("the Wait action requires 'interval' or 'until'")
}

func logicUnitDuration(unit string, n int64) (time.Duration, error) {
	const day = logicHoursPerDay * time.Hour

	var base time.Duration

	switch unit {
	case logicUnitSecond:
		base = time.Second
	case logicUnitMinute:
		base = time.Minute
	case logicUnitHour:
		base = time.Hour
	case logicUnitDay:
		base = day
	case logicUnitWeek:
		base = logicDaysPerWeek * day
	case logicUnitMonth:
		base = logicDaysPerMonth * day
	default:
		return 0, inputErr("the time unit '%s' is not valid", unit)
	}

	return time.Duration(n * int64(base)), nil
}

func (rs *logicRunState) runTerminate(env *logicEnv, name string, def map[string]any) logicOutcome {
	in, err := rs.evalInputs(env, def)
	if err != nil {
		return exprFailure(name, err)
	}

	status := logicStFailed

	switch strings.ToLower(stringOf(in["runStatus"])) {
	case logicStrSucceeded:
		status = logicStSucceeded
	case logicRunCancelled:
		status = logicStCancelled
	}

	t := &logicTermination{status: status}

	if status == logicStFailed {
		re, _ := objectOf(in["runError"])
		t.err = map[string]any{logicKCode: stringOf(re[logicKCode]), fieldMessage: stringOf(re[fieldMessage])}
	}

	rs.p.mu.Lock("terminate")
	if rs.term == nil {
		rs.term = t
	}

	rs.p.mu.Unlock()

	rs.stop()

	return logicOutcome{status: logicStSucceeded, inputs: in, hasIn: true}
}

func (rs *logicRunState) runResponse(env *logicEnv, name string, def map[string]any) logicOutcome {
	in, err := rs.evalInputs(env, def)
	if err != nil {
		return exprFailure(name, err)
	}

	status := int64(200) //nolint:mnd // default HTTP status

	if s, has := in["statusCode"]; has {
		n, ok := logicInt(normLogic(s))
		if !ok || n < 200 || n > 599 || n >= 300 && n < 400 {
			msg := fmt.Sprintf("The response status code '%s' is not valid.", strs.EscapeSingleQuotes(fmt.Sprint(s)))

			return failOutcome(logicCodeInvalidTmpl, msg)
		}

		status = n
	}

	if rs.live == nil || rs.live.resp == nil {
		return failOutcome(
			logicCodeActionFailed,
			"The workflow run was not triggered by a request, so there is no caller to respond to.",
		)
	}

	if !rs.live.sent.CompareAndSwap(false, true) {
		return failOutcome(logicCodeActionFailed, "The response was already sent for this request.")
	}

	headers := map[string]string{}

	if h, ok := objectOf(in[logicKHeaders]); ok {
		for k, v := range h {
			headers[k] = logicString(v)
		}
	}

	rs.live.resp <- logicResponse{status: int(status), headers: headers, body: in[logicKBody]}

	out := map[string]any{"statusCode": status, logicKHeaders: mapStrAny(headers)}
	if b, has := in[logicKBody]; has {
		out[logicKBody] = b
	}

	return logicOutcome{status: logicStSucceeded, inputs: in, outputs: out, hasIn: true, hasOut: true}
}

func mapStrAny(m map[string]string) map[string]any {
	out := make(map[string]any, len(m))
	for k, v := range m {
		out[k] = v
	}

	return out
}

// evalCondition evaluates a WDL condition: an expression string or an {and|or|not|equals|...: [...]} object.
func (rs *logicRunState) evalCondition(env *logicEnv, v any) (bool, error) {
	val, err := rs.condValue(env, v)
	if err != nil {
		return false, err
	}

	return logicToBool(val)
}

func (rs *logicRunState) condValue(env *logicEnv, v any) (any, error) {
	obj, ok := objectOf(v)
	if !ok || len(obj) != 1 {
		return rs.eval(env, v)
	}

	for fname, raw := range obj {
		fn, known := logicFuncTable()[strings.ToLower(fname)]
		if !known {
			return rs.eval(env, v)
		}

		args := []any{raw}
		if arr, isArr := raw.([]any); isArr {
			args = arr
		}

		nodes := make([]*logicExpr, len(args))

		for i, a := range args {
			av, err := rs.condValue(env, a)
			if err != nil {
				return nil, err
			}

			nodes[i] = &logicExpr{kind: exprLit, lit: av}
		}

		return fn(rs.evalCtx(env), nodes)
	}

	return nil, nil //nolint:nilnil // unreachable: the object has exactly one key
}

func (rs *logicRunState) runIf(env *logicEnv, name string, def map[string]any) logicOutcome {
	cond, err := rs.evalCondition(env, def["expression"])
	if err != nil {
		return exprFailure(name, err)
	}

	thenActs, _ := objectOf(def[logicKActions])
	elseDef, _ := objectOf(def["else"])
	elseActs, _ := objectOf(elseDef[logicKActions])

	taken, other := thenActs, elseActs
	if !cond {
		taken, other = elseActs, thenActs
	}

	rs.skipAll(env, other, logicCodeBranchSkipped, "")

	out := containerOutcome(taken, rs.execActions(env, taken))
	out.inputs, out.hasIn = map[string]any{"expression": cond}, false

	return out
}

func (rs *logicRunState) runSwitch(env *logicEnv, name string, def map[string]any) logicOutcome {
	val, err := rs.eval(env, def["expression"])
	if err != nil {
		return exprFailure(name, err)
	}

	cases, _ := objectOf(def["cases"])
	names := make([]string, 0, len(cases))

	for n := range cases {
		names = append(names, n)
	}

	sort.Strings(names)

	var taken map[string]any

	matched := false

	for _, n := range names {
		c, _ := objectOf(cases[n])
		acts, _ := objectOf(c[logicKActions])

		cv, cerr := rs.eval(env, c["case"])
		if cerr != nil {
			return exprFailure(name, cerr)
		}

		if !matched && logicEquals(val, cv) {
			matched, taken = true, acts

			continue
		}

		rs.skipAll(env, acts, logicCodeBranchSkipped, "")
	}

	dflt, _ := objectOf(def["default"])
	dacts, _ := objectOf(dflt[logicKActions])

	if matched {
		rs.skipAll(env, dacts, logicCodeBranchSkipped, "")
	} else {
		taken = dacts
	}

	return containerOutcome(taken, rs.execActions(env, taken))
}

func foreachConcurrency(def map[string]any) int {
	if strings.EqualFold(stringOf(def["operationOptions"]), "Sequential") {
		return 1
	}

	rt, _ := objectOf(def["runtimeConfiguration"])
	cc, _ := objectOf(rt["concurrency"])

	if n, ok := logicInt(normLogic(cc["repetitions"])); ok && n >= 1 {
		if n > logicMaxConcurrency {
			return logicMaxConcurrency
		}

		return int(n)
	}

	return logicDefaultConc
}

func (rs *logicRunState) runForeach(env *logicEnv, name string, def map[string]any) logicOutcome {
	v, err := rs.eval(env, def["foreach"])
	if err != nil {
		return exprFailure(name, err)
	}

	items, ok := v.([]any)
	if !ok {
		return failOutcome(logicCodeInvalidTmpl, "The execution of template action '"+name+
			"' failed: the 'foreach' expression must evaluate to an array.")
	}

	inner, _ := objectOf(def[logicKActions])
	envs := make([]*logicEnv, len(items))
	iterStatus := make([]string, len(items))
	sem := make(chan struct{}, foreachConcurrency(def))

	var wg sync.WaitGroup

	for i, it := range items {
		sem <- struct{}{}

		wg.Go(func() {
			defer func() { <-sem }()

			if rs.ctx.Err() != nil {
				iterStatus[i] = logicStCancelled

				return
			}

			e := env.child(&logicFrame{name: name, item: it, index: i}, true)
			envs[i] = e
			st, _ := scopeStatus(inner, rs.execActions(e, inner))
			iterStatus[i] = st
		})
	}

	wg.Wait()
	rs.aggregate(env, inner, envs)

	return foreachOutcome(iterStatus)
}

func foreachOutcome(statuses []string) logicOutcome {
	for _, want := range []string{logicStFailed, logicStTimedOut, logicStCancelled} {
		if slices.Contains(statuses, want) {
			out := logicOutcome{status: want}
			if want == logicStFailed {
				out.code = logicCodeActionFailed
				out.err = map[string]any{logicKCode: logicCodeActionFailed, fieldMessage: logicMsgActionFailed}
			}

			return out
		}
	}

	return okOutcome()
}

// aggregate folds the per-iteration records of a loop body into one record per action in the enclosing env.
func (rs *logicRunState) aggregate(env *logicEnv, inner map[string]any, iters []*logicEnv) {
	names := map[string]bool{}

	for _, e := range iters {
		if e == nil {
			continue
		}

		rs.p.mu.RLock("aggregate")
		for n := range e.local {
			names[n] = true
		}
		rs.p.mu.RUnlock()
	}

	for n := range names {
		agg := &logicActionRec{Name: n, Status: logicStSkipped, Code: logicCodeSkipped}
		def, _ := objectOf(inner[n])
		agg.Type = stringOf(def[fieldType])

		rs.p.mu.RLock("aggregate")
		for _, e := range iters {
			if e != nil {
				mergeAggregate(agg, e.local[n])
			}
		}
		rs.p.mu.RUnlock()

		rs.addAggregate(env, agg)
	}
}

func mergeAggregate(agg, rec *logicActionRec) {
	if rec == nil {
		return
	}

	if agg.Start.IsZero() || rec.Start.Before(agg.Start) {
		agg.Start = rec.Start
	}

	if rec.End.After(agg.End) {
		agg.End = rec.End
	}

	if agg.Type == "" {
		agg.Type = rec.Type
	}

	rank := map[string]int{
		logicStSkipped: rankSkipped, logicStSucceeded: rankSucceeded, logicStCancelled: rankCancelled,
		logicStTimedOut: rankTimedOut, logicStFailed: rankFailed,
	}

	if rank[rec.Status] > rank[agg.Status] || agg.Status == logicStSkipped && rec.Status == logicStSucceeded {
		agg.Status = rec.Status
		agg.Code = rec.Code
		agg.Error = rec.Error
	}
}

func parseISODuration(s string) (time.Duration, error) {
	orig := s
	if !strings.HasPrefix(s, "P") {
		return 0, inputErr("duration %q is not ISO 8601", orig)
	}

	s = s[1:]
	inTime := false

	var total time.Duration

	num := ""

	for _, c := range s {
		switch {
		case c == 'T':
			inTime = true
		case c >= '0' && c <= '9' || c == '.':
			num += string(c)
		default:
			var f float64
			if _, err := fmt.Sscanf(num, "%g", &f); err != nil {
				return 0, inputErr("duration %q is not ISO 8601", orig)
			}

			unit, err := isoUnit(c, inTime)
			if err != nil {
				return 0, inputErr("duration %q is not ISO 8601", orig)
			}

			total += time.Duration(f * float64(unit))
			num = ""
		}
	}

	return total, nil
}

func isoUnit(c rune, inTime bool) (time.Duration, error) {
	day := logicHoursPerDay * time.Hour

	switch {
	case c == 'D' && !inTime:
		return day, nil
	case c == 'W' && !inTime:
		return logicDaysPerWeek * day, nil
	case c == 'M' && !inTime:
		return logicDaysPerMonth * day, nil
	case c == 'Y' && !inTime:
		return logicDaysPerYear * day, nil
	case c == 'H' && inTime:
		return time.Hour, nil
	case c == 'M' && inTime:
		return time.Minute, nil
	case c == 'S' && inTime:
		return time.Second, nil
	}

	return 0, inputErr("bad unit")
}

func (rs *logicRunState) runUntil(env *logicEnv, name string, def map[string]any) logicOutcome {
	limit, _ := objectOf(def["limit"])
	count := int64(logicDefaultUntilCount)
	timeout := logicDefaultUntilLimit

	if n, ok := logicInt(normLogic(limit["count"])); ok {
		count = n
	}

	if t := stringOf(limit["timeout"]); t != "" {
		d, err := parseISODuration(t)
		if err != nil {
			return failOutcome(logicCodeInvalidTmpl, err.Error())
		}

		timeout = d
	}

	deadline := rs.clock().Add(timeout)
	inner, _ := objectOf(def[logicKActions])

	var iters []*logicEnv

	defer func() { rs.aggregate(env, inner, iters) }()

	for i := int64(0); ; i++ {
		if i >= count || !rs.clock().Before(deadline) {
			return logicOutcome{status: logicStTimedOut}
		}

		if rs.ctx.Err() != nil {
			return logicOutcome{status: logicStCancelled}
		}

		e := env.child(&logicFrame{name: name, index: int(i)}, true)
		iters = append(iters, e)

		st, serr := scopeStatus(inner, rs.execActions(e, inner))
		if st != logicStSucceeded {
			return logicOutcome{status: st, err: serr, code: codeFor(st)}
		}

		done, err := rs.evalCondition(e, def["expression"])
		if err != nil {
			return exprFailure(name, err)
		}

		if done {
			return okOutcome()
		}
	}
}

func codeFor(status string) string {
	if status == logicStFailed {
		return logicCodeActionFailed
	}

	return ""
}

var logicVarTypes = map[string]bool{ //nolint:gochecknoglobals // static type set
	logicVarBoolean: true,
	logicVarInteger: true,
	logicVarFloat:   true,
	logicVarString:  true,
	logicVarObject:  true,
	logicVarArray:   true,
}

func logicCoerceVar(typ string, v any) (any, error) {
	ok := false

	switch typ {
	case logicVarBoolean:
		_, ok = v.(bool)
	case logicVarInteger:
		_, ok = v.(int64)
	case logicVarFloat:
		_, isF := v.(float64)
		_, isI := v.(int64)
		ok = isF || isI
	case logicVarString:
		_, ok = v.(string)
	case logicVarObject:
		_, ok = v.(map[string]any)
	case logicVarArray:
		_, ok = v.([]any)
	}

	if !ok {
		return nil, inputErr("the value is not of the variable type '%s'", typ)
	}

	return v, nil
}

func logicZero(typ string) any {
	switch typ {
	case logicVarBoolean:
		return false
	case logicVarInteger:
		return int64(0)
	case logicVarFloat:
		return float64(0)
	case logicVarString:
		return ""
	case logicVarObject:
		return map[string]any{}
	}

	return []any{}
}

func (rs *logicRunState) runVariable(env *logicEnv, name, typ string, def map[string]any) logicOutcome {
	in, err := rs.evalInputs(env, def)
	if err != nil {
		return exprFailure(name, err)
	}

	out := logicOutcome{status: logicStSucceeded, inputs: in, hasIn: true}

	if typ == "initializevariable" {
		if msg := rs.initVariables(in); msg != "" {
			return failOutcome(logicCodeInvalidTmpl, msg)
		}

		return out
	}

	if msg := rs.updateVariable(typ, in); msg != "" {
		return failOutcome(logicCodeInvalidTmpl, msg)
	}

	return out
}

func (rs *logicRunState) initVariables(in map[string]any) string {
	vars, _ := in["variables"].([]any)
	if len(vars) == 0 {
		return "The InitializeVariable action requires 'variables'."
	}

	rs.p.mu.Lock("initVariables")
	defer rs.p.mu.Unlock()

	for _, raw := range vars {
		v, _ := objectOf(raw)
		vname, vtyp := stringOf(v[fieldName]), strings.ToLower(stringOf(v[fieldType]))

		if vname == "" || !logicVarTypes[vtyp] {
			return "The variable definition is not valid."
		}

		if _, exists := rs.vars[vname]; exists {
			return "The variable '" + strs.EscapeSingleQuotes(vname) + "' has already been declared."
		}

		val := logicZero(vtyp)

		if given, has := v["value"]; has {
			c, err := logicCoerceVar(vtyp, given)
			if err != nil {
				return "The variable '" + strs.EscapeSingleQuotes(vname) + "' cannot be initialized: " + err.Error()
			}

			val = c
		}

		rs.vars[vname] = &logicVar{typ: vtyp, value: val}
	}

	return ""
}

func (rs *logicRunState) updateVariable(typ string, in map[string]any) string {
	vname := stringOf(in[fieldName])

	rs.p.mu.Lock("updateVariable")
	defer rs.p.mu.Unlock()

	v, ok := rs.vars[vname]
	if !ok {
		return "The variable '" + strs.EscapeSingleQuotes(vname) + "' has not been declared."
	}

	val := in["value"]

	switch typ {
	case "setvariable":
		c, err := logicCoerceVar(v.typ, val)
		if err != nil {
			return "The variable '" + strs.EscapeSingleQuotes(vname) + "' cannot be set: " + err.Error()
		}

		v.value = c
	case "incrementvariable", "decrementvariable":
		return stepVariable(v, vname, val, typ == "decrementvariable")
	case "appendtoarrayvariable":
		arr, isArr := v.value.([]any)
		if !isArr {
			return "The variable '" + strs.EscapeSingleQuotes(vname) + "' is not an array."
		}

		v.value = append(slices.Clone(arr), val)
	case "appendtostringvariable":
		s, isStr := v.value.(string)
		if !isStr {
			return "The variable '" + strs.EscapeSingleQuotes(vname) + "' is not a string."
		}

		v.value = s + logicString(val)
	}

	return ""
}

func stepVariable(v *logicVar, name string, by any, dec bool) string {
	if by == nil {
		by = int64(1)
	}

	delta, isInt, ok := logicNum(by)
	if !ok || v.typ != logicVarInteger && v.typ != logicVarFloat {
		return "The variable '" + strs.EscapeSingleQuotes(name) + "' cannot be incremented or decremented."
	}

	if dec {
		delta = -delta
	}

	switch cur := v.value.(type) {
	case int64:
		if !isInt {
			return "The variable '" + strs.EscapeSingleQuotes(name) + "' is an integer; the step must be an integer."
		}

		v.value = cur + int64(delta)
	case float64:
		v.value = cur + delta
	}

	return ""
}

func (rs *logicRunState) runDataOp(env *logicEnv, name, typ string, def map[string]any) logicOutcome {
	raw, _ := objectOf(def["inputs"])

	var (
		res    any
		err    error
		inputs any
	)

	switch typ {
	case "parsejson":
		inputs, res, err = rs.parseJSONOp(env, raw)
	default:
		inputs, res, err = rs.collectionOp(env, typ, raw)
	}

	if err != nil {
		return exprFailure(name, err)
	}

	return logicOutcome{
		status: logicStSucceeded, inputs: inputs, outputs: map[string]any{logicKBody: res}, hasIn: true, hasOut: true,
	}
}

func (rs *logicRunState) parseJSONOp(env *logicEnv, raw map[string]any) (any, any, error) {
	content, err := rs.eval(env, raw[logicKContent])
	if err != nil {
		return nil, nil, err
	}

	val := content

	if s, ok := content.(string); ok {
		var parsed any
		if jerr := json.Unmarshal([]byte(s), &parsed); jerr != nil {
			return nil, nil, exprErr("the content is not valid JSON: %v", jerr)
		}

		val = normLogic(parsed)
	}

	if schema, ok := objectOf(raw["schema"]); ok {
		if verr := validateLogicSchema(schema, val); verr != nil {
			return nil, nil, exprErr("the content does not match the schema: %v", verr)
		}
	}

	return map[string]any{logicKContent: content, "schema": raw["schema"]}, val, nil
}

func (rs *logicRunState) collectionOp(env *logicEnv, typ string, raw map[string]any) (any, any, error) {
	from, err := rs.eval(env, raw["from"])
	if err != nil {
		return nil, nil, err
	}

	arr, ok := from.([]any)
	if !ok {
		return nil, nil, exprErr("the 'from' value must be an array")
	}

	inputs := map[string]any{"from": arr}

	switch typ {
	case "select":
		inputs["select"] = raw["select"]
		res, serr := rs.mapItems(env, arr, func(e *logicEnv) (any, error) { return rs.eval(e, raw["select"]) })

		return inputs, res, serr
	case "query":
		inputs["where"] = raw["where"]
		res, qerr := rs.filterItems(env, arr, raw["where"])

		return inputs, res, qerr
	case "join":
		sep := logicString(raw["joinWith"])
		parts := make([]string, len(arr))

		for i, v := range arr {
			parts[i] = logicString(v)
		}

		inputs["joinWith"] = sep

		return inputs, strings.Join(parts, sep), nil
	}

	res, terr := rs.tableOp(env, arr, raw)

	return inputs, res, terr
}

func (rs *logicRunState) mapItems(env *logicEnv, arr []any, f func(*logicEnv) (any, error)) ([]any, error) {
	out := make([]any, len(arr))

	for i, it := range arr {
		v, err := f(env.child(&logicFrame{item: it, index: i}, false))
		if err != nil {
			return nil, err
		}

		out[i] = v
	}

	return out, nil
}

func (rs *logicRunState) filterItems(env *logicEnv, arr []any, where any) ([]any, error) {
	out := []any{}

	for i, it := range arr {
		e := env.child(&logicFrame{item: it, index: i}, false)

		ok, err := rs.evalCondition(e, where)
		if err != nil {
			return nil, err
		}

		if ok {
			out = append(out, it)
		}
	}

	return out, nil
}

func (rs *logicRunState) tableOp(env *logicEnv, arr []any, raw map[string]any) (string, error) {
	format := strings.ToUpper(stringOf(raw["format"]))
	if format != "CSV" && format != "HTML" {
		return "", exprErr("the table format must be CSV or HTML")
	}

	headers, cols := tableColumns(arr, raw["columns"])
	rows := make([][]string, 0, len(arr))

	for i, it := range arr {
		e := env.child(&logicFrame{item: it, index: i}, false)
		row := make([]string, len(cols))

		for c, expr := range cols {
			v, err := rs.eval(e, expr)
			if err != nil {
				return "", err
			}

			row[c] = logicString(v)
		}

		rows = append(rows, row)
	}

	return renderTable(format, headers, rows), nil
}

func tableColumns(arr []any, spec any) ([]string, []any) {
	if cols, ok := spec.([]any); ok {
		headers := make([]string, 0, len(cols))
		exprs := make([]any, 0, len(cols))

		for _, c := range cols {
			cm, _ := objectOf(c)
			headers = append(headers, stringOf(cm["header"]))
			exprs = append(exprs, cm["value"])
		}

		return headers, exprs
	}

	if len(arr) == 0 {
		return nil, nil
	}

	first, _ := objectOf(arr[0])
	keys := make([]string, 0, len(first))

	for k := range first {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	exprs := make([]any, len(keys))
	for i, k := range keys {
		exprs[i] = "@item()?['" + strings.ReplaceAll(k, "'", "''") + "']"
	}

	return keys, exprs
}

func renderTable(format string, headers []string, rows [][]string) string {
	var sb strings.Builder

	if format == "CSV" {
		sb.WriteString(strings.Join(headers, ",") + "\n")

		for _, r := range rows {
			sb.WriteString(strings.Join(r, ",") + "\n")
		}

		return sb.String()
	}

	sb.WriteString("<table><thead><tr>")

	for _, h := range headers {
		sb.WriteString("<th>" + html.EscapeString(h) + "</th>")
	}

	sb.WriteString("</tr></thead><tbody>")

	for _, r := range rows {
		sb.WriteString("<tr>")

		for _, c := range r {
			sb.WriteString("<td>" + html.EscapeString(c) + "</td>")
		}

		sb.WriteString("</tr>")
	}

	sb.WriteString("</tbody></table>")

	return sb.String()
}
