package azurearm

import (
	"slices"
	"sort"
	"strings"
)

const (
	logicTrigRequest    = "request"
	logicTrigRecurrence = "recurrence"
	logicActResponse    = "response"
	logicActTerminate   = "terminate"
	logicActForeach     = "foreach"
	logicActUntil       = "until"
	logicActScope       = "scope"
	logicActIf          = "if"
	logicActSwitch      = "switch"
	logicNameMaxLen     = 80
)

//nolint:gochecknoglobals // static type sets, read-only after init
var (
	logicRunnableActions = setOf(
		"compose",
		logicSchemeHTTP,
		logicActResponse,
		logicActTerminate,
		"wait",
		logicActForeach,
		logicActIf,
		logicActSwitch,
		logicActUntil,
		logicActScope,
		"initializevariable",
		"setvariable",
		"incrementvariable",
		"decrementvariable",
		"appendtoarrayvariable",
		"appendtostringvariable",
		"parsejson",
		"select",
		"query",
		"join",
		logicTypeTable,
	)
	logicKnownActions = setOf(
		logicKWorkflow,
		"function",
		"apiconnection",
		"apiconnectionwebhook",
		"httpwebhook",
		"apimanagement",
		"xslt",
		"xmlvalidation",
		"flatfileencoding",
		"flatfiledecoding",
		"integrationaccountartifactlookup",
		"liquid",
		"batch",
		"openapiconnection",
		"serviceprovider",
		"managedapiconnection",
	)
	logicKnownTriggers = setOf(logicTrigRequest, logicTrigRecurrence, logicSchemeHTTP, "httpwebhook", "apiconnection",
		"apiconnectionnotification", "apiconnectionwebhook", logicKWorkflow, "slidingwindow", "serviceprovider",
		logicTrigManual)
	logicFrequencies = map[string]int{
		logicUnitSecond: maxSecondInterval,
		logicUnitMinute: maxMinuteInterval,
		logicUnitHour:   maxHourInterval,
		logicUnitDay:    maxDayInterval,
		logicUnitWeek:   maxDayInterval,
		logicUnitMonth:  maxMonthInterval,
	}
)

func setOf(items ...string) map[string]bool {
	m := make(map[string]bool, len(items))
	for _, i := range items {
		m[i] = true
	}

	return m
}

func objectOf(v any) (map[string]any, bool) {
	m, ok := v.(map[string]any)

	return m, ok
}

func stringOf(v any) string {
	s, _ := v.(string)

	return s
}

func typeOf(def any) string {
	m, _ := objectOf(def)

	return strings.ToLower(stringOf(m[fieldType]))
}

// validateLogicDefinition checks a workflow definition the way the service does on save.
func validateLogicDefinition(def map[string]any) error {
	triggers, hasT := def[logicSegTriggers]
	if hasT {
		if _, ok := objectOf(triggers); !ok {
			return invalidTemplate("'triggers' must be an object")
		}
	}

	if actions, ok := def[logicKActions]; ok {
		if _, isObj := objectOf(actions); !isObj {
			return invalidTemplate("'actions' must be an object")
		}
	}

	tm, _ := objectOf(triggers)
	hasRequest := false

	for name, t := range tm {
		if err := validateLogicTrigger(name, t); err != nil {
			return err
		}

		hasRequest = hasRequest || typeOf(t) == logicTrigRequest || typeOf(t) == logicTrigManual
	}

	am, _ := objectOf(def[logicKActions])
	if err := validateLogicActions(am, hasRequest, false); err != nil {
		return err
	}

	return validateLogicExpressions(def)
}

func validateLogicTrigger(name string, t any) error {
	if err := validateLogicName("trigger", name); err != nil {
		return err
	}

	m, ok := objectOf(t)
	if !ok {
		return invalidTemplate("the trigger '%s' must be an object", name)
	}

	typ := strings.ToLower(stringOf(m[fieldType]))
	if !logicKnownTriggers[typ] {
		return invalidTemplate("the trigger '%s' has unknown type '%s'", name, stringOf(m[fieldType]))
	}

	switch typ {
	case logicTrigRecurrence:
		return validateLogicRecurrence(name, m)
	case logicTrigRequest, logicTrigManual:
		return validateLogicRequestTrigger(name, m)
	}

	return nil
}

func validateLogicName(kind, name string) error {
	if name == "" || len(name) > logicNameMaxLen || strings.ContainsAny(name, "/\\?#") {
		return invalidTemplate("the %s name '%s' is not valid", kind, name)
	}

	return nil
}

func validateLogicRequestTrigger(name string, m map[string]any) error {
	in, _ := objectOf(m["inputs"])

	if method := stringOf(in["method"]); method != "" {
		switch strings.ToUpper(method) {
		case "GET", "PUT", "POST", "PATCH", "DELETE":
		default:
			return invalidTemplate("the trigger '%s' has unsupported method '%s'", name, method)
		}
	}

	if s, ok := in["schema"]; ok {
		if _, isObj := objectOf(s); !isObj {
			return invalidTemplate("the trigger '%s' input schema must be an object", name)
		}
	}

	return nil
}

func validateLogicRecurrence(name string, m map[string]any) error {
	rc, ok := objectOf(m["recurrence"])
	if !ok {
		return invalidTemplate("the trigger '%s' of type 'Recurrence' requires a 'recurrence' object", name)
	}

	freq := strings.ToLower(stringOf(rc["frequency"]))

	maxInterval, ok := logicFrequencies[freq]
	if !ok {
		return invalidTemplate(
			"the trigger '%s' has unsupported recurrence frequency '%s'",
			name,
			stringOf(rc["frequency"]),
		)
	}

	interval, ok := logicInt(normLogic(rc["interval"]))
	if !ok || interval < 1 || int(interval) > maxInterval {
		return invalidTemplate(
			"the trigger '%s' recurrence interval must be an integer between 1 and %d",
			name,
			maxInterval,
		)
	}

	if tz := stringOf(rc["timeZone"]); tz != "" && !strings.EqualFold(tz, "UTC") {
		return invalidTemplate("the trigger '%s' recurrence time zone '%s' is not supported; use UTC", name, tz)
	}

	if st := stringOf(rc[logicKStartTime]); st != "" {
		if _, err := parseLogicTime(st); err != nil {
			return invalidTemplate("the trigger '%s' recurrence start time '%s' is not valid", name, st)
		}
	}

	return validateLogicSchedule(name, rc)
}

func validateLogicSchedule(name string, rc map[string]any) error {
	sc, ok := objectOf(rc["schedule"])
	if !ok {
		return nil
	}

	for field, bounds := range map[string][2]int64{"hours": {0, 23}, "minutes": {0, 59}, "monthDays": {-31, 31}} {
		arr, present := sc[field].([]any)
		if !present {
			continue
		}

		for _, v := range arr {
			n, isInt := logicInt(normLogic(v))
			if !isInt || n < bounds[0] || n > bounds[1] {
				return invalidTemplate(
					"the trigger '%s' recurrence schedule %s value %v is out of range",
					name,
					field,
					v,
				)
			}
		}
	}

	if days, present := sc["weekDays"].([]any); present {
		for _, d := range days {
			if _, valid := logicWeekdays[strings.ToLower(stringOf(d))]; !valid {
				return invalidTemplate("the trigger '%s' recurrence schedule weekday %v is not valid", name, d)
			}
		}
	}

	return nil
}

func validateLogicActions(defs map[string]any, hasRequest, inLoop bool) error {
	for name, a := range defs {
		if err := validateLogicName("action", name); err != nil {
			return err
		}

		m, ok := objectOf(a)
		if !ok {
			return invalidTemplate("the action '%s' must be an object", name)
		}

		typ := strings.ToLower(stringOf(m[fieldType]))
		if !logicRunnableActions[typ] && !logicKnownActions[typ] {
			return invalidTemplate("the action '%s' has unknown type '%s'", name, stringOf(m[fieldType]))
		}

		if err := validateLogicSingle(name, typ, m, hasRequest, inLoop); err != nil {
			return err
		}
	}

	return validateLogicRunAfter(defs)
}

func validateLogicSingle(name, typ string, m map[string]any, hasRequest, inLoop bool) error {
	switch typ {
	case logicActResponse:
		if !hasRequest {
			return invalidTemplate("the action '%s' of type 'Response' requires a Request trigger", name)
		}

		if inLoop {
			return invalidTemplate("the action '%s' of type 'Response' cannot run inside a loop", name)
		}
	case logicActTerminate:
		if inLoop {
			return invalidTemplate("the action '%s' of type 'Terminate' cannot run inside a loop", name)
		}

		if err := validateLogicTerminate(name, m); err != nil {
			return err
		}
	case logicActForeach, logicActUntil:
		if err := validateLogicLoop(name, typ, m); err != nil {
			return err
		}
	}

	for _, child := range logicChildScopes(typ, m) {
		loop := inLoop || typ == logicActForeach || typ == logicActUntil

		if err := validateLogicActions(child, hasRequest, loop); err != nil {
			return err
		}
	}

	return nil
}

func validateLogicTerminate(name string, m map[string]any) error {
	in, _ := objectOf(m["inputs"])

	switch strings.ToLower(stringOf(in["runStatus"])) {
	case "failed", logicRunCancelled, logicStrSucceeded:
		return nil
	}

	return invalidTemplate("the action '%s' of type 'Terminate' has an invalid runStatus", name)
}

func validateLogicLoop(name, typ string, m map[string]any) error {
	if typ == logicActForeach {
		if _, ok := m["foreach"]; !ok {
			return invalidTemplate("the action '%s' of type 'Foreach' requires 'foreach'", name)
		}

		rt, _ := objectOf(m["runtimeConfiguration"])
		cc, _ := objectOf(rt["concurrency"])

		if r, ok := cc["repetitions"]; ok {
			n, isInt := logicInt(normLogic(r))
			if !isInt || n < 1 || n > logicMaxConcurrency {
				return invalidTemplate(
					"the action '%s' concurrency repetitions must be between 1 and %d",
					name,
					logicMaxConcurrency,
				)
			}
		}

		return nil
	}

	if _, ok := m["expression"]; !ok {
		return invalidTemplate("the action '%s' of type 'Until' requires 'expression'", name)
	}

	return nil
}

// logicChildScopes lists the nested action maps of a container action.
func logicChildScopes(typ string, m map[string]any) []map[string]any {
	var out []map[string]any

	add := func(v any) {
		if a, ok := objectOf(v); ok {
			out = append(out, a)
		}
	}

	switch typ {
	case logicActForeach, logicActUntil, logicActScope:
		add(m[logicKActions])
	case logicActIf:
		add(m[logicKActions])

		if e, ok := objectOf(m["else"]); ok {
			add(e[logicKActions])
		}
	case logicActSwitch:
		cases, _ := objectOf(m["cases"])
		for _, c := range cases {
			if cm, ok := objectOf(c); ok {
				add(cm[logicKActions])
			}
		}

		if d, ok := objectOf(m["default"]); ok {
			add(d[logicKActions])
		}
	}

	return out
}

func logicRunAfterOf(m map[string]any) map[string][]string {
	ra, _ := objectOf(m["runAfter"])
	out := make(map[string][]string, len(ra))

	for dep, v := range ra {
		var sts []string

		if arr, ok := v.([]any); ok {
			for _, s := range arr {
				sts = append(sts, stringOf(s))
			}
		}

		out[dep] = sts
	}

	return out
}

func validateLogicRunAfter(defs map[string]any) error {
	graph := map[string][]string{}

	for name, a := range defs {
		m, _ := objectOf(a)

		for dep, sts := range logicRunAfterOf(m) {
			if _, ok := defs[dep]; !ok {
				return invalidTemplate(
					"the action '%s' runAfter references action '%s' which is not defined in the same scope",
					name,
					dep,
				)
			}

			for _, s := range sts {
				switch strings.ToLower(s) {
				case logicStrSucceeded, "failed", "skipped", "timedout":
				default:
					return invalidTemplate("the action '%s' runAfter status '%s' is not valid", name, s)
				}
			}

			graph[name] = append(graph[name], dep)
		}
	}

	return detectLogicCycle(graph)
}

func detectLogicCycle(graph map[string][]string) error {
	const (
		visiting = 1
		done     = 2
	)

	state := map[string]int{}

	var visit func(n string) bool

	visit = func(n string) bool {
		switch state[n] {
		case visiting:
			return true
		case done:
			return false
		}

		state[n] = visiting

		if slices.ContainsFunc(graph[n], visit) {
			return true
		}

		state[n] = done

		return false
	}

	names := make([]string, 0, len(graph))
	for n := range graph {
		names = append(names, n)
	}

	sort.Strings(names)

	for _, n := range names {
		if visit(n) {
			return invalidTemplate("the workflow actions contain a circular runAfter dependency involving '%s'", n)
		}
	}

	return nil
}

// validateLogicExpressions parses every template string so syntax errors and unknown functions fail the save.
func validateLogicExpressions(v any) error {
	switch t := v.(type) {
	case string:
		return checkLogicTemplate(t)
	case map[string]any:
		for k, x := range t {
			if k == "schema" || k == logicKMetadata {
				continue
			}

			if err := validateLogicExpressions(x); err != nil {
				return err
			}
		}
	case []any:
		for _, x := range t {
			if err := validateLogicExpressions(x); err != nil {
				return err
			}
		}
	}

	return nil
}

func checkLogicTemplate(s string) error {
	if !strings.Contains(s, "@") {
		return nil
	}

	var exprs []string

	switch {
	case strings.HasPrefix(s, "@@"):
	case strings.HasPrefix(s, "@") && !strings.HasPrefix(s, "@{"):
		exprs = append(exprs, s[1:])
	}

	for i := 0; i < len(s); i++ {
		if s[i] != '@' {
			continue
		}

		if i+1 < len(s) && s[i+1] == '@' {
			i++

			continue
		}

		if i+1 < len(s) && s[i+1] == '{' {
			end := matchingBrace(s, i+1)
			if end < 0 {
				return invalidTemplate("the template expression '%s' is not valid: missing closing brace", s)
			}

			exprs = append(exprs, s[i+2:end])
			i = end
		}
	}

	for _, e := range exprs {
		n, err := parseLogicExpr(e)
		if err != nil {
			return invalidTemplate("the template expression '%s' is not valid: %v", s, err)
		}

		if ferr := checkLogicFuncs(n); ferr != nil {
			return invalidTemplate("the template expression '%s' is not valid: %v", s, ferr)
		}
	}

	return nil
}

func checkLogicFuncs(n *logicExpr) error {
	if n == nil {
		return nil
	}

	if n.kind == exprCall {
		if _, ok := logicFuncTable()[strings.ToLower(n.name)]; !ok {
			return exprErr("the template language function '%s' is not defined or not valid", n.name)
		}
	}

	for _, a := range n.args {
		if err := checkLogicFuncs(a); err != nil {
			return err
		}
	}

	if err := checkLogicFuncs(n.base); err != nil {
		return err
	}

	return checkLogicFuncs(n.idx)
}
