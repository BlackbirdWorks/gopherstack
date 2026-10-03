package asl

import (
	"encoding/json"
	"fmt"
	"maps"
	"regexp"
	"slices"
	"strings"
)

var (
	statesResultRe = regexp.MustCompile(`\$states\s*\.\s*result\b`)
	statesErrorRe  = regexp.MustCompile(`\$states\s*\.\s*errorOutput\b`)
)

// jxState holds a JSONata state's decoded fields and compiled expressions,
// built once at Parse time and read-only afterwards.
type jxState struct {
	exprs    map[string]*jsonataExpr
	args     any
	output   any
	items    any
	itemSel  any
	hasArgs  bool
	hasOut   bool
	hasItems bool
}

// exprCtx says which $states members an expression may read.
type exprCtx struct {
	result      bool
	errorOutput bool
}

func resolveLang(where, own, inherited string) (string, error) {
	switch own {
	case "":
		return inherited, nil
	case queryLangJSONPath, queryLangJSONata:
		return own, nil
	}

	return "", fmt.Errorf(
		"%w: %s: QueryLanguage %q must be \"JSONPath\" or \"JSONata\"", ErrParseError, where, own)
}

func validateQueryLanguage(sm *StateMachine) error {
	lang, err := resolveLang("state machine", sm.QueryLanguage, queryLangJSONPath)
	if err != nil {
		return err
	}

	return walkStates(sm.States, lang, nil)
}

type nestedMachine struct {
	states map[string]*State
	ql     string
}

func nestedMachines(st *State) []nestedMachine {
	var subs []nestedMachine

	if st.Iterator != nil {
		subs = append(subs, nestedMachine{st.Iterator.States, st.Iterator.QueryLanguage})
	}

	if st.ItemProcessor != nil {
		subs = append(subs, nestedMachine{st.ItemProcessor.States, st.ItemProcessor.QueryLanguage})
	}

	for _, b := range st.Branches {
		subs = append(subs, nestedMachine{b.States, b.QueryLanguage})
	}

	return subs
}

// walkStates prepares one scope, then recurses; an inner scope may not
// assign a name an outer scope assigns (AWS "Variable scope").
func walkStates(states map[string]*State, lang string, outer map[string]struct{}) error {
	own, err := scopeVariables(states)
	if err != nil {
		return err
	}

	inner := make(map[string]struct{}, len(own)+len(outer))
	maps.Copy(inner, outer)

	for name := range own {
		if _, dup := outer[name]; dup {
			return fmt.Errorf(
				"%w: variable %q is assigned in an inner scope and an outer scope", ErrParseError, name)
		}

		inner[name] = struct{}{}
	}

	names := slices.Sorted(maps.Keys(states))

	for _, name := range names {
		if states[name] == nil {
			continue
		}

		if err = prepareState(name, states[name], lang); err != nil {
			return err
		}

		if err = walkNested(name, states[name], lang, inner); err != nil {
			return err
		}
	}

	return nil
}

func walkNested(name string, st *State, lang string, inner map[string]struct{}) error {
	for _, sub := range nestedMachines(st) {
		subLang, err := resolveLang(fmt.Sprintf("state %q", name), sub.ql, lang)
		if err != nil {
			return err
		}

		if err = walkStates(sub.states, subLang, inner); err != nil {
			return err
		}
	}

	return nil
}

func decodeAssign(raw json.RawMessage) (map[string]any, error) {
	if len(raw) == 0 {
		return nil, nil //nolint:nilnil // field absent
	}

	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil || m == nil {
		return nil, fmt.Errorf("%w: Assign must be a JSON object", ErrParseError)
	}

	return m, nil
}

func assignNames(raw json.RawMessage, into map[string]struct{}) error {
	m, err := decodeAssign(raw)
	if err != nil {
		return err
	}

	for k := range m {
		name := strings.TrimSuffix(k, ".$")
		if !validVariableName(name) {
			return fmt.Errorf("%w: invalid variable name %q in Assign", ErrParseError, name)
		}

		into[name] = struct{}{}
	}

	return nil
}

func scopeVariables(states map[string]*State) (map[string]struct{}, error) {
	names := map[string]struct{}{}

	for _, st := range states {
		if st == nil {
			continue
		}

		if err := assignNames(st.Assign, names); err != nil {
			return nil, err
		}

		for i := range st.Choices {
			if err := assignNames(st.Choices[i].Assign, names); err != nil {
				return nil, err
			}
		}

		for i := range st.Catch {
			if err := assignNames(st.Catch[i].Assign, names); err != nil {
				return nil, err
			}
		}
	}

	return names, nil
}

func prepareState(name string, st *State, machineLang string) error {
	lang, err := resolveLang(fmt.Sprintf("state %q", name), st.QueryLanguage, machineLang)
	if err != nil {
		return err
	}

	st.lang = lang

	if err = prepareAssign(name, st); err != nil {
		return err
	}

	if lang == queryLangJSONata {
		return prepareJSONata(name, st)
	}

	return rejectJSONataFields(name, st)
}

func prepareAssign(name string, st *State) error {
	if len(st.Assign) > 0 && (st.Type == stateTypeSucceed || st.Type == stateTypeFail) {
		return fmt.Errorf("%w: state %q: %s states do not support Assign", ErrParseError, name, st.Type)
	}

	var err error
	if st.assignVals, err = decodeAssign(st.Assign); err != nil {
		return fmt.Errorf("state %q: %w", name, err)
	}

	for i := range st.Choices {
		if st.Choices[i].assignVals, err = decodeAssign(st.Choices[i].Assign); err != nil {
			return fmt.Errorf("state %q: %w", name, err)
		}
	}

	for i := range st.Catch {
		if st.Catch[i].assignVals, err = decodeAssign(st.Catch[i].Assign); err != nil {
			return fmt.Errorf("state %q: %w", name, err)
		}
	}

	return nil
}

func jsonataOnlyErr(name, field string) error {
	return fmt.Errorf("%w: state %q: field %q is only supported when QueryLanguage is JSONata",
		ErrParseError, name, field)
}

func jsonPathOnlyErr(name, field string) error {
	return fmt.Errorf("%w: state %q: field %q is only supported when QueryLanguage is JSONPath",
		ErrParseError, name, field)
}

func rejectJSONataFields(name string, st *State) error {
	checks := []struct {
		field string
		set   bool
	}{
		{"Arguments", len(st.Arguments) > 0},
		{"Output", len(st.Output) > 0},
		{fieldItems, len(st.Items) > 0},
		{"Seconds/TimeoutSeconds/HeartbeatSeconds/MaxConcurrency expression", len(st.numExprs) > 0},
	}

	for _, c := range checks {
		if c.set {
			return jsonataOnlyErr(name, c.field)
		}
	}

	for _, r := range st.Choices {
		if r.Condition != "" {
			return jsonataOnlyErr(name, "Condition")
		}
	}

	for _, c := range st.Catch {
		if len(c.Output) > 0 {
			return jsonataOnlyErr(name, "Output")
		}
	}

	return nil
}

func rejectJSONPathFields(name string, st *State) error {
	checks := []struct {
		field string
		set   bool
	}{
		{"InputPath", st.InputPath != ""},
		{"Parameters", len(st.Parameters) > 0},
		{"ResultSelector", len(st.ResultSelector) > 0},
		{"ResultPath", st.ResultPath != ""},
		{"OutputPath", st.OutputPath != ""},
		{"Result", len(st.Result) > 0},
		{"ItemsPath", st.ItemsPath != ""},
		{"SecondsPath", st.SecondsPath != ""},
		{"TimestampPath", st.TimestampPath != "" && st.Type == stateTypeWait},
		{"TimeoutSecondsPath", st.TimeoutSecondsPath != ""},
		{"HeartbeatSecondsPath", st.HeartbeatSecondsPath != ""},
		{"MaxConcurrencyPath", st.MaxConcurrencyPath != ""},
		{"ToleratedFailureCountPath", st.ToleratedFailureCountPath != ""},
		{"ToleratedFailurePercentagePath", st.ToleratedFailurePercentagePath != ""},
	}

	for _, c := range checks {
		if c.set {
			return jsonPathOnlyErr(name, c.field)
		}
	}

	for _, r := range st.Choices {
		if r.Variable != "" || len(r.And) > 0 || len(r.Or) > 0 || r.Not != nil {
			return jsonPathOnlyErr(name, "Variable/And/Or/Not")
		}
	}

	for _, c := range st.Catch {
		if c.ResultPath != "" {
			return jsonPathOnlyErr(name, "ResultPath")
		}
	}

	return nil
}

func decodeRaw(name, field string, raw json.RawMessage) (any, error) {
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		return nil, fmt.Errorf("%w: state %q: invalid %s: %w", ErrParseError, name, field, err)
	}

	return v, nil
}

func (j *jxState) scan(name, field string, v any, ctx exprCtx) error {
	switch t := v.(type) {
	case string:
		return j.scanString(name, field, t, ctx)
	case map[string]any:
		for _, val := range t {
			if err := j.scan(name, field, val, ctx); err != nil {
				return err
			}
		}
	case []any:
		for _, val := range t {
			if err := j.scan(name, field, val, ctx); err != nil {
				return err
			}
		}
	}

	return nil
}

func (j *jxState) scanString(name, field, s string, ctx exprCtx) error {
	if malformedJSONata(s) {
		return fmt.Errorf("%w: state %q: %s: JSONata expression %q must start with \"{%%\" and end with \"%%}\" "+
			"with no surrounding whitespace", ErrParseError, name, field, s)
	}

	src, ok := wrappedJSONata(s)
	if !ok {
		return nil
	}

	if !ctx.result && statesResultRe.MatchString(src) {
		return fmt.Errorf("%w: state %q: %s: $states.result is not accessible here", ErrParseError, name, field)
	}

	if !ctx.errorOutput && statesErrorRe.MatchString(src) {
		return fmt.Errorf("%w: state %q: %s: $states.errorOutput is only accessible in a Catch Assign or Output",
			ErrParseError, name, field)
	}

	if _, seen := j.exprs[src]; seen {
		return nil
	}

	x, err := compileJSONata(src)
	if err != nil {
		return fmt.Errorf("%w: state %q: %s: invalid JSONata expression: %w", ErrParseError, name, field, err)
	}

	j.exprs[src] = x

	return nil
}

func rejectJSONataStateFields(name string, st *State) error {
	if err := rejectJSONPathFields(name, st); err != nil {
		return err
	}

	for field := range st.numExprs {
		if st.Type != numExprFieldTypes[field] {
			return fmt.Errorf("%w: state %q: field %q does not accept a JSONata expression", ErrParseError, name, field)
		}
	}

	return nil
}

func prepareJSONata(name string, st *State) error {
	if err := rejectJSONataStateFields(name, st); err != nil {
		return err
	}

	if err := validateJSONataFieldTypes(name, st); err != nil {
		return err
	}

	j := &jxState{exprs: map[string]*jsonataExpr{}}
	st.jx = j

	return j.prepareFields(name, st)
}

func validateJSONataFieldTypes(name string, st *State) error {
	switch {
	case len(st.Arguments) > 0 && st.Type != stateTypeTask && st.Type != stateTypeParallel:
		return fmt.Errorf("%w: state %q: %s states do not support Arguments", ErrParseError, name, st.Type)
	case len(st.Items) > 0 && st.Type != StateTypeMap:
		return fmt.Errorf("%w: state %q: only Map states support Items", ErrParseError, name)
	case len(st.Output) > 0 && st.Type == stateTypeFail:
		return fmt.Errorf("%w: state %q: Fail states do not support Output", ErrParseError, name)
	}

	return nil
}

func (j *jxState) prepareFields(name string, st *State) error {
	hasResult := st.Type == stateTypeTask || st.Type == stateTypeParallel || st.Type == StateTypeMap
	plain := exprCtx{}

	fields := []struct {
		dst   *any
		set   *bool
		field string
		raw   json.RawMessage
		ctx   exprCtx
	}{
		{&j.args, &j.hasArgs, "Arguments", st.Arguments, plain},
		{&j.output, &j.hasOut, "Output", st.Output, exprCtx{result: hasResult}},
		{&j.items, &j.hasItems, fieldItems, st.Items, plain},
		{&j.itemSel, new(bool), "ItemSelector", st.ItemSelector, plain},
	}

	for _, f := range fields {
		if len(f.raw) == 0 {
			continue
		}

		v, err := decodeRaw(name, f.field, f.raw)
		if err != nil {
			return err
		}

		*f.dst, *f.set = v, true

		if err = j.scan(name, f.field, v, f.ctx); err != nil {
			return err
		}
	}

	return j.prepareRest(name, st, exprCtx{result: hasResult})
}

func (j *jxState) prepareRest(name string, st *State, assignCtx exprCtx) error {
	for _, v := range st.assignVals {
		if err := j.scan(name, "Assign", v, assignCtx); err != nil {
			return err
		}
	}

	for field, src := range st.numExprs {
		if err := j.scan(name, field, src, exprCtx{}); err != nil {
			return err
		}
	}

	for _, s := range []string{st.Timestamp, st.Error, st.Cause} {
		if err := j.scan(name, "Timestamp/Error/Cause", s, exprCtx{}); err != nil {
			return err
		}
	}

	return j.prepareRulesAndCatchers(name, st)
}

func (j *jxState) prepareRulesAndCatchers(name string, st *State) error {
	for i := range st.Choices {
		if err := j.prepareRule(name, i, &st.Choices[i]); err != nil {
			return err
		}
	}

	for i := range st.Catch {
		if err := j.prepareCatcher(name, &st.Catch[i]); err != nil {
			return err
		}
	}

	return nil
}

func (j *jxState) prepareRule(name string, idx int, r *ChoiceRule) error {
	if _, ok := wrappedJSONata(r.Condition); !ok {
		return fmt.Errorf("%w: state %q: Choice rule %d requires a \"{%% %%}\" Condition", ErrParseError, name, idx)
	}

	if err := j.scan(name, "Condition", r.Condition, exprCtx{}); err != nil {
		return err
	}

	for _, v := range r.assignVals {
		if err := j.scan(name, "Assign", v, exprCtx{}); err != nil {
			return err
		}
	}

	return nil
}

func (j *jxState) prepareCatcher(name string, c *Catcher) error {
	ctx := exprCtx{errorOutput: true}

	if len(c.Output) > 0 {
		v, err := decodeRaw(name, "Catch Output", c.Output)
		if err != nil {
			return err
		}

		c.outputVal = v

		if err = j.scan(name, "Catch Output", v, ctx); err != nil {
			return err
		}
	}

	for _, v := range c.assignVals {
		if err := j.scan(name, "Catch Assign", v, ctx); err != nil {
			return err
		}
	}

	return nil
}
