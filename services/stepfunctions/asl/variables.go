package asl

import (
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"strings"
	"unicode"
)

// maxVariableNameLen is the AWS limit ("Variable name syntax").
const maxVariableNameLen = 80

// ErrVariableNotDefined is returned when a path references an unassigned variable.
var ErrVariableNotDefined = errors.New("variable is not defined")

type varFunc func(name string) (any, bool)

func (e *Executor) lookupVar(name string) (any, bool) {
	if v, ok := e.vars[name]; ok {
		return v, true
	}

	v, ok := e.outerVars[name]

	return v, ok
}

func (e *Executor) hasVars() bool { return len(e.vars) > 0 || len(e.outerVars) > 0 }

// varFn returns the lookup for path evaluation, or nil when no variable exists.
func (e *Executor) varFn() varFunc {
	if !e.hasVars() {
		return nil
	}

	return e.lookupVar
}

// visibleVars returns local plus outer-scope variables; callers must not mutate it.
func (e *Executor) visibleVars() map[string]any {
	switch {
	case len(e.outerVars) == 0:
		return e.vars
	case len(e.vars) == 0:
		return e.outerVars
	}

	m := make(map[string]any, len(e.vars)+len(e.outerVars))
	maps.Copy(m, e.outerVars)
	maps.Copy(m, e.vars)

	return m
}

const (
	maxAssignBytes        = 256 * 1024
	maxExecutionVarsBytes = 10 * 1024 * 1024
	errCodeDataLimit      = "States.DataLimitExceeded"
)

func dataLimitError(format string, args ...any) error {
	return &FailError{ErrCode: errCodeDataLimit, Cause: fmt.Sprintf(format, args...)}
}

// setVars stores vals after enforcing the "Limits" section of the AWS
// variables guide: 256 KiB per Assign field and 10 MiB of variables per execution.
func (e *Executor) setVars(vals map[string]any) error {
	if len(vals) == 0 {
		return nil
	}

	sizes := make(map[string]int, len(vals))
	assignTotal := 0

	for name, v := range vals {
		b, err := json.Marshal(v)
		if err != nil {
			return dataLimitError("variable %q cannot be serialized: %v", name, err)
		}

		sizes[name] = len(b)
		assignTotal += len(b)
	}

	if assignTotal > maxAssignBytes {
		return dataLimitError("the variables assigned by a single Assign field total %d bytes, over the %d byte limit",
			assignTotal, maxAssignBytes)
	}

	total := e.outerVarBytes + assignTotal
	for name, n := range e.varSizes {
		if _, replaced := sizes[name]; !replaced {
			total += n
		}
	}

	if total > maxExecutionVarsBytes {
		return dataLimitError("stored variables total %d bytes, over the %d byte per-execution limit",
			total, maxExecutionVarsBytes)
	}

	if e.vars == nil {
		e.vars = make(map[string]any, len(vals))
		e.varSizes = make(map[string]int, len(vals))
	}

	maps.Copy(e.vars, vals)
	maps.Copy(e.varSizes, sizes)

	return nil
}

// varBytes is the serialized size of this scope's own variables.
func (e *Executor) varBytes() int {
	n := 0
	for _, s := range e.varSizes {
		n += s
	}

	return n
}

// splitVarRef splits "$name.a.b" into ("name", "a.b"); "$." and "$$" are not variables.
func splitVarRef(path string) (string, string, bool) {
	if len(path) < 2 || path[0] != '$' {
		return "", "", false
	}

	r := []rune(path[1:])[0]
	if !isIDStart(r) {
		return "", "", false
	}

	name, rest, _ := strings.Cut(path[1:], ".")

	return name, rest, true
}

func resolveVarRef(path string, in pathEvalInput, cache *jsonPathCache) (any, error) {
	name, rest, _ := splitVarRef(path)

	var (
		val any
		ok  bool
	)

	if in.vars != nil {
		val, ok = in.vars(name)
	}

	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrVariableNotDefined, name)
	}

	return jsonPathGet(rest, val, cache)
}

func isIDStart(r rune) bool { return unicode.IsLetter(r) || unicode.Is(unicode.Nl, r) }

func isIDContinue(r rune) bool {
	return isIDStart(r) || unicode.In(r, unicode.Mn, unicode.Mc, unicode.Nd, unicode.Pc)
}

// validVariableName applies "Variable name syntax": Unicode ID_Start then
// ID_Continue characters, at most 80 long, and not the reserved $states.
func validVariableName(name string) bool {
	rs := []rune(name)
	if len(rs) == 0 || len(rs) > maxVariableNameLen || name == "states" {
		return false
	}

	if !isIDStart(rs[0]) {
		return false
	}

	for _, r := range rs[1:] {
		if !isIDContinue(r) {
			return false
		}
	}

	return true
}

// assignJSONPath evaluates a JSONPath-mode Assign payload template against
// data ("$" in the template) and then assigns every variable.
func (e *Executor) assignJSONPath(tmpl map[string]any, data any) error {
	if len(tmpl) == 0 {
		return nil
	}

	in := pathEvalInput{data: data, context: e.buildContextObject(), vars: e.varFn()}

	vals, err := evalTemplateMap(tmpl, in)
	if err != nil {
		return fmt.Errorf("assign error: %w", err)
	}

	return e.setVars(vals)
}
