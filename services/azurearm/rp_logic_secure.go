package azurearm

import (
	"encoding/json/v2"
	"regexp"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

const (
	logicKRuntimeConfig = "runtimeConfiguration"
	logicKStaticResult  = "staticResult"
)

var logicTriggerRef = regexp.MustCompile(
	`(?i)\btrigger(?:body|outputs|formdatavalue|formdatamultivalues|multipartbody)?\(`,
)

var logicActionRef = regexp.MustCompile(`(?i)\b(?:outputs|body|actions)\(\s*'([^']+)'\s*\)`)

const (
	typInitVar     = "initializevariable"
	typSetVar      = "setvariable"
	typIncVar      = "incrementvariable"
	typDecVar      = "decrementvariable"
	typAppendArray = "appendtoarrayvariable"
	typAppendStr   = "appendtostringvariable"
	typCompose     = "compose"
	typParseJSON   = "parsejson"
	typWait        = "wait"
	typJoin        = "join"
	typSelect      = "select"
	typQuery       = "query"
)

func noSecureInputs(typ string) bool {
	switch typ {
	case typAppendStr, typAppendArray, typDecVar, typIncVar, typInitVar, typSetVar, "foreach", "if", "recurrence",
		"scope", "switch", "terminate", "until":
		return true
	}

	return false
}

func noSecureOutputs(typ string) bool {
	return noSecureInputs(typ) || typ == typCompose || typ == typParseJSON || typ == "response" || typ == typWait
}

func inputsOnly(typ string) bool {
	return typ == typCompose || typ == typParseJSON || typ == "response"
}

// secureData reads runtimeConfiguration.secureData.properties of a trigger or action definition.
func secureData(def map[string]any) (bool, bool) {
	var in, out bool

	rc, _ := objectOf(def[logicKRuntimeConfig])
	sd, _ := objectOf(rc["secureData"])
	props, _ := sd["properties"].([]any)

	for _, p := range props {
		switch strings.ToLower(stringOf(p)) {
		case logicKInputs:
			in = true
		case logicKOutputs:
			out = true
		}
	}

	return in, out
}

// secureHides applies the documented per-type support: Compose, Parse JSON and Response hide their outputs
// together with their inputs, and ignore the outputs setting.
func secureHides(typ string, in, out bool) (bool, bool) {
	typ = strings.ToLower(typ)

	if inputsOnly(typ) && in {
		return true, true
	}

	return in && !noSecureInputs(typ), out && !noSecureOutputs(typ)
}

func hideContent(in, out *logicContent, hideIn, hideOut bool) {
	in.Hidden = in.Hidden || hideIn
	out.Hidden = out.Hidden || hideOut
}

// hideTriggerContent applies a trigger definition's secureData to the recorded content.
func hideTriggerContent(def map[string]any, in, out *logicContent) {
	si, so := secureData(def)
	hi, ho := secureHides(stringOf(def[fieldType]), si, so)
	hideContent(in, out, hi, ho)
}

// taintedInputs reports whether the action's inputs explicitly use the outputs of a trigger or action whose secured
// data the documentation says propagates to its readers.
func (rs *logicRunState) taintedInputs(env *logicEnv, def map[string]any) bool {
	raw, err := json.Marshal(def[logicKInputs], jsonx.V1())
	if err != nil {
		return false
	}

	if logicTriggerRef.Match(raw) {
		rs.p.mu.RLock("taintedInputs")
		hidden := rs.run.Trigger.Inputs.Hidden || rs.run.Trigger.Outputs.Hidden
		rs.p.mu.RUnlock()

		if hidden {
			return true
		}
	}

	for _, m := range logicActionRef.FindAllStringSubmatch(string(raw), -1) {
		src, ok := rs.lookupAction(env, m[1])
		if !ok {
			continue
		}

		rs.p.mu.RLock("taintedInputs")
		tainted := src.Taints
		rs.p.mu.RUnlock()

		if tainted {
			return true
		}
	}

	return false
}

type secureHide struct{ in, out, source bool }

// secureFor decides what the run history hides for a finished action: its own secureData setting and, when it
// reads secured outputs, the documented propagation to downstream actions.
func (rs *logicRunState) secureFor(env *logicEnv, typ string, def map[string]any) secureHide {
	si, so := secureData(def)
	hideIn, hideOut := secureHides(typ, si, so)
	source := !inputsOnly(strings.ToLower(typ)) && (hideIn || hideOut)

	if !hideIn && rs.taintedInputs(env, def) {
		hideIn, hideOut = secureHides(typ, true, false)
	}

	return secureHide{in: hideIn, out: hideOut, source: source}
}

func logicStaticCapable(typ string) bool {
	switch typ {
	case typCompose, logicActResponse, logicActTerminate, typWait, logicActScope, logicActIf, logicActSwitch,
		logicActForeach, logicActUntil, typParseJSON, typSelect, typQuery, typJoin, logicTypeTable:
		return false
	}

	return !strings.HasSuffix(typ, "variable")
}

// staticResult returns the mock outcome of an action whose runtimeConfiguration.staticResult is Enabled; the
// action itself does not run.
func (rs *logicRunState) staticResult(typ string, def map[string]any) (logicOutcome, bool) {
	rc, _ := objectOf(def[logicKRuntimeConfig])
	sr, _ := objectOf(rc[logicKStaticResult])

	if !logicStaticCapable(typ) || !strings.EqualFold(stringOf(sr["staticResultOptions"]), "Enabled") {
		return logicOutcome{}, false
	}

	name := stringOf(sr[fieldName])
	defs, _ := objectOf(rs.def["staticResults"])

	res, ok := objectOf(defs[name])
	if !ok {
		return failOutcome(logicCodeInvalidTmpl, "The static result '"+name+"' is not defined in staticResults."), true
	}

	status := logicStSucceeded

	if s := stringOf(res[logicKStatus]); s != "" {
		status = s
	}

	switch status {
	case logicStSucceeded, logicStFailed, logicStTimedOut, logicStCancelled:
	default:
		return failOutcome(
			logicCodeInvalidTmpl,
			"The static result '"+name+"' has the unsupported status '"+status+"'.",
		), true
	}

	out := logicOutcome{status: status, code: stringOf(res[logicKCode])}

	if o, has := res[logicKOutputs]; has {
		out.outputs, out.hasOut = o, true
	}

	if status != logicStSucceeded {
		out.err = staticError(res, out.code)
	}

	return out, true
}

func staticError(res map[string]any, code string) any {
	if e, ok := objectOf(res[logicKError]); ok {
		return e
	}

	if o, ok := objectOf(res[logicKOutputs]); ok {
		if errs, isList := o["errors"].([]any); isList && len(errs) > 0 {
			return errs[0]
		}
	}

	return map[string]any{
		logicKCode:   orStr(code, logicCodeActionFailed),
		fieldMessage: "The static result reports a failure.",
	}
}
