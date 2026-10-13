package rolesanywhere

import (
	"errors"
	"regexp"
	"strings"
)

var (
	leadingCodeRe  = regexp.MustCompile(`^[A-Za-z]+(Exception|Error): `)
	trailingCodeRe = regexp.MustCompile(`: [A-Za-z]+(Exception|Error)$`)
	bareCodeRe     = regexp.MustCompile(`^[A-Za-z]+(Exception|Error)$`)
)

// publicMessage strips the sentinel/code text that error wrapping adds so
// clients never see "Code: Code: detail"; a bare code falls back to a generic sentence for code.
func publicMessage(err error, code string) string {
	msg := err.Error()

	for e := errors.Unwrap(err); e != nil; e = errors.Unwrap(e) {
		msg = strings.TrimPrefix(msg, e.Error()+": ")
		msg = strings.TrimSuffix(msg, ": "+e.Error())
	}

	msg = leadingCodeRe.ReplaceAllString(msg, "")
	msg = trailingCodeRe.ReplaceAllString(msg, "")

	if msg == "" || bareCodeRe.MatchString(msg) {
		return fallbackMessage(code)
	}

	return msg
}

func fallbackMessage(code string) string {
	switch {
	case strings.Contains(code, "NotFound"):
		return "The specified resource does not exist."
	case strings.Contains(code, "Exists"), strings.Contains(code, "Conflict"), strings.Contains(code, "InUse"):
		return "The request conflicts with the current state of the resource."
	case strings.Contains(code, "Validation"), strings.Contains(code, "Invalid"), strings.Contains(code, "Limit"),
		strings.Contains(code, "Quota"), strings.Contains(code, "TooMany"):
		return "The request was rejected."
	default:
		return "Internal error."
	}
}
