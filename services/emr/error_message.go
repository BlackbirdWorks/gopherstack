package emr

import (
	"errors"
	"regexp"
	"strings"
)

var leadingCodeRe = regexp.MustCompile(`^[A-Za-z]+(Exception|Error|Fault): `)

// publicMessage drops the sentinel/code text that error wrapping prepends, so
// clients do not see "Code: Code: detail".
func publicMessage(err error) string {
	msg := err.Error()

	for e := errors.Unwrap(err); e != nil; e = errors.Unwrap(e) {
		msg = strings.TrimPrefix(msg, e.Error()+": ")
	}

	if out := leadingCodeRe.ReplaceAllString(msg, ""); out != "" {
		return out
	}

	return msg
}
