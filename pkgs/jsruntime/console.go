package jsruntime

import (
	"strings"

	"github.com/dop251/goja"
)

func (s *Sandbox) installConsole() {
	c := s.VM.NewObject()

	for _, level := range []string{"log", "info", "warn", "error", "debug"} {
		_ = c.Set(level, s.logFn)
	}

	_ = s.VM.Set("console", c)
}

func (s *Sandbox) logFn(call goja.FunctionCall) goja.Value {
	if len(s.logs) >= s.maxLogs {
		return goja.Undefined()
	}

	parts := make([]string, len(call.Arguments))
	for i, a := range call.Arguments {
		parts[i] = s.logString(a)
	}

	s.logs = append(s.logs, strings.Join(parts, " "))

	return goja.Undefined()
}

func (s *Sandbox) logString(v goja.Value) string {
	if _, isObj := v.(*goja.Object); isObj {
		if str, ok, err := s.Stringify(v); err == nil && ok {
			return str
		}
	}

	return v.String()
}
