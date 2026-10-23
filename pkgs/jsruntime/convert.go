package jsruntime

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/dop251/goja"
)

// ErrNotJSON means a value could not be converted to or from JSON.
var ErrNotJSON = errors.New("value is not JSON-serializable")

// ToJS builds a fresh JS value from plain Go data via JSON.parse.
func (s *Sandbox) ToJS(v any) (goja.Value, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrNotJSON, err)
	}

	parse, _ := goja.AssertFunction(s.VM.Get("JSON").ToObject(s.VM).Get("parse"))

	out, err := parse(goja.Undefined(), s.VM.ToValue(string(b)))
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrNotJSON, err)
	}

	return out, nil
}

// Stringify returns JSON.stringify(v); ok is false for undefined results.
func (s *Sandbox) Stringify(v goja.Value) (string, bool, error) {
	if v == nil || goja.IsUndefined(v) {
		return "", false, nil
	}

	stringify, _ := goja.AssertFunction(s.VM.Get("JSON").ToObject(s.VM).Get("stringify"))

	out, err := stringify(goja.Undefined(), v)
	if err != nil {
		return "", false, fmt.Errorf("stringify: %w", err)
	}

	if goja.IsUndefined(out) {
		return "", false, nil
	}

	return out.String(), true, nil
}

// FromJS converts a JS value to plain JSON data; undefined and null give nil.
func (s *Sandbox) FromJS(v goja.Value) (any, error) {
	str, ok, err := s.Stringify(v)
	if err != nil || !ok {
		return nil, err
	}

	var out any
	if err = json.Unmarshal([]byte(str), &out); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrNotJSON, err)
	}

	return out, nil
}

// ToJSString parses a JSON document into a JS value.
func (s *Sandbox) ToJSString(doc string) (goja.Value, error) {
	parse, _ := goja.AssertFunction(s.VM.Get("JSON").ToObject(s.VM).Get("parse"))

	out, err := parse(goja.Undefined(), s.VM.ToValue(doc))
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrNotJSON, err)
	}

	return out, nil
}
