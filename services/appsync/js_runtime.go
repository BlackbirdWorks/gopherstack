package appsync

import (
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"regexp"
	"strings"
	"time"

	"github.com/dop251/goja"
)

const (
	jsHandlerRequest  = "request"
	jsHandlerResponse = "response"

	jsCtxKeyArguments = "arguments"
	jsCtxKeyResult    = "result"
	jsCtxKeyPrev      = "prev"
	jsCtxKeySource    = "source"

	jsExecTimeout    = 750 * time.Millisecond
	jsMaxCallStack   = 200
	asParts          = 2
	jsModUtils       = "@aws-appsync/utils"
	jsModDynamoDB    = "@aws-appsync/utils/dynamodb"
	jsErrTypeRuntime = "APPSYNC_JS_RUNTIME_ERROR"
)

// errJSNoHandler marks a module that does not export the requested handler.
var errJSNoHandler = errors.New("handler not found")

var (
	reJSImport     = regexp.MustCompile(`(?s)\bimport\s+([^;'"]*?)\s*from\s*['"]([^'"]+)['"]\s*;?`)
	reJSExportDecl = regexp.MustCompile(`\bexport\s+((?:async\s+)?function|const|let|var|class)\b`)
	reJSExportList = regexp.MustCompile(`(?s)\bexport\s*\{[^}]*\}\s*;?`)
	reJSExportDflt = regexp.MustCompile(`\bexport\s+default\b`)
	reJSAsName     = regexp.MustCompile(`\s+as\s+`)
)

type jsOutcome struct {
	Value any
	Early bool
}

// jsModuleSource rewrites ES module syntax into a plain script.
func jsModuleSource(code string) (string, error) {
	if reJSExportDflt.MatchString(code) {
		return "", fmt.Errorf("%w: default exports are not supported", ErrValidation)
	}

	var impErr error

	out := reJSImport.ReplaceAllStringFunc(code, func(m string) string {
		sub := reJSImport.FindStringSubmatch(m)

		stmt, err := jsImportStmt(strings.TrimSpace(sub[1]), sub[2])
		if err != nil {
			impErr = err
		}

		return stmt
	})
	if impErr != nil {
		return "", impErr
	}

	out = reJSExportList.ReplaceAllString(out, "")

	return reJSExportDecl.ReplaceAllString(out, "$1"), nil
}

func jsImportStmt(clause, mod string) (string, error) {
	var obj string

	switch mod {
	case jsModUtils:
		obj = "__mod_utils"
	case jsModDynamoDB:
		obj = "__mod_ddb"
	default:
		return "", fmt.Errorf("%w: unsupported module %q", ErrValidation, mod)
	}

	switch {
	case strings.HasPrefix(clause, "*"):
		as := reJSAsName.Split(clause, asParts)
		if len(as) != asParts || strings.TrimSpace(as[1]) == "" {
			return "", fmt.Errorf("%w: unsupported import form", ErrValidation)
		}

		name := strings.TrimSpace(as[1])

		return "var " + name + " = " + obj + ";", nil
	case strings.HasPrefix(clause, "{"):
		inner := strings.Trim(clause, "{} \n\t\r")
		parts := strings.Split(inner, ",")

		for i, p := range parts {
			if as := reJSAsName.Split(strings.TrimSpace(p), asParts); len(as) == asParts {
				parts[i] = as[0] + ": " + as[1]
			}
		}

		return "var {" + strings.Join(parts, ",") + "} = " + obj + ";", nil
	}

	return "", fmt.Errorf("%w: unsupported import form", ErrValidation)
}

// runJS runs the exported handler (request|response) of an APPSYNC_JS module
// against ctx. ctx["stash"] is shared and updated in place.
func runJS(code, handler string, ctx map[string]any, ue *utilEnv) (jsOutcome, error) {
	src, err := jsModuleSource(code)
	if err != nil {
		return jsOutcome{}, err
	}

	if ue == nil {
		ue = &utilEnv{}
	}

	vm := goja.New()
	vm.SetMaxCallStackSize(jsMaxCallStack)

	bindJSGlobals(vm, ue)

	timer := time.AfterFunc(jsExecTimeout, func() { vm.Interrupt("execution timed out") })
	defer timer.Stop()

	if _, err = vm.RunString(jsPrelude); err != nil {
		return jsOutcome{}, fmt.Errorf("%w: prelude: %w", ErrValidation, err)
	}

	if _, err = vm.RunString(src); err != nil {
		return jsOutcome{}, jsError(err)
	}

	fn, ok := goja.AssertFunction(vm.Get(handler))
	if !ok {
		return jsOutcome{}, fmt.Errorf("%w: %s", errJSNoHandler, handler)
	}

	ctxVal, err := jsFromGo(vm, ctx)
	if err != nil {
		return jsOutcome{}, err
	}

	res, callErr := fn(goja.Undefined(), ctxVal)

	syncStash(vm, ctxVal, ctx)

	if callErr != nil {
		if e, isEarly := earlyFrom(callErr); isEarly {
			return jsOutcome{Value: e.value, Early: true}, nil
		}

		return jsOutcome{}, jsError(callErr)
	}

	val, err := jsToGo(vm, res)
	if err != nil {
		return jsOutcome{}, err
	}

	return jsOutcome{Value: val}, nil
}

func earlyFrom(err error) (*earlyReturnError, bool) {
	return errors.AsType[*earlyReturnError](err)
}

// jsError maps a goja failure to a resolver error (user util.error) or a
// runtime error.
func jsError(err error) error {
	if re, ok := errors.AsType[*resolverError](err); ok {
		return re
	}

	if _, ok := errors.AsType[*goja.InterruptedError](err); ok {
		return &resolverError{Message: "code execution timed out", Type: jsErrTypeRuntime}
	}

	if exc, ok := errors.AsType[*goja.Exception](err); ok {
		return &resolverError{Message: exc.Value().String(), Type: jsErrTypeRuntime}
	}

	return &resolverError{Message: err.Error(), Type: jsErrTypeRuntime}
}

func jsFromGo(vm *goja.Runtime, v any) (goja.Value, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("%w: context is not JSON-serializable", ErrValidation)
	}

	parse, _ := goja.AssertFunction(vm.Get("JSON").ToObject(vm).Get("parse"))

	return parse(goja.Undefined(), vm.ToValue(string(b)))
}

// jsToGo converts a JS value to plain JSON data via JSON.stringify.
func jsToGo(vm *goja.Runtime, v goja.Value) (any, error) {
	if v == nil || goja.IsUndefined(v) || goja.IsNull(v) {
		return noValue(), nil
	}

	stringify, _ := goja.AssertFunction(vm.Get("JSON").ToObject(vm).Get("stringify"))

	s, err := stringify(goja.Undefined(), v)
	if err != nil {
		return nil, jsError(err)
	}

	if goja.IsUndefined(s) {
		return noValue(), nil
	}

	var out any
	if err = json.Unmarshal([]byte(s.String()), &out); err != nil {
		return nil, fmt.Errorf("%w: result is not JSON-serializable", ErrValidation)
	}

	return out, nil
}

func syncStash(vm *goja.Runtime, ctxVal goja.Value, ctx map[string]any) {
	st, ok := ctx["stash"].(map[string]any)
	if !ok {
		return
	}

	obj := ctxVal.ToObject(vm)

	got, err := jsToGo(vm, obj.Get("stash"))
	if err != nil {
		return
	}

	m, _ := got.(map[string]any)

	clear(st)
	maps.Copy(st, m)
}

func bindJSGlobals(vm *goja.Runtime, ue *utilEnv) {
	util := vm.NewObject()
	ext := vm.NewObject()

	for _, name := range ue.names() {
		switch rest, isExt := strings.CutPrefix(name, "extensions."); {
		case isExt:
			bindUtilFn(vm, ext, rest, name, ue)
		case name == "qr" || name == "quiet" || name == "toJson" || name == "parseJson":
		default:
			bindUtilFn(vm, util, name, name, ue)
		}
	}

	_ = vm.Set("__util", util)
	_ = vm.Set("__extensions", ext)

	rt := vm.NewObject()
	_ = rt.Set("earlyReturn", func(call goja.FunctionCall) goja.Value {
		v, _ := jsToGo(vm, call.Argument(0))

		panic(vm.NewGoError(&earlyReturnError{value: v}))
	})
	_ = vm.Set("__runtime", rt)

	console := vm.NewObject()
	noop := func(goja.FunctionCall) goja.Value { return goja.Undefined() }
	_ = console.Set("log", noop)
	_ = console.Set("error", noop)
	_ = vm.Set("console", console)
}

// bindUtilFn exposes registry function key on obj at the dotted path,
// creating namespace objects on the way.
func bindUtilFn(vm *goja.Runtime, obj *goja.Object, path, key string, ue *utilEnv) {
	parts := strings.Split(path, ".")
	cur := obj

	for _, p := range parts[:len(parts)-1] {
		next, ok := cur.Get(p).(*goja.Object)
		if !ok || next == nil {
			next = vm.NewObject()
			_ = cur.Set(p, next)
		}

		cur = next
	}

	_ = cur.Set(parts[len(parts)-1], func(call goja.FunctionCall) goja.Value {
		args := make([]any, len(call.Arguments))
		for i, a := range call.Arguments {
			args[i] = exportJS(vm, a)
		}

		out, err := ue.call(key, args)
		if err != nil {
			panic(vm.NewGoError(err))
		}

		v, err := jsFromGo(vm, out)
		if err != nil {
			panic(vm.NewGoError(err))
		}

		return v
	})
}

func exportJS(vm *goja.Runtime, v goja.Value) any {
	if goja.IsUndefined(v) || goja.IsNull(v) {
		return nil
	}

	if _, isObj := v.(*goja.Object); isObj {
		g, err := jsToGo(vm, v)
		if err == nil {
			return g
		}
	}

	return v.Export()
}
