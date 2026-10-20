package appsync

import (
	"errors"
	"fmt"
	"maps"
	"strings"
	"time"

	"github.com/dop251/goja"

	"github.com/blackbirdworks/gopherstack/pkgs/jsruntime"
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
	jsModUtils       = "@aws-appsync/utils"
	jsModDynamoDB    = "@aws-appsync/utils/dynamodb"
	jsModRDS         = "@aws-appsync/utils/rds"
	jsErrTypeRuntime = "APPSYNC_JS_RUNTIME_ERROR"
)

// errJSNoHandler marks a module that does not export the requested handler.
var errJSNoHandler = errors.New("handler not found")

func jsModules() map[string]string {
	return map[string]string{jsModUtils: "__mod_utils", jsModDynamoDB: "__mod_ddb", jsModRDS: "__mod_rds"}
}

type jsOutcome struct {
	Value any
	Logs  []string
	Early bool
}

// jsModuleSource rewrites ES module syntax into a plain script.
func jsModuleSource(code string) (string, error) {
	src, err := jsruntime.RewriteModule(code, jsModules())
	if err != nil {
		return "", fmt.Errorf("%w: %w", ErrValidation, err)
	}

	return src, nil
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

	sb := jsruntime.New(jsruntime.Options{
		Timeout: jsExecTimeout, MaxCallStack: jsMaxCallStack, Console: true,
	})
	defer sb.Close()

	out, err := execJS(sb, src, handler, ctx, ue)
	out.Logs = sb.Logs()

	return out, err
}

func execJS(sb *jsruntime.Sandbox, src, handler string, ctx map[string]any, ue *utilEnv) (jsOutcome, error) {
	bindJSGlobals(sb, ue)

	if _, err := sb.Run(jsPrelude + jsPreludeRDS); err != nil {
		return jsOutcome{}, fmt.Errorf("%w: prelude: %w", ErrValidation, err)
	}

	if _, err := sb.Run(src); err != nil {
		return jsOutcome{}, jsError(err)
	}

	ctxVal, err := sb.ToJS(ctx)
	if err != nil {
		return jsOutcome{}, fmt.Errorf("%w: context is not JSON-serializable", ErrValidation)
	}

	res, callErr := sb.Call(handler, ctxVal)

	syncStash(sb, ctxVal, ctx)

	if callErr != nil {
		if errors.Is(callErr, jsruntime.ErrNoFunction) {
			return jsOutcome{}, fmt.Errorf("%w: %s", errJSNoHandler, handler)
		}

		if e, isEarly := earlyFrom(callErr); isEarly {
			return jsOutcome{Value: e.value, Early: true}, nil
		}

		return jsOutcome{}, jsError(callErr)
	}

	val, err := jsToGo(sb, res)
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

	kind, msg := jsruntime.Classify(err)
	if kind == jsruntime.KindTimeout {
		msg = "code execution timed out"
	}

	return &resolverError{Message: msg, Type: jsErrTypeRuntime}
}

// jsToGo converts a JS value to plain JSON data.
func jsToGo(sb *jsruntime.Sandbox, v goja.Value) (any, error) {
	out, err := sb.FromJS(v)
	if err != nil {
		return nil, jsError(err)
	}

	return out, nil
}

func syncStash(sb *jsruntime.Sandbox, ctxVal goja.Value, ctx map[string]any) {
	st, ok := ctx["stash"].(map[string]any)
	if !ok {
		return
	}

	got, err := jsToGo(sb, ctxVal.ToObject(sb.VM).Get("stash"))
	if err != nil {
		return
	}

	m, _ := got.(map[string]any)

	clear(st)
	maps.Copy(st, m)
}

func bindJSGlobals(sb *jsruntime.Sandbox, ue *utilEnv) {
	vm := sb.VM
	util := vm.NewObject()
	ext := vm.NewObject()

	for _, name := range ue.names() {
		switch rest, isExt := strings.CutPrefix(name, "extensions."); {
		case isExt:
			bindUtilFn(sb, ext, rest, name, ue)
		case name == "qr" || name == "quiet" || name == "toJson" || name == "parseJson":
		default:
			bindUtilFn(sb, util, name, name, ue)
		}
	}

	_ = vm.Set("__util", util)
	_ = vm.Set("__extensions", ext)

	rt := vm.NewObject()
	_ = rt.Set("earlyReturn", func(call goja.FunctionCall) goja.Value {
		v, _ := jsToGo(sb, call.Argument(0))

		panic(vm.NewGoError(&earlyReturnError{value: v}))
	})
	_ = vm.Set("__runtime", rt)
}

// bindUtilFn exposes registry function key on obj at the dotted path,
// creating namespace objects on the way.
func bindUtilFn(sb *jsruntime.Sandbox, obj *goja.Object, path, key string, ue *utilEnv) {
	vm := sb.VM
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
			args[i] = exportJS(sb, a)
		}

		out, err := ue.call(key, args)
		if err != nil {
			panic(vm.NewGoError(err))
		}

		v, err := sb.ToJS(out)
		if err != nil {
			panic(vm.NewGoError(err))
		}

		return v
	})
}

func exportJS(sb *jsruntime.Sandbox, v goja.Value) any {
	if goja.IsUndefined(v) || goja.IsNull(v) {
		return nil
	}

	if _, isObj := v.(*goja.Object); isObj {
		if g, err := jsToGo(sb, v); err == nil {
			return g
		}
	}

	return v.Export()
}
