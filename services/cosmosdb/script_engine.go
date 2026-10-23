package cosmosdb

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/dop251/goja"

	"github.com/blackbirdworks/gopherstack/pkgs/jsruntime"
)

const (
	// defaultScriptTimeout mirrors the Cosmos 5 second server-side script limit.
	defaultScriptTimeout = 5 * time.Second
	jsMaxCallStack       = 500
)

func (b *InMemoryBackend) scriptLimit() time.Duration {
	if b.scriptTimeout > 0 {
		return b.scriptTimeout
	}

	return defaultScriptTimeout
}

// newSandbox returns a bare goja runtime (no require, console, fs or network) that is interrupted after the
// script time limit; stop must be called when the script finishes.
func (b *InMemoryBackend) newSandbox() (*goja.Runtime, func()) {
	sb := jsruntime.New(jsruntime.Options{
		Timeout: b.scriptLimit(), MaxCallStack: jsMaxCallStack, InterruptMsg: "script execution timed out",
	})

	return sb.VM, sb.Close
}

// compileScript checks that body parses as a single function.
func compileScript(body string) error {
	if _, err := goja.Compile("", "("+body+"\n)", false); err != nil {
		return fmt.Errorf("%w: %s", ErrInvalidScript, err.Error())
	}

	return nil
}

func loadFunction(vm *goja.Runtime, body string) (goja.Callable, error) {
	v, err := vm.RunString("(" + body + "\n)")
	if err != nil {
		return nil, scriptError(err)
	}

	fn, ok := goja.AssertFunction(v)
	if !ok {
		return nil, fmt.Errorf("%w: the script body is not a function", ErrScriptFailed)
	}

	return fn, nil
}

// scriptError classifies an error from goja: a timeout, or a script failure carrying the JS message.
func scriptError(err error) error {
	if err == nil {
		return nil
	}

	if _, interrupted := errors.AsType[*goja.InterruptedError](err); interrupted {
		return ErrScriptTimeout
	}

	if errors.Is(err, ErrScriptTimeout) {
		return ErrScriptTimeout
	}

	if ex, ok := errors.AsType[*goja.Exception](err); ok {
		return fmt.Errorf("%w: Encountered exception while executing Javascript. Exception = %s",
			ErrScriptFailed, ex.Error())
	}

	return fmt.Errorf("%w: %s", ErrScriptFailed, err.Error())
}

// toJS converts a stored JSON value to a JS value; objects and arrays are built fresh so scripts cannot alias
// stored state.
func toJS(vm *goja.Runtime, v any) goja.Value {
	switch t := v.(type) {
	case nil:
		return goja.Null()
	case undef:
		return goja.Undefined()
	case json.Number:
		return numberToJS(vm, t)
	case map[string]any:
		o := vm.NewObject()
		for k, e := range t {
			_ = o.Set(k, toJS(vm, e))
		}

		return o
	case []any:
		items := make([]any, len(t))
		for i, e := range t {
			items[i] = toJS(vm, e)
		}

		return vm.NewArray(items...)
	default:
		return vm.ToValue(t)
	}
}

func numberToJS(vm *goja.Runtime, n json.Number) goja.Value {
	if i, err := n.Int64(); err == nil {
		return vm.ToValue(i)
	}

	f, _ := n.Float64()

	return vm.ToValue(f)
}

// fromJS converts a JS value to a JSON-shaped Go value; undefined becomes undef{}.
func fromJS(v goja.Value) any {
	if v == nil || goja.IsUndefined(v) {
		return undef{}
	}

	if goja.IsNull(v) {
		return nil
	}

	return normalizeExport(v.Export())
}

func normalizeExport(v any) any {
	switch t := v.(type) {
	case int64:
		return json.Number(strconv.FormatInt(t, 10))
	case int:
		return json.Number(strconv.Itoa(t))
	case float64:
		if math.IsNaN(t) || math.IsInf(t, 0) {
			return nil
		}

		return json.Number(strconv.FormatFloat(t, 'g', -1, 64))
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, e := range t {
			out[k] = normalizeExport(e)
		}

		return out
	case []any:
		out := make([]any, len(t))
		for i, e := range t {
			out[i] = normalizeExport(e)
		}

		return out
	default:
		return v
	}
}

// jsonValue is fromJS for places that need a plain value (undefined becomes nil).
func jsonValue(v goja.Value) any {
	if r := fromJS(v); !isUndef(r) {
		return r
	}

	return nil
}
