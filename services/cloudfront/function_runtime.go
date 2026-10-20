package cloudfront

import (
	"crypto/hmac"
	"crypto/md5"  //nolint:gosec // exposed to functions because the real crypto module offers md5
	"crypto/sha1" //nolint:gosec // exposed to functions because the real crypto module offers sha1
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"strconv"
	"strings"
	"time"

	"github.com/dop251/goja"

	"github.com/blackbirdworks/gopherstack/pkgs/jsruntime"
)

const (
	runtimeJS1 = "cloudfront-js-1.0"
	runtimeJS2 = "cloudfront-js-2.0"

	fnExecTimeout = 250 * time.Millisecond
	fnMaxStack    = 200
	// fnComputeBudget is the real per-invocation limit that ComputeUtilization is a percentage of.
	fnComputeBudget = time.Millisecond
	fnMaxUtil       = 100
	fnErrPrefix     = "The CloudFront function associated with the CloudFront distribution is invalid or could not " +
		"run. Error: "
)

// fnError is a message-only failure of a running function.
type fnError string

func (e fnError) Error() string { return string(e) }

const (
	errFnNoHandler  fnError = "handler is not defined"
	errFnIncomplete fnError = "the function did not complete"
)

// errFnInvalidOutput means the handler returned neither a request nor a response object.
var errFnInvalidOutput = errors.New("the function returned an invalid value; expected a request or response object")

// kvsReader resolves Key Value Store reads for a running function.
type kvsReader interface {
	// KVSIDs lists the stores associated with the function.
	KVSIDs() []string
	// Value returns the value of key in store id.
	Value(id, key string) (string, bool)
}

// fnResult is one function execution outcome.
type fnResult struct {
	Output      string
	ErrMsg      string
	Utilization string
	Logs        []string
}

const modQuerystring = "querystring"

func fnModules() map[string]string {
	return map[string]string{"cloudfront": "__mod_cf", "crypto": "__mod_crypto", modQuerystring: "__mod_qs"}
}

// runCloudFrontFunction executes handler(event) from code under the given runtime.
func runCloudFrontFunction(runtime, code string, event any, kvs kvsReader) fnResult {
	sb := jsruntime.New(jsruntime.Options{
		Timeout: fnExecTimeout, MaxCallStack: fnMaxStack, Console: true, InterruptMsg: "function timed out",
	})
	defer sb.Close()

	out, err := execCloudFrontFunction(sb, runtime, code, event, kvs)

	res := fnResult{Output: out, Logs: sb.Logs(), Utilization: computeUtilization(sb.Elapsed())}
	if err != nil {
		res.Output = ""
		res.ErrMsg = fnErrPrefix + err.Error()
	}

	return res
}

func computeUtilization(d time.Duration) string {
	pct := min(max(int(d*fnMaxUtil/fnComputeBudget), 1), fnMaxUtil)

	return strconv.Itoa(pct)
}

func execCloudFrontFunction(sb *jsruntime.Sandbox, runtime, code string, event any, kvs kvsReader) (string, error) {
	src, err := jsruntime.RewriteModule(code, fnModules())
	if err != nil {
		return "", err
	}

	bindFnGlobals(sb, runtime, kvs)

	if _, err = sb.Run(fnPrelude); err != nil {
		return "", fmt.Errorf("prelude: %w", err)
	}

	if _, err = sb.Run(src); err != nil {
		return "", fnJSError(err)
	}

	ev, err := sb.ToJS(event)
	if err != nil {
		return "", err
	}

	res, err := sb.Call("handler", ev)
	if err != nil {
		return "", fnJSError(err)
	}

	res, err = settlePromise(res)
	if err != nil {
		return "", err
	}

	return fnOutput(sb, res)
}

func fnJSError(err error) error {
	if errors.Is(err, jsruntime.ErrNoFunction) {
		return errFnNoHandler
	}

	_, msg := jsruntime.Classify(err)

	return fnError(msg)
}

// settlePromise unwraps the result of an async handler.
func settlePromise(v goja.Value) (goja.Value, error) {
	p, ok := v.Export().(*goja.Promise)
	if !ok {
		return v, nil
	}

	switch p.State() {
	case goja.PromiseStateFulfilled:
		return p.Result(), nil
	case goja.PromiseStateRejected:
		return nil, fnError(p.Result().String())
	case goja.PromiseStatePending:
	}

	return nil, errFnIncomplete
}

func fnOutput(sb *jsruntime.Sandbox, res goja.Value) (string, error) {
	if res == nil || goja.IsUndefined(res) || goja.IsNull(res) {
		return "", errFnInvalidOutput
	}

	if _, isObj := res.(*goja.Object); !isObj {
		return "", errFnInvalidOutput
	}

	s, ok, err := sb.Stringify(res)
	if err != nil || !ok {
		return "", errFnInvalidOutput
	}

	return s, nil
}

func bindFnGlobals(sb *jsruntime.Sandbox, runtime string, kvs kvsReader) {
	sb.Set("__hash", func(alg, data, enc string) (string, error) { return digestOf(alg, "", data, enc, false) })
	sb.Set("__hmac", func(alg, key, data, enc string) (string, error) { return digestOf(alg, key, data, enc, true) })

	allowed := []string{"crypto", modQuerystring}
	if runtime == runtimeJS2 {
		allowed = append(allowed, "cloudfront")
		sb.Set("__kvs", kvsBinding(sb, kvs))
	}

	sb.Set("__allowed", allowed)
}

func digestOf(alg, key, data, enc string, mac bool) (string, error) {
	var newHash func() hash.Hash

	switch strings.ToLower(alg) {
	case "md5":
		newHash = md5.New
	case "sha1":
		newHash = sha1.New
	case "sha256":
		newHash = sha256.New
	default:
		return "", fnError(fmt.Sprintf("unsupported hash algorithm %q", alg))
	}

	h := newHash()
	if mac {
		h = hmac.New(newHash, []byte(key))
	}

	h.Write([]byte(data))

	sum := h.Sum(nil)

	switch enc {
	case "base64":
		return base64.StdEncoding.EncodeToString(sum), nil
	case "hex", "":
		return hex.EncodeToString(sum), nil
	}

	return "", fnError(fmt.Sprintf("unsupported digest encoding %q", enc))
}

// kvsBinding returns the native backing of the cloudfront module's kvs().
func kvsBinding(sb *jsruntime.Sandbox, kvs kvsReader) func(goja.FunctionCall) goja.Value {
	vm := sb.VM

	return func(call goja.FunctionCall) goja.Value {
		id, err := pickKVS(kvs, call.Argument(0))
		if err != nil {
			panic(vm.NewGoError(err))
		}

		h := vm.NewObject()
		_ = h.Set("get", func(c goja.FunctionCall) goja.Value { return kvsGet(sb, kvs, id, c) })
		_ = h.Set("exists", func(c goja.FunctionCall) goja.Value {
			_, ok := kvs.Value(id, c.Argument(0).String())

			return promiseOf(vm, vm.ToValue(ok), nil)
		})

		return h
	}
}

func pickKVS(kvs kvsReader, arg goja.Value) (string, error) {
	ids := kvs.KVSIDs()
	if arg == nil || goja.IsUndefined(arg) {
		if len(ids) != 1 {
			return "", fnError("a Key Value Store id is required when the function has no single association")
		}

		return ids[0], nil
	}

	want := arg.String()
	for _, id := range ids {
		if id == want {
			return id, nil
		}
	}

	return "", fnError(fmt.Sprintf("Key Value Store %q is not associated with this function", want))
}

func kvsGet(sb *jsruntime.Sandbox, kvs kvsReader, id string, c goja.FunctionCall) goja.Value {
	vm := sb.VM
	key := c.Argument(0).String()

	val, ok := kvs.Value(id, key)
	if !ok {
		return promiseOf(vm, nil, vm.NewGoError(fnError(fmt.Sprintf("KeyNotFound: The key %q does not exist", key))))
	}

	if wantsJSON(c.Argument(1)) {
		parsed, err := sb.ToJSString(val)
		if err != nil {
			return promiseOf(vm, nil, vm.NewGoError(err))
		}

		return promiseOf(vm, parsed, nil)
	}

	return promiseOf(vm, vm.ToValue(val), nil)
}

func promiseOf(vm *goja.Runtime, v goja.Value, rejection goja.Value) goja.Value {
	p, resolve, reject := vm.NewPromise()

	if rejection != nil {
		_ = reject(rejection)
	} else {
		_ = resolve(v)
	}

	return vm.ToValue(p)
}

func wantsJSON(opts goja.Value) bool {
	o, ok := opts.(*goja.Object)
	if !ok {
		return false
	}

	f := o.Get("format")

	return f != nil && f.String() == "json"
}
