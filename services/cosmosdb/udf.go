package cosmosdb

import (
	"fmt"

	"github.com/dop251/goja"
)

// udfRuntime runs a container's user-defined functions for one query. The VM is created on first use and every
// function is compiled once per query.
type udfRuntime struct {
	b       *InMemoryBackend
	scripts map[string]*storedScript
	vm      *goja.Runtime
	fns     map[string]goja.Callable
	stop    func()
	err     error
}

func (b *InMemoryBackend) newUDFRuntime(scripts map[string]*storedScript) *udfRuntime {
	return &udfRuntime{b: b, scripts: scripts, fns: make(map[string]goja.Callable)}
}

func (u *udfRuntime) close() {
	if u.stop != nil {
		u.stop()
	}
}

// call evaluates udf.name(args); a failure is recorded in u.err and surfaces when the query ends.
func (u *udfRuntime) call(name string, args []any) any {
	if u.err != nil {
		return undef{}
	}

	fn, err := u.function(name)
	if err != nil {
		u.err = err

		return undef{}
	}

	jsArgs := make([]goja.Value, len(args))
	for i, a := range args {
		jsArgs[i] = toJS(u.vm, a)
	}

	res, callErr := fn(goja.Undefined(), jsArgs...)
	if callErr != nil {
		u.err = scriptError(callErr)

		return undef{}
	}

	return fromJS(res)
}

func (u *udfRuntime) function(name string) (goja.Callable, error) {
	if fn, ok := u.fns[name]; ok {
		return fn, nil
	}

	s, ok := u.scripts[name]
	if !ok || u.b == nil {
		return nil, fmt.Errorf("%w: user-defined function %q does not exist", ErrQueryParse, "udf."+name)
	}

	if u.vm == nil {
		u.vm, u.stop = u.b.newSandbox()
	}

	fn, err := loadFunction(u.vm, s.Body)
	if err != nil {
		return nil, err
	}

	u.fns[name] = fn

	return fn, nil
}
