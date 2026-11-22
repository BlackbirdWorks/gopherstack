package athena

import (
	"strings"
	"sync"
	"sync/atomic"

	sqlitedriver "modernc.org/sqlite"
)

// funcPrefix namespaces every SQLite function this service registers: the driver registry is
// process-wide and shared with other services' engines.
const funcPrefix = "athena_"

type funcEntry struct {
	full           string
	standaloneOnly bool
}

type funcRegistry struct {
	names          map[string]funcEntry
	mu             sync.RWMutex
	standaloneOnly atomic.Bool
}

//nolint:gochecknoglobals // the driver's function registry is process-wide
var funcNames = &funcRegistry{names: map[string]funcEntry{}}

func prefixedFuncName(name string) string {
	if strings.HasPrefix(name, funcPrefix) {
		return name
	}

	return funcPrefix + name
}

func (r *funcRegistry) record(name string) string {
	full := prefixedFuncName(name)

	r.mu.Lock()
	r.names[strings.ToLower(name)] = funcEntry{full: full, standaloneOnly: r.standaloneOnly.Load()}
	r.mu.Unlock()

	return full
}

func (r *funcRegistry) lookup(lw string) (funcEntry, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	e, ok := r.names[lw]

	return e, ok
}

// standaloneOnlyFuncs runs register with every function it records hidden from embedding services.
func standaloneOnlyFuncs(register func()) {
	funcNames.standaloneOnly.Store(true)
	defer funcNames.standaloneOnly.Store(false)

	register()
}

// registeredFuncNames lists the SQLite names this service registered.
func registeredFuncNames() []string {
	registerEngineFuncs()

	funcNames.mu.RLock()
	defer funcNames.mu.RUnlock()

	out := make([]string, 0, len(funcNames.names))
	for _, e := range funcNames.names {
		out = append(out, e.full)
	}

	return out
}

// EmbeddedFuncNames lists the SQLite names an embedding service can reach through TranslateEmbedded.
func EmbeddedFuncNames() []string {
	registerEngineFuncs()

	funcNames.mu.RLock()
	defer funcNames.mu.RUnlock()

	out := make([]string, 0, len(funcNames.names))

	for _, e := range funcNames.names {
		if !e.standaloneOnly {
			out = append(out, e.full)
		}
	}

	return out
}

// athenaFunc maps a Trino function name to the registered SQLite name.
func athenaFunc(lw string) (string, bool) {
	registerEngineFuncs()

	e, ok := funcNames.lookup(lw)

	return e.full, ok
}

// embeddedFunc maps a Trino function name for an embedding service, skipping standalone-only names
// and the names the host owns.
func embeddedFunc(lw string, owned map[string]struct{}) (string, bool) {
	registerEngineFuncs()

	if _, own := owned[lw]; own {
		return "", false
	}

	e, ok := funcNames.lookup(lw)
	if !ok || e.standaloneOnly {
		return "", false
	}

	return e.full, true
}

func registerScalar(name string, nargs int32, fn scalarFn) {
	sqlitedriver.MustRegisterDeterministicScalarFunction(funcNames.record(name), nargs, fn)
}

func registerNondeterministic(name string, nargs int32, fn scalarFn) {
	sqlitedriver.MustRegisterScalarFunction(funcNames.record(name), nargs, fn)
}
