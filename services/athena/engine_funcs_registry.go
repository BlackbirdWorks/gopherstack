package athena

import (
	"strings"
	"sync"

	sqlitedriver "modernc.org/sqlite"
)

// funcPrefix namespaces every SQLite function this service registers: the driver registry is
// process-wide and shared with other services' engines.
const funcPrefix = "athena_"

type funcRegistry struct {
	names map[string]string
	mu    sync.RWMutex
}

//nolint:gochecknoglobals // the driver's function registry is process-wide
var funcNames = &funcRegistry{names: map[string]string{}}

func prefixedFuncName(name string) string {
	if strings.HasPrefix(name, funcPrefix) {
		return name
	}

	return funcPrefix + name
}

func (r *funcRegistry) record(name string) string {
	full := prefixedFuncName(name)

	r.mu.Lock()
	r.names[strings.ToLower(name)] = full
	r.mu.Unlock()

	return full
}

func (r *funcRegistry) lookup(lw string) (string, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	full, ok := r.names[lw]

	return full, ok
}

// registeredFuncNames lists the SQLite names this service registered.
func registeredFuncNames() []string {
	registerEngineFuncs()

	funcNames.mu.RLock()
	defer funcNames.mu.RUnlock()

	out := make([]string, 0, len(funcNames.names))
	for _, full := range funcNames.names {
		out = append(out, full)
	}

	return out
}

// athenaFunc maps a Trino function name to the registered SQLite name.
func athenaFunc(lw string) (string, bool) {
	registerEngineFuncs()

	return funcNames.lookup(lw)
}

func registerScalar(name string, nargs int32, fn scalarFn) {
	sqlitedriver.MustRegisterDeterministicScalarFunction(funcNames.record(name), nargs, fn)
}

func registerNondeterministic(name string, nargs int32, fn scalarFn) {
	sqlitedriver.MustRegisterScalarFunction(funcNames.record(name), nargs, fn)
}

// isEmbedderOwned names the Trino functions an embedding service registers under their plain names.
func isEmbedderOwned(lw string) bool { return lw == "to_milliseconds" || lw == "parse_duration" }
