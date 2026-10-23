// Package quotalimit lets a service consult Service Quotas applied values without importing it.
package quotalimit

import "sync/atomic"

// Func returns the applied limit of a quota for an account and Region; ok is false when none is known.
type Func func(account, region, serviceCode, quotaCode string) (limit float64, ok bool)

// Limit is a resolved quota limit; the zero value enforces nothing.
type Limit struct {
	Value float64
	Set   bool
}

// Reached reports whether creating one more resource beyond current would exceed the limit.
func (l Limit) Reached(current int) bool { return l.Set && float64(current) >= l.Value }

// Gate holds the optional lookup; the zero value is ready to use and enforces nothing.
type Gate struct{ fn atomic.Pointer[Func] }

// Set installs the lookup; nil disables enforcement.
func (g *Gate) Set(f Func) {
	if f == nil {
		g.fn.Store(nil)

		return
	}

	g.fn.Store(&f)
}

// Lookup resolves the limit. Call it before taking a service lock: the lookup takes Service Quotas' own lock.
func (g *Gate) Lookup(account, region, serviceCode, quotaCode string) Limit {
	if g == nil {
		return Limit{}
	}

	f := g.fn.Load()
	if f == nil {
		return Limit{}
	}

	v, ok := (*f)(account, region, serviceCode, quotaCode)

	return Limit{Value: v, Set: ok}
}
