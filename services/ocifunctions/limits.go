package ocifunctions

import (
	"net/http"
	"strconv"
)

type limits struct {
	apps          int
	funcs         int
	provisionedMB int64
}

func defaultLimits() limits {
	return limits{
		apps: DefaultMaxApplications, funcs: DefaultMaxFunctions, provisionedMB: DefaultProvisionedMB,
	}
}

func errLimit(name string, limit int64) *APIError {
	return newErr(http.StatusBadRequest, "LimitExceeded",
		"The service limit "+name+" ("+strconv.FormatInt(limit, 10)+") has been reached")
}

// liveAppCount and liveFuncCount must be called with the lock held.
func (b *InMemoryBackend) liveAppCount() int {
	n := 0

	for _, a := range b.apps {
		if a.State != stateDeleted {
			n++
		}
	}

	return n
}

func (b *InMemoryBackend) liveFuncCount() int {
	n := 0

	for _, f := range b.funcs {
		if f.State != stateDeleted {
			n++
		}
	}

	return n
}

func provisionedMB(f *Function) int64 {
	if f.Provisioned.Strategy != concConstant || f.Provisioned.Count == nil {
		return 0
	}

	return int64(*f.Provisioned.Count) * f.Memory
}

// checkProvisioned must be called with the lock held; except is the function being changed.
func (b *InMemoryBackend) checkProvisioned(except string, want int64) *APIError {
	total := want

	for id, f := range b.funcs {
		if id != except && f.State != stateDeleted {
			total += provisionedMB(f)
		}
	}

	if b.limits.provisionedMB > 0 && total > b.limits.provisionedMB {
		return errLimit("provisioned-concurrency-mb", b.limits.provisionedMB)
	}

	return nil
}
