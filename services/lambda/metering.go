package lambda

import "time"

const architectureX86 = "x86_64"

// InvocationUsage describes one executed invocation attempt, for usage metering.
type InvocationUsage struct {
	Tags         map[string]string
	FunctionName string
	FunctionARN  string
	Region       string
	Account      string
	Architecture string
	Duration     time.Duration
	MemoryMB     int
}

// InvocationObserver receives one InvocationUsage per invocation attempt.
type InvocationObserver func(InvocationUsage)

type invocationObserverHolder struct{ fn InvocationObserver }

// SetInvocationObserver installs fn on the home backend and every region sibling built so far;
// siblings built later inherit it. nil removes it.
func (h *Handler) SetInvocationObserver(fn InvocationObserver) {
	for _, bk := range h.RegionBackends() {
		if mem, ok := bk.(*InMemoryBackend); ok {
			mem.SetInvocationObserver(fn)
		}
	}
}

// SetInvocationObserver installs fn on this backend; nil removes it.
func (b *InMemoryBackend) SetInvocationObserver(fn InvocationObserver) {
	if fn == nil {
		b.observer.Store(nil)

		return
	}

	b.observer.Store(&invocationObserverHolder{fn: fn})
}

func (b *InMemoryBackend) observeInvocation(functionName string, elapsed time.Duration) {
	holder := b.observer.Load()
	if holder == nil {
		return
	}

	fn, err := b.GetFunction(functionName)
	if err != nil {
		return
	}

	arch := architectureX86
	if len(fn.Architectures) > 0 {
		arch = fn.Architectures[0]
	}

	holder.fn(InvocationUsage{
		FunctionName: fn.FunctionName, FunctionARN: fn.FunctionArn, Region: b.region, Account: b.accountID,
		Architecture: arch, Duration: elapsed, MemoryMB: fn.MemorySize, Tags: fn.Tags,
	})
}
