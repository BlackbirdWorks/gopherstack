package lockmetrics_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

// BenchmarkRWMutex_LockUnlock_SameOp measures the common case of a backend
// repeatedly locking the same operation name.
func BenchmarkRWMutex_LockUnlock_SameOp(b *testing.B) {
	m := lockmetrics.New("bench.lock." + b.Name())

	b.ReportAllocs()

	for b.Loop() {
		m.Lock("PutItem")
		m.Unlock()
	}
}

// BenchmarkRWMutex_RLockRUnlock_SameOp is RLock/RUnlock's analogue.
func BenchmarkRWMutex_RLockRUnlock_SameOp(b *testing.B) {
	m := lockmetrics.New("bench.rlock." + b.Name())

	b.ReportAllocs()

	for b.Loop() {
		m.RLock("GetItem")
		m.RUnlock()
	}
}
