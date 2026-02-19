package lockmetrics_test

import (
	"sync"
	"testing"
	"time"

	"Gopherstack/pkgs/lockmetrics"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNew(t *testing.T) {
	t.Parallel()
	m := lockmetrics.New("test.lock")
	require.NotNil(t, m)
}

func TestLockUnlock(t *testing.T) {
	t.Parallel()

	m := lockmetrics.New("test.write")
	m.Lock("TestOp")
	m.Unlock()
}

func TestRLockRUnlock(t *testing.T) {
	t.Parallel()

	m := lockmetrics.New("test.read")
	m.RLock("TestReadOp")
	m.RUnlock()
}

func TestMutualExclusion(t *testing.T) {
	t.Parallel()

	m := lockmetrics.New("test.exclusive")
	var counter int
	var wg sync.WaitGroup

	const goroutines = 50
	for range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			m.Lock("increment")
			counter++
			m.Unlock()
		}()
	}

	wg.Wait()
	assert.Equal(t, goroutines, counter)
}

func TestReadersCanCoexist(t *testing.T) {
	t.Parallel()

	m := lockmetrics.New("test.readers")
	m.Lock("setup")
	m.Unlock()

	var wg sync.WaitGroup
	started := make(chan struct{})

	const readers = 10
	for range readers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			m.RLock("concurrent-read")
			started <- struct{}{}
			time.Sleep(10 * time.Millisecond)
			m.RUnlock()
		}()
	}

	// Collect all started signals to confirm all readers ran concurrently.
	go func() {
		wg.Wait()
		close(started)
	}()

	count := 0
	for range started {
		count++
	}
	assert.Equal(t, readers, count)
}

func TestWriteBlocksRead(t *testing.T) {
	t.Parallel()

	m := lockmetrics.New("test.write-blocks-read")
	m.Lock("holding")

	done := make(chan struct{})
	go func() {
		m.RLock("waiting-reader")
		m.RUnlock()
		close(done)
	}()

	// The reader goroutine should still be blocked.
	select {
	case <-done:
		t.Fatal("reader should be blocked while write lock is held")
	case <-time.After(20 * time.Millisecond):
		// expected: reader is waiting
	}

	m.Unlock()

	select {
	case <-done:
		// reader unblocked after write lock released
	case <-time.After(time.Second):
		t.Fatal("reader should have unblocked after write lock released")
	}
}

func TestDeferPattern(t *testing.T) {
	t.Parallel()

	m := lockmetrics.New("test.defer")

	func() {
		m.Lock("deferOp")
		defer m.Unlock()
	}()

	// Lock should be released after the above function returns.
	acquired := make(chan struct{})
	go func() {
		m.Lock("afterDefer")
		defer m.Unlock()
		close(acquired)
	}()

	select {
	case <-acquired:
	case <-time.After(time.Second):
		t.Fatal("lock should have been acquirable after deferred unlock")
	}
}

func TestDeferRLockPattern(t *testing.T) {
	t.Parallel()

	m := lockmetrics.New("test.defer-rlock")

	func() {
		m.RLock("readOp")
		defer m.RUnlock()
	}()

	// Write lock should be acquirable after the read lock is released.
	acquired := make(chan struct{})
	go func() {
		m.Lock("afterRUnlock")
		defer m.Unlock()
		close(acquired)
	}()

	select {
	case <-acquired:
	case <-time.After(time.Second):
		t.Fatal("write lock should be acquirable after read lock is released")
	}
}
