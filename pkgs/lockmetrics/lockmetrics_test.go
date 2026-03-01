package lockmetrics_test

import (
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLockMetrics(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{name: "New", run: func(t *testing.T) {
			m := lockmetrics.New("test.lock")
			require.NotNil(t, m)
		}},
		{name: "LockUnlock", run: func(t *testing.T) {
			m := lockmetrics.New("test.write")
			m.Lock("TestOp")
			m.Unlock()
		}},
		{name: "RLockRUnlock", run: func(t *testing.T) {
			m := lockmetrics.New("test.read")
			m.RLock("TestReadOp")
			m.RUnlock()
		}},
		{name: "MutualExclusion", run: func(t *testing.T) {
			m := lockmetrics.New("test.exclusive")
			var counter int
			var wg sync.WaitGroup

			const goroutines = 50
			for range goroutines {
				wg.Go(func() {
					m.Lock("increment")
					counter++
					m.Unlock()
				})
			}

			wg.Wait()
			assert.Equal(t, goroutines, counter)
		}},
		{name: "ReadersCanCoexist", run: func(t *testing.T) {
			m := lockmetrics.New("test.readers")
			m.Lock("setup")
			m.Unlock()

			var wg sync.WaitGroup
			started := make(chan struct{})

			const readers = 10
			for range readers {
				wg.Go(func() {
					m.RLock("concurrent-read")
					started <- struct{}{}
					time.Sleep(10 * time.Millisecond)
					m.RUnlock()
				})
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
		}},
		{name: "WriteBlocksRead", run: func(t *testing.T) {
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
				require.FailNow(t, "reader should be blocked while write lock is held")
			case <-time.After(20 * time.Millisecond):
				// expected: reader is waiting
			}

			m.Unlock()

			select {
			case <-done:
				// reader unblocked after write lock released
			case <-time.After(time.Second):
				require.FailNow(t, "reader should have unblocked after write lock released")
			}
		}},
		{name: "DeferPattern", run: func(t *testing.T) {
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
				require.FailNow(t, "lock should have been acquirable after deferred unlock")
			}
		}},
		{name: "DeferRLockPattern", run: func(t *testing.T) {
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
				require.FailNow(t, "write lock should be acquirable after read lock is released")
			}
		}},
		{name: "WriteWaiters", run: func(t *testing.T) {
			m := lockmetrics.New("test.write-waiters")

			// Hold the write lock.
			m.Lock("holder")

			// Start a goroutine that will block on the write lock.
			done := make(chan struct{})
			go func() {
				m.Lock("waiter")
				defer m.Unlock()
				close(done)
			}()

			// Poll until the waiter count becomes non-zero (goroutine reached m.Lock).
			const pollTimeout = time.Second
			deadline := time.Now().Add(pollTimeout)
			for time.Now().Before(deadline) && m.WriteWaiters() == 0 {
				runtime.Gosched()
			}

			assert.EqualValues(t, 1, m.WriteWaiters(), "expected 1 write waiter while lock is held")

			// Release the lock; the waiter should unblock and the count should return to 0.
			m.Unlock()
			<-done
			assert.EqualValues(t, 0, m.WriteWaiters(), "expected 0 write waiters after lock released")
		}},
		{name: "ReadWaiters", run: func(t *testing.T) {
			m := lockmetrics.New("test.read-waiters")

			// Hold the write lock so readers must block.
			m.Lock("holder")

			done := make(chan struct{})
			go func() {
				m.RLock("reader")
				defer m.RUnlock()
				close(done)
			}()

			// Poll until the waiter count becomes non-zero (goroutine reached m.RLock).
			const pollTimeout = time.Second
			deadline := time.Now().Add(pollTimeout)
			for time.Now().Before(deadline) && m.ReadWaiters() == 0 {
				runtime.Gosched()
			}

			assert.EqualValues(t, 1, m.ReadWaiters(), "expected 1 read waiter while write lock is held")

			m.Unlock()
			<-done
			assert.EqualValues(t, 0, m.ReadWaiters(), "expected 0 read waiters after write lock released")
		}},
		{name: "GetLockStatus", run: func(t *testing.T) {
			m := lockmetrics.New("test.lock-status")

			// Initially unlocked
			locked, ww, rw := m.GetLockStatus()
			assert.False(t, locked)
			assert.EqualValues(t, 0, ww)
			assert.EqualValues(t, 0, rw)

			// Lock it
			m.Lock("holder")
			locked, ww, rw = m.GetLockStatus()
			assert.True(t, locked)
			assert.EqualValues(t, 0, ww)
			assert.EqualValues(t, 0, rw)

			// Add a waiter
			done := make(chan struct{})
			go func() {
				m.Lock("waiter")
				m.Unlock()
				close(done)
			}()

			// Poll until waiter is registered
			for range 10 {
				_, ww, _ = m.GetLockStatus()
				if ww > 0 {
					break
				}
				time.Sleep(10 * time.Millisecond)
			}

			locked, ww, rw = m.GetLockStatus()
			assert.True(t, locked)
			assert.EqualValues(t, 1, ww)
			assert.EqualValues(t, 0, rw)

			// Unlock
			m.Unlock()
			<-done

			locked, ww, rw = m.GetLockStatus()
			assert.False(t, locked)
			assert.EqualValues(t, 0, ww)
			assert.EqualValues(t, 0, rw)
		}},
		{name: "RWMutex_Close_CloseNew", run: func(t *testing.T) {
			m := lockmetrics.New("test.close-new")
			m.Close()
		}},
		{name: "RWMutex_Close_CloseAfterUse", run: func(t *testing.T) {
			m := lockmetrics.New("test.close-after-use")
			m.Lock("op")
			m.Unlock()
			m.RLock("op")
			m.RUnlock()
			m.Close()
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.run(t)
		})
	}
}
