package appconfigdata_test

import (
	"context"
	"testing"
	"time"

	"github.com/blackbirdworks/gopherstack/services/appconfigdata"
)

// TestJanitor_RunExitsOnContextCancel verifies that the session-sweeping janitor
// goroutine is properly ctx-parented: cancelling the context must cause Run to
// return promptly, rather than leaking a goroutine that outlives the caller.
func TestJanitor_RunExitsOnContextCancel(t *testing.T) {
	t.Parallel()

	b := appconfigdata.NewInMemoryBackend()
	j := appconfigdata.NewJanitor(b)
	j.Interval = 5 * time.Millisecond

	ctx, cancel := context.WithCancel(t.Context())

	done := make(chan struct{})

	go func() {
		defer close(done)
		j.Run(ctx)
	}()

	cancel()

	select {
	case <-done:
		// clean exit
	case <-time.After(500 * time.Millisecond):
		t.Fatal("janitor Run did not exit after context cancellation")
	}
}

// TestJanitor_SweepsExpiredSessionsOnTick verifies that the janitor's ticker
// actually invokes SweepExpiredSessions on the configured interval, evicting
// idle-expired sessions without requiring a manual sweep call.
func TestJanitor_SweepsExpiredSessionsOnTick(t *testing.T) {
	t.Parallel()

	b := appconfigdata.NewInMemoryBackend()
	if err := b.SetConfiguration("app", "env", "p", `{}`, "application/json"); err != nil {
		t.Fatalf("SetConfiguration failed: %v", err)
	}

	token, err := b.StartSession("app", "env", "p", 0)
	if err != nil {
		t.Fatalf("StartSession failed: %v", err)
	}

	if b.LookupSession(token) == nil {
		t.Fatal("session must exist immediately after StartSession")
	}

	j := appconfigdata.NewJanitor(b)
	j.Interval = 5 * time.Millisecond
	j.SessionTTL = 0 // every session is immediately idle-expired

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	done := make(chan struct{})

	go func() {
		defer close(done)
		j.Run(ctx)
	}()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if b.LookupSession(token) == nil {
			cancel()
			<-done

			return
		}

		time.Sleep(5 * time.Millisecond)
	}

	cancel()
	<-done
	t.Fatal("janitor did not sweep the expired session within the deadline")
}
