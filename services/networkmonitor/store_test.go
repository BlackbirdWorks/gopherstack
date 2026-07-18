package networkmonitor_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/services/networkmonitor"
)

func newTestBackend(t *testing.T) *networkmonitor.InMemoryBackend {
	t.Helper()

	return networkmonitor.NewInMemoryBackend("us-east-1", "000000000000")
}

func ptr[T any](v T) *T {
	p := new(T)
	*p = v

	return p
}

func TestRegionIsolation(t *testing.T) {
	t.Parallel()

	b := networkmonitor.NewInMemoryBackend("us-east-1", "000000000000")

	ctxEast := networkmonitor.WithRegion("us-east-1")
	ctxWest := networkmonitor.WithRegion("us-west-2")

	if _, err := b.CreateMonitor(ctxEast, "regional-mon", nil, nil, nil); err != nil {
		t.Fatalf("create in us-east-1: %v", err)
	}

	if _, err := b.GetMonitor(ctxWest, "regional-mon"); err == nil {
		t.Fatal("expected not-found in us-west-2")
	}

	if _, err := b.GetMonitor(ctxEast, "regional-mon"); err != nil {
		t.Fatalf("expected found in us-east-1: %v", err)
	}
}
