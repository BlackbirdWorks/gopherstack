package amp_test

import (
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/amp"
)

func TestNotificationLogSurvivesRestart(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		early bool
		wait  time.Duration
		want  int
	}{
		{name: "within repeat", wait: time.Hour, want: 0},
		{name: "after repeat", wait: 5 * time.Hour, want: 1},
		{name: "no log renotifies", early: true, wait: time.Hour, want: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			src := newFixture(t)
			pub := &recordedSNS{}
			src.backend.SetSNSPublisher(pub)

			ws := src.workspace(t)
			src.putAlertManager(t, ws, silenceConfig)
			src.backend.Tick(t.Context())

			early := src.backend.Snapshot(t.Context())

			labels := map[string]string{"alertname": "Down"}
			far := src.clock.now().Add(24 * 7 * time.Hour)
			src.postAlerts(t, ws, []map[string]any{alertBody(labels, far)})
			src.clock.advance(15 * time.Second)
			src.backend.Tick(t.Context())
			require.Len(t, pub.take(), 1)

			snap := src.backend.Snapshot(t.Context())
			if tt.early {
				snap = early
			}

			dst := newFixtureFromBackend(t, amp.NewInMemoryBackend(testAccount, "us-east-1"),
				&fakeClock{t: src.clock.now()})
			require.NoError(t, dst.backend.Restore(t.Context(), snap))

			dpub := &recordedSNS{}
			dst.backend.SetSNSPublisher(dpub)
			dst.clock.advance(tt.wait)
			dst.backend.Tick(t.Context())
			dst.postAlerts(t, ws, []map[string]any{alertBody(labels, dst.clock.now().Add(24*7*time.Hour))})
			dst.clock.advance(15 * time.Second)
			dst.backend.Tick(t.Context())
			dst.clock.advance(time.Minute)
			dst.backend.Tick(t.Context())

			assert.Len(t, dpub.take(), tt.want)
		})
	}
}

func TestNotificationLogSweptOnBackendClock(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		wait time.Duration
		kept bool
	}{
		{name: "fresh kept", wait: time.Hour, kept: true},
		{name: "stale swept", wait: 130 * time.Hour, kept: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			src := newFixture(t)
			src.backend.SetSNSPublisher(&recordedSNS{})

			ws := src.workspace(t)
			src.putAlertManager(t, ws, silenceConfig)
			src.backend.Tick(t.Context())
			src.postAlerts(t, ws, []map[string]any{
				alertBody(map[string]string{"alertname": "Down"}, src.clock.now().Add(300*time.Hour)),
			})
			src.clock.advance(15 * time.Second)
			src.backend.Tick(t.Context())

			dst := newFixtureFromBackend(t, amp.NewInMemoryBackend(testAccount, "us-east-1"),
				&fakeClock{t: src.clock.now()})
			require.NoError(t, dst.backend.Restore(t.Context(), src.backend.Snapshot(t.Context())))
			dst.clock.advance(tt.wait)
			dst.backend.Tick(t.Context())

			assert.Equal(t, tt.kept, strings.Contains(string(dst.backend.Snapshot(t.Context())), `"alertLog"`))
		})
	}
}

func TestNotificationLogConcurrentSnapshot(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	f.backend.SetSNSPublisher(&recordedSNS{})

	ws := f.workspace(t)
	f.putAlertManager(t, ws, silenceConfig)
	f.backend.Tick(t.Context())

	var wg sync.WaitGroup

	for i := range 4 {
		wg.Go(func() {
			for range 20 {
				if i%2 == 0 {
					f.backend.Tick(t.Context())
				} else {
					assert.NotEmpty(t, f.backend.Snapshot(t.Context()))
				}
			}
		})
	}

	wg.Wait()
}
