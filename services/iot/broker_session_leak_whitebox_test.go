package iot

import (
	"net"
	"testing"

	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/stretchr/testify/assert"
)

func newTestMQTTClient(t *testing.T, id string) *mqtt.Client {
	t.Helper()

	srv, peer := net.Pipe()
	t.Cleanup(func() { _ = srv.Close(); _ = peer.Close() })

	cl := mqtt.New(nil).NewClient(srv, "t", id, false)

	return cl
}

func countSessions(b *Broker) int {
	n := 0

	b.sessions.Range(func(_, _ any) bool { n++; return true })

	return n
}

func TestBroker_SessionsReclaimedWhenConnackFails(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		established bool
		want        int
	}{
		{name: "connack_failed_is_swept", established: false, want: 1},
		{name: "established_kept_for_disconnect_event", established: true, want: 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := &Broker{}
			failed := newTestMQTTClient(t, "dev-failed")

			b.publishConnected(failed)

			if tt.established {
				b.notifyConnected(failed)
			}

			failed.Stop(nil)

			b.publishConnected(newTestMQTTClient(t, "dev-next"))

			assert.Equal(t, tt.want, countSessions(b))
		})
	}
}

func TestAuthHook_CustomSessionsSweptForClosedClients(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		closed      bool
		wantKept    bool
		wantStopped bool
	}{
		{name: "closed_client_dropped_and_timer_stopped", closed: true, wantStopped: true},
		{name: "live_client_kept", wantKept: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := &authHook{broker: &Broker{}}
			cl := newTestMQTTClient(t, "dev-custom")
			stopped := false

			h.sessions.Store(cl, &customSession{stop: func() bool { stopped = true; return true }})

			if tt.closed {
				cl.Stop(nil)
			}

			h.sweepClosed()

			_, kept := h.sessions.Load(cl)
			assert.Equal(t, tt.wantKept, kept)
			assert.Equal(t, tt.wantStopped, stopped)
		})
	}
}
