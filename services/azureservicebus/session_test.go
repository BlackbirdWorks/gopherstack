package azureservicebus_test

import (
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/azureservicebus"
)

func TestSessions_Backend(t *testing.T) {
	t.Parallel()

	ref := azureservicebus.EntityRef{Queue: "sq"}

	tests := []struct {
		run  func(t *testing.T, b *azureservicebus.InMemoryBackend, clk *fakeClock)
		name string
	}{
		{name: "send requires session id", run: func(t *testing.T, b *azureservicebus.InMemoryBackend, _ *fakeClock) {
			t.Helper()

			_, err := b.Send(ref, azureservicebus.NewMessage{Body: []byte("x")})
			require.ErrorIs(t, err, azureservicebus.ErrSessionIDRequired)

			_, err = b.Send(ref, azureservicebus.NewMessage{Body: []byte("x"), SessionID: "s"})
			require.NoError(t, err)
		}},
		{name: "plain receive refused", run: func(t *testing.T, b *azureservicebus.InMemoryBackend, _ *fakeClock) {
			t.Helper()

			_, err := b.PeekLock(ref, false, 0)
			require.ErrorIs(t, err, azureservicebus.ErrSessionRequired)

			_, err = b.ReceiveAndDelete(ref, false)
			require.ErrorIs(t, err, azureservicebus.ErrSessionRequired)
		}},
		{name: "lock check follows clock", run: func(t *testing.T, b *azureservicebus.InMemoryBackend, clk *fakeClock) {
			t.Helper()

			_, err := b.AcceptSessionWait(t.Context(), ref, "s1", "owner", 0)
			require.NoError(t, err)
			require.NoError(t, b.CheckSessionLock(ref, "s1", "owner"))
			require.ErrorIs(t, b.CheckSessionLock(ref, "s1", "other"), azureservicebus.ErrSessionLockLost)

			clk.advance(31 * time.Second)

			require.ErrorIs(t, b.CheckSessionLock(ref, "s1", "owner"), azureservicebus.ErrSessionLockLost)
			_, err = b.RenewSessionLock(ref, "s1", "owner")
			require.ErrorIs(t, err, azureservicebus.ErrSessionLockLost)
		}},
		{name: "state and lock survive snapshot", run: func(
			t *testing.T, b *azureservicebus.InMemoryBackend, _ *fakeClock,
		) {
			t.Helper()

			_, err := b.AcceptSessionWait(t.Context(), ref, "s1", "owner", 0)
			require.NoError(t, err)
			require.NoError(t, b.SetSessionState(ref, "s1", "owner", []byte("st")))

			b2 := azureservicebus.NewInMemoryBackend()
			require.NoError(t, b2.Restore(t.Context(), b.Snapshot(t.Context())))

			got, err := b2.GetSessionState(ref, "s1", "owner")
			require.NoError(t, err)
			assert.Equal(t, []byte("st"), got)

			_, err = b2.AcceptSessionWait(t.Context(), ref, "s1", "intruder", 0)
			require.ErrorIs(t, err, azureservicebus.ErrSessionCannotBeLocked)
		}},
		{name: "janitor clears expired lock", run: func(
			t *testing.T, b *azureservicebus.InMemoryBackend, clk *fakeClock,
		) {
			t.Helper()

			_, err := b.AcceptSessionWait(t.Context(), ref, "s1", "owner", 0)
			require.NoError(t, err)

			clk.advance(time.Minute)
			azureservicebus.SweepOnce(b, clk.now())

			_, ok := b.SessionOf(ref, "owner")
			assert.False(t, ok)
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := azureservicebus.NewInMemoryBackend()
			clk := &fakeClock{}
			azureservicebus.SetNowFunc(b, clk.now)
			_, err := b.CreateQueue("sq", sessionCfg())
			require.NoError(t, err)

			tt.run(t, b, clk)
		})
	}
}

func TestSessions_REST(t *testing.T) {
	t.Parallel()

	create := []byte(`<entry><content><QueueDescription><RequiresSession>true</RequiresSession>` +
		`</QueueDescription></content></entry>`)

	tests := []struct {
		hdr    map[string]string
		name   string
		method string
		path   string
		want   int
	}{
		{name: "send without session id", method: http.MethodPost, path: "/sq/messages", want: http.StatusBadRequest},
		{
			name: "send with session id", method: http.MethodPost, path: "/sq/messages", want: http.StatusCreated,
			hdr: map[string]string{"BrokerProperties": `{"SessionId":"s1"}`},
		},
		{name: "peek lock refused", method: http.MethodPost, path: "/sq/messages/head", want: http.StatusBadRequest},
		{
			name: "destructive read refused", method: http.MethodDelete, path: "/sq/messages/head",
			want: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			require.Equal(t, http.StatusCreated, doRequest(t, h, http.MethodPut, "/sq", create, nil).Code)

			info, err := h.Backend.GetQueueInfo("sq")
			require.NoError(t, err)
			require.True(t, info.RequiresSession)

			assert.Equal(t, tt.want, doRequest(t, h, tt.method, tt.path, []byte("x"), tt.hdr).Code)
		})
	}
}
