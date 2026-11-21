package azurefile

import (
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type stubConn struct {
	net.Conn

	data []byte
}

func (s *stubConn) Read(p []byte) (int, error) { return copy(p, s.data), nil }

func TestTrackedConnReadSurvivesTrackerPanic(t *testing.T) {
	t.Parallel()

	var nilMap map[string]int

	tests := []struct {
		probe func()
		name  string
	}{
		{probe: func() { panic("boom") }, name: "string"},
		{probe: func() { nilMap["x"] = 1 }, name: "runtime"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			head := req("PUT", "/s/f", []string{"x-ms-meta-Foo: v"}, "")
			tr := newHeadTracker()
			tr.probe = tt.probe
			c := &trackedConn{Conn: &stubConn{data: []byte(head)}, t: tr}
			buf := make([]byte, 256)

			var (
				n   int
				err error
			)

			require.NotPanics(t, func() { n, err = c.Read(buf) })
			require.NoError(t, err)
			assert.Equal(t, head, string(buf[:n]))
			assert.Equal(t, phaseBroken, tr.phase)
			assert.Equal(t, 0, queued(tr))
		})
	}
}
