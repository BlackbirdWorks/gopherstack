package sgfilter_test

import (
	"context"
	"net"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
)

type panicOnceDialer struct {
	echoDialer
	calls atomic.Int32
}

func (d *panicOnceDialer) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	if d.calls.Add(1) == 1 {
		panic("injected dial panic")
	}

	return d.echoDialer.DialContext(ctx, network, addr)
}

func TestConnectionPanicClosesOnlyThatClient(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "dial panic"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			addr := startProxy(t, nil, &panicOnceDialer{})

			assert.False(t, roundTrip(t, addr), "panicking connection must be closed without an echo")
			assert.True(t, roundTrip(t, addr), "proxy must keep serving other clients")
		})
	}
}
