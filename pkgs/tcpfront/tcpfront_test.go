package tcpfront_test

import (
	"bufio"
	"context"
	"net"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/tcpfront"
)

func greeter(t *testing.T, greeting string) int {
	t.Helper()

	var lc net.ListenConfig

	ln, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)

	t.Cleanup(func() { _ = ln.Close() })

	go func() {
		for {
			c, aerr := ln.Accept()
			if aerr != nil {
				return
			}

			_, _ = c.Write([]byte(greeting + "\n"))
			_ = c.Close()
		}
	}()

	return ln.Addr().(*net.TCPAddr).Port //nolint:forcetypeassert // a TCP listener
}

func dialGreeting(t *testing.T, host string, port int) string {
	t.Helper()

	var d net.Dialer

	c, err := d.DialContext(t.Context(), "tcp", net.JoinHostPort(host, strconv.Itoa(port)))
	require.NoError(t, err)

	defer c.Close()

	line, _ := bufio.NewReader(c).ReadString('\n')

	return line
}

func TestFrontFollowsItsTarget(t *testing.T) {
	t.Parallel()

	a, b := greeter(t, "a"), greeter(t, "b")

	var cur atomic.Int64

	cur.Store(int64(a))

	f, err := tcpfront.New(t.Context(), func() (string, int) { return "127.0.0.1", int(cur.Load()) })
	require.NoError(t, err)

	t.Cleanup(f.Close)

	host, port := f.Addr()
	assert.Equal(t, "a\n", dialGreeting(t, host, port))

	cur.Store(int64(b))
	assert.Equal(t, "b\n", dialGreeting(t, host, port), "the same address now reaches the new target")

	cur.Store(0)

	var d net.Dialer

	c, err := d.DialContext(context.WithoutCancel(t.Context()), "tcp", net.JoinHostPort(host, strconv.Itoa(port)))
	require.NoError(t, err)

	defer c.Close()

	_, rerr := bufio.NewReader(c).ReadString('\n')
	assert.Error(t, rerr, "a zero port refuses the connection")
}
