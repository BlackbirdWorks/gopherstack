package neptune_test

import (
	"bufio"
	"context"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/neptune"
)

const wsGUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

type fakeGremlin struct {
	requests chan string
	addr     string
	status   int
	rejectWS bool
}

func startFakeGremlin(t *testing.T, status int, rejectWS bool) *fakeGremlin {
	t.Helper()

	l, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = l.Close() })

	f := &fakeGremlin{status: status, rejectWS: rejectWS, addr: l.Addr().String(), requests: make(chan string, 4)}

	go func() {
		for {
			conn, aerr := l.Accept()
			if aerr != nil {
				return
			}

			go f.serve(conn)
		}
	}()

	return f
}

func (f *fakeGremlin) serve(conn net.Conn) {
	defer func() { _ = conn.Close() }()

	br := bufio.NewReader(conn)

	req, err := http.ReadRequest(br)
	if err != nil {
		return
	}

	if f.rejectWS {
		_, _ = io.WriteString(conn, "HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\n\r\n")

		return
	}

	sum := sha1.Sum([]byte(req.Header.Get("Sec-WebSocket-Key") + wsGUID))
	_, _ = io.WriteString(conn, "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\n"+
		"Connection: Upgrade\r\nSec-WebSocket-Accept: "+base64.StdEncoding.EncodeToString(sum[:])+"\r\n\r\n")

	f.requests <- readClientMessage(br)

	body := fmt.Sprintf(`{"requestId":"x","status":{"message":"m","code":%d,"attributes":{}},"result":{}}`, f.status)
	_, _ = conn.Write(append([]byte{0x81, byte(len(body))}, body...))
}

func readClientMessage(br *bufio.Reader) string {
	var hdr [2]byte
	if _, err := io.ReadFull(br, hdr[:]); err != nil {
		return ""
	}

	size := int(hdr[1] & 0x7F)

	switch size {
	case 126:
		var ext [2]byte
		_, _ = io.ReadFull(br, ext[:])
		size = int(binary.BigEndian.Uint16(ext[:]))
	case 127:
		var ext [8]byte
		_, _ = io.ReadFull(br, ext[:])
		size = int(binary.BigEndian.Uint64(ext[:]))
	}

	var mask [4]byte
	_, _ = io.ReadFull(br, mask[:])

	payload := make([]byte, size)
	_, _ = io.ReadFull(br, payload)

	for i := range payload {
		payload[i] ^= mask[i%4]
	}

	return string(payload)
}

func TestProbeGremlin(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		status   int
		rejectWS bool
		wantErr  bool
	}{
		{name: "ok", status: 200},
		{name: "script_error", status: 597, wantErr: true},
		{name: "handshake_rejected", rejectWS: true, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := startFakeGremlin(t, tt.status, tt.rejectWS)
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()

			err := neptune.ProbeGremlin(ctx, f.addr)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)

			select {
			case got := <-f.requests:
				assert.Contains(t, got, "application/vnd.gremlin-v3.0+json")
				assert.Contains(t, got, `"gremlin":"g.V().count()"`)
				assert.Contains(t, got, `"op":"eval"`)
			case <-ctx.Done():
				t.Fatal("no gremlin request received")
			}
		})
	}
}

func TestProbeGremlinUnreachable(t *testing.T) {
	t.Parallel()

	l, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)

	addr := l.Addr().String()
	require.NoError(t, l.Close())

	require.Error(t, neptune.ProbeGremlin(t.Context(), addr))
}
