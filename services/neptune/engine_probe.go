package neptune

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/sha1" //nolint:gosec // RFC 6455 mandates SHA-1 for the handshake accept key
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"slices"
	"time"

	"github.com/google/uuid"
)

const (
	wsGUID          = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
	wsOpBinary      = 0x2
	wsOpClose       = 0x8
	wsOpPing        = 0x9
	wsFinBit        = 0x80
	wsMaskBit       = 0x80
	wsLen16         = 126
	wsLen64         = 127
	wsMaxPayload    = 1 << 20
	wsMaxLen16      = 0xFFFF
	wsKeyBytes      = 16
	gremlinMime     = "application/vnd.gremlin-v3.0+json"
	gremlinProbeQ   = "g.V().count()"
	gremlinPath     = "/gremlin"
	gremlinOKStatus = 200
	probeIOTimeout  = 5 * time.Second
)

var (
	errWSHandshake = errors.New("websocket handshake rejected")
	errWSClosed    = errors.New("websocket closed before a gremlin response")
	errWSTooLarge  = errors.New("websocket frame too large")
	errGremlin     = errors.New("gremlin request failed")
)

type gremlinRequest struct {
	Args      map[string]any `json:"args"`
	RequestID string         `json:"requestId"`
	Op        string         `json:"op"`
	Processor string         `json:"processor"`
}

type gremlinResponse struct {
	Status struct {
		Message string `json:"message"`
		Code    int    `json:"code"`
	} `json:"status"`
}

// ProbeGremlin proves a Gremlin Server answers on addr: a published docker port accepts TCP before the server
// is up, so only a successful g.V().count() over the WebSocket protocol counts.
func ProbeGremlin(ctx context.Context, addr string) error {
	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", addr)
	if err != nil {
		return fmt.Errorf("dial gremlin: %w", err)
	}

	defer func() { _ = conn.Close() }()

	deadline := time.Now().Add(probeIOTimeout)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}

	if err = conn.SetDeadline(deadline); err != nil {
		return fmt.Errorf("set deadline: %w", err)
	}

	br := bufio.NewReader(conn)
	if err = wsHandshake(conn, br, addr); err != nil {
		return err
	}

	payload, err := gremlinPayload()
	if err != nil {
		return err
	}

	if err = writeMaskedFrame(conn, wsOpBinary, payload); err != nil {
		return err
	}

	reply, err := readMessage(br, conn)
	if err != nil {
		return err
	}

	var resp gremlinResponse
	if err = json.Unmarshal(reply, &resp); err != nil {
		return fmt.Errorf("decode gremlin response: %w", err)
	}

	if resp.Status.Code != gremlinOKStatus {
		return fmt.Errorf("%w: status %d: %s", errGremlin, resp.Status.Code, resp.Status.Message)
	}

	return nil
}

func gremlinPayload() ([]byte, error) {
	body, err := json.Marshal(gremlinRequest{
		RequestID: uuid.NewString(),
		Op:        "eval",
		Args:      map[string]any{"gremlin": gremlinProbeQ, "language": "gremlin-groovy"},
	})
	if err != nil {
		return nil, fmt.Errorf("encode gremlin request: %w", err)
	}

	hdr := append([]byte{byte(len(gremlinMime))}, gremlinMime...)

	return slices.Concat(hdr, body), nil
}

func wsHandshake(conn net.Conn, br *bufio.Reader, addr string) error {
	raw := make([]byte, wsKeyBytes)
	if _, err := rand.Read(raw); err != nil {
		return fmt.Errorf("websocket key: %w", err)
	}

	key := base64.StdEncoding.EncodeToString(raw)
	req := "GET " + gremlinPath + " HTTP/1.1\r\nHost: " + addr + "\r\nUpgrade: websocket\r\n" +
		"Connection: Upgrade\r\nSec-WebSocket-Key: " + key + "\r\nSec-WebSocket-Version: 13\r\n\r\n"

	if _, err := io.WriteString(conn, req); err != nil {
		return fmt.Errorf("write handshake: %w", err)
	}

	resp, err := http.ReadResponse(br, nil)
	if err != nil {
		return fmt.Errorf("read handshake: %w", err)
	}

	_ = resp.Body.Close()

	sum := sha1.Sum([]byte(key + wsGUID)) //nolint:gosec // see import
	if resp.StatusCode != http.StatusSwitchingProtocols ||
		resp.Header.Get("Sec-WebSocket-Accept") != base64.StdEncoding.EncodeToString(sum[:]) {
		return fmt.Errorf("%w: status %d", errWSHandshake, resp.StatusCode)
	}

	return nil
}

func writeMaskedFrame(w io.Writer, opcode byte, payload []byte) error {
	frame := []byte{wsFinBit | opcode}

	switch n := len(payload); {
	case n < wsLen16:
		frame = append(frame, wsMaskBit|byte(n))
	case n <= wsMaxLen16:
		frame = append(frame, wsMaskBit|wsLen16)
		frame = binary.BigEndian.AppendUint16(frame, uint16(n))
	default:
		frame = append(frame, wsMaskBit|wsLen64)
		frame = binary.BigEndian.AppendUint64(frame, uint64(n))
	}

	mask := make([]byte, 4) //nolint:mnd // RFC 6455 masking key size
	if _, err := rand.Read(mask); err != nil {
		return fmt.Errorf("websocket mask: %w", err)
	}

	frame = append(frame, mask...)

	for i, c := range payload {
		frame = append(frame, c^mask[i%len(mask)])
	}

	if _, err := w.Write(frame); err != nil {
		return fmt.Errorf("write frame: %w", err)
	}

	return nil
}

// readMessage returns the next complete data message, answering pings and failing on close.
func readMessage(br *bufio.Reader, w io.Writer) ([]byte, error) {
	var msg []byte

	for {
		fin, opcode, payload, err := readFrame(br)
		if err != nil {
			return nil, err
		}

		switch opcode {
		case wsOpClose:
			return nil, errWSClosed
		case wsOpPing:
			if err = writeMaskedFrame(w, 0xA, payload); err != nil { //nolint:mnd // pong opcode
				return nil, err
			}

			continue
		}

		msg = append(msg, payload...)
		if fin {
			return msg, nil
		}
	}
}

func readFrame(br *bufio.Reader) (bool, byte, []byte, error) {
	var hdr [2]byte
	if _, err := io.ReadFull(br, hdr[:]); err != nil {
		return false, 0, nil, fmt.Errorf("read frame header: %w", err)
	}

	size := uint64(hdr[1] & 0x7F) //nolint:mnd // payload length bits

	switch size {
	case wsLen16:
		var ext [2]byte
		if _, err := io.ReadFull(br, ext[:]); err != nil {
			return false, 0, nil, fmt.Errorf("read frame length: %w", err)
		}

		size = uint64(binary.BigEndian.Uint16(ext[:]))
	case wsLen64:
		var ext [8]byte
		if _, err := io.ReadFull(br, ext[:]); err != nil {
			return false, 0, nil, fmt.Errorf("read frame length: %w", err)
		}

		size = binary.BigEndian.Uint64(ext[:])
	}

	if size > wsMaxPayload {
		return false, 0, nil, errWSTooLarge
	}

	payload := make([]byte, size)
	if _, err := io.ReadFull(br, payload); err != nil {
		return false, 0, nil, fmt.Errorf("read frame payload: %w", err)
	}

	return hdr[0]&wsFinBit != 0, hdr[0] & 0x0F, payload, nil //nolint:mnd // opcode nibble
}
