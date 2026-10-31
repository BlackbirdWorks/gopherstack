package ssm

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const (
	dataChannelPathPrefix   = "/ssmmessages/"
	dataChannelPath         = "/ssmmessages/v1/data-channel/"
	dataChannelOpenTimeout  = 15 * time.Second
	dataChannelWriteTimeout = 10 * time.Second
	dataChannelMaxPending   = 1024
	dataChannelMaxFrame     = 1 << 20
	shellReadChunk          = 1024
	defaultShellCols        = 80
	defaultShellRows        = 24
	ackFlags                = 3
)

var (
	errNoShellBackend     = errors.New("no shell backend is configured (EC2 compute provider is not docker)")
	errNoStreamBase       = errors.New("the request carries no host to build a data channel URL from")
	errTargetNotContainer = errors.New("target is not a running docker-backed managed instance")
)

func (h *Handler) handleDataChannel(c *echo.Context) error {
	bk, ok := h.Backend.(*InMemoryBackend)
	sid := strings.TrimPrefix(c.Request().URL.Path, dataChannelPath)

	if !ok || sid == "" || strings.Contains(sid, "/") || !strings.HasPrefix(c.Request().URL.Path, dataChannelPath) {
		return c.NoContent(http.StatusNotFound)
	}

	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}

	ws, err := upgrader.Upgrade(c.Response(), c.Request(), nil)
	if err != nil {
		logger.Load(c.Request().Context()).Debug("ssmmessages upgrade failed", "error", err)

		return nil
	}

	newDataChannel(bk, ws, sid).serve(c.Request().Context())

	return nil
}

type dataChannel struct {
	ws        *websocket.Conn
	b         *InMemoryBackend
	shell     Shell
	conn      net.Conn
	pending   map[int64]*StreamMessage
	params    map[string][]string
	exited    chan struct{}
	started   time.Time
	sessionID string
	region    string
	target    string
	container string
	command   string
	document  string
	wg        sync.WaitGroup
	wmu       sync.Mutex
	outSeq    int64
	expected  int64
	cols      uint
	rows      uint
}

func newDataChannel(b *InMemoryBackend, ws *websocket.Conn, sessionID string) *dataChannel {
	return &dataChannel{
		ws: ws, b: b, sessionID: sessionID,
		pending: make(map[int64]*StreamMessage), exited: make(chan struct{}),
		cols: defaultShellCols, rows: defaultShellRows,
	}
}

func (d *dataChannel) serve(parent context.Context) {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	defer d.ws.Close()

	d.ws.SetReadLimit(dataChannelMaxFrame)
	_ = d.ws.SetReadDeadline(time.Now().Add(dataChannelOpenTimeout))

	_, raw, err := d.ws.ReadMessage()
	if err != nil {
		return
	}

	_ = d.ws.SetReadDeadline(time.Time{})

	var open OpenDataChannelInput
	if jerr := json.Unmarshal(raw, &open); jerr != nil {
		d.reject("malformed OpenDataChannel request")

		return
	}

	sess, region, ok := d.b.authorizeDataChannel(d.sessionID, open.TokenValue)
	if !ok {
		d.reject("invalid session token")

		return
	}

	d.region, d.target, d.document, d.params = region, sess.Target, sess.DocumentName, sess.Parameters
	d.container, d.command = sess.ContainerID, sess.ExecCommand
	d.started = time.Now()

	d.b.trackChannel(d.sessionID, cancel)
	defer d.b.untrackChannel(d.sessionID)

	frames := make(chan []byte)
	d.wg.Go(func() { d.readFrames(ctx, frames) })

	defer d.wg.Wait()
	defer d.closeShell()
	defer d.ws.Close()

	if d.sendHandshakeRequest() != nil {
		return
	}

	d.run(ctx, frames)
}

func (d *dataChannel) reject(reason string) {
	_ = d.ws.WriteControl(
		websocket.CloseMessage,
		websocket.FormatCloseMessage(websocket.ClosePolicyViolation, reason),
		time.Now().Add(dataChannelWriteTimeout),
	)
}

func (d *dataChannel) readFrames(ctx context.Context, frames chan<- []byte) {
	defer close(frames)

	for {
		typ, raw, err := d.ws.ReadMessage()
		if err != nil {
			return
		}

		if typ != websocket.BinaryMessage {
			continue
		}

		select {
		case frames <- raw:
		case <-ctx.Done():
			return
		}
	}
}

func (d *dataChannel) run(ctx context.Context, frames <-chan []byte) {
	for {
		select {
		case <-ctx.Done():
			d.finish("Session terminated.")

			return
		case <-d.exited:
			d.endSession("Exiting session with sessionId: " + d.sessionID + ".")

			return
		case raw, ok := <-frames:
			if !ok {
				return
			}

			if d.handleFrame(ctx, raw) {
				return
			}
		}
	}
}

func (d *dataChannel) handleFrame(ctx context.Context, raw []byte) bool {
	msg, err := ParseStreamMessage(raw)
	if err != nil || msg.MessageType != MsgInputStreamData {
		return false
	}

	_ = d.sendAck(msg)

	if msg.SequenceNumber < d.expected || len(d.pending) >= dataChannelMaxPending {
		return false
	}

	d.pending[msg.SequenceNumber] = msg

	for {
		next, ok := d.pending[d.expected]
		if !ok {
			return false
		}

		delete(d.pending, d.expected)
		d.expected++

		if d.process(ctx, next) {
			return true
		}
	}
}

func (d *dataChannel) process(ctx context.Context, m *StreamMessage) bool {
	switch m.PayloadType {
	case PayloadHandshakeResponse:
		return d.completeHandshake(ctx, m.Payload)
	case PayloadSize:
		var sz SizePayload
		if json.Unmarshal(m.Payload, &sz) == nil && sz.Cols > 0 && sz.Rows > 0 {
			d.cols, d.rows = sz.Cols, sz.Rows
			if d.shell != nil {
				_ = d.shell.Resize(ctx, sz.Cols, sz.Rows)
			}
		}
	case PayloadOutput:
		switch {
		case d.conn != nil:
			_, _ = d.conn.Write(m.Payload)
		case d.shell != nil:
			_, _ = d.shell.Write(m.Payload)
		}
	case PayloadFlag:
		if len(m.Payload) == 4 && binary.BigEndian.Uint32(m.Payload) == flagTerminateSession {
			d.endSession("Session terminated by client.")

			return true
		}
	}

	return false
}

func (d *dataChannel) sendHandshakeRequest() error {
	version, sessionType, props := agentVersion, sessionTypeStandard, map[string]any(nil)

	if t, err := parsePortTarget(d.document, d.params); err == nil {
		version, sessionType, props = portAgentVersion, sessionTypePort, t.properties()
	} else if isPortDocument(d.document) {
		version, sessionType = portAgentVersion, sessionTypePort
	}

	body, _ := json.Marshal(HandshakeRequestPayload{
		AgentVersion: version,
		RequestedClientActions: []RequestedClientAction{{
			ActionType: actionTypeSessionType,
			ActionParameters: map[string]any{
				"SessionType": sessionType,
				"Properties":  props,
			},
		}},
	})

	return d.sendOutput(PayloadHandshakeRequest, body)
}

func (d *dataChannel) completeHandshake(ctx context.Context, payload []byte) bool {
	var resp HandshakeResponsePayload
	if err := json.Unmarshal(payload, &resp); err != nil {
		d.endSession("Malformed handshake response.")

		return true
	}

	for _, a := range resp.ProcessedClientActions {
		if a.ActionStatus != actionStatusSuccess {
			d.endSession("Handshake failed: " + a.Error)

			return true
		}
	}

	done, _ := json.Marshal(HandshakeCompletePayload{HandshakeTimeToComplete: int64(time.Since(d.started))})
	if d.sendOutput(PayloadHandshakeComplete, done) != nil {
		return true
	}

	if err := d.startShell(ctx); err != nil {
		d.endSession("Cannot start session: " + err.Error())

		return true
	}

	return false
}

func (d *dataChannel) startShell(ctx context.Context) error {
	if isPortDocument(d.document) {
		return d.startPort(ctx)
	}

	opener := d.b.shellOpener()
	if opener == nil {
		return errNoShellBackend
	}

	if d.container != "" {
		return d.startContainerCommand(ctx, opener)
	}

	containerID, ok := d.b.shellContainer(d.region, d.target)
	if !ok {
		return fmt.Errorf("%w: %s", errTargetNotContainer, d.target)
	}

	sh, err := opener.OpenShell(ctx, containerID, d.cols, d.rows)
	if err != nil {
		return err
	}

	d.shell = sh
	d.wg.Go(func() { d.pumpShell(sh) })

	return nil
}

func (d *dataChannel) startContainerCommand(ctx context.Context, opener ShellOpener) error {
	cmdOpener, ok := opener.(CommandShellOpener)
	if !ok {
		return errNoShellBackend
	}

	sh, err := cmdOpener.OpenCommand(ctx, d.container, []string{"sh", "-c", d.command}, d.cols, d.rows)
	if err != nil {
		return err
	}

	d.shell = sh
	d.wg.Go(func() { d.pumpShell(sh) })

	return nil
}

func (d *dataChannel) pumpShell(sh Shell) {
	defer close(d.exited)

	buf := make([]byte, shellReadChunk)

	for {
		n, err := sh.Read(buf)
		if n > 0 && d.sendOutput(PayloadOutput, buf[:n]) != nil {
			return
		}

		if err != nil {
			return
		}
	}
}

func (d *dataChannel) closeShell() {
	if d.shell != nil {
		_ = d.shell.Close()
	}

	if d.conn != nil {
		_ = d.conn.Close()
	}
}

func (d *dataChannel) endSession(reason string) {
	d.b.endSession(d.region, d.sessionID)
	d.finish(reason)
}

func (d *dataChannel) finish(reason string) {
	body, _ := json.Marshal(ChannelClosedPayload{
		MessageType:   MsgChannelClosed,
		MessageID:     uuid.NewString(),
		DateTime:      time.Now().UTC().Format(messageTimeFormat),
		SessionID:     d.sessionID,
		SchemaVersion: msgSchemaVersion,
		Output:        reason,
	})

	_ = d.send(NewStreamMessage(MsgChannelClosed, 0, 0, body))
	d.closeNormal()
}

func (d *dataChannel) closeNormal() {
	_ = d.ws.WriteControl(
		websocket.CloseMessage,
		websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""),
		time.Now().Add(dataChannelWriteTimeout),
	)
}

func (d *dataChannel) sendAck(m *StreamMessage) error {
	body, _ := json.Marshal(AckPayload{
		MessageType: m.MessageType, MessageID: m.MessageID.String(),
		SequenceNumber: m.SequenceNumber, IsSequentialMessage: true,
	})

	ack := NewStreamMessage(MsgAcknowledge, 0, 0, body)
	ack.Flags = ackFlags

	return d.send(ack)
}

func (d *dataChannel) sendOutput(payloadType uint32, payload []byte) error {
	d.wmu.Lock()
	defer d.wmu.Unlock()

	msg := NewStreamMessage(MsgOutputStreamData, d.outSeq, payloadType, payload)
	d.outSeq++

	return d.writeLocked(msg)
}

func (d *dataChannel) send(msg *StreamMessage) error {
	d.wmu.Lock()
	defer d.wmu.Unlock()

	return d.writeLocked(msg)
}

func (d *dataChannel) writeLocked(msg *StreamMessage) error {
	_ = d.ws.SetWriteDeadline(time.Now().Add(dataChannelWriteTimeout))

	return d.ws.WriteMessage(websocket.BinaryMessage, msg.Marshal())
}

// endSession marks a session terminated when its shell or client ends it.
func (b *InMemoryBackend) endSession(region, sessionID string) {
	_, _ = b.TerminateSession(WithRegion(context.Background(), region), &TerminateSessionInput{SessionID: sessionID})
}
