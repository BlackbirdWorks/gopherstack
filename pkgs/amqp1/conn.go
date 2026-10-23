package amqp1

import (
	"bufio"
	"context"
	"io"
	"net"
	"sync"
	"time"
)

const (
	maxChannel   uint16 = 65535
	maxSessions         = 1024
	maxLinksPer         = 256
	readBufBytes        = 32 << 10
)

func readFull(r io.Reader, b []byte) (int, error) { return io.ReadFull(r, b) }

type conn struct {
	nc       net.Conn
	r        *bufio.Reader
	srv      *Server
	ctx      context.Context
	ch       ConnHandler
	sessions map[uint16]*session
	idle     time.Duration
	maxFrame uint32
	wmu      sync.Mutex
	smu      sync.Mutex
	once     sync.Once
}

func newConn(s *Server, nc net.Conn) *conn {
	return &conn{
		srv:      s,
		nc:       nc,
		r:        bufio.NewReaderSize(nc, readBufBytes),
		sessions: map[uint16]*session{},
		maxFrame: MinMaxFrameSize,
	}
}

func (c *conn) shutdown() {
	c.once.Do(func() { _ = c.nc.Close() })
}

func (c *conn) writeRaw(b []byte) error {
	c.wmu.Lock()
	defer c.wmu.Unlock()

	_ = c.nc.SetWriteDeadline(time.Now().Add(writeTimeout))

	_, err := c.nc.Write(b)

	return err
}

func (c *conn) writePerformative(ft byte, channel uint16, p Performative) error {
	return c.writeWithPayload(ft, channel, p, nil)
}

func (c *conn) writeWithPayload(ft byte, channel uint16, p Performative, payload []byte) error {
	body, err := EncodePerformative(p)
	if err != nil {
		return err
	}

	return c.writeRaw(AppendFrame(nil, Frame{Type: ft, Channel: channel, Body: append(body, payload...)}))
}

func (c *conn) closeWithError(e *Error) {
	_ = c.writePerformative(FrameAMQP, 0, &Close{Error: e})
}

func (c *conn) closeSessions() {
	c.smu.Lock()
	all := make([]*session, 0, len(c.sessions))

	for _, s := range c.sessions {
		all = append(all, s)
	}

	c.sessions = map[uint16]*session{}
	c.smu.Unlock()

	for _, s := range all {
		s.teardown()
	}
}

// dispatch routes one frame; it returns false when the connection is done.
func (c *conn) dispatch(f Frame) bool {
	p, _, err := DecodePerformative(f.Body)
	if err != nil {
		c.closeWithError(&Error{Condition: CondDecode, Description: err.Error()})

		return false
	}

	switch v := p.(type) {
	case *Close:
		_ = c.writePerformative(FrameAMQP, 0, &Close{})

		return false
	case *Begin:
		return c.begin(f.Channel, v)
	case *Open:
		c.closeWithError(&Error{Condition: CondIllegalState, Description: "duplicate open"})

		return false
	}

	c.smu.Lock()
	s := c.sessions[f.Channel]
	c.smu.Unlock()

	if s == nil {
		c.closeWithError(&Error{Condition: CondIllegalState, Description: "frame on unmapped channel"})

		return false
	}

	return s.handle(p)
}

func (c *conn) begin(channel uint16, b *Begin) bool {
	c.smu.Lock()
	_, dup := c.sessions[channel]
	full := len(c.sessions) >= maxSessions

	if dup || full {
		c.smu.Unlock()
		c.closeWithError(&Error{Condition: CondResourceLimit, Description: "cannot begin session"})

		return false
	}

	s := newSession(c, channel, b)
	c.sessions[channel] = s
	c.smu.Unlock()

	rc := channel
	reply := &Begin{
		RemoteChannel: &rc, NextOutgoingID: 0, IncomingWindow: sessionWindow, OutgoingWindow: sessionWindow,
		HandleMax: maxLinksPer - 1,
	}

	return c.writePerformative(FrameAMQP, channel, reply) == nil
}

func (c *conn) dropSession(channel uint16) {
	c.smu.Lock()
	delete(c.sessions, channel)
	c.smu.Unlock()
}
