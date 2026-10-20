package bolt

import (
	"bufio"
	"fmt"
	"io"
)

// Client is a minimal Bolt client for protocol tests.
type Client struct {
	rw  io.ReadWriter
	r   *bufio.Reader
	enc *Encoder
	// Version is the negotiated protocol version.
	Version Version
}

// Response is one server message.
type Response struct {
	Fields []any
	Tag    byte
}

// Meta returns the first field of a SUCCESS/FAILURE response as a map.
func (r Response) Meta() map[string]any {
	if len(r.Fields) == 0 {
		return nil
	}

	m, _ := r.Fields[0].(map[string]any)

	return m
}

// Response tags a client can see.
const (
	TagSuccess = msgSuccess
	TagRecord  = msgRecord
	TagIgnore  = msgIgnored
	TagFailure = msgFailure
)

// Request tags a client can send.
const (
	TagHello    = msgHello
	TagGoodbye  = msgGoodbye
	TagReset    = msgReset
	TagRun      = msgRun
	TagBegin    = msgBegin
	TagCommit   = msgCommit
	TagRollback = msgRollback
	TagDiscard  = msgDiscard
	TagPull     = msgPull
	TagRoute    = msgRoute
	TagLogon    = msgLogon
	TagLogoff   = msgLogoff
)

// Dial performs the handshake proposing the given versions (newest preference first, at most four).
func Dial(rw io.ReadWriter, versions ...Version) (*Client, error) {
	hs := make([]byte, 0, handshakeLen)
	m := magic()
	hs = append(hs, m[:]...)

	for i := range versionSlots {
		slot := []byte{0, 0, 0, 0}
		if i < len(versions) {
			slot = []byte{0, 0, versions[i].Minor, versions[i].Major}
		}

		hs = append(hs, slot...)
	}

	return dialRaw(rw, hs)
}

// DialRaw sends the given 16 version bytes after the preamble.
func DialRaw(rw io.ReadWriter, slots [16]byte) (*Client, error) {
	m := magic()

	return dialRaw(rw, append(append([]byte{}, m[:]...), slots[:]...))
}

func dialRaw(rw io.ReadWriter, hs []byte) (*Client, error) {
	if _, err := rw.Write(hs); err != nil {
		return nil, fmt.Errorf("write handshake: %w", err)
	}

	var reply [4]byte
	if _, err := io.ReadFull(rw, reply[:]); err != nil {
		return nil, fmt.Errorf("read handshake: %w", err)
	}

	v := Version{Major: reply[3], Minor: reply[2]}
	if v.Major == 0 {
		return nil, ErrNoVersion
	}

	return &Client{rw: rw, r: bufio.NewReader(rw), enc: NewEncoder(v), Version: v}, nil
}

// Send writes one request.
func (c *Client) Send(tag byte, fields ...any) error {
	c.enc.Reset()

	if err := c.enc.encodeStruct(tag, fields, 0); err != nil {
		return err
	}

	return writeMessage(c.rw, c.enc.Bytes())
}

// Recv reads one server message.
func (c *Client) Recv() (Response, error) {
	raw, err := readMessage(c.r)
	if err != nil {
		return Response{}, err
	}

	tag, fields, err := parseMessage(raw)

	return Response{Tag: tag, Fields: fields}, err
}

// Call sends a request and reads one response.
func (c *Client) Call(tag byte, fields ...any) (Response, error) {
	if err := c.Send(tag, fields...); err != nil {
		return Response{}, err
	}

	return c.Recv()
}

// Query runs an autocommit query and pulls every record, returning the rows and the final summary.
func (c *Client) Query(query string, params map[string]any) ([][]any, Response, error) {
	if params == nil {
		params = map[string]any{}
	}

	res, err := c.Call(TagRun, query, params, map[string]any{})
	if err != nil || res.Tag != TagSuccess {
		return nil, res, err
	}

	return c.PullAll(-1)
}

// PullAll sends PULL and collects records until the terminating summary.
func (c *Client) PullAll(n int64) ([][]any, Response, error) {
	if err := c.Send(TagPull, map[string]any{"n": n}); err != nil {
		return nil, Response{}, err
	}

	var rows [][]any

	for {
		r, err := c.Recv()
		if err != nil {
			return rows, r, err
		}

		if r.Tag != TagRecord {
			return rows, r, nil
		}

		row, _ := r.Fields[0].([]any)
		rows = append(rows, row)
	}
}
