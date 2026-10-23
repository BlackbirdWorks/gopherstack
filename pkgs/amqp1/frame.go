//nolint:mnd,gosec // AMQP field indexes and wire widths are spec constants; lengths are bounded by frame size
package amqp1

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// Frame types.
const (
	FrameAMQP byte = 0
	FrameSASL byte = 1
)

// Protocol headers.
const (
	HeaderAMQP = "AMQP\x00\x01\x00\x00"
	HeaderSASL = "AMQP\x03\x01\x00\x00"
	HeaderTLS  = "AMQP\x02\x01\x00\x00"
)

const (
	frameHeaderLen = 8
	// MinMaxFrameSize is the smallest max-frame-size AMQP permits.
	MinMaxFrameSize = 512
)

// ErrFrameTooLarge reports a frame exceeding the negotiated maximum.
var ErrFrameTooLarge = errors.New("amqp1: frame exceeds max-frame-size")

// ErrBadFrame reports a malformed frame header.
var ErrBadFrame = errors.New("amqp1: malformed frame")

// Frame is one AMQP frame: type, channel and body (performative plus any
// transfer payload).
type Frame struct {
	Body    []byte
	Channel uint16
	Type    byte
}

// ReadFrame reads one frame, rejecting any larger than maxSize. An empty
// (keep-alive) frame has a nil Body.
func ReadFrame(r io.Reader, maxSize uint32) (Frame, error) {
	var hdr [frameHeaderLen]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return Frame{}, err
	}

	size := binary.BigEndian.Uint32(hdr[:4])
	doff := int(hdr[4]) * 4

	if size < frameHeaderLen || doff < frameHeaderLen || uint32(doff) > size {
		return Frame{}, ErrBadFrame
	}

	if size > maxSize {
		return Frame{}, fmt.Errorf("%w: %d > %d", ErrFrameTooLarge, size, maxSize)
	}

	rest := make([]byte, size-frameHeaderLen)
	if _, err := io.ReadFull(r, rest); err != nil {
		return Frame{}, err
	}

	f := Frame{Type: hdr[5], Channel: binary.BigEndian.Uint16(hdr[6:8])}
	if body := rest[doff-frameHeaderLen:]; len(body) > 0 {
		f.Body = body
	}

	return f, nil
}

// AppendFrame appends the wire form of a frame.
func AppendFrame(dst []byte, f Frame) []byte {
	dst = binary.BigEndian.AppendUint32(dst, uint32(frameHeaderLen+len(f.Body)))
	dst = append(dst, 2, f.Type)
	dst = binary.BigEndian.AppendUint16(dst, f.Channel)

	return append(dst, f.Body...)
}
