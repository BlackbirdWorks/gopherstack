package bolt

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// Version is a Bolt protocol version.
type Version struct {
	Major byte
	Minor byte
}

// Preamble is the length of the Bolt magic preamble.
const Preamble = 4

// magic is the 4-byte preamble every Bolt connection starts with.
func magic() [4]byte { return [4]byte{0x60, 0x60, 0xB0, 0x17} }

const (
	handshakeLen  = 20
	maxChunk      = 0xFFFF
	maxMessage    = 16 << 20
	versionSlots  = 4
	minor4Min     = 4
	maxMinor5     = 4
	firstMajor    = 4
	secondMajor   = 5
	logonMinMinor = 1
)

// Errors raised by the transport layer.
var (
	ErrBadMagic   = errors.New("bolt: bad handshake preamble")
	ErrNoVersion  = errors.New("bolt: no common protocol version")
	ErrMsgTooBig  = errors.New("bolt: message exceeds size limit")
	ErrBadMessage = errors.New("bolt: malformed message")
)

// Supported lists the versions the server offers, newest first.
func Supported() []Version {
	vs := make([]Version, 0, maxMinor5+2) //nolint:mnd // 5.0..5.4 plus 4.4

	for m := byte(maxMinor5); ; m-- {
		vs = append(vs, Version{Major: secondMajor, Minor: m})
		if m == 0 {
			break
		}
	}

	return append(vs, Version{Major: firstMajor, Minor: minor4Min})
}

// Negotiate reads the client's handshake and returns the chosen version, writing the 4-byte reply.
func Negotiate(rw io.ReadWriter) (Version, error) {
	var hs [handshakeLen]byte
	if _, err := io.ReadFull(rw, hs[:]); err != nil {
		return Version{}, fmt.Errorf("read handshake: %w", err)
	}

	if [4]byte(hs[:4]) != magic() {
		return Version{}, ErrBadMagic
	}

	v, ok := pick(hs[4:])
	reply := []byte{0, 0, 0, 0}

	if ok {
		reply = []byte{0, 0, v.Minor, v.Major}
	}

	if _, err := rw.Write(reply); err != nil {
		return Version{}, fmt.Errorf("write handshake: %w", err)
	}

	if !ok {
		return Version{}, ErrNoVersion
	}

	return v, nil
}

// pick honours the client's preference order; each slot is [reserved, range, minor, major].
func pick(slots []byte) (Version, bool) {
	for i := range versionSlots {
		s := slots[i*versionSlots : (i+1)*versionSlots]
		major, minor, rng := s[3], s[2], s[1]

		for _, v := range Supported() {
			if v.Major == major && v.Minor <= minor && int(v.Minor) >= int(minor)-int(rng) {
				return v, true
			}
		}
	}

	return Version{}, false
}

// readMessage assembles one chunked message, skipping NOOP chunks.
func readMessage(r *bufio.Reader) ([]byte, error) {
	var msg []byte

	var hdr [2]byte

	for {
		if _, err := io.ReadFull(r, hdr[:]); err != nil {
			return nil, fmt.Errorf("read chunk header: %w", err)
		}

		n := int(binary.BigEndian.Uint16(hdr[:]))
		if n == 0 {
			if len(msg) == 0 {
				continue
			}

			return msg, nil
		}

		if len(msg)+n > maxMessage {
			return nil, ErrMsgTooBig
		}

		start := len(msg)
		msg = append(msg, make([]byte, n)...)

		if _, err := io.ReadFull(r, msg[start:]); err != nil {
			return nil, fmt.Errorf("read chunk: %w", err)
		}
	}
}

// writeMessage chunks msg and flushes the terminator.
func writeMessage(w io.Writer, msg []byte) error {
	for len(msg) > 0 {
		n := min(len(msg), maxChunk)
		if _, err := w.Write(binary.BigEndian.AppendUint16(nil, uint16(n))); err != nil {
			return fmt.Errorf("write chunk header: %w", err)
		}

		if _, err := w.Write(msg[:n]); err != nil {
			return fmt.Errorf("write chunk: %w", err)
		}

		msg = msg[n:]
	}

	if _, err := w.Write([]byte{0, 0}); err != nil {
		return fmt.Errorf("write terminator: %w", err)
	}

	return nil
}
