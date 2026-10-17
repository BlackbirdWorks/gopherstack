package rds

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"slices"
)

const (
	myCapFoundRows       = 1 << 1
	myCapLongFlag        = 1 << 2
	myCapConnectWithDB   = 1 << 3
	myCapProtocol41      = 1 << 9
	myCapSSL             = 1 << 11
	myCapTransactions    = 1 << 13
	myCapSecureConn      = 1 << 15
	myCapMultiStatements = 1 << 16
	myCapMultiResults    = 1 << 17
	myCapPluginAuth      = 1 << 19
	myCapConnectAttrs    = 1 << 20
	myCapLenencData      = 1 << 21
	myCapDeprecateEOF    = 1 << 24
	myCapLongPassword    = 1 << 0

	myPluginClear      = "mysql_clear_password"
	myProtocolV10      = 10
	myHeaderLen        = 4
	myMaxHandshakePkt  = 64 << 10
	myMaxPayload       = 1<<24 - 1
	myScrambleLen      = 20
	myScramblePart1    = 8
	myFillerLen        = 23
	myCharsetUTF8MB4   = 45
	myStatusAutocommit = 2
	myErrAccessDenied  = 1045
	myErrNotSupported  = 1251
	myStateNoConnect   = "08004"
	myErrUnknown       = 1105
	myStateDenied      = "28000"
	myStateGeneral     = "HY000"
	myLenencTwo        = 0xfc
	myLenencThree      = 0xfd
	myLenencEight      = 0xfe
	myLenencMax        = 0xfb
	myGreetingReserved = 10
	myByte1Shift       = 8
	myByte2Shift       = 16
	myErrMarker        = 0xff
	myAuthSwitchMarker = 0xfe
)

var (
	errMyPacketTooLarge = errors.New("mysql packet too large")
	errMyMalformed      = errors.New("malformed mysql packet")
)

// myHandshakeResponse is the client's HandshakeResponse41 (or the SSLRequest prefix of it).
type myHandshakeResponse struct {
	user     string
	database string
	plugin   string
	authData []byte
	caps     uint32
	charset  byte
}

func readMyPacket(r io.Reader) (byte, []byte, error) {
	hdr := make([]byte, myHeaderLen)
	if _, err := io.ReadFull(r, hdr); err != nil {
		return 0, nil, fmt.Errorf("read packet header: %w", err)
	}

	n := int(hdr[0]) | int(hdr[1])<<myByte1Shift | int(hdr[2])<<myByte2Shift
	if n > myMaxHandshakePkt {
		return 0, nil, errMyPacketTooLarge
	}

	body, err := io.ReadAll(io.LimitReader(r, int64(n)))
	if err != nil {
		return 0, nil, fmt.Errorf("read packet body: %w", err)
	}

	if len(body) != n {
		return 0, nil, io.ErrUnexpectedEOF
	}

	return hdr[3], body, nil
}

func writeMyPacket(w io.Writer, seq byte, payload []byte) error {
	if len(payload) > myMaxPayload {
		return errMyPacketTooLarge
	}

	n := len(payload)
	hdr := binary.LittleEndian.AppendUint32(nil, uint32(n&myMaxPayload))
	hdr[myHeaderLen-1] = seq

	if _, err := w.Write(slices.Concat(hdr, payload)); err != nil {
		return fmt.Errorf("write packet: %w", err)
	}

	return nil
}

// encodeMyGreeting builds a protocol-10 server greeting advertising auth plugin plugin.
func encodeMyGreeting(version string, connID uint32, scramble []byte, caps uint32, plugin string) []byte {
	if len(scramble) != myScrambleLen {
		scramble = make([]byte, myScrambleLen)
	}

	out := []byte{myProtocolV10}
	out = append(out, version...)
	out = append(out, 0)
	out = binary.LittleEndian.AppendUint32(out, connID)
	out = append(out, scramble[:myScramblePart1]...)
	out = append(out, 0)
	capsLE := binary.LittleEndian.AppendUint32(nil, caps)
	out = append(out, capsLE[:2]...)
	out = append(out, myCharsetUTF8MB4)
	out = binary.LittleEndian.AppendUint16(out, myStatusAutocommit)
	out = append(out, capsLE[2:]...)
	out = append(out, myScrambleLen+1)
	out = append(out, make([]byte, myGreetingReserved)...)
	out = append(out, scramble[myScramblePart1:]...)
	out = append(out, 0)
	out = append(out, plugin...)

	return append(out, 0)
}

func encodeMyOK() []byte {
	return []byte{0, 0, 0, myStatusAutocommit, 0, 0, 0}
}

func encodeMyErr(code uint16, state, msg string) []byte {
	out := []byte{myErrMarker}
	out = binary.LittleEndian.AppendUint16(out, code)
	out = append(out, '#')
	out = append(out, state...)

	return append(out, msg...)
}

func decodeMyHandshakeResponse(p []byte) (*myHandshakeResponse, error) {
	const fixed = 4 + 4 + 1 + myFillerLen
	if len(p) < fixed {
		return nil, errMyMalformed
	}

	resp := &myHandshakeResponse{caps: binary.LittleEndian.Uint32(p), charset: p[8]}
	rest := p[fixed:]

	if resp.caps&myCapProtocol41 == 0 {
		return nil, errMyMalformed
	}

	if len(rest) == 0 && resp.caps&myCapSSL != 0 {
		return resp, nil
	}

	var ok bool

	if resp.user, rest, ok = cutMyString(rest); !ok {
		return nil, errMyMalformed
	}

	var err error
	if resp.authData, rest, err = cutMyAuth(rest, resp.caps); err != nil {
		return nil, err
	}

	if resp.caps&myCapConnectWithDB != 0 {
		if resp.database, rest, ok = cutMyString(rest); !ok {
			return nil, errMyMalformed
		}
	}

	if resp.caps&myCapPluginAuth != 0 {
		resp.plugin, _, _ = cutMyString(rest)
	}

	return resp, nil
}

func cutMyString(b []byte) (string, []byte, bool) {
	s, rest, ok := bytes.Cut(b, []byte{0})

	return string(s), rest, ok
}

func cutMyAuth(b []byte, caps uint32) ([]byte, []byte, error) {
	switch {
	case caps&myCapLenencData != 0:
		n, rest, ok := readMyLenenc(b)
		if !ok || n > uint64(len(rest)) {
			return nil, nil, errMyMalformed
		}

		return rest[:n], rest[n:], nil
	case caps&myCapSecureConn != 0:
		if len(b) == 0 || int(b[0]) > len(b)-1 {
			return nil, nil, errMyMalformed
		}

		return b[1 : 1+int(b[0])], b[1+int(b[0]):], nil
	default:
		s, rest, ok := bytes.Cut(b, []byte{0})
		if !ok {
			return nil, nil, errMyMalformed
		}

		return s, rest, nil
	}
}

func readMyLenenc(b []byte) (uint64, []byte, bool) {
	if len(b) == 0 {
		return 0, nil, false
	}

	switch first := b[0]; {
	case first < myLenencMax:
		return uint64(first), b[1:], true
	case first == myLenencTwo && len(b) >= 3:
		return uint64(binary.LittleEndian.Uint16(b[1:])), b[3:], true
	case first == myLenencThree && len(b) >= 4:
		return uint64(b[1]) | uint64(b[2])<<8 | uint64(b[3])<<16, b[4:], true
	case first == myLenencEight && len(b) >= 9:
		return binary.LittleEndian.Uint64(b[1:]), b[9:], true
	default:
		return 0, nil, false
	}
}
