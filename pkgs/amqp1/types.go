// Package amqp1 implements the server side of AMQP 1.0 (OASIS standard):
// the type system, framing, SASL, and a connection/session/link engine that
// delegates message semantics to a Handler.
package amqp1

import "errors"

// Symbol is an AMQP symbol (ASCII string).
type Symbol string

// UUID is an AMQP uuid.
type UUID [16]byte

// Described is a described type: a descriptor (usually a uint64 code) and a
// value.
type Described struct {
	Descriptor any
	Value      any
}

// List is a decoded AMQP list.
type List []any

// Map is a decoded AMQP map.
type Map map[any]any

// Wire type codes used by the codec.
const (
	codeDescribed  = 0x00
	codeNull       = 0x40
	codeTrue       = 0x41
	codeFalse      = 0x42
	codeUint0      = 0x43
	codeUlong0     = 0x44
	codeList0      = 0x45
	codeUbyte      = 0x50
	codeByte       = 0x51
	codeBoolean    = 0x56
	codeSmallUint  = 0x52
	codeSmallUlong = 0x53
	codeSmallInt   = 0x54
	codeSmallLong  = 0x55
	codeUshort     = 0x60
	codeShort      = 0x61
	codeUint       = 0x70
	codeInt        = 0x71
	codeFloat      = 0x72
	codeChar       = 0x73
	codeUlong      = 0x80
	codeLong       = 0x81
	codeDouble     = 0x82
	codeTimestamp  = 0x83
	codeUUID       = 0x98
	codeBin8       = 0xa0
	codeStr8       = 0xa1
	codeSym8       = 0xa3
	codeBin32      = 0xb0
	codeStr32      = 0xb1
	codeSym32      = 0xb3
	codeList8      = 0xc0
	codeMap8       = 0xc1
	codeList32     = 0xd0
	codeMap32      = 0xd1
	codeArray8     = 0xe0
	codeArray32    = 0xf0
)

// Codec errors.
var (
	ErrShortBuffer     = errors.New("amqp1: short buffer")
	ErrUnknownType     = errors.New("amqp1: unknown type code")
	ErrTooDeep         = errors.New("amqp1: value nested too deeply")
	ErrBadValue        = errors.New("amqp1: malformed value")
	ErrUnsupportedType = errors.New("amqp1: cannot encode Go type")
)

const maxDepth = 32
