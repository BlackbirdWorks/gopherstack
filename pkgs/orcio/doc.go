// Package orcio reads and writes Apache ORC files for the SQL engines
// (specification: https://orc.apache.org/specification/ORCv1/).
package orcio

import "errors"

var (
	// ErrBadFile is returned when the bytes are not a readable ORC file.
	ErrBadFile = errors.New("invalid orc file")
	// ErrUnsupported is returned for ORC features this package does not implement.
	ErrUnsupported = errors.New("unsupported orc feature")
	// ErrBadValue is returned when a cell does not parse as its column type.
	ErrBadValue = errors.New("invalid orc cell value")
)

// Type kinds from orc_proto.proto.
const (
	kindBoolean = iota
	kindByte
	kindShort
	kindInt
	kindLong
	kindFloat
	kindDouble
	kindString
	kindBinary
	kindTimestamp
	kindList
	kindMap
	kindStruct
	kindUnion
	kindDecimal
	kindDate
	kindVarchar
	kindChar
	kindTimestampInstant
)

// Stream kinds.
const (
	streamPresent = iota
	streamData
	streamLength
	streamDictionaryData
	streamDictionaryCount
	streamSecondary
	streamRowIndex
)

// Column encodings.
const (
	encDirect = iota
	encDictionary
	encDirectV2
	encDictionaryV2
)

// Compression kinds.
const (
	compNone = iota
	compZlib
	compSnappy
	compLZO
	compLZ4
	compZstd
)

const (
	orcMagic          = "ORC"
	icebergIDAttr     = "iceberg.id"
	timestampBaseUnix = 1420070400
	defaultBlockSize  = 256 * 1024
)
