package bedrockruntime

import (
	"encoding/binary"
	"hash/crc32"
	"math"
	"net/http"
)

// Event stream frame constants (AWS binary event stream protocol).
const (
	eventStreamPreludeLen = 12 // 4 (total-len) + 4 (headers-len) + 4 (prelude-CRC)
	eventStreamMsgCRCLen  = 4

	// eventStreamHeaderValueTypeString is the AWS event stream type byte for string headers.
	eventStreamHeaderValueTypeString = 7
	// eventStreamHeaderValueLenBytes is the number of bytes used to encode a header value length.
	eventStreamHeaderValueLenBytes = 2
)

// eventStreamHeaderInitialCap is the initial capacity for event stream header encoding.
const eventStreamHeaderInitialCap = 256

// encodeEventStreamMsg encodes a single AWS event stream binary message.
// Format: totalLen(4) | headersLen(4) | preludeCRC(4) | headers | payload | msgCRC(4).
// Uses the same framing as the Kinesis event stream implementation.
func encodeEventStreamMsg(hdrs [][2]string, payload []byte) []byte {
	hdrBytes := buildEventStreamHeaders(hdrs)
	headerLen := len(hdrBytes)
	payloadLen := len(payload)

	// Prelude (12 bytes) + headers + payload + message CRC (4 bytes).
	// Guard against integer overflow when calculating totalLen.
	totalLen := uint64(eventStreamPreludeLen) + uint64(headerLen) + uint64(payloadLen) + uint64(eventStreamMsgCRCLen)
	if totalLen > math.MaxInt32 {
		return nil
	}

	buf := make([]byte, totalLen)

	binary.BigEndian.PutUint32(buf[0:4], uint32(totalLen))
	//nolint:gosec // headerLen is bounded by the overflow check above
	binary.BigEndian.PutUint32(buf[4:8], uint32(headerLen))

	preludeCRC := crc32.ChecksumIEEE(buf[0:8])
	binary.BigEndian.PutUint32(buf[8:eventStreamPreludeLen], preludeCRC)

	copy(buf[eventStreamPreludeLen:eventStreamPreludeLen+headerLen], hdrBytes)
	copy(buf[eventStreamPreludeLen+headerLen:eventStreamPreludeLen+headerLen+payloadLen], payload)

	msgCRC := crc32.ChecksumIEEE(buf[0 : eventStreamPreludeLen+headerLen+payloadLen])
	binary.BigEndian.PutUint32(buf[eventStreamPreludeLen+headerLen+payloadLen:], msgCRC)

	return buf
}

// buildEventStreamHeaders encodes name/value header pairs in AWS event stream binary format.
// It uses a dynamic slice to avoid silent truncation on overflow.
func buildEventStreamHeaders(hdrs [][2]string) []byte {
	buf := make([]byte, 0, eventStreamHeaderInitialCap)

	for _, kv := range hdrs {
		name, value := kv[0], kv[1]
		nameLen := len(name)

		if nameLen > math.MaxUint8 {
			// AWS event stream protocol: header name must fit in a single byte length field.
			continue
		}

		if len(value) > math.MaxUint16 {
			// AWS event stream protocol: header value length is 2 bytes.
			continue
		}

		buf = append(buf, byte(nameLen))
		buf = append(buf, name...)
		buf = append(buf, eventStreamHeaderValueTypeString)

		var lenBuf [eventStreamHeaderValueLenBytes]byte
		//nolint:gosec // len(value) bounded by MaxUint16 check above
		binary.BigEndian.PutUint16(lenBuf[:], uint16(len(value)))
		buf = append(buf, lenBuf[:]...)
		buf = append(buf, value...)
	}

	return buf
}

// flushResponse flushes the response writer if it implements http.Flusher.
func flushResponse(w http.ResponseWriter) {
	if f, ok := w.(http.Flusher); ok {
		f.Flush()
	}
}
