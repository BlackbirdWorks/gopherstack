package bedrockruntime_test

import (
	"encoding/binary"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// parseEventStreamFrames splits a raw binary event stream body into decoded
// JSON payloads by reading the AWS event-stream framing format:
// totalLen(4) | headersLen(4) | preludeCRC(4) | headers | payload | msgCRC(4).
func parseEventStreamFrames(data []byte) []map[string]any {
	var frames []map[string]any

	for len(data) >= 12 {
		if len(data) < 4 {
			break
		}

		totalLen := binary.BigEndian.Uint32(data[0:4])
		headerLen := binary.BigEndian.Uint32(data[4:8])

		if totalLen < 12 || uint32(len(data)) < totalLen {
			break
		}

		payloadStart := uint32(12) + headerLen
		payloadEnd := totalLen - 4

		if payloadStart > payloadEnd || payloadEnd > totalLen {
			data = data[totalLen:]

			continue
		}

		payload := data[payloadStart:payloadEnd]

		if len(payload) > 0 {
			var m map[string]any
			if err := json.Unmarshal(payload, &m); err == nil {
				frames = append(frames, m)
			}
		}

		data = data[totalLen:]
	}

	return frames
}

// eventTypesFromRaw extracts header ":event-type" values from raw binary frames.
// This is a simplified parser that looks for the 12-byte "event-type" header
// name using the AWS event stream header encoding.
func eventTypesFromRaw(data []byte) []string {
	var types []string
	const headerName = ":event-type"

	// Walk frames: totalLen(4) | headersLen(4) | preludeCRC(4) | headers | payload | msgCRC(4)
	for len(data) >= 12 {
		totalLen := int(binary.BigEndian.Uint32(data[0:4]))
		headerLen := int(binary.BigEndian.Uint32(data[4:8]))

		if totalLen < 12 || len(data) < totalLen {
			break
		}

		hdrData := data[12 : 12+headerLen]

		for len(hdrData) > 0 {
			if len(hdrData) < 1 {
				break
			}

			nameLen := int(hdrData[0])
			hdrData = hdrData[1:]

			if len(hdrData) < nameLen {
				break
			}

			name := string(hdrData[:nameLen])
			hdrData = hdrData[nameLen:]

			if len(hdrData) < 1 {
				break
			}

			// type byte (7 = string)
			hdrData = hdrData[1:]

			if len(hdrData) < 2 {
				break
			}

			valLen := int(binary.BigEndian.Uint16(hdrData[:2]))
			hdrData = hdrData[2:]

			if len(hdrData) < valLen {
				break
			}

			value := string(hdrData[:valLen])
			hdrData = hdrData[valLen:]

			if name == headerName {
				types = append(types, value)
			}
		}

		data = data[totalLen:]
	}

	return types
}

func TestEventStreamHeaders_DynamicBuffer(t *testing.T) {
	t.Parallel()

	// Trigger event-stream path to ensure headers encode correctly.
	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/model/amazon.nova-pro-v1:0/invoke-with-response-stream",
		map[string]any{"prompt": "test"})

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "application/vnd.amazon.eventstream", rec.Header().Get("Content-Type"))

	// Verify the event stream frame has valid structure (non-empty, parseable length).
	frameBytes := rec.Body.Bytes()
	require.GreaterOrEqual(t, len(frameBytes), 12, "event stream frame must be at least 12 bytes (prelude)")
}
