package ssm

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/google/uuid"
)

// Byte layout follows aws/session-manager-plugin message/clientmessage.go (offsets and
// SerializeClientMessage/DeserializeClientMessage) as spoken over communicator/websocketchannel.go.
const (
	msgHeaderLength      = 116
	msgMessageTypeOff    = 4
	msgMessageTypeLen    = 32
	msgSchemaVersionOff  = 36
	msgCreatedDateOff    = 40
	msgSequenceNumberOff = 48
	msgFlagsOff          = 56
	msgMessageIDOff      = 64
	msgPayloadDigestOff  = 80
	msgPayloadTypeOff    = 112
	msgPayloadLengthOff  = 116
	msgPayloadOff        = 120
	msgUUIDHalf          = 8
	msgSchemaVersion     = 1

	// MaxStreamPayload bounds one frame payload (plugin stream data limit is 1024).
	MaxStreamPayload = 1 << 20
)

// Data channel message types.
const (
	MsgInputStreamData  = "input_stream_data"
	MsgOutputStreamData = "output_stream_data"
	MsgAcknowledge      = "acknowledge"
	MsgChannelClosed    = "channel_closed"
	MsgStartPublication = "start_publication"
	MsgPausePublication = "pause_publication"
)

// Data channel payload types (message/clientmessage.go PayloadType).
const (
	PayloadOutput            uint32 = 1
	PayloadSize              uint32 = 3
	PayloadHandshakeRequest  uint32 = 5
	PayloadHandshakeResponse uint32 = 6
	PayloadHandshakeComplete uint32 = 7
	PayloadFlag              uint32 = 10
)

// Flag payload values the client sends as a 4-byte big-endian integer.
const (
	flagDisconnectToPort uint32 = 1
	flagTerminateSession uint32 = 2
)

var (
	errMsgTooShort      = errors.New("data channel message too short")
	errMsgBadHeader     = errors.New("data channel message header length mismatch")
	errMsgBadPayloadLen = errors.New("data channel message payload length mismatch")
	errMsgBadDigest     = errors.New("data channel message payload digest mismatch")
)

// StreamMessage is one binary frame of the Session Manager data channel.
type StreamMessage struct {
	MessageType    string
	Payload        []byte
	SequenceNumber int64
	Flags          uint64
	CreatedDate    uint64
	SchemaVersion  uint32
	PayloadType    uint32
	MessageID      uuid.UUID
}

// NewStreamMessage builds a message stamped with a fresh ID and the current time.
func NewStreamMessage(messageType string, seq int64, payloadType uint32, payload []byte) *StreamMessage {
	return &StreamMessage{
		MessageType:    messageType,
		SchemaVersion:  msgSchemaVersion,
		CreatedDate:    uint64(time.Now().UnixMilli()),
		SequenceNumber: seq,
		MessageID:      uuid.New(),
		PayloadType:    payloadType,
		Payload:        payload,
	}
}

// Marshal encodes the message with its SHA-256 payload digest.
func (m *StreamMessage) Marshal() []byte {
	payload := m.Payload
	if len(payload) > MaxStreamPayload {
		payload = payload[:MaxStreamPayload]
	}

	out := make([]byte, msgPayloadOff)

	binary.BigEndian.PutUint32(out, msgHeaderLength)

	typ := out[msgMessageTypeOff : msgMessageTypeOff+msgMessageTypeLen]
	for i := range typ {
		typ[i] = ' '
	}

	copy(typ, m.MessageType)

	binary.BigEndian.PutUint32(out[msgSchemaVersionOff:], m.SchemaVersion)
	binary.BigEndian.PutUint64(out[msgCreatedDateOff:], m.CreatedDate)
	seq := uint64(m.SequenceNumber) //nolint:gosec // two's complement on the wire
	binary.BigEndian.PutUint64(out[msgSequenceNumberOff:], seq)
	binary.BigEndian.PutUint64(out[msgFlagsOff:], m.Flags)

	copy(out[msgMessageIDOff:], m.MessageID[msgUUIDHalf:])
	copy(out[msgMessageIDOff+msgUUIDHalf:], m.MessageID[:msgUUIDHalf])

	digest := sha256.Sum256(payload)
	copy(out[msgPayloadDigestOff:], digest[:])

	binary.BigEndian.PutUint32(out[msgPayloadTypeOff:], m.PayloadType)
	binary.BigEndian.PutUint32(out[msgPayloadLengthOff:], uint32(len(payload)&math.MaxUint32))
	out = append(out, payload...)

	return out
}

// ParseStreamMessage decodes and verifies a data channel frame.
func ParseStreamMessage(b []byte) (*StreamMessage, error) {
	if len(b) < msgPayloadOff {
		return nil, errMsgTooShort
	}

	if binary.BigEndian.Uint32(b) != msgHeaderLength {
		return nil, errMsgBadHeader
	}

	payloadLen := binary.BigEndian.Uint32(b[msgPayloadLengthOff:])
	if payloadLen > MaxStreamPayload || int64(payloadLen) != int64(len(b)-msgPayloadOff) {
		return nil, fmt.Errorf("%w: %d", errMsgBadPayloadLen, payloadLen)
	}

	payload := bytes.Clone(b[msgPayloadOff:])

	digest := sha256.Sum256(payload)
	if !bytes.Equal(digest[:], b[msgPayloadDigestOff:msgPayloadDigestOff+sha256.Size]) {
		return nil, errMsgBadDigest
	}

	seq := int64(binary.BigEndian.Uint64(b[msgSequenceNumberOff:])) //nolint:gosec // two's complement on the wire

	m := &StreamMessage{
		MessageType: strings.Trim(
			strings.Trim(string(b[msgMessageTypeOff:msgMessageTypeOff+msgMessageTypeLen]), "\x00"),
			" ",
		),
		SchemaVersion:  binary.BigEndian.Uint32(b[msgSchemaVersionOff:]),
		CreatedDate:    binary.BigEndian.Uint64(b[msgCreatedDateOff:]),
		SequenceNumber: seq,
		Flags:          binary.BigEndian.Uint64(b[msgFlagsOff:]),
		PayloadType:    binary.BigEndian.Uint32(b[msgPayloadTypeOff:]),
		Payload:        payload,
	}

	copy(m.MessageID[msgUUIDHalf:], b[msgMessageIDOff:msgMessageIDOff+msgUUIDHalf])
	copy(m.MessageID[:msgUUIDHalf], b[msgMessageIDOff+msgUUIDHalf:msgMessageIDOff+msgUUIDHalf*2])

	return m, nil
}

// OpenDataChannelInput is the first (text) frame a client sends (communicator/websocketchannel.go).
type OpenDataChannelInput struct {
	MessageSchemaVersion string `json:"MessageSchemaVersion"`
	RequestID            string `json:"RequestId"`
	TokenValue           string `json:"TokenValue"`
	ClientID             string `json:"ClientId"`
	ClientVersion        string `json:"ClientVersion"`
}

// AckPayload is the body of an acknowledge message (datachannel/streaming.go AcknowledgeContent).
type AckPayload struct {
	MessageType         string `json:"AcknowledgedMessageType"`
	MessageID           string `json:"AcknowledgedMessageId"`
	SequenceNumber      int64  `json:"AcknowledgedMessageSequenceNumber"`
	IsSequentialMessage bool   `json:"IsSequentialMessage"`
}

// ChannelClosedPayload is the body of a channel_closed message.
type ChannelClosedPayload struct {
	MessageType   string `json:"MessageType"`
	MessageID     string `json:"MessageId"`
	DateTime      string `json:"CreatedDate"`
	SessionID     string `json:"SessionId"`
	Output        string `json:"Output"`
	SchemaVersion int    `json:"SchemaVersion"`
}

// HandshakeRequestPayload is the agent's first output payload (PayloadHandshakeRequest).
type HandshakeRequestPayload struct {
	AgentVersion           string                  `json:"AgentVersion"`
	RequestedClientActions []RequestedClientAction `json:"RequestedClientActions"`
}

// RequestedClientAction asks the client to perform one handshake action.
type RequestedClientAction struct {
	ActionParameters map[string]any `json:"ActionParameters"`
	ActionType       string         `json:"ActionType"`
}

// HandshakeResponsePayload is the client's reply (PayloadHandshakeResponse).
type HandshakeResponsePayload struct {
	ClientVersion          string                  `json:"ClientVersion"`
	ProcessedClientActions []ProcessedClientAction `json:"ProcessedClientActions"`
	Errors                 []string                `json:"Errors"`
}

// ProcessedClientAction reports the outcome of one requested action.
type ProcessedClientAction struct {
	ActionResult any    `json:"ActionResult"`
	ActionType   string `json:"ActionType"`
	Error        string `json:"Error"`
	ActionStatus int    `json:"ActionStatus"`
}

// HandshakeCompletePayload ends the handshake (PayloadHandshakeComplete).
type HandshakeCompletePayload struct {
	CustomerMessage         string `json:"CustomerMessage"`
	HandshakeTimeToComplete int64  `json:"HandshakeTimeToComplete"`
}

// SizePayload is a terminal resize (PayloadSize).
type SizePayload struct {
	Cols uint `json:"cols"`
	Rows uint `json:"rows"`
}

const (
	actionTypeSessionType = "SessionType"
	sessionTypeStandard   = "Standard_Stream"
	sessionTypePort       = "Port"
	actionStatusSuccess   = 1
	agentVersion          = "3.3.0.0"
)
