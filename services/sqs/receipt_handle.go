package sqs

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"strings"
)

const (
	receiptHandleVersion = 1
	receiptHandleMACLen  = 16
	receiptHandleTagLen  = 8
	receiptHandleKeyLen  = 32
)

// receiptHandleCodec issues and verifies opaque, HMAC-signed receipt handles.
// The key is per-process: handles persisted in a snapshot stay valid through
// the exact-match in-flight index, only superseded handles do not survive a restart.
type receiptHandleCodec struct {
	key []byte
}

func newReceiptHandleCodec() *receiptHandleCodec {
	key := make([]byte, receiptHandleKeyLen)
	if _, err := rand.Read(key); err != nil {
		panic("sqs: cannot read random bytes for receipt handle key: " + err.Error())
	}

	return &receiptHandleCodec{key: key}
}

func (c *receiptHandleCodec) queueTag(q *Queue) []byte {
	sum := sha256.Sum256([]byte(queueKey(q.Region, q.Name)))

	return sum[:receiptHandleTagLen]
}

func (c *receiptHandleCodec) mac(payload []byte) []byte {
	h := hmac.New(sha256.New, c.key)
	h.Write(payload)

	return h.Sum(nil)[:receiptHandleMACLen]
}

// issue builds the handle for receive number attempt of messageID on q.
func (c *receiptHandleCodec) issue(q *Queue, messageID string, attempt int) string {
	payload := make([]byte, 0, 1+receiptHandleTagLen+binary.MaxVarintLen64*2+len(messageID)+receiptHandleMACLen)
	payload = append(payload, receiptHandleVersion)
	payload = append(payload, c.queueTag(q)...)
	payload = binary.AppendUvarint(payload, uint64(attempt)) //nolint:gosec // attempt is a positive receive count
	payload = binary.AppendUvarint(payload, uint64(len(messageID)))
	payload = append(payload, messageID...)
	payload = append(payload, c.mac(payload)...)

	return base64.RawURLEncoding.EncodeToString(payload)
}

// parse verifies handle's signature and queue binding and returns the
// message ID and receive attempt it encodes.
func (c *receiptHandleCodec) parse(q *Queue, handle string) (string, int, bool) {
	raw, err := base64.RawURLEncoding.DecodeString(handle)
	if err != nil || len(raw) < 1+receiptHandleTagLen+2+receiptHandleMACLen {
		return "", 0, false
	}

	payload, sig := raw[:len(raw)-receiptHandleMACLen], raw[len(raw)-receiptHandleMACLen:]
	if !hmac.Equal(sig, c.mac(payload)) || payload[0] != receiptHandleVersion {
		return "", 0, false
	}

	if !hmac.Equal(payload[1:1+receiptHandleTagLen], c.queueTag(q)) {
		return "", 0, false
	}

	rest := payload[1+receiptHandleTagLen:]

	attempt, n := binary.Uvarint(rest)
	if n <= 0 {
		return "", 0, false
	}

	rest = rest[n:]

	idLen, n := binary.Uvarint(rest)
	if n <= 0 || idLen != uint64(len(rest)-n) { //nolint:gosec // len is non-negative
		return "", 0, false
	}

	id := string(rest[n:])
	if strings.TrimSpace(id) == "" {
		return "", 0, false
	}

	return id, int(attempt), true //nolint:gosec // bounded by the receive count that was encoded
}

type handleStatus int

const (
	handleInvalid handleStatus = iota
	handleCurrent
	handleStale
	handleNotInflight
)

// lookup resolves handle against q's in-flight messages. Caller must hold q.mu.
// handleStale means the handle is genuine but a later receive superseded it;
// handleNotInflight means it is genuine but its message is not in flight.
func (c *receiptHandleCodec) lookup(q *Queue, handle string) (*InFlightMessage, string, handleStatus) {
	if inf, ok := q.inFlightByHandle[handle]; ok {
		return inf, inf.Msg.MessageID, handleCurrent
	}

	id, attempt, ok := c.parse(q, handle)
	if !ok {
		return nil, "", handleInvalid
	}

	for _, inf := range q.inFlightMessages {
		if inf.Msg.MessageID != id {
			continue
		}

		if inf.Msg.ApproximateReceiveCount == attempt {
			return inf, id, handleCurrent
		}

		return inf, id, handleStale
	}

	return nil, id, handleNotInflight
}

// removeVisibleMessage drops the visible (not in-flight) message id from q. Caller must hold q.mu.
func removeVisibleMessage(q *Queue, id string) bool {
	for i, msg := range q.messages {
		if msg.MessageID != id {
			continue
		}

		copy(q.messages[i:], q.messages[i+1:])
		q.messages[len(q.messages)-1] = nil
		q.messages = q.messages[:len(q.messages)-1]

		return true
	}

	return false
}
