package gremlin

import (
	"bytes"
	"encoding/json"
	"strings"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
)

const uuidBytes = 16

// FrameRequest is a client request frame inspected without being executed.
type FrameRequest struct {
	ID     string
	Access Access
	codec  codec
}

// ClassifyFrame parses a client request frame (GraphSON text or mime-prefixed binary) and reports the access its
// script or bytecode needs. Frames that cannot be understood, or ops other than eval, bytecode, close and
// authentication, yield an error and a FrameRequest carrying whatever correlation could be recovered.
func ClassifyFrame(messageType int, data []byte) (FrameRequest, error) {
	if messageType != websocket.TextMessage && messageType != websocket.BinaryMessage {
		return FrameRequest{ID: uuid.Nil.String()}, invalidArg("Unsupported WebSocket frame type")
	}

	msg, err := parseFrame(messageType, data)
	if err != nil {
		return recoverFrame(messageType, data), err
	}

	req := FrameRequest{ID: msg.id, codec: msg.codec}

	switch msg.op {
	case opEval:
		script, ok := msg.args["gremlin"].(string)
		if !ok || strings.TrimSpace(script) == "" {
			return req, invalidArg("A message with an [eval] op code requires a [gremlin] argument.")
		}

		req.Access = ScriptAccess(script)
	case opBytecode:
		tr, ok := msg.args["gremlin"].(*Traversal)
		if !ok {
			return req, invalidArg("A message with a [bytecode] op code requires a [gremlin] argument of bytecode.")
		}

		req.Access = tr.Access()
	case "close", "authentication":
	default:
		return req, invalidArg("Message with op code [%s] is not recognized.", msg.op)
	}

	return req, nil
}

// recoverFrame extracts the request id and codec from a frame that failed to parse, best effort.
func recoverFrame(messageType int, data []byte) FrameRequest {
	req := FrameRequest{ID: uuid.Nil.String()}

	body := data

	if messageType == websocket.BinaryMessage {
		var mime string

		mime, body = splitMime(data)
		if mime == mimeGraphBinary {
			return recoverBinary(req, body)
		}

		if mime == mimeGraphSONv2 {
			req.codec = codecV2
		}
	}

	var raw struct {
		RequestID string `json:"requestId"`
	}

	if json.NewDecoder(bytes.NewReader(body)).Decode(&raw) == nil && raw.RequestID != "" {
		req.ID = raw.RequestID
	}

	return req
}

// DeniedFrame builds the response frame refusing req with err, in the codec the client used.
func DeniedFrame(req FrameRequest, err error) (int, []byte) {
	e := asGremlinError(err)

	return encodeFrame(&message{id: req.ID, codec: req.codec}, e.Status, errorMessage(req.ID, e), nil, false)
}

// DeniedError is the AccessDeniedException a client receives when detail explains the refusal.
func DeniedError(detail string) error {
	return &Error{Code: CodeUnauthorized, Status: StatusForbidden, Detail: detail}
}

func splitMime(data []byte) (string, []byte) {
	if len(data) == 0 || int(data[0])+1 > len(data) {
		return "", nil
	}

	n := int(data[0])

	return string(data[1 : 1+n]), data[1+n:]
}

func recoverBinary(req FrameRequest, body []byte) FrameRequest {
	if len(body) >= 1+uuidBytes && body[0] == gbVersion {
		req.ID = formatUUID(body[1 : 1+uuidBytes])
		req.codec = codecBinary
	}

	return req
}
