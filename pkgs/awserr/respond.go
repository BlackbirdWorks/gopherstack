package awserr

import (
	"encoding/json"
	"encoding/xml"
	"errors"
	"hash/crc32"
	"net/http"
	"strconv"

	"github.com/labstack/echo/v5"
)

// Protocol identifies the AWS wire protocol used to encode an error envelope.
// Different AWS services speak different protocols, so the same logical error
// (an [APIError]) must be serialised differently depending on the service.
type Protocol int

const (
	// ProtocolJSON10 is the AWS JSON 1.0 protocol (e.g. DynamoDB). Errors are
	// encoded as {"__type":"<Code>","message":"<Message>"} with a CRC32 header.
	ProtocolJSON10 Protocol = iota
	// ProtocolJSON11 is the AWS JSON 1.1 protocol (e.g. Kinesis, many newer
	// services). Same envelope as JSON 1.0 with a different Content-Type.
	ProtocolJSON11
	// ProtocolQueryXML is the AWS Query protocol (e.g. classic SQS/SNS/EC2).
	// Errors are wrapped in <ErrorResponse><Error>...</Error></ErrorResponse>.
	ProtocolQueryXML
	// ProtocolRestXML is the AWS REST-XML protocol (e.g. S3). Errors are
	// encoded as a bare <Error>...</Error> document.
	ProtocolRestXML
)

// APIError is a protocol-agnostic description of an AWS API error. Services
// declare a table mapping their wrapped sentinel errors (see [New]) to an
// APIError, then call [Write] to serialise it for the wire.
type APIError struct {
	// Code is the AWS error code (e.g. "ResourceNotFoundException"). For the
	// JSON protocols it may include the Coral namespace prefix
	// (e.g. "com.amazonaws.dynamodb.v20120810#ResourceNotFoundException").
	Code string
	// Message is the human-readable error message.
	Message string
	// HTTPStatus is the HTTP status code to return (e.g. 400, 404, 409).
	HTTPStatus int
}

// Error implements the error interface so an APIError can itself be returned
// and matched with [errors.As].
func (e APIError) Error() string { return e.Code + ": " + e.Message }

// Classify maps err to an [APIError] using table. Each table key is a sentinel
// (or wrapped sentinel) and is matched against err with [errors.Is], so callers
// can wrap sentinels with [New]/[Newf] and still classify them here. The first
// matching entry wins; if nothing matches, fallback is returned (typically a
// 500 InternalFailure). The matched entry's Message is overridden with err's
// message when err carries a more specific one via [New]/[Newf].
func Classify(err error, table map[error]APIError, fallback APIError) APIError {
	if err == nil {
		return fallback
	}

	for sentinel, apiErr := range table {
		if errors.Is(err, sentinel) {
			// Prefer the concrete error message when the service wrapped a
			// sentinel with a specific message via New/Newf.
			if msg := err.Error(); msg != "" && msg != sentinel.Error() {
				apiErr.Message = msg
			}

			return apiErr
		}
	}

	return fallback
}

// jsonErrorEnvelope is the shared shape for the AWS JSON protocols.
type jsonErrorEnvelope struct {
	Type    string `json:"__type"`
	Message string `json:"message"`
}

// restXMLError is the S3-style bare <Error> document.
type restXMLError struct {
	XMLName   xml.Name `xml:"Error"`
	Code      string   `xml:"Code"`
	Message   string   `xml:"Message"`
	RequestID string   `xml:"RequestId"`
}

// queryErrorResponse is the Query-protocol <ErrorResponse> wrapper.
type queryErrorResponse struct {
	XMLName   xml.Name `xml:"ErrorResponse"`
	Type      string   `xml:"Error>Type"`
	Code      string   `xml:"Error>Code"`
	Message   string   `xml:"Error>Message"`
	RequestID string   `xml:"RequestId"`
}

// Write serialises e onto c using the envelope for proto, setting the
// appropriate Content-Type (and X-Amz-Crc32 for the JSON protocols) and the
// HTTP status from e.HTTPStatus. It returns the echo error from the underlying
// write so handlers can `return awserr.Write(...)` directly.
func Write(c *echo.Context, proto Protocol, e APIError) error {
	switch proto {
	case ProtocolJSON10:
		return writeJSON(c, "application/x-amz-json-1.0", e)
	case ProtocolJSON11:
		return writeJSON(c, "application/x-amz-json-1.1", e)
	case ProtocolQueryXML:
		return writeXML(c, queryErrorResponse{
			Type:      "Sender",
			Code:      e.Code,
			Message:   e.Message,
			RequestID: requestID(c),
		}, e.HTTPStatus)
	case ProtocolRestXML:
		return writeXML(c, restXMLError{
			Code:      e.Code,
			Message:   e.Message,
			RequestID: requestID(c),
		}, e.HTTPStatus)
	default:
		return writeJSON(c, "application/x-amz-json-1.0", e)
	}
}

func writeJSON(c *echo.Context, contentType string, e APIError) error {
	body, _ := json.Marshal(jsonErrorEnvelope{Type: e.Code, Message: e.Message})
	checksum := crc32.ChecksumIEEE(body)
	c.Response().Header().Set("X-Amz-Crc32", strconv.FormatUint(uint64(checksum), 10))
	c.Response().Header().Set("Content-Type", contentType)

	return c.JSONBlob(e.HTTPStatus, body)
}

func writeXML(c *echo.Context, payload any, status int) error {
	body, err := xml.Marshal(payload)
	if err != nil {
		return c.NoContent(http.StatusInternalServerError)
	}
	out := append([]byte(xml.Header), body...)
	c.Response().Header().Set("Content-Type", "application/xml")

	return c.XMLBlob(status, out)
}

// requestID returns the request's amazon request id header if present, so the
// echoed <RequestId> matches what the SDK sent.
func requestID(c *echo.Context) string {
	if c == nil || c.Request() == nil {
		return ""
	}
	if id := c.Request().Header.Get("x-amzn-RequestId"); id != "" {
		return id
	}

	return c.Response().Header().Get("x-amzn-RequestId")
}
