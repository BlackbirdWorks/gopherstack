package azureservicebus

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/textproto"
	"strconv"
	"strings"

	"github.com/labstack/echo/v5"
)

// Auto dead-letter reasons real Service Bus stamps on system dead-lettering.
const (
	ReasonMaxDeliveryCountExceeded = "MaxDeliveryCountExceeded"
	ReasonTTLExpired               = "TTLExpiredException"

	descMaxDeliveryCountExceeded = "Message could not be consumed after maximum delivery attempts."
	descTTLExpired               = "The message expired and was dead lettered."

	defaultPeekCount = 1
	maxPeekCount     = 256
)

func isApplicationHeader(name string) bool {
	switch name {
	case "Authorization", "Brokerproperties", "Content-Type", "Content-Length", "Content-Encoding",
		"Host", "User-Agent", "Accept", "Accept-Encoding", "Accept-Language", "Connection", "Expect",
		"Transfer-Encoding", "Date", "Cookie", "Origin", "Referer", "Traceparent", "Tracestate":
		return false
	}

	return !strings.HasPrefix(name, "X-") && !strings.HasPrefix(name, "Sec-")
}

// customHeadersFrom extracts application properties from request headers.
func customHeadersFrom(h http.Header) map[string]string {
	var out map[string]string

	for name, vals := range h {
		if len(vals) == 0 || !isApplicationHeader(textproto.CanonicalMIMEHeaderKey(name)) {
			continue
		}

		if out == nil {
			out = make(map[string]string)
		}

		out[name] = vals[0]
	}

	return out
}

func setCustomHeaders(hdr http.Header, info MessageInfo) {
	for k, v := range info.CustomHeaders {
		hdr.Set(k, v)
	}

	if info.DeadLetterReason != "" {
		hdr.Set("Deadletterreason", info.DeadLetterReason)
	}

	if info.DeadLetterErrorDescription != "" {
		hdr.Set("Deadlettererrordescription", info.DeadLetterErrorDescription)
	}
}

func brokerPropertiesFor(info MessageInfo) brokerProperties {
	return brokerProperties{
		MessageID:                  info.MessageID,
		CorrelationID:              info.CorrelationID,
		SessionID:                  info.SessionID,
		Label:                      info.Label,
		ReplyTo:                    info.ReplyTo,
		EnqueuedTimeUtc:            info.EnqueuedTime.UTC().Format(http.TimeFormat),
		SequenceNumber:             info.SequenceNumber,
		DeliveryCount:              info.DeliveryCount,
		DeadLetterReason:           info.DeadLetterReason,
		DeadLetterErrorDescription: info.DeadLetterErrorDescription,
	}
}

// deadLetterRequest reports whether a PUT on a locked message asks for
// dead-lettering: BrokerProperties carrying DeadLetterReason or
// DeadLetterErrorDescription. Emulator extension; the REST reference
// documents no body on Unlock.
func deadLetterRequest(r *http.Request) (string, string, bool) {
	bp := brokerPropertiesFromHeader(r)

	return bp.DeadLetterReason, bp.DeadLetterErrorDescription,
		bp.DeadLetterReason != "" || bp.DeadLetterErrorDescription != ""
}

func (h *Handler) deadLetterMessage(c *echo.Context, req parsedRequest, reason, desc string) error {
	if req.DeadLetter {
		return h.writeLockOpError(c, ErrDeadLetterSource)
	}

	if err := h.Backend.DeadLetter(entityRefFor(req), req.MessageID, req.LockToken, reason, desc); err != nil {
		return h.writeLockOpError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

// handleReceiveAndDelete serves DELETE .../messages/head.
func (h *Handler) handleReceiveAndDelete(c *echo.Context, req parsedRequest) error {
	if err := h.checkEntityExists(req); err != nil {
		return h.writeEntityLookupError(c, err)
	}

	info, err := h.Backend.ReceiveAndDelete(entityRefFor(req), req.DeadLetter)
	if err != nil {
		if errors.Is(err, ErrMessageNotFound) {
			return c.NoContent(http.StatusNoContent)
		}

		return h.writeEntityLookupError(c, err)
	}

	return h.writeReceivedMessage(c, info)
}

func (h *Handler) writeReceivedMessage(c *echo.Context, info MessageInfo) error {
	bpJSON, err := json.Marshal(brokerPropertiesFor(info))
	if err != nil {
		return h.writeError(c, http.StatusInternalServerError, "InternalError", "Failed to marshal BrokerProperties.")
	}

	hdr := c.Response().Header()
	hdr.Set(brokerPropertiesHeader, string(bpJSON))
	hdr.Set("X-Ms-Request-Id", newRequestID())
	setCustomHeaders(hdr, info)

	contentType := info.ContentType
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	return c.Blob(http.StatusOK, contentType, info.Body)
}

// peekedMessage is one element of the GET .../messages response array.
type peekedMessage struct {
	CustomProperties map[string]string `json:"CustomProperties,omitempty"`
	ContentType      string            `json:"ContentType,omitempty"`
	Body             []byte            `json:"Body"`
	BrokerProperties brokerProperties  `json:"BrokerProperties"`
}

// handlePeekMessages serves GET .../messages?fromSequenceNumber=&maxMessages=:
// a non-locking peek (emulator extension; the SDK peeks over AMQP).
func (h *Handler) handlePeekMessages(c *echo.Context, req parsedRequest) error {
	if err := h.checkEntityExists(req); err != nil {
		return h.writeEntityLookupError(c, err)
	}

	from, _ := strconv.ParseInt(c.QueryParam("fromSequenceNumber"), 10, 64)
	count, err := strconv.Atoi(c.QueryParam("maxMessages"))

	if err != nil || count < 1 {
		count = defaultPeekCount
	}

	count = min(count, maxPeekCount)

	infos, err := h.Backend.Peek(entityRefFor(req), req.DeadLetter, from, count)
	if err != nil {
		return h.writeEntityLookupError(c, err)
	}

	out := make([]peekedMessage, 0, len(infos))

	for _, info := range infos {
		props := info.CustomHeaders
		out = append(out, peekedMessage{
			BrokerProperties: brokerPropertiesFor(info),
			ContentType:      info.ContentType,
			Body:             info.Body,
			CustomProperties: props,
		})
	}

	return c.JSON(http.StatusOK, out)
}
