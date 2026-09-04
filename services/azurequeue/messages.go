package azurequeue

import (
	"encoding/xml"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

// handleMessagesLevel serves the three /messages-scoped operations: Put
// Message (POST), Get/Peek Messages (GET, disambiguated by ?peekonly=true),
// and Clear Messages (DELETE).
func (h *Handler) handleMessagesLevel(c *echo.Context, queue string) error {
	switch c.Request().Method {
	case http.MethodPost:
		return h.putMessage(c, queue)
	case http.MethodGet:
		if c.QueryParam(queryPeekOnly) == "true" {
			return h.peekMessages(c, queue)
		}

		return h.getMessages(c, queue)
	case http.MethodDelete:
		return h.clearMessages(c, queue)
	default:
		return h.writeError(c, http.StatusMethodNotAllowed, "UnsupportedHttpVerb",
			"The resource doesn't support the specified HTTP verb.")
	}
}

// handleMessageLevel serves the two /messages/<id>-scoped operations: Delete
// Message (DELETE) and Update Message (PUT).
func (h *Handler) handleMessageLevel(c *echo.Context, queue, messageID string) error {
	switch c.Request().Method {
	case http.MethodDelete:
		return h.deleteMessage(c, queue, messageID)
	case http.MethodPut:
		return h.updateMessage(c, queue, messageID)
	default:
		return h.writeError(c, http.StatusMethodNotAllowed, "UnsupportedHttpVerb",
			"The resource doesn't support the specified HTTP verb.")
	}
}

func (h *Handler) putMessage(c *echo.Context, queue string) error {
	r := c.Request()

	ttl, ok, errResp := h.parseMessageTTL(c)
	if !ok {
		return errResp
	}

	// Put Message defaults visibilitytimeout to 0 (immediately visible), not
	// Get Messages' DefaultVisibilityTimeout -- a freshly-enqueued message
	// with no caller-specified delay must be dequeuable right away. Its
	// upper bound is also checked against ttl: a message can never be
	// created invisible for as long as, or longer than, its own TTL.
	visibilityTimeout, ok, errResp := h.parseVisibilityTimeoutSeconds(
		c, queryVisibilityTimeout, 0, 0, maxVisibilityTimeoutSeconds,
	)
	if !ok {
		return errResp
	}

	if visibilityTimeout >= ttl {
		return h.writeError(c, http.StatusBadRequest, "OutOfRangeQueryParameterValue",
			"The visibilitytimeout must be less than the message's time-to-live.")
	}

	body, err := httputils.ReadBody(r)
	if err != nil {
		return h.writeError(c, http.StatusInternalServerError, "InternalError", "Failed to read request body.")
	}

	var reqBody putMessageBody
	if len(body) > 0 {
		if unmarshalErr := xml.Unmarshal(body, &reqBody); unmarshalErr != nil {
			return h.writeError(c, http.StatusBadRequest, "InvalidXmlDocument",
				"XML specified is not syntactically valid.")
		}
	}

	info, err := h.Backend.PutMessage(queue, reqBody.MessageText, visibilityTimeout, ttl)
	if err != nil {
		return h.writeQueueNotFoundError(c)
	}

	return h.writeXML(c, http.StatusCreated, queueMessagesList{
		Messages: []queueMessageXML{messageXML(info, true, false)},
	})
}

func (h *Handler) getMessages(c *echo.Context, queue string) error {
	numOfMessages, ok, errResp := h.parseNumOfMessages(c)
	if !ok {
		return errResp
	}

	// Unlike Put/Update, Get Messages requires a strictly positive
	// visibilitytimeout (a dequeued message must actually be hidden for at
	// least one second) -- 0 is rejected, not defaulted.
	visibilityTimeout, ok, errResp := h.parseVisibilityTimeoutSeconds(
		c, queryVisibilityTimeout, DefaultVisibilityTimeout, 1, maxVisibilityTimeoutSeconds,
	)
	if !ok {
		return errResp
	}

	infos, err := h.Backend.GetMessages(queue, numOfMessages, visibilityTimeout)
	if err != nil {
		return h.writeMessagesListError(c, err)
	}

	entries := make([]queueMessageXML, 0, len(infos))
	for _, info := range infos {
		entries = append(entries, messageXML(info, true, true))
	}

	return h.writeXML(c, http.StatusOK, queueMessagesList{Messages: entries})
}

func (h *Handler) peekMessages(c *echo.Context, queue string) error {
	numOfMessages, ok, errResp := h.parseNumOfMessages(c)
	if !ok {
		return errResp
	}

	infos, err := h.Backend.PeekMessages(queue, numOfMessages)
	if err != nil {
		return h.writeMessagesListError(c, err)
	}

	entries := make([]queueMessageXML, 0, len(infos))
	for _, info := range infos {
		entries = append(entries, messageXML(info, false, true))
	}

	return h.writeXML(c, http.StatusOK, queueMessagesList{Messages: entries})
}

// writeMessagesListError maps a Get/Peek Messages backend error to the
// corresponding Azure error code/status. ErrOutOfRangeQueryParam is only
// reachable here as a defense-in-depth backstop (see StorageBackend's
// GetMessages/PeekMessages doc comments) -- the handler's own
// parseNumOfMessages already rejects an out-of-range numofmessages before
// the backend is ever called.
func (h *Handler) writeMessagesListError(c *echo.Context, err error) error {
	if errors.Is(err, ErrOutOfRangeQueryParam) {
		return h.writeError(c, http.StatusBadRequest, "OutOfRangeQueryParameterValue",
			"A query parameter specified in the request URI is outside the permissible range.")
	}

	return h.writeQueueNotFoundError(c)
}

func (h *Handler) clearMessages(c *echo.Context, queue string) error {
	if err := h.Backend.ClearMessages(queue); err != nil {
		return h.writeQueueNotFoundError(c)
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) deleteMessage(c *echo.Context, queue, messageID string) error {
	popReceipt := c.QueryParam(queryPopReceipt)
	if popReceipt == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidQueryParameterValue",
			"A required query parameter (popreceipt) was not specified.")
	}

	err := h.Backend.DeleteMessage(queue, messageID, popReceipt)

	return h.writeMessageOpError(c, err, http.StatusNoContent)
}

func (h *Handler) updateMessage(c *echo.Context, queue, messageID string) error {
	popReceipt := c.QueryParam(queryPopReceipt)
	if popReceipt == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidQueryParameterValue",
			"A required query parameter (popreceipt) was not specified.")
	}

	if c.QueryParam(queryVisibilityTimeout) == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidQueryParameterValue",
			"A required query parameter (visibilitytimeout) was not specified.")
	}

	// Update Message's visibilitytimeout is mandatory (checked above), so the
	// default value passed here is never actually used; range is 0..604800,
	// same as Put Message but with no TTL comparison (Update does not change
	// a message's TTL).
	visibilityTimeout, ok, errResp := h.parseVisibilityTimeoutSeconds(
		c, queryVisibilityTimeout, 0, 0, maxVisibilityTimeoutSeconds,
	)
	if !ok {
		return errResp
	}

	var text *string

	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return h.writeError(c, http.StatusInternalServerError, "InternalError", "Failed to read request body.")
	}

	if len(body) > 0 {
		var reqBody putMessageBody
		if unmarshalErr := xml.Unmarshal(body, &reqBody); unmarshalErr != nil {
			return h.writeError(c, http.StatusBadRequest, "InvalidXmlDocument",
				"XML specified is not syntactically valid.")
		}

		text = &reqBody.MessageText
	}

	info, err := h.Backend.UpdateMessage(queue, messageID, popReceipt, visibilityTimeout, text)
	if err != nil {
		return h.writeMessageOpError(c, err, http.StatusNoContent)
	}

	hdr := c.Response().Header()
	hdr.Set("X-Ms-Popreceipt", info.PopReceipt)
	hdr.Set("X-Ms-Time-Next-Visible", info.TimeNextVisible.Format(http.TimeFormat))

	return c.NoContent(http.StatusNoContent)
}

// writeMessageOpError maps a Delete/Update Message backend error to the
// corresponding Azure error code/status, or (err == nil) writes a bare
// successStatus response.
func (h *Handler) writeMessageOpError(c *echo.Context, err error, successStatus int) error {
	switch {
	case err == nil:
		return c.NoContent(successStatus)
	case errors.Is(err, ErrQueueNotFound):
		return h.writeQueueNotFoundError(c)
	case errors.Is(err, ErrMessageNotFound):
		return h.writeError(c, http.StatusNotFound, "MessageNotFound", "The specified message does not exist.")
	case errors.Is(err, ErrPopReceiptMismatch):
		return h.writeError(c, http.StatusBadRequest, "PopReceiptMismatch",
			"The specified pop receipt did not match the pop receipt for a dequeued message.")
	default:
		return h.writeError(c, http.StatusInternalServerError, "InternalError", err.Error())
	}
}

// messageXML converts a MessageInfo into its wire shape. includePopReceipt
// mirrors StorageBackend's own includePopReceipt distinction (false for
// Peek Messages); includeDequeueCount is false for Put Message's response,
// which never echoes DequeueCount (a brand-new message has none) or the
// message body back.
func messageXML(info MessageInfo, includePopReceipt, includeDequeueCount bool) queueMessageXML {
	entry := queueMessageXML{
		MessageID:      info.ID,
		InsertionTime:  info.InsertionTime.Format(http.TimeFormat),
		ExpirationTime: info.ExpirationTime.Format(http.TimeFormat),
	}

	if includePopReceipt {
		entry.PopReceipt = info.PopReceipt
		entry.TimeNextVisible = info.TimeNextVisible.Format(http.TimeFormat)
	}

	if includeDequeueCount {
		dequeueCount := info.DequeueCount
		entry.DequeueCount = &dequeueCount
		entry.MessageText = info.Text
	}

	return entry
}

// parseNumOfMessages parses and range-checks the numofmessages query
// parameter, defaulting to 1 when absent. ok is false if an error response
// has already been written to c.
func (h *Handler) parseNumOfMessages(c *echo.Context) (int, bool, error) {
	raw := c.QueryParam(queryNumOfMessages)
	if raw == "" {
		return MinNumOfMessages, true, nil
	}

	n, err := strconv.Atoi(raw)
	if err != nil {
		return 0, false, h.writeError(c, http.StatusBadRequest, "InvalidQueryParameterValue",
			"Value for one of the query parameters specified in the request URI is invalid.")
	}

	if n < MinNumOfMessages || n > MaxNumOfMessages {
		return 0, false, h.writeError(c, http.StatusBadRequest, "OutOfRangeQueryParameterValue",
			"A query parameter specified in the request URI is outside the permissible range.")
	}

	return n, true, nil
}

// neverExpireTTL is used when a Put Message caller passes messagettl=-1,
// Azure's documented "never expire" sentinel. True infinite retention isn't
// modeled (see PARITY.md known gaps); a 100-year TTL is used as a practical
// stand-in so such messages are never spuriously swept by the janitor.
const neverExpireTTL = 100 * 365 * 24 * time.Hour

// parseMessageTTL parses the messagettl query parameter for Put Message,
// defaulting to DefaultMessageTTL when absent and special-casing Azure's
// documented messagettl=-1 "never expire" sentinel (see neverExpireTTL). Any
// other negative value, or zero, is rejected as out of range: real Azure
// Queue Storage only accepts -1 or a strictly positive TTL -- a zero TTL
// would create a message that expires the instant it's created. ok is false
// if an error response has already been written to c.
func (h *Handler) parseMessageTTL(c *echo.Context) (time.Duration, bool, error) {
	raw := c.QueryParam(queryMessageTTL)
	if raw == "" {
		return DefaultMessageTTL, true, nil
	}

	n, err := strconv.Atoi(raw)
	if err != nil {
		return 0, false, h.writeError(c, http.StatusBadRequest, "InvalidQueryParameterValue",
			"Value for one of the query parameters specified in the request URI is invalid.")
	}

	switch {
	case n == -1:
		return neverExpireTTL, true, nil
	case n <= 0:
		return 0, false, h.writeError(c, http.StatusBadRequest, "OutOfRangeQueryParameterValue",
			"A query parameter specified in the request URI is outside the permissible range.")
	default:
		return time.Duration(n) * time.Second, true, nil
	}
}

// maxVisibilityTimeoutSeconds is the largest visibilitytimeout (in seconds)
// any operation accepts, matching real Azure Queue Storage's documented
// seven-day cap. It happens to equal DefaultMessageTTL expressed in seconds.
const maxVisibilityTimeoutSeconds = 7 * 24 * 60 * 60

// parseVisibilityTimeoutSeconds parses the visibilitytimeout query parameter
// as an integer number of seconds in [minSeconds, maxSeconds], returning def
// if the parameter is absent. The permitted range differs per operation (see
// callers): Get Messages requires a strictly positive value, while Put and
// Update Message permit zero. ok is false if an error response has already
// been written to c.
func (h *Handler) parseVisibilityTimeoutSeconds(
	c *echo.Context, name string, def time.Duration, minSeconds, maxSeconds int,
) (time.Duration, bool, error) {
	raw := c.QueryParam(name)
	if raw == "" {
		return def, true, nil
	}

	n, err := strconv.Atoi(raw)
	if err != nil {
		return 0, false, h.writeError(c, http.StatusBadRequest, "InvalidQueryParameterValue",
			"Value for one of the query parameters specified in the request URI is invalid.")
	}

	if n < minSeconds || n > maxSeconds {
		return 0, false, h.writeError(c, http.StatusBadRequest, "OutOfRangeQueryParameterValue",
			"A query parameter specified in the request URI is outside the permissible range.")
	}

	return time.Duration(n) * time.Second, true, nil
}
