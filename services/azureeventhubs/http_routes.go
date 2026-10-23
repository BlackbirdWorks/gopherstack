package azureeventhubs

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/amqp1"
)

const (
	shortMessagesPath = 2
	longMessagesPath  = 4
)

var errBadPath = errors.New("azureeventhubs: unrecognized path")

type segKind int

const (
	segHub segKind = iota
	segHubList
	segGroupList
	segGroup
	segMessages
)

type request struct {
	hub       string
	group     string
	partition int
	kind      segKind
}

func parsePath(p string) (request, error) {
	parts := splitPath(p)
	req := request{partition: -1}

	switch {
	case len(parts) == 2 && parts[0] == "$Resources" && strings.EqualFold(parts[1], "EventHubs"):
		req.kind = segHubList
	case len(parts) == 1:
		req.hub = parts[0]
	case len(parts) == 2 && strings.EqualFold(parts[1], "consumergroups"):
		req.hub, req.kind = parts[0], segGroupList
	case len(parts) == 3 && strings.EqualFold(parts[1], "consumergroups"):
		req.hub, req.group, req.kind = parts[0], parts[2], segGroup
	case len(parts) >= 2 && strings.EqualFold(parts[len(parts)-1], "messages"):
		return messagesRequest(parts)
	default:
		return request{}, errBadPath
	}

	return req, nil
}

func messagesRequest(parts []string) (request, error) {
	req := request{hub: parts[0], kind: segMessages, partition: -1}

	switch {
	case len(parts) == shortMessagesPath:
	case len(parts) == longMessagesPath && strings.EqualFold(parts[1], "partitions"):
		n, err := strconv.Atoi(parts[2])
		if err != nil || n < 0 {
			return request{}, errBadPath
		}

		req.partition = n
	case len(parts) == longMessagesPath && strings.EqualFold(parts[1], "publishers"):
	default:
		return request{}, errBadPath
	}

	return req, nil
}

func operationFor(method string, req request) string {
	type key struct {
		method string
		kind   segKind
	}

	ops := map[key]string{
		{http.MethodPut, segHub}: opCreateHub, {http.MethodGet, segHub}: opGetHub,
		{http.MethodDelete, segHub}: opDeleteHub, {http.MethodGet, segHubList}: opListHubs,
		{http.MethodPut, segGroup}: opCreateConsumerGroup, {http.MethodGet, segGroup}: opGetConsumerGroup,
		{http.MethodDelete, segGroup}: opDeleteConsumerGroup, {http.MethodGet, segGroupList}: opListConsumerGroups,
		{http.MethodPost, segMessages}: opSendEvent,
	}

	if op, ok := ops[key{method, req.kind}]; ok {
		return op
	}

	return unknownOperation
}

// Handler returns the echo handler of the HTTP listener.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		c.Response().Header().Set("Server", "Microsoft-HTTPAPI/2.0")

		req, err := parsePath(c.Request().URL.Path)
		if err != nil {
			return writeError(
				c,
				http.StatusBadRequest,
				"The requested URI does not represent any resource on the server.",
			)
		}

		right := RightManage
		if req.kind == segMessages {
			right = RightSend
		}

		if !h.authorized(c.Request(), right) {
			return writeError(
				c,
				http.StatusUnauthorized,
				"The SAS token signature is invalid or lacks the required claim.",
			)
		}

		if req.kind == segMessages {
			return h.handleSend(c, req)
		}

		return h.handleAtom(c, req)
	}
}

func (h *Handler) authorized(r *http.Request, right string) bool {
	if !h.ValidateSAS {
		return true
	}

	auth := r.Header.Get("Authorization")
	if !strings.HasPrefix(auth, sasPrefix) {
		return false
	}

	rights, ok := h.Backend.claimRights(auth)

	return ok && hasRight(rights, right)
}

func writeError(c *echo.Context, status int, msg string) error {
	body, err := json.Marshal(map[string]string{"Code": http.StatusText(status), "Message": msg})
	if err != nil {
		return c.String(http.StatusInternalServerError, "internal error")
	}

	return c.Blob(status, "application/json", body)
}

type brokerProperties struct {
	PartitionKey string `json:"PartitionKey"`
}

type jsonEvent struct {
	UserProperties   map[string]any   `json:"UserProperties"`
	BrokerProperties brokerProperties `json:"BrokerProperties"`
	Body             string           `json:"Body"`
}

const batchContentType = "application/vnd.microsoft.servicebus.json"

func (h *Handler) handleSend(c *echo.Context, req request) error {
	if c.Request().Method != http.MethodPost {
		return writeError(c, http.StatusMethodNotAllowed, "The resource doesn't support the specified HTTP verb.")
	}

	ref, ok := h.Backend.ResolveHub(req.hub)
	if !ok {
		return writeError(c, http.StatusNotFound, "The messaging entity '"+req.hub+"' could not be found.")
	}

	body, err := io.ReadAll(io.LimitReader(c.Request().Body, MaxEventBytes+1))
	if err != nil || len(body) > MaxEventBytes {
		return writeError(c, http.StatusRequestEntityTooLarge, "The request body exceeds the maximum event size.")
	}

	evs, err := restEvents(c.Request(), body)
	if err != nil {
		return writeError(c, http.StatusBadRequest, err.Error())
	}

	if _, appendErr := h.Backend.Append(ref, req.partition, evs); appendErr != nil {
		return writeError(c, sendStatus(appendErr), appendErr.Error())
	}

	return c.NoContent(http.StatusCreated)
}

func sendStatus(err error) int {
	switch {
	case errors.Is(err, ErrEventTooLarge):
		return http.StatusRequestEntityTooLarge
	case errors.Is(err, ErrHubNotFound), errors.Is(err, ErrPartitionNotFound):
		return http.StatusNotFound
	}

	return http.StatusInternalServerError
}

func restEvents(r *http.Request, body []byte) ([]NewEvent, error) {
	ct := r.Header.Get("Content-Type")
	if strings.HasPrefix(ct, batchContentType) {
		return batchEvents(body)
	}

	key := ""

	if bp := r.Header.Get("Brokerproperties"); bp != "" {
		var props brokerProperties
		if err := json.Unmarshal([]byte(bp), &props); err != nil {
			return nil, err
		}

		key = props.PartitionKey
	}

	ev, err := restEvent(body, ct, key, nil)

	return []NewEvent{ev}, err
}

func batchEvents(body []byte) ([]NewEvent, error) {
	var items []jsonEvent
	if err := json.Unmarshal(body, &items); err != nil {
		var one jsonEvent
		if err2 := json.Unmarshal(body, &one); err2 != nil {
			return nil, err
		}

		items = []jsonEvent{one}
	}

	out := make([]NewEvent, 0, len(items))

	for _, it := range items {
		ev, err := restEvent([]byte(it.Body), "", it.BrokerProperties.PartitionKey, it.UserProperties)
		if err != nil {
			return nil, err
		}

		out = append(out, ev)
	}

	return out, nil
}

func restEvent(body []byte, contentType, key string, props map[string]any) (NewEvent, error) {
	m := &amqp1.Message{Data: [][]byte{body}, Properties: &amqp1.Properties{ContentType: contentType}}

	if len(props) > 0 {
		m.ApplicationProperties = amqp1.Map{}
		for k, v := range props {
			m.ApplicationProperties[k] = v
		}
	}

	if key != "" {
		m.Annotations = amqp1.Map{amqp1.Symbol(annPartitionKey): key}
	}

	raw, err := m.Encode()

	return NewEvent{Key: key, Raw: raw}, err
}
