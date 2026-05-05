package iotdataplane

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	iotDPMatchPriority = 88
	// maxPublishBodyBytes limits the size of MQTT publish request bodies.
	maxPublishBodyBytes = 128 * 1024
	// maxShadowBodyBytes limits the size of shadow document request bodies.
	maxShadowBodyBytes = 8 * 1024
	// retainedMessagePath is the URL path prefix for retained message operations.
	retainedMessagePath = "/retainedMessage"
	// retainedMessagePathSlash is the prefix used to match individual topic paths.
	retainedMessagePathSlash = retainedMessagePath + "/"
	// listThingsWithShadowsPath is the admin path for listing things that have shadows.
	listThingsWithShadowsPath = "/api/things/shadow/ListThingsWithShadows"
	// listNamedShadowsPrefix is the prefix for ListNamedShadowsForThing.
	listNamedShadowsPrefix = "/api/things/shadow/ListNamedShadowsForThing/"
	// connectionsPath is the URL path for managing connections.
	connectionsPath = "/connections"
	// connectionsPathSlash is the prefix for individual connection operations.
	connectionsPathSlash = connectionsPath + "/"

	keyError            = "error"
	keyMessage          = "message"
	errMethodNotAllowed = "method not allowed"
	opUnknown           = "Unknown"

	// shadowPathParts is the number of parts when splitting a shadow URL on "/shadow".
	shadowPathParts = 2
)

// Handler is the Echo HTTP handler for IoT Data Plane operations.
type Handler struct {
	Backend StorageBackend
}

// NewHandler creates a new IoT Data Plane Handler.
func NewHandler(backend StorageBackend) *Handler {
	return &Handler{Backend: backend}
}

// Reset clears all handler state by delegating to the backend.
func (h *Handler) Reset() {
	h.Backend.Reset()
}

// Name returns the service name.
func (h *Handler) Name() string { return "IoTDataPlane" }

// GetSupportedOperations returns the list of supported operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"DeleteConnection",
		"DeleteThingShadow",
		"GetRetainedMessage",
		"GetThingShadow",
		"ListConnections",
		"ListNamedShadowsForThing",
		"ListRetainedMessages",
		"ListThingsWithShadows",
		"Publish",
		"RegisterConnection",
		"UpdateThingShadow",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "iotdata" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this IoT Data Plane instance handles.
func (h *Handler) ChaosRegions() []string { return []string{config.DefaultRegion} }

// RouteMatcher returns a function matching IoT Data Plane requests.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		path := c.Request().URL.Path

		return strings.HasPrefix(path, "/topics/") ||
			isShadowPath(path) ||
			strings.HasPrefix(path, listNamedShadowsPrefix) ||
			path == listThingsWithShadowsPath ||
			path == connectionsPath ||
			strings.HasPrefix(path, connectionsPathSlash) ||
			path == retainedMessagePath ||
			strings.HasPrefix(path, retainedMessagePathSlash)
	}
}

// isShadowPath returns true for paths of the form /things/{thingName}/shadow
// with no additional path segments after "shadow".
func isShadowPath(path string) bool {
	after, ok := strings.CutPrefix(path, "/things/")
	if !ok {
		return false
	}

	// Must end with exactly /shadow or /shadow?... (no further segments).
	parts := strings.SplitN(after, "/shadow", shadowPathParts)
	if len(parts) != shadowPathParts {
		return false
	}

	tail := parts[1]

	return tail == "" || strings.HasPrefix(tail, "?")
}

// MatchPriority returns the routing priority for the IoT Data Plane handler.
func (h *Handler) MatchPriority() int { return iotDPMatchPriority }

// extractConnectionOperation returns the operation name for /connections paths.
func extractConnectionOperation(path, method string) string {
	if path == connectionsPath {
		if method == http.MethodGet {
			return "ListConnections"
		}

		return opUnknown
	}

	switch method {
	case http.MethodDelete:
		return "DeleteConnection"
	case http.MethodPost:
		return "RegisterConnection"
	}

	return opUnknown
}

// extractShadowOperation returns the operation name for /things/{name}/shadow paths.
func extractShadowOperation(method string) string {
	switch method {
	case http.MethodGet:
		return "GetThingShadow"
	case http.MethodPost:
		return "UpdateThingShadow"
	case http.MethodDelete:
		return "DeleteThingShadow"
	}

	return opUnknown
}

// ExtractOperation returns the operation name.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	path := c.Request().URL.Path
	method := c.Request().Method
	switch {
	case strings.HasPrefix(path, "/topics/"):
		return "Publish"
	case strings.HasPrefix(path, listNamedShadowsPrefix):
		return "ListNamedShadowsForThing"
	case path == listThingsWithShadowsPath:
		return "ListThingsWithShadows"
	case path == connectionsPath || strings.HasPrefix(path, connectionsPathSlash):
		return extractConnectionOperation(path, method)
	case path == retainedMessagePath && method == http.MethodGet:
		return "ListRetainedMessages"
	case strings.HasPrefix(path, retainedMessagePathSlash) && method == http.MethodGet:
		return "GetRetainedMessage"
	case isShadowPath(path):
		return extractShadowOperation(method)
	}

	return opUnknown
}

// ExtractResource extracts the topic or thing name from the URL path.
func (h *Handler) ExtractResource(c *echo.Context) string {
	path := c.Request().URL.Path
	if after, ok := strings.CutPrefix(path, "/topics/"); ok {
		return after
	}

	if after, ok := strings.CutPrefix(path, listNamedShadowsPrefix); ok {
		return after
	}

	if path == listThingsWithShadowsPath {
		return ""
	}

	if path == connectionsPath {
		return ""
	}

	if after, ok := strings.CutPrefix(path, connectionsPathSlash); ok {
		return after
	}

	if path == retainedMessagePath {
		return ""
	}

	if after, ok := strings.CutPrefix(path, retainedMessagePathSlash); ok {
		return after
	}

	// /things/{thingName}/shadow
	thingName := parseShadowPath(path)

	return thingName
}

// parseShadowPath extracts thingName from a /things/{thingName}/shadow path.
func parseShadowPath(path string) string {
	trimmed := strings.TrimPrefix(path, "/things/")
	parts := strings.SplitN(trimmed, "/shadow", shadowPathParts)

	return parts[0]
}

// handleError maps backend errors to appropriate HTTP status codes.
func (h *Handler) handleError(c *echo.Context, err error) error {
	switch {
	case errors.Is(err, ErrShadowNotFound), errors.Is(err, ErrRetainedMessageNotFound):
		return c.JSON(http.StatusNotFound, map[string]string{
			keyError:   "ResourceNotFoundException",
			keyMessage: err.Error(),
		})
	case errors.Is(err, ErrVersionConflict):
		return c.JSON(http.StatusConflict, map[string]string{
			keyError:   "VersionConflictException",
			keyMessage: err.Error(),
		})
	case errors.Is(err, ErrValidation):
		return c.JSON(http.StatusBadRequest, map[string]string{
			keyError:   "InvalidRequestException",
			keyMessage: err.Error(),
		})
	case errors.Is(err, ErrConnectionExists):
		return c.JSON(http.StatusConflict, map[string]string{
			keyError:   "ResourceAlreadyExistsException",
			keyMessage: err.Error(),
		})
	default:
		return c.JSON(http.StatusInternalServerError, map[string]string{keyError: err.Error()})
	}
}

// Handler returns the Echo handler function for IoT Data Plane operations.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		path := c.Request().URL.Path
		switch {
		case strings.HasPrefix(path, "/topics/"):
			return h.handlePublish(c)
		case strings.HasPrefix(path, listNamedShadowsPrefix):
			return h.handleListNamedShadows(c)
		case path == listThingsWithShadowsPath:
			return h.handleListThingsWithShadows(c)
		case path == connectionsPath:
			return h.handleConnections(c)
		case strings.HasPrefix(path, connectionsPathSlash):
			return h.handleConnectionByID(c)
		case path == retainedMessagePath:
			return h.handleListRetainedMessages(c)
		case strings.HasPrefix(path, retainedMessagePathSlash):
			return h.handleGetRetainedMessage(c)
		case isShadowPath(path):
			return h.handleShadow(c)
		default:
			return c.JSON(http.StatusNotFound, map[string]string{keyError: "not found"})
		}
	}
}

// parsePublishQoS extracts and validates the qos query parameter.
// Returns ErrValidation wrapped with a description on invalid input.
func parsePublishQoS(qosStr string) (int32, error) {
	if qosStr == "" {
		return 0, nil
	}

	qosVal, err := strconv.ParseInt(qosStr, 10, 32)
	if err != nil || qosVal < 0 || qosVal > 1 {
		return 0, fmt.Errorf("%w: qos must be 0 or 1", ErrValidation)
	}

	return int32(qosVal), nil
}

// unwrapPublishPayload unwraps a JSON `{"payload":"..."}` envelope if present,
// otherwise returns the original body bytes unchanged.
func unwrapPublishPayload(body []byte) []byte {
	var wrapper struct {
		Payload *json.RawMessage `json:"payload"`
	}

	if jsonErr := json.Unmarshal(body, &wrapper); jsonErr == nil && wrapper.Payload != nil {
		var payloadStr string
		if unmarshalErr := json.Unmarshal(*wrapper.Payload, &payloadStr); unmarshalErr == nil {
			return []byte(payloadStr)
		}
	}

	return body
}

// handlePublish processes POST /topics/{topic} requests.
// Query params: qos (int), retain (bool).
// When no broker is configured the publish is silently accepted with HTTP 200.
func (h *Handler) handlePublish(c *echo.Context) error {
	log := logger.Load(c.Request().Context())

	if c.Request().Method != http.MethodPost {
		return c.JSON(http.StatusMethodNotAllowed, map[string]string{keyError: errMethodNotAllowed})
	}

	topic := strings.TrimPrefix(c.Request().URL.Path, "/topics/")
	if topic == "" {
		return c.JSON(http.StatusBadRequest, map[string]string{keyError: "topic is required"})
	}

	if err := validateTopic(topic); err != nil {
		return h.handleError(c, err)
	}

	qos, qosErr := parsePublishQoS(c.Request().URL.Query().Get("qos"))
	if qosErr != nil {
		return h.handleError(c, qosErr)
	}

	// Limit the request body size to prevent excessive memory usage.
	c.Request().Body = http.MaxBytesReader(c.Response(), c.Request().Body, maxPublishBodyBytes)

	body, err := io.ReadAll(c.Request().Body)
	if err != nil {
		return c.JSON(http.StatusRequestEntityTooLarge, map[string]string{keyError: "request body too large"})
	}

	payload := unwrapPublishPayload(body)

	if publishErr := h.Backend.Publish(topic, payload, qos); publishErr != nil {
		if errors.Is(publishErr, ErrNoBroker) {
			// No broker is normal in test/local environments – accept the message.
			log.Warn("iot data plane: no broker configured, message dropped", "topic", topic)
		} else {
			log.Error("iot data plane publish failed", "topic", topic, "error", publishErr)

			return c.JSON(http.StatusInternalServerError, map[string]string{keyError: publishErr.Error()})
		}
	}

	return h.handleRetain(c, topic, payload, qos)
}

// handleRetain stores a retained message when ?retain=true/1 is set.
func (h *Handler) handleRetain(c *echo.Context, topic string, payload []byte, qos int32) error {
	log := logger.Load(c.Request().Context())

	retainStr := strings.ToLower(c.Request().URL.Query().Get("retain"))
	if retainStr == "true" || retainStr == "1" {
		if storeErr := h.Backend.StoreRetainedMessage(topic, payload, qos); storeErr != nil {
			if errors.Is(storeErr, ErrValidation) {
				return h.handleError(c, storeErr)
			}

			log.Warn("failed to store retained message", "topic", topic, "error", storeErr)
		}
	}

	return c.JSON(http.StatusOK, map[string]string{})
}

// handleConnections dispatches GET /connections requests.
func (h *Handler) handleConnections(c *echo.Context) error {
	if c.Request().Method != http.MethodGet {
		return c.JSON(http.StatusMethodNotAllowed, map[string]string{keyError: errMethodNotAllowed})
	}

	return h.handleListConnections(c)
}

// handleConnectionByID dispatches requests for /connections/{clientId}.
func (h *Handler) handleConnectionByID(c *echo.Context) error {
	switch c.Request().Method {
	case http.MethodDelete:
		return h.handleDeleteConnection(c)
	case http.MethodPost:
		return h.handleRegisterConnection(c)
	default:
		return c.JSON(http.StatusMethodNotAllowed, map[string]string{keyError: errMethodNotAllowed})
	}
}

// handleListConnections processes GET /connections requests.
func (h *Handler) handleListConnections(c *echo.Context) error {
	conns := h.Backend.ListConnections()

	type connResp struct {
		ClientID    string `json:"clientId"`
		SourceIP    string `json:"sourceIp,omitempty"`
		ConnectedAt int64  `json:"connectedAt"`
	}

	out := make([]connResp, 0, len(conns))
	for _, conn := range conns {
		out = append(out, connResp{
			ClientID:    conn.ClientID,
			SourceIP:    conn.SourceIP,
			ConnectedAt: conn.ConnectedAt.Unix(),
		})
	}

	return c.JSON(http.StatusOK, map[string]any{"connections": out})
}

// handleRegisterConnection processes POST /connections/{clientId} requests.
func (h *Handler) handleRegisterConnection(c *echo.Context) error {
	clientID := strings.TrimPrefix(c.Request().URL.Path, connectionsPathSlash)
	if clientID == "" {
		return c.JSON(http.StatusBadRequest, map[string]string{keyError: "clientId is required"})
	}

	sourceIP := c.RealIP()

	if err := h.Backend.RegisterConnection(clientID, sourceIP); err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusCreated, map[string]string{"clientId": clientID})
}

// handleDeleteConnection processes DELETE /connections/{clientId} requests.
// Query params: cleanSession (bool), preventWillMessage (bool) – accepted but not enforced.
func (h *Handler) handleDeleteConnection(c *echo.Context) error {
	clientID := strings.TrimPrefix(c.Request().URL.Path, connectionsPathSlash)
	if clientID == "" {
		return c.JSON(http.StatusBadRequest, map[string]string{keyError: "clientId is required"})
	}

	if err := h.Backend.DeleteConnection(clientID); err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]string{})
}

// handleGetRetainedMessage processes GET /retainedMessage/{topic} requests.
func (h *Handler) handleGetRetainedMessage(c *echo.Context) error {
	if c.Request().Method != http.MethodGet {
		return c.JSON(http.StatusMethodNotAllowed, map[string]string{keyError: errMethodNotAllowed})
	}

	topic := strings.TrimPrefix(c.Request().URL.Path, retainedMessagePathSlash)
	if topic == "" {
		return c.JSON(http.StatusBadRequest, map[string]string{keyError: "topic is required"})
	}

	msg, err := h.Backend.GetRetainedMessage(topic)
	if err != nil {
		return h.handleError(c, err)
	}

	resp := map[string]any{
		"topic":            msg.Topic,
		"payload":          msg.Payload,
		"qos":              msg.Qos,
		"lastModifiedTime": msg.LastModifiedTime,
	}

	return c.JSON(http.StatusOK, resp)
}

// handleListRetainedMessages processes GET /retainedMessage requests.
// Query params: nextToken (string), maxResults (int).
func (h *Handler) handleListRetainedMessages(c *echo.Context) error {
	if c.Request().Method != http.MethodGet {
		return c.JSON(http.StatusMethodNotAllowed, map[string]string{keyError: errMethodNotAllowed})
	}

	msgs, err := h.Backend.ListRetainedMessages()
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{keyError: err.Error()})
	}

	// Apply simple nextToken pagination using the topic as the cursor.
	nextTokenIn := c.Request().URL.Query().Get("nextToken")
	maxResultsStr := c.Request().URL.Query().Get("maxResults")

	maxResults := len(msgs)
	if maxResultsStr != "" {
		if v, parseErr := strconv.Atoi(maxResultsStr); parseErr == nil && v > 0 {
			maxResults = v
		}
	}

	// Find the start index from the nextToken (which is a topic name cursor).
	startIdx := 0

	if nextTokenIn != "" {
		for i, msg := range msgs {
			if msg.Topic == nextTokenIn {
				startIdx = i

				break
			}
		}
	}

	end := min(startIdx+maxResults, len(msgs))

	page := msgs[startIdx:end]

	summaries := make([]map[string]any, 0, len(page))
	for _, msg := range page {
		summaries = append(summaries, map[string]any{
			"topic":            msg.Topic,
			"payloadSize":      int64(len(msg.Payload)),
			"qos":              msg.Qos,
			"lastModifiedTime": msg.LastModifiedTime,
		})
	}

	resp := map[string]any{
		"retainedTopics": summaries,
	}

	if end < len(msgs) {
		resp["nextToken"] = msgs[end].Topic
	}

	return c.JSON(http.StatusOK, resp)
}

// handleListThingsWithShadows processes GET /api/things/shadow/ListThingsWithShadows.
func (h *Handler) handleListThingsWithShadows(c *echo.Context) error {
	if c.Request().Method != http.MethodGet {
		return c.JSON(http.StatusMethodNotAllowed, map[string]string{keyError: errMethodNotAllowed})
	}

	things := h.Backend.ListThingsWithShadows()

	// Apply simple nextToken pagination.
	nextTokenIn := c.Request().URL.Query().Get("nextToken")
	maxResultsStr := c.Request().URL.Query().Get("maxResults")

	maxResults := len(things)
	if maxResultsStr != "" {
		if v, parseErr := strconv.Atoi(maxResultsStr); parseErr == nil && v > 0 {
			maxResults = v
		}
	}

	startIdx := 0
	if nextTokenIn != "" {
		for i, name := range things {
			if name == nextTokenIn {
				startIdx = i

				break
			}
		}
	}

	end := min(startIdx+maxResults, len(things))
	page := things[startIdx:end]

	if page == nil {
		page = []string{}
	}

	resp := map[string]any{
		"things":     page,
		keyTimestamp: time.Now().Unix(),
	}

	if end < len(things) {
		resp["nextToken"] = things[end]
	}

	return c.JSON(http.StatusOK, resp)
}

// handleShadow dispatches GET/POST/DELETE /things/{thingName}/shadow requests.
func (h *Handler) handleShadow(c *echo.Context) error {
	thingName := parseShadowPath(c.Request().URL.Path)
	if thingName == "" {
		return c.JSON(http.StatusBadRequest, map[string]string{keyError: "thingName is required"})
	}

	// Named shadow support via ?name= query parameter.
	shadowName := c.Request().URL.Query().Get("name")

	if err := validateShadowName(shadowName); err != nil {
		return h.handleError(c, err)
	}

	switch c.Request().Method {
	case http.MethodGet:
		return h.handleGetThingShadow(c, thingName, shadowName)
	case http.MethodPost:
		return h.handleUpdateThingShadow(c, thingName, shadowName)
	case http.MethodDelete:
		return h.handleDeleteThingShadow(c, thingName, shadowName)
	default:
		return c.JSON(http.StatusMethodNotAllowed, map[string]string{keyError: errMethodNotAllowed})
	}
}

// handleGetThingShadow processes GET /things/{thingName}/shadow.
func (h *Handler) handleGetThingShadow(c *echo.Context, thingName, shadowName string) error {
	doc, err := h.Backend.GetThingShadow(thingName, shadowName)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.Blob(http.StatusOK, "application/json", doc)
}

// handleUpdateThingShadow processes POST /things/{thingName}/shadow.
func (h *Handler) handleUpdateThingShadow(c *echo.Context, thingName, shadowName string) error {
	c.Request().Body = http.MaxBytesReader(c.Response(), c.Request().Body, maxShadowBodyBytes)

	body, err := io.ReadAll(c.Request().Body)
	if err != nil {
		return c.JSON(http.StatusRequestEntityTooLarge, map[string]string{keyError: "request body too large"})
	}

	updated, updateErr := h.Backend.UpdateThingShadow(thingName, shadowName, body)
	if updateErr != nil {
		return h.handleError(c, updateErr)
	}

	return c.Blob(http.StatusOK, "application/json", updated)
}

// handleDeleteThingShadow processes DELETE /things/{thingName}/shadow.
// Returns the deleted shadow state as payload (AWS contract).
func (h *Handler) handleDeleteThingShadow(c *echo.Context, thingName, shadowName string) error {
	payload, err := h.Backend.DeleteThingShadow(thingName, shadowName)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.Blob(http.StatusOK, "application/json", payload)
}

// handleListNamedShadows processes GET /api/things/shadow/ListNamedShadowsForThing/{thingName}.
// Response includes results, nextToken (pagination), and timestamp (AWS SDK compat).
func (h *Handler) handleListNamedShadows(c *echo.Context) error {
	if c.Request().Method != http.MethodGet {
		return c.JSON(http.StatusMethodNotAllowed, map[string]string{keyError: errMethodNotAllowed})
	}

	thingName := strings.TrimPrefix(c.Request().URL.Path, listNamedShadowsPrefix)
	if thingName == "" {
		return c.JSON(http.StatusBadRequest, map[string]string{keyError: "thingName is required"})
	}

	names, err := h.Backend.ListNamedShadowsForThing(thingName)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{keyError: err.Error()})
	}

	// Apply simple nextToken pagination using the shadow name as cursor.
	nextTokenIn := c.Request().URL.Query().Get("nextToken")
	maxResultsStr := c.Request().URL.Query().Get("maxResults")

	maxResults := len(names)
	if maxResultsStr != "" {
		if v, parseErr := strconv.Atoi(maxResultsStr); parseErr == nil && v > 0 {
			maxResults = v
		}
	}

	startIdx := 0

	if nextTokenIn != "" {
		for i, name := range names {
			if name == nextTokenIn {
				startIdx = i

				break
			}
		}
	}

	end := min(startIdx+maxResults, len(names))

	page := names[startIdx:end]

	// Ensure page is never nil so the JSON encoder always emits [].
	if page == nil {
		page = []string{}
	}

	resp := map[string]any{
		"results":    page,
		keyTimestamp: time.Now().Unix(),
	}

	if end < len(names) {
		resp["nextToken"] = names[end]
	}

	return c.JSON(http.StatusOK, resp)
}
