package iotwireless

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	iotwirelessService       = "iotwireless"
	iotwirelessMatchPriority = 86
)

// Handler is the HTTP handler for the IoT Wireless REST API.
type Handler struct {
	Backend       StorageBackend
	AccountID     string
	DefaultRegion string
}

// NewHandler creates a new IoT Wireless handler.
func NewHandler(backend StorageBackend) *Handler {
	return &Handler{Backend: backend}
}

// Name returns the service name.
func (h *Handler) Name() string { return "IoTWireless" }

// GetSupportedOperations returns the list of supported IoT Wireless operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CreateWirelessDevice",
		"GetWirelessDevice",
		"ListWirelessDevices",
		"DeleteWirelessDevice",
		"CreateWirelessGateway",
		"GetWirelessGateway",
		"ListWirelessGateways",
		"DeleteWirelessGateway",
		"CreateServiceProfile",
		"GetServiceProfile",
		"ListServiceProfiles",
		"DeleteServiceProfile",
		"CreateDestination",
		"GetDestination",
		"ListDestinations",
		"DeleteDestination",
		"TagResource",
		"UntagResource",
		"ListTagsForResource",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return iotwirelessService }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this IoT Wireless instance handles.
func (h *Handler) ChaosRegions() []string { return []string{h.DefaultRegion} }

// RouteMatcher returns a function that matches IoT Wireless REST API requests.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		path := c.Request().URL.Path

		for _, prefix := range []string{
			"/wireless-devices",
			"/wireless-gateways",
			"/service-profiles",
			"/destinations",
		} {
			if path == prefix || strings.HasPrefix(path, prefix+"/") {
				return true
			}
		}

		if strings.HasPrefix(path, "/tags/") {
			return httputils.ExtractServiceFromRequest(c.Request()) == iotwirelessService
		}

		return false
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return iotwirelessMatchPriority }

// ExtractOperation extracts the IoT Wireless operation name from the request.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	op, _ := parseIoTWirelessPath(c.Request().Method, c.Request().URL.Path)

	return op
}

// ExtractResource extracts the resource ID from the URL path.
func (h *Handler) ExtractResource(c *echo.Context) string {
	_, resource := parseIoTWirelessPath(c.Request().Method, c.Request().URL.Path)

	return resource
}

// Handler returns the Echo handler function for IoT Wireless requests.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		ctx := c.Request().Context()
		log := logger.Load(ctx)

		method := c.Request().Method
		path := c.Request().URL.Path

		op, resource := parseIoTWirelessPath(method, path)
		if op == "" {
			return writeError(c, http.StatusNotFound, "resource not found")
		}

		body, err := httputils.ReadBody(c.Request())
		if err != nil {
			log.ErrorContext(ctx, "iotwireless: failed to read request body", "error", err)

			return writeError(c, http.StatusInternalServerError, "failed to read request body")
		}

		log.DebugContext(ctx, "iotwireless request", "op", op, "resource", resource)

		return h.dispatch(c, op, resource, body, c.Request().URL.Query())
	}
}

// parseIoTWirelessPath maps a method+path to an operation and resource identifier.
func parseIoTWirelessPath(method, path string) (string, string) {
	// Strip leading slash and split.
	trimmed := strings.TrimPrefix(path, "/")
	parts := strings.SplitN(trimmed, "/", 3) //nolint:mnd // max 3 parts

	if len(parts) == 0 {
		return "", ""
	}

	base := parts[0]
	hasID := len(parts) >= 2 && parts[1] != "" //nolint:mnd // check for ID segment

	// Handle /tags/{ResourceArn}
	if base == "tags" {
		if !hasID {
			return "", ""
		}

		arnEncoded := strings.Join(parts[1:], "/")

		switch method {
		case http.MethodGet:
			return "ListTagsForResource", arnEncoded
		case http.MethodPost:
			return "TagResource", arnEncoded
		case http.MethodDelete:
			return "UntagResource", arnEncoded
		}

		return "", ""
	}

	id := ""
	if len(parts) >= 2 { //nolint:mnd // index 1 holds the resource ID
		id = parts[1]
	}

	switch base {
	case "wireless-devices":
		return parseCollectionPath(method, "WirelessDevice", hasID, id)
	case "wireless-gateways":
		return parseCollectionPath(method, "WirelessGateway", hasID, id)
	case "service-profiles":
		return parseCollectionPath(method, "ServiceProfile", hasID, id)
	case "destinations":
		return parseCollectionPath(method, "Destination", hasID, id)
	}

	return "", ""
}

// parseCollectionPath handles standard CRUD routing for a resource collection.
func parseCollectionPath(method, resourceType string, hasID bool, id string) (string, string) {
	if !hasID {
		switch method {
		case http.MethodPost:
			return "Create" + resourceType, ""
		case http.MethodGet:
			return "List" + resourceType + "s", ""
		}

		return "", ""
	}

	switch method {
	case http.MethodGet:
		return "Get" + resourceType, id
	case http.MethodDelete:
		return "Delete" + resourceType, id
	}

	return "", ""
}

// dispatch routes to the appropriate handler based on the operation name.
func (h *Handler) dispatch(c *echo.Context, op, resource string, body []byte, query url.Values) error {
	if handled, result := h.dispatchWirelessDevice(c, op, resource, body); handled {
		return result
	}

	if handled, result := h.dispatchWirelessGateway(c, op, resource, body); handled {
		return result
	}

	if handled, result := h.dispatchServiceProfile(c, op, resource, body); handled {
		return result
	}

	if handled, result := h.dispatchDestination(c, op, resource, body); handled {
		return result
	}

	switch op {
	case "ListTagsForResource":
		return h.listTagsForResource(c, resource)
	case "TagResource":
		return h.tagResource(c, resource, body)
	case "UntagResource":
		return h.untagResource(c, resource, query)
	}

	return writeError(c, http.StatusNotFound, "unknown operation")
}

// dispatchWirelessDevice handles wireless device operations.
func (h *Handler) dispatchWirelessDevice(c *echo.Context, op, resource string, body []byte) (bool, error) {
	switch op {
	case "CreateWirelessDevice":
		return true, h.createWirelessDevice(c, body)
	case "GetWirelessDevice":
		return true, h.getWirelessDevice(c, resource)
	case "ListWirelessDevices":
		return true, h.listWirelessDevices(c)
	case "DeleteWirelessDevice":
		return true, h.deleteWirelessDevice(c, resource)
	}

	return false, nil
}

// dispatchWirelessGateway handles wireless gateway operations.
func (h *Handler) dispatchWirelessGateway(c *echo.Context, op, resource string, body []byte) (bool, error) {
	switch op {
	case "CreateWirelessGateway":
		return true, h.createWirelessGateway(c, body)
	case "GetWirelessGateway":
		return true, h.getWirelessGateway(c, resource)
	case "ListWirelessGateways":
		return true, h.listWirelessGateways(c)
	case "DeleteWirelessGateway":
		return true, h.deleteWirelessGateway(c, resource)
	}

	return false, nil
}

// dispatchServiceProfile handles service profile operations.
func (h *Handler) dispatchServiceProfile(c *echo.Context, op, resource string, body []byte) (bool, error) {
	switch op {
	case "CreateServiceProfile":
		return true, h.createServiceProfile(c, body)
	case "GetServiceProfile":
		return true, h.getServiceProfile(c, resource)
	case "ListServiceProfiles":
		return true, h.listServiceProfiles(c)
	case "DeleteServiceProfile":
		return true, h.deleteServiceProfile(c, resource)
	}

	return false, nil
}

// dispatchDestination handles destination operations.
func (h *Handler) dispatchDestination(c *echo.Context, op, resource string, body []byte) (bool, error) {
	switch op {
	case "CreateDestination":
		return true, h.createDestination(c, body)
	case "GetDestination":
		return true, h.getDestination(c, resource)
	case "ListDestinations":
		return true, h.listDestinations(c)
	case "DeleteDestination":
		return true, h.deleteDestination(c, resource)
	}

	return false, nil
}

// JSON request/response types.

type createWirelessDeviceRequest struct {
	Tags            map[string]string `json:"Tags"`
	Name            string            `json:"Name"`
	Type            string            `json:"Type"`
	DestinationName string            `json:"DestinationName"`
	Description     string            `json:"Description"`
}

type createWirelessDeviceResponse struct {
	Arn string `json:"Arn"`
	ID  string `json:"Id"`
}

type wirelessDeviceEntry struct {
	Arn             string `json:"Arn"`
	ID              string `json:"Id"`
	Name            string `json:"Name"`
	Type            string `json:"Type"`
	DestinationName string `json:"DestinationName"`
	Description     string `json:"Description"`
}

type listWirelessDevicesResponse struct {
	WirelessDeviceList []wirelessDeviceEntry `json:"WirelessDeviceList"`
}

type createWirelessGatewayRequest struct {
	Tags        map[string]string `json:"Tags"`
	Name        string            `json:"Name"`
	Description string            `json:"Description"`
}

type createWirelessGatewayResponse struct {
	Arn string `json:"Arn"`
	ID  string `json:"Id"`
}

type wirelessGatewayEntry struct {
	Arn         string `json:"Arn"`
	ID          string `json:"Id"`
	Name        string `json:"Name"`
	Description string `json:"Description"`
}

type listWirelessGatewaysResponse struct {
	WirelessGatewayList []wirelessGatewayEntry `json:"WirelessGatewayList"`
}

type createServiceProfileRequest struct {
	Tags map[string]string `json:"Tags"`
	Name string            `json:"Name"`
}

type createServiceProfileResponse struct {
	Arn string `json:"Arn"`
	ID  string `json:"Id"`
}

type serviceProfileEntry struct {
	Arn  string `json:"Arn"`
	ID   string `json:"Id"`
	Name string `json:"Name"`
}

type listServiceProfilesResponse struct {
	ServiceProfileList []serviceProfileEntry `json:"ServiceProfileList"`
}

type createDestinationRequest struct {
	Tags           map[string]string `json:"Tags"`
	Name           string            `json:"Name"`
	Expression     string            `json:"Expression"`
	ExpressionType string            `json:"ExpressionType"`
	RoleArn        string            `json:"RoleArn"`
	Description    string            `json:"Description"`
}

type destinationEntry struct {
	Arn            string `json:"Arn"`
	Name           string `json:"Name"`
	Expression     string `json:"Expression"`
	ExpressionType string `json:"ExpressionType"`
	RoleArn        string `json:"RoleArn"`
	Description    string `json:"Description"`
}

type listDestinationsResponse struct {
	DestinationList []destinationEntry `json:"DestinationList"`
}

type tagResourceRequest struct {
	Tags map[string]string `json:"Tags"`
}

type listTagsResponse struct {
	Tags map[string]string `json:"Tags"`
}

type errorResponse struct {
	Message string `json:"Message"`
}

// writeError writes a JSON error response.
func writeError(c *echo.Context, status int, message string) error {
	c.Response().Header().Set("Content-Type", "application/json")
	c.Response().WriteHeader(status)

	_ = json.NewEncoder(c.Response()).Encode(errorResponse{Message: message})

	return nil
}

// writeJSON writes a JSON response with the given status code.
func writeJSON(c *echo.Context, status int, v any) error {
	c.Response().Header().Set("Content-Type", "application/json")
	c.Response().WriteHeader(status)

	_ = json.NewEncoder(c.Response()).Encode(v)

	return nil
}

// decodeNotFoundError maps not-found sentinel errors to 404.
func isNotFound(err error) bool {
	return errors.Is(err, ErrDeviceNotFound) ||
		errors.Is(err, ErrGatewayNotFound) ||
		errors.Is(err, ErrServiceProfileNotFound) ||
		errors.Is(err, ErrDestinationNotFound)
}

// decodeARN URL-decodes an ARN path segment.
func decodeARN(encoded string) string {
	decoded, err := url.PathUnescape(encoded)
	if err != nil {
		return encoded
	}

	return decoded
}

// --- Wireless Device handlers ---

func (h *Handler) createWirelessDevice(c *echo.Context, body []byte) error {
	var req createWirelessDeviceRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "invalid request body")
	}

	d, err := h.Backend.CreateWirelessDevice(
		h.AccountID, h.DefaultRegion,
		req.Name, req.Type, req.DestinationName, req.Description, req.Tags,
	)
	if err != nil {
		return writeError(c, http.StatusInternalServerError, err.Error())
	}

	return writeJSON(c, http.StatusCreated, createWirelessDeviceResponse{Arn: d.ARN, ID: d.ID})
}

func (h *Handler) getWirelessDevice(c *echo.Context, id string) error {
	d, err := h.Backend.GetWirelessDevice(h.AccountID, h.DefaultRegion, id)
	if err != nil {
		if isNotFound(err) {
			return writeError(c, http.StatusNotFound, err.Error())
		}

		return writeError(c, http.StatusInternalServerError, err.Error())
	}

	return writeJSON(c, http.StatusOK, wirelessDeviceEntry{
		Arn:             d.ARN,
		ID:              d.ID,
		Name:            d.Name,
		Type:            d.Type,
		DestinationName: d.DestinationName,
		Description:     d.Description,
	})
}

func (h *Handler) listWirelessDevices(c *echo.Context) error {
	devices := h.Backend.ListWirelessDevices(h.AccountID, h.DefaultRegion)
	entries := make([]wirelessDeviceEntry, 0, len(devices))

	for _, d := range devices {
		entries = append(entries, wirelessDeviceEntry{
			Arn:             d.ARN,
			ID:              d.ID,
			Name:            d.Name,
			Type:            d.Type,
			DestinationName: d.DestinationName,
			Description:     d.Description,
		})
	}

	return writeJSON(c, http.StatusOK, listWirelessDevicesResponse{WirelessDeviceList: entries})
}

func (h *Handler) deleteWirelessDevice(c *echo.Context, id string) error {
	err := h.Backend.DeleteWirelessDevice(h.AccountID, h.DefaultRegion, id)
	if err != nil {
		if isNotFound(err) {
			return writeError(c, http.StatusNotFound, err.Error())
		}

		return writeError(c, http.StatusInternalServerError, err.Error())
	}

	c.Response().WriteHeader(http.StatusNoContent)

	return nil
}

// --- Wireless Gateway handlers ---

func (h *Handler) createWirelessGateway(c *echo.Context, body []byte) error {
	var req createWirelessGatewayRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "invalid request body")
	}

	gw, err := h.Backend.CreateWirelessGateway(
		h.AccountID, h.DefaultRegion,
		req.Name, req.Description, req.Tags,
	)
	if err != nil {
		return writeError(c, http.StatusInternalServerError, err.Error())
	}

	return writeJSON(c, http.StatusCreated, createWirelessGatewayResponse{Arn: gw.ARN, ID: gw.ID})
}

func (h *Handler) getWirelessGateway(c *echo.Context, id string) error {
	gw, err := h.Backend.GetWirelessGateway(h.AccountID, h.DefaultRegion, id)
	if err != nil {
		if isNotFound(err) {
			return writeError(c, http.StatusNotFound, err.Error())
		}

		return writeError(c, http.StatusInternalServerError, err.Error())
	}

	return writeJSON(c, http.StatusOK, wirelessGatewayEntry{
		Arn:         gw.ARN,
		ID:          gw.ID,
		Name:        gw.Name,
		Description: gw.Description,
	})
}

func (h *Handler) listWirelessGateways(c *echo.Context) error {
	gws := h.Backend.ListWirelessGateways(h.AccountID, h.DefaultRegion)
	entries := make([]wirelessGatewayEntry, 0, len(gws))

	for _, gw := range gws {
		entries = append(entries, wirelessGatewayEntry{
			Arn:         gw.ARN,
			ID:          gw.ID,
			Name:        gw.Name,
			Description: gw.Description,
		})
	}

	return writeJSON(c, http.StatusOK, listWirelessGatewaysResponse{WirelessGatewayList: entries})
}

func (h *Handler) deleteWirelessGateway(c *echo.Context, id string) error {
	err := h.Backend.DeleteWirelessGateway(h.AccountID, h.DefaultRegion, id)
	if err != nil {
		if isNotFound(err) {
			return writeError(c, http.StatusNotFound, err.Error())
		}

		return writeError(c, http.StatusInternalServerError, err.Error())
	}

	c.Response().WriteHeader(http.StatusNoContent)

	return nil
}

// --- Service Profile handlers ---

func (h *Handler) createServiceProfile(c *echo.Context, body []byte) error {
	var req createServiceProfileRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "invalid request body")
	}

	sp, err := h.Backend.CreateServiceProfile(h.AccountID, h.DefaultRegion, req.Name, req.Tags)
	if err != nil {
		return writeError(c, http.StatusInternalServerError, err.Error())
	}

	return writeJSON(c, http.StatusCreated, createServiceProfileResponse{Arn: sp.ARN, ID: sp.ID})
}

func (h *Handler) getServiceProfile(c *echo.Context, id string) error {
	sp, err := h.Backend.GetServiceProfile(h.AccountID, h.DefaultRegion, id)
	if err != nil {
		if isNotFound(err) {
			return writeError(c, http.StatusNotFound, err.Error())
		}

		return writeError(c, http.StatusInternalServerError, err.Error())
	}

	return writeJSON(c, http.StatusOK, serviceProfileEntry{
		Arn:  sp.ARN,
		ID:   sp.ID,
		Name: sp.Name,
	})
}

func (h *Handler) listServiceProfiles(c *echo.Context) error {
	profiles := h.Backend.ListServiceProfiles(h.AccountID, h.DefaultRegion)
	entries := make([]serviceProfileEntry, 0, len(profiles))

	for _, sp := range profiles {
		entries = append(entries, serviceProfileEntry{
			Arn:  sp.ARN,
			ID:   sp.ID,
			Name: sp.Name,
		})
	}

	return writeJSON(c, http.StatusOK, listServiceProfilesResponse{ServiceProfileList: entries})
}

func (h *Handler) deleteServiceProfile(c *echo.Context, id string) error {
	err := h.Backend.DeleteServiceProfile(h.AccountID, h.DefaultRegion, id)
	if err != nil {
		if isNotFound(err) {
			return writeError(c, http.StatusNotFound, err.Error())
		}

		return writeError(c, http.StatusInternalServerError, err.Error())
	}

	c.Response().WriteHeader(http.StatusNoContent)

	return nil
}

// --- Destination handlers ---

func (h *Handler) createDestination(c *echo.Context, body []byte) error {
	var req createDestinationRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "invalid request body")
	}

	dest, err := h.Backend.CreateDestination(
		h.AccountID, h.DefaultRegion,
		req.Name, req.Expression, req.ExpressionType, req.RoleArn, req.Description, req.Tags,
	)
	if err != nil {
		return writeError(c, http.StatusInternalServerError, err.Error())
	}

	return writeJSON(c, http.StatusCreated, destinationEntry{
		Arn:            dest.ARN,
		Name:           dest.Name,
		Expression:     dest.Expression,
		ExpressionType: dest.ExpressionType,
		RoleArn:        dest.RoleArn,
		Description:    dest.Description,
	})
}

func (h *Handler) getDestination(c *echo.Context, name string) error {
	dest, err := h.Backend.GetDestination(h.AccountID, h.DefaultRegion, name)
	if err != nil {
		if isNotFound(err) {
			return writeError(c, http.StatusNotFound, err.Error())
		}

		return writeError(c, http.StatusInternalServerError, err.Error())
	}

	return writeJSON(c, http.StatusOK, destinationEntry{
		Arn:            dest.ARN,
		Name:           dest.Name,
		Expression:     dest.Expression,
		ExpressionType: dest.ExpressionType,
		RoleArn:        dest.RoleArn,
		Description:    dest.Description,
	})
}

func (h *Handler) listDestinations(c *echo.Context) error {
	dests := h.Backend.ListDestinations(h.AccountID, h.DefaultRegion)
	entries := make([]destinationEntry, 0, len(dests))

	for _, dest := range dests {
		entries = append(entries, destinationEntry{
			Arn:            dest.ARN,
			Name:           dest.Name,
			Expression:     dest.Expression,
			ExpressionType: dest.ExpressionType,
			RoleArn:        dest.RoleArn,
			Description:    dest.Description,
		})
	}

	return writeJSON(c, http.StatusOK, listDestinationsResponse{DestinationList: entries})
}

func (h *Handler) deleteDestination(c *echo.Context, name string) error {
	err := h.Backend.DeleteDestination(h.AccountID, h.DefaultRegion, name)
	if err != nil {
		if isNotFound(err) {
			return writeError(c, http.StatusNotFound, err.Error())
		}

		return writeError(c, http.StatusInternalServerError, err.Error())
	}

	c.Response().WriteHeader(http.StatusNoContent)

	return nil
}

// --- Tag handlers ---

func (h *Handler) listTagsForResource(c *echo.Context, arnEncoded string) error {
	arn := decodeARN(arnEncoded)

	tags, err := h.Backend.ListTagsForResource(arn)
	if err != nil {
		return writeError(c, http.StatusInternalServerError, err.Error())
	}

	return writeJSON(c, http.StatusOK, listTagsResponse{Tags: tags})
}

func (h *Handler) tagResource(c *echo.Context, arnEncoded string, body []byte) error {
	arn := decodeARN(arnEncoded)

	var req tagResourceRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "invalid request body")
	}

	if err := h.Backend.TagResource(arn, req.Tags); err != nil {
		return writeError(c, http.StatusInternalServerError, err.Error())
	}

	c.Response().WriteHeader(http.StatusNoContent)

	return nil
}

func (h *Handler) untagResource(c *echo.Context, arnEncoded string, query url.Values) error {
	arn := decodeARN(arnEncoded)
	tagKeys := query["tagKeys"]

	if err := h.Backend.UntagResource(arn, tagKeys); err != nil {
		return writeError(c, http.StatusInternalServerError, err.Error())
	}

	c.Response().WriteHeader(http.StatusNoContent)

	return nil
}
