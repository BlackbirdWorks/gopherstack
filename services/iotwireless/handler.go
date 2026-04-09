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
		"AssociateAwsAccountWithPartnerAccount",
		"AssociateMulticastGroupWithFuotaTask",
		"AssociateWirelessDeviceWithFuotaTask",
		"AssociateWirelessDeviceWithMulticastGroup",
		"AssociateWirelessDeviceWithThing",
		"AssociateWirelessGatewayWithCertificate",
		"AssociateWirelessGatewayWithThing",
		"CancelMulticastGroupSession",
		"CreateDeviceProfile",
		"CreateFuotaTask",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return iotwirelessService }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this IoT Wireless instance handles.
func (h *Handler) ChaosRegions() []string { return []string{h.DefaultRegion} }

// RouteMatcher returns a function that matches IoT Wireless REST API requests.
// All paths are disambiguated via the SigV4 credential-scope service name to
// prevent mis-routing with other REST-JSON services that share similar paths.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		path := c.Request().URL.Path

		for _, prefix := range []string{
			"/" + pathBaseWirelessDevices,
			"/" + pathBaseWirelessGateways,
			"/" + pathBaseServiceProfiles,
			"/" + pathBaseDestinations,
			"/" + pathBaseDeviceProfiles,
			"/" + pathBaseFuotaTasks,
			"/" + pathBaseMulticastGroups,
			"/" + pathBasePartnerAccounts,
		} {
			if path == prefix || strings.HasPrefix(path, prefix+"/") {
				return httputils.ExtractServiceFromRequest(c.Request()) == iotwirelessService
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

const (
	// maxPathParts is the maximum number of path segments to split when parsing IoT Wireless paths.
	maxPathParts = 3
	// idSegmentIndex is the index of the resource ID segment in the split path.
	idSegmentIndex = 2

	// path base segment constants used in route matching and path parsing.
	pathBaseWirelessDevices  = "wireless-devices"
	pathBaseWirelessGateways = "wireless-gateways"
	pathBaseServiceProfiles  = "service-profiles"
	pathBaseDestinations     = "destinations"
	pathBaseDeviceProfiles   = "device-profiles"
	pathBaseFuotaTasks       = "fuota-tasks"
	pathBaseMulticastGroups  = "multicast-groups"
	pathBasePartnerAccounts  = "partner-accounts"
)

// parseIoTWirelessPath maps a method+path to an operation and resource identifier.
func parseIoTWirelessPath(method, path string) (string, string) {
	// Strip leading slash and split into at most maxPathParts segments.
	trimmed := strings.TrimPrefix(path, "/")
	parts := strings.SplitN(trimmed, "/", maxPathParts)

	base := parts[0]
	hasID := len(parts) >= idSegmentIndex && parts[1] != ""

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
	if hasID {
		id = parts[1]
	}

	subPath := ""
	if len(parts) == maxPathParts {
		subPath = parts[2]
	}

	return parseIoTWirelessBase(method, base, id, subPath, hasID)
}

// parseIoTWirelessBase routes path segments to their operation names.
func parseIoTWirelessBase(method, base, id, subPath string, hasID bool) (string, string) {
	switch base {
	case pathBaseWirelessDevices:
		return parseWirelessDevicePath(method, id, subPath, hasID)
	case pathBaseWirelessGateways:
		return parseWirelessGatewayPath(method, id, subPath, hasID)
	case pathBaseServiceProfiles:
		return parseCollectionPath(method, "ServiceProfile", hasID, id)
	case pathBaseDestinations:
		return parseCollectionPath(method, "Destination", hasID, id)
	case pathBaseDeviceProfiles:
		return parseCollectionPath(method, "DeviceProfile", hasID, id)
	case pathBaseFuotaTasks:
		return parseFuotaTaskPath(method, id, subPath, hasID)
	case pathBaseMulticastGroups:
		return parseMulticastGroupPath(method, id, subPath, hasID)
	case pathBasePartnerAccounts:
		if hasID && method == http.MethodPut {
			return "AssociateAwsAccountWithPartnerAccount", id
		}

		return "", ""
	}

	return "", ""
}

// parseWirelessDevicePath handles wireless-devices sub-path routing.
func parseWirelessDevicePath(method, id, subPath string, hasID bool) (string, string) {
	if hasID && subPath == "thing" && method == http.MethodPut {
		return "AssociateWirelessDeviceWithThing", id
	}

	return parseCollectionPath(method, "WirelessDevice", hasID, id)
}

// parseWirelessGatewayPath handles wireless-gateways sub-path routing.
func parseWirelessGatewayPath(method, id, subPath string, hasID bool) (string, string) {
	if hasID && subPath == "certificate" && method == http.MethodPut {
		return "AssociateWirelessGatewayWithCertificate", id
	}

	if hasID && subPath == "thing" && method == http.MethodPut {
		return "AssociateWirelessGatewayWithThing", id
	}

	return parseCollectionPath(method, "WirelessGateway", hasID, id)
}

// parseFuotaTaskPath handles fuota-tasks sub-path routing.
func parseFuotaTaskPath(method, id, subPath string, hasID bool) (string, string) {
	if hasID {
		switch subPath {
		case pathBaseMulticastGroups:
			if method == http.MethodPut {
				return "AssociateMulticastGroupWithFuotaTask", id
			}
		case pathBaseWirelessDevices:
			if method == http.MethodPut {
				return "AssociateWirelessDeviceWithFuotaTask", id
			}
		}
	}

	return parseCollectionPath(method, "FuotaTask", hasID, id)
}

// parseMulticastGroupPath handles multicast-groups sub-path routing.
func parseMulticastGroupPath(method, id, subPath string, hasID bool) (string, string) {
	if hasID {
		switch subPath {
		case pathBaseWirelessDevices:
			if method == http.MethodPut {
				return "AssociateWirelessDeviceWithMulticastGroup", id
			}
		case "session":
			if method == http.MethodDelete {
				return "CancelMulticastGroupSession", id
			}
		}
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

	if handled, result := h.dispatchNewOps(c, op, resource, body); handled {
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

// dispatchNewOps handles the 10 new operations added in this implementation.
func (h *Handler) dispatchNewOps(c *echo.Context, op, resource string, body []byte) (bool, error) {
	switch op {
	case "CreateDeviceProfile":
		return true, h.createDeviceProfile(c, body)
	case "CreateFuotaTask":
		return true, h.createFuotaTask(c, body)
	case "AssociateAwsAccountWithPartnerAccount":
		return true, h.associateAwsAccountWithPartnerAccount(c, resource, body)
	case "AssociateMulticastGroupWithFuotaTask":
		return true, h.associateMulticastGroupWithFuotaTask(c, resource, body)
	case "AssociateWirelessDeviceWithFuotaTask":
		return true, h.associateWirelessDeviceWithFuotaTask(c, resource, body)
	case "AssociateWirelessDeviceWithMulticastGroup":
		return true, h.associateWirelessDeviceWithMulticastGroup(c, resource, body)
	case "AssociateWirelessDeviceWithThing":
		return true, h.associateWirelessDeviceWithThing(c, resource, body)
	case "AssociateWirelessGatewayWithCertificate":
		return true, h.associateWirelessGatewayWithCertificate(c, resource, body)
	case "AssociateWirelessGatewayWithThing":
		return true, h.associateWirelessGatewayWithThing(c, resource, body)
	case "CancelMulticastGroupSession":
		return true, h.cancelMulticastGroupSession(c, resource)
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

// --- Request/response types for new operations ---

type createDeviceProfileRequest struct {
	Tags map[string]string `json:"Tags"`
	Name string            `json:"Name"`
}

type createDeviceProfileResponse struct {
	Arn string `json:"Arn"`
	ID  string `json:"Id"`
}

type createFuotaTaskRequest struct {
	Tags                map[string]string `json:"Tags"`
	Name                string            `json:"Name"`
	Description         string            `json:"Description"`
	FirmwareUpdateImage string            `json:"FirmwareUpdateImage"`
	FirmwareUpdateRole  string            `json:"FirmwareUpdateRole"`
}

type createFuotaTaskResponse struct {
	Arn string `json:"Arn"`
	ID  string `json:"Id"`
}

type associatePartnerAccountRequest struct {
	Tags map[string]string `json:"Tags"`
}

type associatePartnerAccountResponse struct {
	Arn string `json:"Arn"`
}

type associateMulticastGroupRequest struct {
	MulticastGroupID string `json:"MulticastGroupId"`
}

type associateWirelessDeviceWithFuotaRequest struct {
	WirelessDeviceID string `json:"WirelessDeviceId"`
}

type associateWirelessDeviceWithMulticastRequest struct {
	WirelessDeviceID string `json:"WirelessDeviceId"`
}

type associateWirelessDeviceWithThingRequest struct {
	ThingArn string `json:"ThingArn"`
}

type associateWirelessGatewayWithCertificateRequest struct {
	IotCertificateID string `json:"IotCertificateId"`
}

type associateWirelessGatewayWithCertificateResponse struct {
	IotCertificateArn string `json:"IotCertificateArn"`
}

type associateWirelessGatewayWithThingRequest struct {
	ThingArn string `json:"ThingArn"`
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
		errors.Is(err, ErrDestinationNotFound) ||
		errors.Is(err, ErrDeviceProfileNotFound) ||
		errors.Is(err, ErrFuotaTaskNotFound)
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

// --- Device Profile handlers ---

func (h *Handler) createDeviceProfile(c *echo.Context, body []byte) error {
	var req createDeviceProfileRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "invalid request body")
	}

	dp, err := h.Backend.CreateDeviceProfile(h.AccountID, h.DefaultRegion, req.Name, req.Tags)
	if err != nil {
		return writeError(c, http.StatusInternalServerError, err.Error())
	}

	return writeJSON(c, http.StatusCreated, createDeviceProfileResponse{Arn: dp.ARN, ID: dp.ID})
}

// --- FUOTA Task handlers ---

func (h *Handler) createFuotaTask(c *echo.Context, body []byte) error {
	var req createFuotaTaskRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "invalid request body")
	}

	ft, err := h.Backend.CreateFuotaTask(
		h.AccountID, h.DefaultRegion,
		req.Name, req.Description, req.FirmwareUpdateImage, req.FirmwareUpdateRole,
		req.Tags,
	)
	if err != nil {
		return writeError(c, http.StatusInternalServerError, err.Error())
	}

	return writeJSON(c, http.StatusCreated, createFuotaTaskResponse{Arn: ft.ARN, ID: ft.ID})
}

// --- Association handlers ---

func (h *Handler) associateAwsAccountWithPartnerAccount(c *echo.Context, partnerAccountID string, body []byte) error {
	var req associatePartnerAccountRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "invalid request body")
	}

	arn, err := h.Backend.AssociateAwsAccountWithPartnerAccount(h.AccountID, partnerAccountID, req.Tags)
	if err != nil {
		return writeError(c, http.StatusInternalServerError, err.Error())
	}

	return writeJSON(c, http.StatusOK, associatePartnerAccountResponse{Arn: arn})
}

func (h *Handler) associateMulticastGroupWithFuotaTask(c *echo.Context, fuotaTaskID string, body []byte) error {
	var req associateMulticastGroupRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "invalid request body")
	}

	if err := h.Backend.AssociateMulticastGroupWithFuotaTask(fuotaTaskID, req.MulticastGroupID); err != nil {
		return writeError(c, http.StatusInternalServerError, err.Error())
	}

	c.Response().WriteHeader(http.StatusNoContent)

	return nil
}

func (h *Handler) associateWirelessDeviceWithFuotaTask(c *echo.Context, fuotaTaskID string, body []byte) error {
	var req associateWirelessDeviceWithFuotaRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "invalid request body")
	}

	if err := h.Backend.AssociateWirelessDeviceWithFuotaTask(fuotaTaskID, req.WirelessDeviceID); err != nil {
		return writeError(c, http.StatusInternalServerError, err.Error())
	}

	c.Response().WriteHeader(http.StatusNoContent)

	return nil
}

func (h *Handler) associateWirelessDeviceWithMulticastGroup(
	c *echo.Context, multicastGroupID string, body []byte,
) error {
	var req associateWirelessDeviceWithMulticastRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "invalid request body")
	}

	if err := h.Backend.AssociateWirelessDeviceWithMulticastGroup(multicastGroupID, req.WirelessDeviceID); err != nil {
		return writeError(c, http.StatusInternalServerError, err.Error())
	}

	c.Response().WriteHeader(http.StatusNoContent)

	return nil
}

func (h *Handler) associateWirelessDeviceWithThing(c *echo.Context, wirelessDeviceID string, body []byte) error {
	var req associateWirelessDeviceWithThingRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "invalid request body")
	}

	err := h.Backend.AssociateWirelessDeviceWithThing(h.AccountID, h.DefaultRegion, wirelessDeviceID, req.ThingArn)
	if err != nil {
		if isNotFound(err) {
			return writeError(c, http.StatusNotFound, err.Error())
		}

		return writeError(c, http.StatusInternalServerError, err.Error())
	}

	c.Response().WriteHeader(http.StatusNoContent)

	return nil
}

func (h *Handler) associateWirelessGatewayWithCertificate(c *echo.Context, gatewayID string, body []byte) error {
	var req associateWirelessGatewayWithCertificateRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "invalid request body")
	}

	certARN, err := h.Backend.AssociateWirelessGatewayWithCertificate(
		h.AccountID, h.DefaultRegion, gatewayID, req.IotCertificateID,
	)
	if err != nil {
		if isNotFound(err) {
			return writeError(c, http.StatusNotFound, err.Error())
		}

		return writeError(c, http.StatusInternalServerError, err.Error())
	}

	return writeJSON(c, http.StatusOK, associateWirelessGatewayWithCertificateResponse{IotCertificateArn: certARN})
}

func (h *Handler) associateWirelessGatewayWithThing(c *echo.Context, gatewayID string, body []byte) error {
	var req associateWirelessGatewayWithThingRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "invalid request body")
	}

	err := h.Backend.AssociateWirelessGatewayWithThing(h.AccountID, h.DefaultRegion, gatewayID, req.ThingArn)
	if err != nil {
		if isNotFound(err) {
			return writeError(c, http.StatusNotFound, err.Error())
		}

		return writeError(c, http.StatusInternalServerError, err.Error())
	}

	c.Response().WriteHeader(http.StatusNoContent)

	return nil
}

func (h *Handler) cancelMulticastGroupSession(c *echo.Context, multicastGroupID string) error {
	if err := h.Backend.CancelMulticastGroupSession(multicastGroupID); err != nil {
		return writeError(c, http.StatusInternalServerError, err.Error())
	}

	c.Response().WriteHeader(http.StatusNoContent)

	return nil
}
