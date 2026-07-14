package account

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// AWS Account Management is a rest-json1 service: every operation is a POST
// to a fixed path shaped like the operation name, with every parameter
// (including AccountId, AlternateContactType, RegionName, ...) carried in
// the JSON request body rather than as a query string or URL path segment.
// See https://docs.aws.amazon.com/accounts/latest/reference/API_Operations.html.
const (
	accountService = "account"
	matchPriority  = service.PriorityPathVersioned

	pathGetContactInformation    = "/getContactInformation"
	pathPutContactInformation    = "/putContactInformation"
	pathGetAlternateContact      = "/getAlternateContact"
	pathPutAlternateContact      = "/putAlternateContact"
	pathDeleteAlternateContact   = "/deleteAlternateContact"
	pathListRegions              = "/listRegions"
	pathGetRegionOptStatus       = "/getRegionOptStatus"
	pathEnableRegion             = "/enableRegion"
	pathDisableRegion            = "/disableRegion"
	pathGetPrimaryEmail          = "/getPrimaryEmail"
	pathStartPrimaryEmailUpdate  = "/startPrimaryEmailUpdate"
	pathAcceptPrimaryEmailUpdate = "/acceptPrimaryEmailUpdate"
	pathGetAccountInformation    = "/getAccountInformation"
	pathPutAccountName           = "/putAccountName"

	// amznErrorTypeHeader carries the modeled exception type in the AWS
	// rest-json protocol. SDK clients resolve the exception from this
	// header (falling back to the body's __type field), not from the
	// message text -- omitting it causes a generic/unknown client-side
	// error instead of the modeled exception type.
	amznErrorTypeHeader = "X-Amzn-Errortype"
	keyTypeField        = "__type"
	keyMessageField     = "message"

	keyAccountID    = "AccountId"
	keyPrimaryEmail = "PrimaryEmail"
)

// operationNames maps each fixed request path to its AWS operation name.
// Every operation is POST-only, so the path alone identifies the operation.
var operationNames = map[string]string{ //nolint:gochecknoglobals // package-level lookup table; immutable after init
	pathGetContactInformation:    "GetContactInformation",
	pathPutContactInformation:    "PutContactInformation",
	pathGetAlternateContact:      "GetAlternateContact",
	pathPutAlternateContact:      "PutAlternateContact",
	pathDeleteAlternateContact:   "DeleteAlternateContact",
	pathListRegions:              "ListRegions",
	pathGetRegionOptStatus:       "GetRegionOptStatus",
	pathEnableRegion:             "EnableRegion",
	pathDisableRegion:            "DisableRegion",
	pathGetPrimaryEmail:          "GetPrimaryEmail",
	pathStartPrimaryEmailUpdate:  "StartPrimaryEmailUpdate",
	pathAcceptPrimaryEmailUpdate: "AcceptPrimaryEmailUpdate",
	pathGetAccountInformation:    "GetAccountInformation",
	pathPutAccountName:           "PutAccountName",
}

// Handler implements service.Registerable for the AWS Account Management API.
type Handler struct {
	Backend StorageBackend
}

// NewHandler constructs a Handler backed by the given StorageBackend.
func NewHandler(b StorageBackend) *Handler {
	return &Handler{Backend: b}
}

// Name returns the service name.
func (h *Handler) Name() string { return "Account" }

// Reset delegates to the backend.
func (h *Handler) Reset() { h.Backend.Reset() }

// GetSupportedOperations lists the operations the handler implements.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"GetContactInformation",
		"PutContactInformation",
		"GetAlternateContact",
		"PutAlternateContact",
		"DeleteAlternateContact",
		"ListRegions",
		"GetRegionOptStatus",
		"EnableRegion",
		"DisableRegion",
		"GetPrimaryEmail",
		"StartPrimaryEmailUpdate",
		"AcceptPrimaryEmailUpdate",
		"GetAccountInformation",
		"PutAccountName",
	}
}

// RouteMatcher identifies account service requests by inspecting the SigV4 service name
// from the Authorization header and matching one of the fixed operation paths. Every
// operation is a POST -- Account Management has no GET/PUT/DELETE operations.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		if c.Request().Method != http.MethodPost {
			return false
		}

		svc := httputils.ExtractServiceFromRequest(c.Request())
		if svc != accountService {
			return false
		}

		_, ok := operationNames[c.Request().URL.Path]

		return ok
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return matchPriority }

// ExtractOperation returns the operation name for logging/telemetry.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	if op, ok := operationNames[c.Request().URL.Path]; ok {
		return op
	}

	return "Unknown"
}

// ExtractResource returns a resource identifier for logging.
func (h *Handler) ExtractResource(c *echo.Context) string {
	return strings.TrimPrefix(c.Request().URL.Path, "/")
}

// Handler returns the echo handler function.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		return h.route(c)
	}
}

// handlerFunc is the signature every per-operation handler method shares.
type handlerFunc func(*Handler, *echo.Context, []byte) error

// operationHandlers maps each fixed request path to the method that serves
// it. Method expressions (rather than a switch in route) keep route's
// cyclomatic complexity flat as operations are added.
//
//nolint:gochecknoglobals // immutable lookup table
var operationHandlers = map[string]handlerFunc{
	pathGetContactInformation:    (*Handler).handleGetContactInformation,
	pathPutContactInformation:    (*Handler).handlePutContactInformation,
	pathGetAlternateContact:      (*Handler).handleGetAlternateContact,
	pathPutAlternateContact:      (*Handler).handlePutAlternateContact,
	pathDeleteAlternateContact:   (*Handler).handleDeleteAlternateContact,
	pathListRegions:              (*Handler).handleListRegions,
	pathGetRegionOptStatus:       (*Handler).handleGetRegionOptStatus,
	pathEnableRegion:             (*Handler).handleEnableRegion,
	pathDisableRegion:            (*Handler).handleDisableRegion,
	pathGetPrimaryEmail:          (*Handler).handleGetPrimaryEmail,
	pathStartPrimaryEmailUpdate:  (*Handler).handleStartPrimaryEmailUpdate,
	pathAcceptPrimaryEmailUpdate: (*Handler).handleAcceptPrimaryEmailUpdate,
	pathGetAccountInformation:    (*Handler).handleGetAccountInformation,
	pathPutAccountName:           (*Handler).handlePutAccountName,
}

func (h *Handler) route(c *echo.Context) error {
	if c.Request().Method != http.MethodPost {
		return writeError(c, http.StatusMethodNotAllowed, "InvalidAction", "unsupported method")
	}

	fn, ok := operationHandlers[c.Request().URL.Path]
	if !ok {
		return writeError(c, http.StatusNotFound, "InvalidAction", "unsupported operation")
	}

	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return writeError(c, http.StatusBadRequest, "InvalidRequest", err.Error())
	}

	return fn(h, c, body)
}

// decodeJSON unmarshals body into v, treating a fully empty body the same as
// an empty JSON object ("{}") -- AWS Account Management operations whose
// input has no required fields are called with an empty object body, and
// json.Unmarshal rejects a zero-length slice outright.
func decodeJSON(body []byte, v any) error {
	if len(body) == 0 {
		return nil
	}

	return json.Unmarshal(body, v)
}

func (h *Handler) handleGetContactInformation(c *echo.Context, body []byte) error {
	var req struct {
		AccountID string `json:"AccountId"`
	}

	if err := decodeJSON(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "ValidationException", err.Error())
	}

	info, err := h.Backend.GetContactInformation()
	if err != nil {
		return writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"ContactInformation": info})
}

// requiredContactInformationFields lists the ContactInformation members AWS
// requires (AddressLine2/3, CompanyName, DistrictOrCounty, StateOrRegion and
// WebsiteUrl are optional).
func requiredContactInformationFields(info *ContactInformation) []struct{ name, value string } {
	return []struct{ name, value string }{
		{"AddressLine1", info.AddressLine1},
		{"City", info.City},
		{"CountryCode", info.CountryCode},
		{"FullName", info.FullName},
		{"PhoneNumber", info.PhoneNumber},
		{"PostalCode", info.PostalCode},
	}
}

func (h *Handler) handlePutContactInformation(c *echo.Context, body []byte) error {
	var req struct {
		AccountID          string             `json:"AccountId"`
		ContactInformation ContactInformation `json:"ContactInformation"`
	}

	if err := decodeJSON(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "ValidationException", err.Error())
	}

	for _, f := range requiredContactInformationFields(&req.ContactInformation) {
		if strings.TrimSpace(f.value) == "" {
			return writeError(c, http.StatusBadRequest, "ValidationException", f.name+" is required")
		}
	}

	if err := h.Backend.PutContactInformation(&req.ContactInformation); err != nil {
		return writeBackendError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleGetAlternateContact(c *echo.Context, body []byte) error {
	var req struct {
		AccountID            string      `json:"AccountId"`
		AlternateContactType ContactType `json:"AlternateContactType"`
	}

	if err := decodeJSON(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "ValidationException", err.Error())
	}

	if !isValidContactType(req.AlternateContactType) {
		return writeError(c, http.StatusBadRequest, "ValidationException",
			"AlternateContactType is required and must be one of BILLING, OPERATIONS, SECURITY")
	}

	contact, err := h.Backend.GetAlternateContact(req.AlternateContactType)
	if err != nil {
		return writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"AlternateContact": contact})
}

func (h *Handler) handlePutAlternateContact(c *echo.Context, body []byte) error {
	var req struct {
		AlternateContactType ContactType `json:"AlternateContactType"`
		AccountID            string      `json:"AccountId"`
		EmailAddress         string      `json:"EmailAddress"`
		Name                 string      `json:"Name"`
		PhoneNumber          string      `json:"PhoneNumber"`
		Title                string      `json:"Title"`
	}

	if err := decodeJSON(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "ValidationException", err.Error())
	}

	// AWS Account.PutAlternateContact requires AlternateContactType,
	// EmailAddress, Name, PhoneNumber and Title; an empty value is a
	// ValidationException. Checked in a stable order for deterministic messages.
	requiredFields := []struct{ name, value string }{
		{"AlternateContactType", string(req.AlternateContactType)},
		{"EmailAddress", req.EmailAddress},
		{"Name", req.Name},
		{"PhoneNumber", req.PhoneNumber},
		{"Title", req.Title},
	}
	for _, f := range requiredFields {
		if strings.TrimSpace(f.value) == "" {
			return writeError(c, http.StatusBadRequest, "ValidationException", f.name+" is required")
		}
	}

	if !isValidContactType(req.AlternateContactType) {
		return writeError(c, http.StatusBadRequest, "ValidationException",
			"AlternateContactType must be one of BILLING, OPERATIONS, SECURITY")
	}

	contact := &AlternateContact{
		AlternateContactType: req.AlternateContactType,
		EmailAddress:         req.EmailAddress,
		Name:                 req.Name,
		PhoneNumber:          req.PhoneNumber,
		Title:                req.Title,
	}

	if err := h.Backend.PutAlternateContact(contact); err != nil {
		return writeBackendError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleDeleteAlternateContact(c *echo.Context, body []byte) error {
	var req struct {
		AccountID            string      `json:"AccountId"`
		AlternateContactType ContactType `json:"AlternateContactType"`
	}

	if err := decodeJSON(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "ValidationException", err.Error())
	}

	if !isValidContactType(req.AlternateContactType) {
		return writeError(c, http.StatusBadRequest, "ValidationException",
			"AlternateContactType is required and must be one of BILLING, OPERATIONS, SECURITY")
	}

	if err := h.Backend.DeleteAlternateContact(req.AlternateContactType); err != nil {
		return writeBackendError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

// minMaxResults and maxMaxResults are the documented bounds for ListRegions'
// MaxResults input.
const (
	minMaxResults = 1
	maxMaxResults = 50
)

func (h *Handler) handleListRegions(c *echo.Context, body []byte) error {
	var req struct {
		AccountID               string            `json:"AccountId"`
		NextToken               string            `json:"NextToken"`
		RegionOptStatusContains []RegionOptStatus `json:"RegionOptStatusContains"`
		MaxResults              int               `json:"MaxResults"`
	}

	if err := decodeJSON(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "ValidationException", err.Error())
	}

	if req.MaxResults != 0 && (req.MaxResults < minMaxResults || req.MaxResults > maxMaxResults) {
		return writeError(c, http.StatusBadRequest, "ValidationException",
			fmt.Sprintf("MaxResults must be between %d and %d", minMaxResults, maxMaxResults))
	}

	regions, next, err := h.Backend.ListRegions(req.RegionOptStatusContains, req.MaxResults, req.NextToken)
	if err != nil {
		return writeBackendError(c, err)
	}

	resp := map[string]any{"Regions": regions}
	if next != "" {
		resp["NextToken"] = next
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleGetRegionOptStatus(c *echo.Context, body []byte) error {
	var req struct {
		AccountID  string `json:"AccountId"`
		RegionName string `json:"RegionName"`
	}

	if err := decodeJSON(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "ValidationException", err.Error())
	}

	if strings.TrimSpace(req.RegionName) == "" {
		return writeError(c, http.StatusBadRequest, "ValidationException", "RegionName is required")
	}

	status, err := h.Backend.GetRegionOptStatus(req.RegionName)
	if err != nil {
		return writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"RegionName":      req.RegionName,
		"RegionOptStatus": status,
	})
}

// decodeRegionNameRequest decodes the common {AccountId, RegionName} shape
// shared by EnableRegion and DisableRegion, validating that RegionName is
// present.
func decodeRegionNameRequest(body []byte) (string, error) {
	var req struct {
		AccountID  string `json:"AccountId"`
		RegionName string `json:"RegionName"`
	}

	if err := decodeJSON(body, &req); err != nil {
		return "", err
	}

	if strings.TrimSpace(req.RegionName) == "" {
		return "", errMissingRegionName
	}

	return req.RegionName, nil
}

func (h *Handler) handleEnableRegion(c *echo.Context, body []byte) error {
	regionName, decodeErr := decodeRegionNameRequest(body)
	if decodeErr != nil {
		return writeError(c, http.StatusBadRequest, "ValidationException", decodeErr.Error())
	}

	if err := h.Backend.EnableRegion(regionName); err != nil {
		return writeBackendError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleDisableRegion(c *echo.Context, body []byte) error {
	regionName, decodeErr := decodeRegionNameRequest(body)
	if decodeErr != nil {
		return writeError(c, http.StatusBadRequest, "ValidationException", decodeErr.Error())
	}

	if err := h.Backend.DisableRegion(regionName); err != nil {
		return writeBackendError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleGetPrimaryEmail(c *echo.Context, body []byte) error {
	var req struct {
		AccountID string `json:"AccountId"`
	}

	if err := decodeJSON(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "ValidationException", err.Error())
	}

	// Real GetPrimaryEmail requires AccountId (it is only callable from a
	// management/delegated-admin identity against a member account).
	if strings.TrimSpace(req.AccountID) == "" {
		return writeError(c, http.StatusBadRequest, "ValidationException", "AccountId is required")
	}

	email := h.Backend.GetPrimaryEmail()

	return c.JSON(http.StatusOK, map[string]any{keyPrimaryEmail: email})
}

func (h *Handler) handleStartPrimaryEmailUpdate(c *echo.Context, body []byte) error {
	var req struct {
		AccountID    string `json:"AccountId"`
		PrimaryEmail string `json:"PrimaryEmail"`
	}

	if err := decodeJSON(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "ValidationException", err.Error())
	}

	requiredFields := []struct{ name, value string }{
		{keyAccountID, req.AccountID},
		{keyPrimaryEmail, req.PrimaryEmail},
	}
	for _, f := range requiredFields {
		if strings.TrimSpace(f.value) == "" {
			return writeError(c, http.StatusBadRequest, "ValidationException", f.name+" is required")
		}
	}

	if _, err := h.Backend.StartPrimaryEmailUpdate(req.PrimaryEmail); err != nil {
		return writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"Status": "PENDING"})
}

func (h *Handler) handleAcceptPrimaryEmailUpdate(c *echo.Context, body []byte) error {
	var req struct {
		AccountID    string `json:"AccountId"`
		Otp          string `json:"Otp"`
		PrimaryEmail string `json:"PrimaryEmail"`
	}

	if err := decodeJSON(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "ValidationException", err.Error())
	}

	requiredFields := []struct{ name, value string }{
		{keyAccountID, req.AccountID},
		{"Otp", req.Otp},
		{keyPrimaryEmail, req.PrimaryEmail},
	}
	for _, f := range requiredFields {
		if strings.TrimSpace(f.value) == "" {
			return writeError(c, http.StatusBadRequest, "ValidationException", f.name+" is required")
		}
	}

	if err := h.Backend.AcceptPrimaryEmailUpdate(req.Otp, req.PrimaryEmail); err != nil {
		return writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"Status": "ACCEPTED"})
}

func (h *Handler) handleGetAccountInformation(c *echo.Context, body []byte) error {
	var req struct {
		AccountID string `json:"AccountId"`
	}

	if err := decodeJSON(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "ValidationException", err.Error())
	}

	info, err := h.Backend.GetAccountInformation()
	if err != nil {
		return writeBackendError(c, err)
	}

	// Unlike most Account operations, GetAccountInformationOutput has no
	// wrapper -- AccountId/AccountName/AccountCreatedDate/AccountState are
	// top-level response fields.
	return c.JSON(http.StatusOK, info)
}

func (h *Handler) handlePutAccountName(c *echo.Context, body []byte) error {
	var req struct {
		AccountID   string `json:"AccountId"`
		AccountName string `json:"AccountName"`
	}

	if err := decodeJSON(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "ValidationException", err.Error())
	}

	if strings.TrimSpace(req.AccountName) == "" {
		return writeError(c, http.StatusBadRequest, "ValidationException", "AccountName is required")
	}

	if err := h.Backend.PutAccountName(req.AccountName); err != nil {
		return writeBackendError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

// errMissingRegionName is returned by decodeRegionNameRequest when the
// request body omits the required RegionName field.
var errMissingRegionName = errors.New("RegionName is required")

// writeError emits an AWS rest-json modeled error: the exception type
// travels in both the X-Amzn-Errortype header and the body's __type field.
// SDK clients resolve the exception from the header/__type, not from the
// message text -- omitting either causes a generic/unknown client-side error
// instead of the modeled exception type.
func writeError(c *echo.Context, status int, code, message string) error {
	c.Response().Header().Set(amznErrorTypeHeader, code)

	return c.JSON(status, map[string]any{
		keyTypeField:    code,
		keyMessageField: message,
	})
}

// writeBackendError classifies a backend error by the AWS exception name
// embedded in its message (see the sentinel errors in backend.go) and emits
// the matching modeled error. Anything unrecognized maps to
// InternalServerException/500, matching real Account Management's fallback.
func writeBackendError(c *echo.Context, err error) error {
	code := "InternalServerException"
	status := http.StatusInternalServerError

	switch {
	case strings.Contains(err.Error(), "ResourceNotFoundException"):
		code, status = "ResourceNotFoundException", http.StatusNotFound
	case strings.Contains(err.Error(), "ValidationException"):
		code, status = "ValidationException", http.StatusBadRequest
	case strings.Contains(err.Error(), "ConflictException"):
		code, status = "ConflictException", http.StatusConflict
	case strings.Contains(err.Error(), "AccessDeniedException"):
		code, status = "AccessDeniedException", http.StatusForbidden
	case strings.Contains(err.Error(), "TooManyRequestsException"):
		code, status = "TooManyRequestsException", http.StatusTooManyRequests
	}

	return writeError(c, status, code, err.Error())
}
