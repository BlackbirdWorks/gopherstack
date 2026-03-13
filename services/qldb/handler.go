package qldb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	qldbService        = "qldb"
	qldbMatchPriority  = 87
	qldbLedgersPath    = "/ledgers"
	qldbTagsPrefix     = "/tags/"
	qldbLedgersSegment = "ledgers"
	// qldbLedgerPathMinSegments is the minimum path segments for /ledgers/{name}: ["ledgers", "{name}"].
	qldbLedgerPathMinSegments = 2
)

var (
	errUnknownAction  = errors.New("unknown action")
	errInvalidRequest = errors.New("invalid request")
	errMissingARN     = errors.New("missing resource ARN in path")
)

// Handler is the HTTP handler for the QLDB REST API.
type Handler struct {
	Backend   *InMemoryBackend
	AccountID string
	Region    string
}

// NewHandler creates a new QLDB handler.
func NewHandler(backend *InMemoryBackend) *Handler {
	return &Handler{
		Backend:   backend,
		AccountID: backend.accountID,
		Region:    backend.region,
	}
}

// Name returns the service name.
func (h *Handler) Name() string { return "QLDB" }

// GetSupportedOperations returns the list of supported QLDB operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CreateLedger",
		"DescribeLedger",
		"ListLedgers",
		"UpdateLedger",
		"DeleteLedger",
		"TagResource",
		"UntagResource",
		"ListTagsForResource",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return qldbService }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this handler handles.
func (h *Handler) ChaosRegions() []string { return []string{h.Region} }

// RouteMatcher returns a function that matches QLDB API requests.
// All path-based matches are gated on the SigV4 service name to prevent
// routing conflicts with other services that share similar REST paths.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		if httputils.ExtractServiceFromRequest(c.Request()) != qldbService {
			return false
		}

		path := c.Request().URL.Path

		return strings.HasPrefix(path, qldbLedgersPath) || strings.HasPrefix(path, qldbTagsPrefix)
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return qldbMatchPriority }

// ExtractOperation extracts the operation name from the HTTP method and path.
// Tags paths (/tags/*) are matched first, then /ledgers for list/create,
// and /ledgers/{name} for describe, update, and delete operations.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	method := c.Request().Method
	path := c.Request().URL.Path

	if strings.HasPrefix(path, qldbTagsPrefix) {
		switch method {
		case http.MethodGet:
			return "ListTagsForResource"
		case http.MethodPost:
			return "TagResource"
		case http.MethodDelete:
			return "UntagResource"
		}
	}

	segments := strings.Split(strings.TrimPrefix(path, "/"), "/")

	switch {
	case len(segments) == 1 && segments[0] == qldbLedgersSegment:
		switch method {
		case http.MethodGet:
			return "ListLedgers"
		case http.MethodPost:
			return "CreateLedger"
		}
	case len(segments) == qldbLedgerPathMinSegments && segments[0] == qldbLedgersSegment && segments[1] != "":
		switch method {
		case http.MethodGet:
			return "DescribeLedger"
		case http.MethodPatch:
			return "UpdateLedger"
		case http.MethodDelete:
			return "DeleteLedger"
		}
	}

	return "Unknown"
}

// ExtractResource extracts the ledger name from the request path.
func (h *Handler) ExtractResource(c *echo.Context) string {
	path := c.Request().URL.Path
	segments := strings.Split(strings.TrimPrefix(path, "/"), "/")

	if len(segments) >= qldbLedgerPathMinSegments && segments[0] == qldbLedgersSegment {
		return segments[1]
	}

	return ""
}

// Handler returns the Echo handler function for QLDB requests.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		ctx := c.Request().Context()
		log := logger.Load(ctx)

		path := c.Request().URL.Path
		query := c.Request().URL.Query()

		body, err := httputils.ReadBody(c.Request())
		if err != nil {
			log.ErrorContext(ctx, "qldb: failed to read request body", "error", err)

			return c.String(http.StatusInternalServerError, "internal server error")
		}

		op := h.ExtractOperation(c)

		result, dispErr := h.dispatch(ctx, op, path, query, body)
		if dispErr != nil {
			return h.handleError(c, dispErr)
		}

		if result == nil {
			return c.NoContent(http.StatusOK)
		}

		return c.JSONBlob(http.StatusOK, result)
	}
}

func (h *Handler) dispatch(ctx context.Context, op, path string, query url.Values, body []byte) ([]byte, error) {
	switch op {
	case "CreateLedger":
		return h.handleCreateLedger(ctx, body)
	case "DescribeLedger":
		return h.handleDescribeLedger(ctx, path)
	case "ListLedgers":
		return h.handleListLedgers(ctx)
	case "UpdateLedger":
		return h.handleUpdateLedger(ctx, path, body)
	case "DeleteLedger":
		return h.handleDeleteLedger(ctx, path)
	case "TagResource":
		return h.handleTagResource(ctx, path, body)
	case "UntagResource":
		return h.handleUntagResource(ctx, path, query)
	case "ListTagsForResource":
		return h.handleListTagsForResource(ctx, path)
	default:
		return nil, fmt.Errorf("%w: %s", errUnknownAction, op)
	}
}

func (h *Handler) handleError(c *echo.Context, err error) error {
	var syntaxErr *json.SyntaxError
	var typeErr *json.UnmarshalTypeError

	switch {
	case errors.Is(err, ErrNotFound):
		payload, _ := json.Marshal(map[string]string{
			"__type":  "ResourceNotFoundException",
			"message": err.Error(),
		})

		return c.JSONBlob(http.StatusNotFound, payload)
	case errors.Is(err, ErrAlreadyExists):
		payload, _ := json.Marshal(map[string]string{
			"__type":  "ResourceAlreadyExistsException",
			"message": err.Error(),
		})

		return c.JSONBlob(http.StatusConflict, payload)
	case errors.Is(err, ErrDeletionProtection):
		payload, _ := json.Marshal(map[string]string{
			"__type":  "ResourcePreconditionNotMetException",
			"message": err.Error(),
		})

		return c.JSONBlob(http.StatusBadRequest, payload)
	case errors.Is(err, errInvalidRequest), errors.Is(err, errUnknownAction),
		errors.As(err, &syntaxErr), errors.As(err, &typeErr):
		return c.JSON(http.StatusBadRequest, map[string]string{"message": err.Error()})
	default:
		return c.JSON(http.StatusInternalServerError, map[string]string{"message": err.Error()})
	}
}

// epochSeconds converts a [time.Time] to Unix epoch seconds as float64,
// as required by the AWS REST-JSON protocol for timestamp fields.
func epochSeconds(t time.Time) float64 {
	return float64(t.Unix())
}

type createLedgerRequest struct {
	Tags              map[string]string `json:"Tags"`
	Name              string            `json:"Name"`
	PermissionsMode   string            `json:"PermissionsMode"`
	DeletionProtected bool              `json:"DeletionProtection"`
}

type ledgerResponse struct {
	Tags              map[string]string `json:"Tags,omitempty"`
	Arn               string            `json:"Arn"`
	Name              string            `json:"Name"`
	State             string            `json:"State"`
	PermissionsMode   string            `json:"PermissionsMode"`
	CreationDateTime  float64           `json:"CreationDateTime"`
	DeletionProtected bool              `json:"DeletionProtection"`
}

func toLedgerResponse(l *Ledger) ledgerResponse {
	return ledgerResponse{
		Arn:               l.ARN,
		Name:              l.Name,
		State:             l.State,
		PermissionsMode:   l.PermissionsMode,
		DeletionProtected: l.DeletionProtected,
		CreationDateTime:  epochSeconds(l.CreationDateTime),
		Tags:              l.Tags,
	}
}

func (h *Handler) handleCreateLedger(_ context.Context, body []byte) ([]byte, error) {
	var req createLedgerRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.Name == "" {
		return nil, fmt.Errorf("%w: missing ledger name in request body", errInvalidRequest)
	}

	l, err := h.Backend.CreateLedger(req.Name, req.PermissionsMode, req.DeletionProtected, req.Tags)
	if err != nil {
		return nil, err
	}

	return json.Marshal(toLedgerResponse(l))
}

func (h *Handler) handleDescribeLedger(_ context.Context, path string) ([]byte, error) {
	name := extractLedgerName(path)
	if name == "" {
		return nil, fmt.Errorf("%w: missing ledger name in path", errInvalidRequest)
	}

	l, err := h.Backend.GetLedger(name)
	if err != nil {
		return nil, err
	}

	return json.Marshal(toLedgerResponse(l))
}

type ledgerSummary struct {
	Arn              string  `json:"Arn"`
	Name             string  `json:"Name"`
	State            string  `json:"State"`
	CreationDateTime float64 `json:"CreationDateTime"`
}

type listLedgersResponse struct {
	NextToken string          `json:"NextToken,omitempty"`
	Ledgers   []ledgerSummary `json:"Ledgers"`
}

func (h *Handler) handleListLedgers(_ context.Context) ([]byte, error) {
	ledgers := h.Backend.ListLedgers()
	items := make([]ledgerSummary, 0, len(ledgers))

	for _, l := range ledgers {
		items = append(items, ledgerSummary{
			Arn:              l.ARN,
			Name:             l.Name,
			State:            l.State,
			CreationDateTime: epochSeconds(l.CreationDateTime),
		})
	}

	return json.Marshal(listLedgersResponse{Ledgers: items})
}

type updateLedgerRequest struct {
	DeletionProtected bool `json:"DeletionProtection"`
}

func (h *Handler) handleUpdateLedger(_ context.Context, path string, body []byte) ([]byte, error) {
	name := extractLedgerName(path)
	if name == "" {
		return nil, fmt.Errorf("%w: missing ledger name in path", errInvalidRequest)
	}

	var req updateLedgerRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	l, err := h.Backend.UpdateLedger(name, req.DeletionProtected)
	if err != nil {
		return nil, err
	}

	return json.Marshal(toLedgerResponse(l))
}

func (h *Handler) handleDeleteLedger(_ context.Context, path string) ([]byte, error) {
	name := extractLedgerName(path)
	if name == "" {
		return nil, fmt.Errorf("%w: missing ledger name in path", errInvalidRequest)
	}

	if err := h.Backend.DeleteLedger(name); err != nil {
		return nil, err
	}

	return nil, nil
}

type tagResourceRequest struct {
	Tags map[string]string `json:"Tags"`
}

func (h *Handler) handleTagResource(_ context.Context, path string, body []byte) ([]byte, error) {
	resourceARN, err := extractTagsARN(path)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	var req tagResourceRequest
	if unmarshalErr := json.Unmarshal(body, &req); unmarshalErr != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, unmarshalErr)
	}

	if tagErr := h.Backend.TagResource(resourceARN, req.Tags); tagErr != nil {
		return nil, tagErr
	}

	return nil, nil
}

func (h *Handler) handleUntagResource(_ context.Context, path string, query url.Values) ([]byte, error) {
	resourceARN, err := extractTagsARN(path)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	tagKeys := query["tagKeys"]

	if untagErr := h.Backend.UntagResource(resourceARN, tagKeys); untagErr != nil {
		return nil, untagErr
	}

	return nil, nil
}

type listTagsResponse struct {
	Tags map[string]string `json:"Tags"`
}

func (h *Handler) handleListTagsForResource(_ context.Context, path string) ([]byte, error) {
	resourceARN, err := extractTagsARN(path)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	tags, err := h.Backend.ListTagsForResource(resourceARN)
	if err != nil {
		return nil, err
	}

	return json.Marshal(listTagsResponse{Tags: tags})
}

// extractLedgerName extracts the ledger name from a /ledgers/{name} path.
func extractLedgerName(path string) string {
	segments := strings.Split(strings.TrimPrefix(path, "/"), "/")
	if len(segments) >= qldbLedgerPathMinSegments && segments[0] == qldbLedgersSegment {
		return segments[1]
	}

	return ""
}

// extractTagsARN extracts and URL-decodes the ARN from a /tags/{arn} path.
func extractTagsARN(path string) (string, error) {
	rawARN := strings.TrimPrefix(path, qldbTagsPrefix)
	if rawARN == "" {
		return "", errMissingARN
	}

	decoded, err := url.PathUnescape(rawARN)
	if err != nil {
		return "", fmt.Errorf("invalid ARN encoding: %w", err)
	}

	return decoded, nil
}
