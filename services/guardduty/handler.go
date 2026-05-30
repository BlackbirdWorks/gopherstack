package guardduty

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	guardDutyService = "guardduty"
	matchPriority    = service.PriorityPathVersioned

	pathDetector       = "detector"
	pathFilter         = "filter"
	pathFindings       = "findings"
	pathIPSet          = "ipset"
	pathThreatIntelSet = "threatintelset"
	pathTags           = "tags"

	keyName   = "name"
	keyStatus = "status"
	keyTags   = "tags"

	opCreateDetector         = "CreateDetector"
	opGetDetector            = "GetDetector"
	opUpdateDetector         = "UpdateDetector"
	opDeleteDetector         = "DeleteDetector"
	opListDetectors          = "ListDetectors"
	opCreateFilter           = "CreateFilter"
	opGetFilter              = "GetFilter"
	opUpdateFilter           = "UpdateFilter"
	opDeleteFilter           = "DeleteFilter"
	opListFilters            = "ListFilters"
	opGetFindings            = "GetFindings"
	opListFindings           = "ListFindings"
	opArchiveFindings        = "ArchiveFindings"
	opUnarchiveFindings      = "UnarchiveFindings"
	opCreateSampleFindings   = "CreateSampleFindings"
	opGetFindingsStatistics  = "GetFindingsStatistics"
	opUpdateFindingsFeedback = "UpdateFindingsFeedback"
	opCreateIPSet            = "CreateIPSet"
	opGetIPSet               = "GetIPSet"
	opUpdateIPSet            = "UpdateIPSet"
	opDeleteIPSet            = "DeleteIPSet"
	opListIPSets             = "ListIPSets"
	opCreateThreatIntelSet   = "CreateThreatIntelSet"
	opGetThreatIntelSet      = "GetThreatIntelSet"
	opUpdateThreatIntelSet   = "UpdateThreatIntelSet"
	opDeleteThreatIntelSet   = "DeleteThreatIntelSet"
	opListThreatIntelSets    = "ListThreatIntelSets"
	opTagResource            = "TagResource"
	opUntagResource          = "UntagResource"
	opListTagsForResource    = "ListTagsForResource"
	opUnknown                = "Unknown"

	// URL depth constants for path parsing.
	depthRoot       = 1 // /detector
	depthResource   = 2 // /detector/{id}
	depthCollection = 3 // /detector/{id}/filter
	depthItem       = 4 // /detector/{id}/filter/{name}
	depthAction     = 4 // /detector/{id}/findings/archive

	minTagPathParts       = 2
	minDetectorSubIDParts = 4
)

// Handler handles GuardDuty HTTP requests.
type Handler struct {
	Backend StorageBackend
}

// NewHandler constructs a new Handler.
func NewHandler(b StorageBackend) *Handler {
	return &Handler{Backend: b}
}

// Name returns the service name.
func (h *Handler) Name() string { return "GuardDuty" }

// Reset resets the backend.
func (h *Handler) Reset() { h.Backend.Reset() }

// GetSupportedOperations returns the list of supported operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		opCreateDetector,
		opGetDetector,
		opUpdateDetector,
		opDeleteDetector,
		opListDetectors,
		opCreateFilter,
		opGetFilter,
		opUpdateFilter,
		opDeleteFilter,
		opListFilters,
		opGetFindings,
		opListFindings,
		opArchiveFindings,
		opUnarchiveFindings,
		opCreateSampleFindings,
		opGetFindingsStatistics,
		opUpdateFindingsFeedback,
		opCreateIPSet,
		opGetIPSet,
		opUpdateIPSet,
		opDeleteIPSet,
		opListIPSets,
		opCreateThreatIntelSet,
		opGetThreatIntelSet,
		opUpdateThreatIntelSet,
		opDeleteThreatIntelSet,
		opListThreatIntelSets,
		opTagResource,
		opUntagResource,
		opListTagsForResource,
	}
}

// RouteMatcher returns a function that matches GuardDuty requests by path prefix.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		path := c.Request().URL.Path

		return strings.HasPrefix(path, "/"+pathDetector) ||
			strings.HasPrefix(path, "/"+pathTags+"/")
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return matchPriority }

// ExtractOperation extracts the operation name from the request.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	op, _ := parseRESTPath(c.Request().Method, c.Request().URL.Path)

	return op
}

// ExtractResource extracts the resource identifier from the request.
func (h *Handler) ExtractResource(c *echo.Context) string {
	_, resource := parseRESTPath(c.Request().Method, c.Request().URL.Path)

	return resource
}

// Handler returns the Echo handler function.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		return h.handleREST(c)
	}
}

func (h *Handler) handleREST(c *echo.Context) error {
	ctx := c.Request().Context()
	log := logger.Load(ctx)

	op, _ := parseRESTPath(c.Request().Method, c.Request().URL.Path)

	if op == opUnknown {
		return c.JSON(http.StatusNotFound, errBody(errResourceNotFound, "not found"))
	}

	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errBody("BadRequestException", "failed to read body"))
	}

	result, statusCode, opErr := h.dispatch(ctx, op, c.Request().URL.Path, c.Request().URL.RawQuery, body)
	if opErr != nil {
		log.Error("guardduty operation error", "op", op, "err", opErr)

		return h.handleError(c, opErr)
	}

	if result == nil {
		return c.JSON(statusCode, struct{}{})
	}

	data, jsonErr := json.Marshal(result)
	if jsonErr != nil {
		return c.JSON(http.StatusInternalServerError, errBody("InternalFailure", "serialization failed"))
	}

	c.Response().Header().Set("Content-Type", "application/json")

	return c.JSONBlob(statusCode, data)
}

func (h *Handler) dispatch(
	_ context.Context,
	op, path, query string,
	body []byte,
) (any, int, error) {
	if result, code, ok, err := h.dispatchDetectorOps(op, path, body); ok {
		return result, code, err
	}

	if result, code, ok, err := h.dispatchFilterOps(op, path, body); ok {
		return result, code, err
	}

	if result, code, ok, err := h.dispatchFindingOps(op, path, body); ok {
		return result, code, err
	}

	if result, code, ok, err := h.dispatchIPSetOps(op, path, body); ok {
		return result, code, err
	}

	if result, code, ok, err := h.dispatchThreatIntelSetOps(op, path, body); ok {
		return result, code, err
	}

	return h.dispatchTagOps(op, path, query, body)
}

// parseRESTPath maps (method, path) → (operation, resource).
//
// GuardDuty REST paths:
//
//	/detector                              → CreateDetector (POST) / ListDetectors (GET)
//	/detector/{id}                         → GetDetector (GET) / UpdateDetector (POST) / DeleteDetector (DELETE)
//	/detector/{id}/filter                  → CreateFilter (POST) / ListFilters (GET)
//	/detector/{id}/filter/{name}           → GetFilter (GET) / UpdateFilter (POST) / DeleteFilter (DELETE)
//	/detector/{id}/findings                → ListFindings (POST)
//	/detector/{id}/findings/get            → GetFindings (POST)
//	/detector/{id}/findings/archive        → ArchiveFindings (POST)
//	/detector/{id}/findings/unarchive      → UnarchiveFindings (POST)
//	/detector/{id}/findings/create         → CreateSampleFindings (POST)
//	/detector/{id}/findings/statistics     → GetFindingsStatistics (POST)
//	/detector/{id}/findings/feedback       → UpdateFindingsFeedback (POST)
//	/detector/{id}/ipset                   → CreateIPSet (POST) / ListIPSets (GET)
//	/detector/{id}/ipset/{ipSetId}         → GetIPSet (GET) / UpdateIPSet (POST) / DeleteIPSet (DELETE)
//	/detector/{id}/threatintelset          → CreateThreatIntelSet (POST) / ListThreatIntelSets (GET)
//	/detector/{id}/threatintelset/{setId}  → GetThreatIntelSet (GET) / UpdateThreatIntelSet (POST) /
//	                                          DeleteThreatIntelSet (DELETE)
//	/tags/{resourceArn}                    → ListTagsForResource (GET) / TagResource (POST) / UntagResource (DELETE)
func parseRESTPath(method, path string) (string, string) {
	parts := strings.Split(strings.TrimPrefix(path, "/"), "/")

	if len(parts) == 0 {
		return opUnknown, ""
	}

	switch parts[0] {
	case pathDetector:
		return parseDetectorPath(method, parts)
	case pathTags:
		return parseTagPath(method, parts)
	}

	return opUnknown, ""
}

//nolint:gocognit,gocyclo,cyclop // intentional switch matrix for REST path parsing
func parseDetectorPath(method string, parts []string) (string, string) {
	switch len(parts) {
	case depthRoot: // /detector
		switch method {
		case http.MethodPost:
			return opCreateDetector, ""
		case http.MethodGet:
			return opListDetectors, ""
		}

	case depthResource: // /detector/{id}
		detectorID := parts[1]
		switch method {
		case http.MethodGet:
			return opGetDetector, detectorID
		case http.MethodPost:
			return opUpdateDetector, detectorID
		case http.MethodDelete:
			return opDeleteDetector, detectorID
		}

	case depthCollection: // /detector/{id}/{collection}
		detectorID := parts[1]
		collection := parts[2]
		switch collection {
		case pathFilter:
			switch method {
			case http.MethodPost:
				return opCreateFilter, detectorID
			case http.MethodGet:
				return opListFilters, detectorID
			}
		case pathFindings:
			if method == http.MethodPost {
				return opListFindings, detectorID
			}
		case pathIPSet:
			switch method {
			case http.MethodPost:
				return opCreateIPSet, detectorID
			case http.MethodGet:
				return opListIPSets, detectorID
			}
		case pathThreatIntelSet:
			switch method {
			case http.MethodPost:
				return opCreateThreatIntelSet, detectorID
			case http.MethodGet:
				return opListThreatIntelSets, detectorID
			}
		}

	case depthItem: // /detector/{id}/{collection}/{item}
		detectorID := parts[1]
		collection := parts[2]
		item := parts[3]
		switch collection {
		case pathFilter:
			switch method {
			case http.MethodGet:
				return opGetFilter, detectorID + "/" + item
			case http.MethodPost:
				return opUpdateFilter, detectorID + "/" + item
			case http.MethodDelete:
				return opDeleteFilter, detectorID + "/" + item
			}
		case pathFindings:
			// /detector/{id}/findings/{action}
			if method == http.MethodPost {
				switch item {
				case "get":
					return opGetFindings, detectorID
				case "archive":
					return opArchiveFindings, detectorID
				case "unarchive":
					return opUnarchiveFindings, detectorID
				case "create":
					return opCreateSampleFindings, detectorID
				case "statistics":
					return opGetFindingsStatistics, detectorID
				case "feedback":
					return opUpdateFindingsFeedback, detectorID
				}
			}
		case pathIPSet:
			switch method {
			case http.MethodGet:
				return opGetIPSet, detectorID + "/" + item
			case http.MethodPost:
				return opUpdateIPSet, detectorID + "/" + item
			case http.MethodDelete:
				return opDeleteIPSet, detectorID + "/" + item
			}
		case pathThreatIntelSet:
			switch method {
			case http.MethodGet:
				return opGetThreatIntelSet, detectorID + "/" + item
			case http.MethodPost:
				return opUpdateThreatIntelSet, detectorID + "/" + item
			case http.MethodDelete:
				return opDeleteThreatIntelSet, detectorID + "/" + item
			}
		}
	}

	return opUnknown, ""
}

func parseTagPath(method string, parts []string) (string, string) {
	if len(parts) < minTagPathParts {
		return opUnknown, ""
	}

	// /tags/{resourceArn} — the ARN may have slashes, rejoin from index 1.
	resourceARN := strings.Join(parts[1:], "/")
	switch method {
	case http.MethodGet:
		return opListTagsForResource, resourceARN
	case http.MethodPost:
		return opTagResource, resourceARN
	case http.MethodDelete:
		return opUntagResource, resourceARN
	}

	return opUnknown, ""
}

func (h *Handler) dispatchDetectorOps(op, path string, body []byte) (any, int, bool, error) {
	switch op {
	case opCreateDetector:
		result, code, err := h.handleCreateDetector(body)

		return result, code, true, err

	case opGetDetector:
		detectorID := extractID(path, pathDetector)
		result, code, err := h.handleGetDetector(detectorID)

		return result, code, true, err

	case opUpdateDetector:
		detectorID := extractID(path, pathDetector)
		code, err := h.handleUpdateDetector(detectorID, body)

		return nil, code, true, err

	case opDeleteDetector:
		detectorID := extractID(path, pathDetector)
		code, err := h.handleDeleteDetector(detectorID)

		return nil, code, true, err

	case opListDetectors:
		result, code := h.handleListDetectors()

		return result, code, true, nil
	}

	return nil, 0, false, nil
}

func (h *Handler) dispatchFilterOps(op, path string, body []byte) (any, int, bool, error) {
	switch op {
	case opCreateFilter:
		detectorID := extractID(path, pathDetector)
		result, code, err := h.handleCreateFilter(detectorID, body)

		return result, code, true, err

	case opGetFilter:
		detectorID, filterName := extractDetectorAndSubID(path)
		result, code, err := h.handleGetFilter(detectorID, filterName)

		return result, code, true, err

	case opUpdateFilter:
		detectorID, filterName := extractDetectorAndSubID(path)
		result, code, err := h.handleUpdateFilter(detectorID, filterName, body)

		return result, code, true, err

	case opDeleteFilter:
		detectorID, filterName := extractDetectorAndSubID(path)
		code, err := h.handleDeleteFilter(detectorID, filterName)

		return nil, code, true, err

	case opListFilters:
		detectorID := extractID(path, pathDetector)
		result, code, err := h.handleListFilters(detectorID)

		return result, code, true, err
	}

	return nil, 0, false, nil
}

func (h *Handler) dispatchFindingOps(op, path string, body []byte) (any, int, bool, error) {
	detectorID := extractID(path, pathDetector)

	switch op {
	case opGetFindings:
		result, code, err := h.handleGetFindings(detectorID, body)

		return result, code, true, err

	case opListFindings:
		result, code, err := h.handleListFindings(detectorID)

		return result, code, true, err

	case opArchiveFindings:
		code, err := h.handleArchiveFindings(detectorID, body)

		return nil, code, true, err

	case opUnarchiveFindings:
		code, err := h.handleUnarchiveFindings(detectorID, body)

		return nil, code, true, err

	case opCreateSampleFindings:
		code, err := h.handleCreateSampleFindings(detectorID, body)

		return nil, code, true, err

	case opGetFindingsStatistics:
		result, code, err := h.handleGetFindingsStatistics(detectorID)

		return result, code, true, err

	case opUpdateFindingsFeedback:
		code, err := h.handleUpdateFindingsFeedback(detectorID, body)

		return nil, code, true, err
	}

	return nil, 0, false, nil
}

func (h *Handler) dispatchIPSetOps(op, path string, body []byte) (any, int, bool, error) {
	switch op {
	case opCreateIPSet:
		detectorID := extractID(path, pathDetector)
		result, code, err := h.handleCreateIPSet(detectorID, body)

		return result, code, true, err

	case opGetIPSet:
		detectorID, ipSetID := extractDetectorAndSubID(path)
		result, code, err := h.handleGetIPSet(detectorID, ipSetID)

		return result, code, true, err

	case opUpdateIPSet:
		detectorID, ipSetID := extractDetectorAndSubID(path)
		code, err := h.handleUpdateIPSet(detectorID, ipSetID, body)

		return nil, code, true, err

	case opDeleteIPSet:
		detectorID, ipSetID := extractDetectorAndSubID(path)
		code, err := h.handleDeleteIPSet(detectorID, ipSetID)

		return nil, code, true, err

	case opListIPSets:
		detectorID := extractID(path, pathDetector)
		result, code, err := h.handleListIPSets(detectorID)

		return result, code, true, err
	}

	return nil, 0, false, nil
}

func (h *Handler) dispatchThreatIntelSetOps(op, path string, body []byte) (any, int, bool, error) {
	switch op {
	case opCreateThreatIntelSet:
		detectorID := extractID(path, pathDetector)
		result, code, err := h.handleCreateThreatIntelSet(detectorID, body)

		return result, code, true, err

	case opGetThreatIntelSet:
		detectorID, setID := extractDetectorAndSubID(path)
		result, code, err := h.handleGetThreatIntelSet(detectorID, setID)

		return result, code, true, err

	case opUpdateThreatIntelSet:
		detectorID, setID := extractDetectorAndSubID(path)
		code, err := h.handleUpdateThreatIntelSet(detectorID, setID, body)

		return nil, code, true, err

	case opDeleteThreatIntelSet:
		detectorID, setID := extractDetectorAndSubID(path)
		code, err := h.handleDeleteThreatIntelSet(detectorID, setID)

		return nil, code, true, err

	case opListThreatIntelSets:
		detectorID := extractID(path, pathDetector)
		result, code, err := h.handleListThreatIntelSets(detectorID)

		return result, code, true, err
	}

	return nil, 0, false, nil
}

func (h *Handler) dispatchTagOps(op, path, query string, body []byte) (any, int, error) {
	resourceARN := extractTagResourceARN(path)

	switch op {
	case opListTagsForResource:
		result, code, err := h.handleListTagsForResource(resourceARN)

		return result, code, err

	case opTagResource:
		code, err := h.handleTagResource(resourceARN, body)

		return nil, code, err

	case opUntagResource:
		code, err := h.handleUntagResource(resourceARN, query)

		return nil, code, err
	}

	return nil, http.StatusNotFound, errorf(errResourceNotFound)
}

// --- detector handlers ---

func (h *Handler) handleCreateDetector(body []byte) (any, int, error) {
	var req struct {
		Enable                     *bool             `json:"enable"`
		ClientToken                string            `json:"clientToken"`
		FindingPublishingFrequency string            `json:"findingPublishingFrequency"`
		Tags                       map[string]string `json:"tags"`
		Features                   []DetectorFeature `json:"features"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, http.StatusBadRequest, ErrValidation
	}

	if req.Enable == nil {
		return nil, http.StatusBadRequest, ErrValidation
	}

	d, err := h.Backend.CreateDetector(*req.Enable, req.FindingPublishingFrequency, req.Tags, req.Features)
	if err != nil {
		return nil, http.StatusBadRequest, err
	}

	return map[string]any{"detectorId": d.DetectorID}, http.StatusOK, nil
}

func (h *Handler) handleGetDetector(detectorID string) (any, int, error) {
	d, err := h.Backend.GetDetector(detectorID)
	if err != nil {
		return nil, http.StatusNotFound, err
	}

	return map[string]any{
		keyStatus:                    d.Status,
		"serviceRole":                d.ServiceRole,
		"findingPublishingFrequency": d.FindingPublishingFrequency,
		"createdAt":                  d.CreatedAt.Format("2006-01-02T15:04:05.000Z"),
		"updatedAt":                  d.UpdatedAt.Format("2006-01-02T15:04:05.000Z"),
		keyTags:                      d.Tags,
		"features":                   d.Features,
	}, http.StatusOK, nil
}

func (h *Handler) handleUpdateDetector(detectorID string, body []byte) (int, error) {
	var req struct {
		Enable                     *bool             `json:"enable"`
		FindingPublishingFrequency string            `json:"findingPublishingFrequency"`
		Features                   []DetectorFeature `json:"features"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return http.StatusBadRequest, ErrValidation
	}

	if err := h.Backend.UpdateDetector(
		detectorID, req.Enable, req.FindingPublishingFrequency, req.Features,
	); err != nil {
		return http.StatusNotFound, err
	}

	return http.StatusOK, nil
}

func (h *Handler) handleDeleteDetector(detectorID string) (int, error) {
	if err := h.Backend.DeleteDetector(detectorID); err != nil {
		return http.StatusNotFound, err
	}

	return http.StatusOK, nil
}

func (h *Handler) handleListDetectors() (any, int) {
	ids := h.Backend.ListDetectors()

	return map[string]any{"detectorIds": ids}, http.StatusOK
}

// --- filter handlers ---

func (h *Handler) handleCreateFilter(detectorID string, body []byte) (any, int, error) {
	var req struct {
		FindingCriteria map[string]any    `json:"findingCriteria"`
		Tags            map[string]string `json:"tags"`
		Name            string            `json:"name"`
		Description     string            `json:"description"`
		Action          string            `json:"action"`
		Rank            int32             `json:"rank"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, http.StatusBadRequest, ErrValidation
	}

	if req.Name == "" {
		return nil, http.StatusBadRequest, ErrValidation
	}

	if req.Action == "" {
		req.Action = "NOOP"
	}

	f, err := h.Backend.CreateFilter(
		detectorID,
		req.Name,
		req.Description,
		req.Action,
		req.Rank,
		req.FindingCriteria,
		req.Tags,
	)
	if err != nil {
		return nil, http.StatusBadRequest, err
	}

	return map[string]any{keyName: f.Name}, http.StatusOK, nil
}

func (h *Handler) handleGetFilter(detectorID, filterName string) (any, int, error) {
	f, err := h.Backend.GetFilter(detectorID, filterName)
	if err != nil {
		return nil, http.StatusNotFound, err
	}

	return map[string]any{
		keyName:           f.Name,
		"description":     f.Description,
		"action":          f.Action,
		"rank":            f.Rank,
		"findingCriteria": f.FindingCriteria,
		keyTags:           f.Tags,
	}, http.StatusOK, nil
}

func (h *Handler) handleUpdateFilter(detectorID, filterName string, body []byte) (any, int, error) {
	var req struct {
		FindingCriteria map[string]any `json:"findingCriteria"`
		Description     string         `json:"description"`
		Action          string         `json:"action"`
		Rank            int32          `json:"rank"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, http.StatusBadRequest, ErrValidation
	}

	f, err := h.Backend.UpdateFilter(detectorID, filterName, req.Description, req.Action, req.Rank, req.FindingCriteria)
	if err != nil {
		return nil, http.StatusNotFound, err
	}

	return map[string]any{keyName: f.Name}, http.StatusOK, nil
}

func (h *Handler) handleDeleteFilter(detectorID, filterName string) (int, error) {
	if err := h.Backend.DeleteFilter(detectorID, filterName); err != nil {
		return http.StatusNotFound, err
	}

	return http.StatusOK, nil
}

func (h *Handler) handleListFilters(detectorID string) (any, int, error) {
	names, err := h.Backend.ListFilters(detectorID)
	if err != nil {
		return nil, http.StatusNotFound, err
	}

	return map[string]any{"filterNames": names}, http.StatusOK, nil
}

// --- finding handlers ---

func (h *Handler) handleGetFindings(detectorID string, body []byte) (any, int, error) {
	var req struct {
		FindingIDs []string `json:"findingIds"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, http.StatusBadRequest, ErrValidation
	}

	findings, err := h.Backend.GetFindings(detectorID, req.FindingIDs)
	if err != nil {
		return nil, http.StatusNotFound, err
	}

	return map[string]any{"findings": findings}, http.StatusOK, nil
}

func (h *Handler) handleListFindings(detectorID string) (any, int, error) {
	ids, err := h.Backend.ListFindings(detectorID)
	if err != nil {
		return nil, http.StatusNotFound, err
	}

	return map[string]any{"findingIds": ids}, http.StatusOK, nil
}

func (h *Handler) handleArchiveFindings(detectorID string, body []byte) (int, error) {
	var req struct {
		FindingIDs []string `json:"findingIds"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return http.StatusBadRequest, ErrValidation
	}

	if err := h.Backend.ArchiveFindings(detectorID, req.FindingIDs); err != nil {
		return http.StatusNotFound, err
	}

	return http.StatusOK, nil
}

func (h *Handler) handleUnarchiveFindings(detectorID string, body []byte) (int, error) {
	var req struct {
		FindingIDs []string `json:"findingIds"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return http.StatusBadRequest, ErrValidation
	}

	if err := h.Backend.UnarchiveFindings(detectorID, req.FindingIDs); err != nil {
		return http.StatusNotFound, err
	}

	return http.StatusOK, nil
}

func (h *Handler) handleCreateSampleFindings(detectorID string, body []byte) (int, error) {
	var req struct {
		FindingTypes []string `json:"findingTypes"`
	}

	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return http.StatusBadRequest, ErrValidation
		}
	}

	if err := h.Backend.CreateSampleFindings(detectorID, req.FindingTypes); err != nil {
		return http.StatusNotFound, err
	}

	return http.StatusOK, nil
}

func (h *Handler) handleGetFindingsStatistics(detectorID string) (any, int, error) {
	stats, err := h.Backend.GetFindingsStatistics(detectorID)
	if err != nil {
		return nil, http.StatusNotFound, err
	}

	return stats, http.StatusOK, nil
}

func (h *Handler) handleUpdateFindingsFeedback(detectorID string, body []byte) (int, error) {
	var req struct {
		FindingIDs []string `json:"findingIds"`
		Feedback   string   `json:"feedback"` //nolint:govet // fieldalignment: logical grouping preferred
		Comments   string   `json:"comments"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return http.StatusBadRequest, ErrValidation
	}

	if err := h.Backend.UpdateFindingsFeedback(detectorID, req.FindingIDs, req.Feedback); err != nil {
		return http.StatusNotFound, err
	}

	return http.StatusOK, nil
}

// --- IPSet handlers ---

//nolint:dupl // IPSet and ThreatIntelSet have identical handler patterns
func (h *Handler) handleCreateIPSet(detectorID string, body []byte) (any, int, error) {
	var req struct {
		Tags     map[string]string `json:"tags"`
		Name     string            `json:"name"`
		Format   string            `json:"format"`
		Location string            `json:"location"`
		Activate *bool             `json:"activate"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, http.StatusBadRequest, ErrValidation
	}

	if req.Name == "" || req.Format == "" || req.Location == "" {
		return nil, http.StatusBadRequest, ErrValidation
	}

	activate := false
	if req.Activate != nil {
		activate = *req.Activate
	}

	s, err := h.Backend.CreateIPSet(detectorID, req.Name, req.Format, req.Location, activate, req.Tags)
	if err != nil {
		return nil, http.StatusBadRequest, err
	}

	return map[string]any{"ipSetId": s.IPSetID}, http.StatusOK, nil
}

func (h *Handler) handleGetIPSet(detectorID, ipSetID string) (any, int, error) {
	s, err := h.Backend.GetIPSet(detectorID, ipSetID)
	if err != nil {
		return nil, http.StatusNotFound, err
	}

	return map[string]any{
		keyName:    s.Name,
		"format":   s.Format,
		"location": s.Location,
		keyStatus:  s.Status,
		keyTags:    s.Tags,
	}, http.StatusOK, nil
}

func (h *Handler) handleUpdateIPSet(detectorID, ipSetID string, body []byte) (int, error) {
	var req struct {
		Name     string `json:"name"`
		Location string `json:"location"`
		Activate *bool  `json:"activate"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return http.StatusBadRequest, ErrValidation
	}

	if err := h.Backend.UpdateIPSet(detectorID, ipSetID, req.Name, req.Location, req.Activate); err != nil {
		return http.StatusNotFound, err
	}

	return http.StatusOK, nil
}

func (h *Handler) handleDeleteIPSet(detectorID, ipSetID string) (int, error) {
	if err := h.Backend.DeleteIPSet(detectorID, ipSetID); err != nil {
		return http.StatusNotFound, err
	}

	return http.StatusOK, nil
}

func (h *Handler) handleListIPSets(detectorID string) (any, int, error) {
	ids, err := h.Backend.ListIPSets(detectorID)
	if err != nil {
		return nil, http.StatusNotFound, err
	}

	return map[string]any{"ipSetIds": ids}, http.StatusOK, nil
}

// --- ThreatIntelSet handlers ---

//nolint:dupl // IPSet and ThreatIntelSet have identical handler patterns
func (h *Handler) handleCreateThreatIntelSet(detectorID string, body []byte) (any, int, error) {
	var req struct {
		Tags     map[string]string `json:"tags"`
		Name     string            `json:"name"`
		Format   string            `json:"format"`
		Location string            `json:"location"`
		Activate *bool             `json:"activate"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, http.StatusBadRequest, ErrValidation
	}

	if req.Name == "" || req.Format == "" || req.Location == "" {
		return nil, http.StatusBadRequest, ErrValidation
	}

	activate := false
	if req.Activate != nil {
		activate = *req.Activate
	}

	s, err := h.Backend.CreateThreatIntelSet(detectorID, req.Name, req.Format, req.Location, activate, req.Tags)
	if err != nil {
		return nil, http.StatusBadRequest, err
	}

	return map[string]any{"threatIntelSetId": s.ThreatIntelSetID}, http.StatusOK, nil
}

func (h *Handler) handleGetThreatIntelSet(detectorID, setID string) (any, int, error) {
	s, err := h.Backend.GetThreatIntelSet(detectorID, setID)
	if err != nil {
		return nil, http.StatusNotFound, err
	}

	return map[string]any{
		keyName:    s.Name,
		"format":   s.Format,
		"location": s.Location,
		keyStatus:  s.Status,
		keyTags:    s.Tags,
	}, http.StatusOK, nil
}

func (h *Handler) handleUpdateThreatIntelSet(detectorID, setID string, body []byte) (int, error) {
	var req struct {
		Name     string `json:"name"`
		Location string `json:"location"`
		Activate *bool  `json:"activate"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return http.StatusBadRequest, ErrValidation
	}

	if err := h.Backend.UpdateThreatIntelSet(detectorID, setID, req.Name, req.Location, req.Activate); err != nil {
		return http.StatusNotFound, err
	}

	return http.StatusOK, nil
}

func (h *Handler) handleDeleteThreatIntelSet(detectorID, setID string) (int, error) {
	if err := h.Backend.DeleteThreatIntelSet(detectorID, setID); err != nil {
		return http.StatusNotFound, err
	}

	return http.StatusOK, nil
}

func (h *Handler) handleListThreatIntelSets(detectorID string) (any, int, error) {
	ids, err := h.Backend.ListThreatIntelSets(detectorID)
	if err != nil {
		return nil, http.StatusNotFound, err
	}

	return map[string]any{"threatIntelSetIds": ids}, http.StatusOK, nil
}

// --- tag handlers ---

func (h *Handler) handleListTagsForResource(resourceARN string) (any, int, error) {
	tags, err := h.Backend.ListTagsForResource(resourceARN)
	if err != nil {
		return nil, http.StatusNotFound, err
	}

	return map[string]any{keyTags: tags}, http.StatusOK, nil
}

func (h *Handler) handleTagResource(resourceARN string, body []byte) (int, error) {
	var req struct {
		Tags map[string]string `json:"tags"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return http.StatusBadRequest, ErrValidation
	}

	if err := h.Backend.TagResource(resourceARN, req.Tags); err != nil {
		return http.StatusBadRequest, err
	}

	return http.StatusOK, nil
}

func (h *Handler) handleUntagResource(resourceARN, query string) (int, error) {
	keys := parseTagKeys(query)

	if err := h.Backend.UntagResource(resourceARN, keys); err != nil {
		return http.StatusBadRequest, err
	}

	return http.StatusOK, nil
}

// --- error handling ---

func (h *Handler) handleError(c *echo.Context, err error) error {
	code := http.StatusInternalServerError
	msg := err.Error()

	switch {
	case errors.Is(err, awserr.ErrNotFound):
		code = http.StatusNotFound
	case errors.Is(err, awserr.ErrConflict):
		code = http.StatusConflict
	case errors.Is(err, awserr.ErrInvalidParameter):
		code = http.StatusBadRequest
	}

	return c.JSON(code, errBody(msg, msg))
}

// --- helpers ---

func errBody(code, message string) map[string]string {
	return map[string]string{
		"__type":  code,
		"message": message,
	}
}

func errorf(code string) error {
	return awserr.New(code, awserr.ErrInvalidParameter)
}

// extractID extracts a resource ID from a path like /prefix/{id}/...
//
//nolint:unparam // prefix is always pathDetector by design
func extractID(path, prefix string) string {
	stripped := strings.TrimPrefix(path, "/"+prefix+"/")
	before, _, found := strings.Cut(stripped, "/")

	if !found {
		return stripped
	}

	return before
}

// extractDetectorAndSubID extracts detectorID and the sub-resource ID from
// paths like /detector/{id}/{collection}/{subID}.
func extractDetectorAndSubID(path string) (string, string) {
	// path: /detector/{detectorID}/{collection}/{subID}
	parts := strings.Split(strings.TrimPrefix(path, "/"), "/")

	if len(parts) < minDetectorSubIDParts {
		return "", ""
	}

	return parts[1], parts[3]
}

// extractTagResourceARN extracts the resource ARN from /tags/{arn}.
func extractTagResourceARN(path string) string {
	return strings.TrimPrefix(path, "/"+pathTags+"/")
}

// parseTagKeys extracts tagKeys from query string: tagKeys=k1&tagKeys=k2.
func parseTagKeys(query string) []string {
	var keys []string

	for part := range strings.SplitSeq(query, "&") {
		if val, ok := strings.CutPrefix(part, "tagKeys="); ok {
			keys = append(keys, val)
		}
	}

	return keys
}
