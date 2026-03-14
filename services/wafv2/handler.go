package wafv2

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	wafv2Service       = "wafv2"
	wafv2TargetPrefix  = "AWSWAF_20190729."
	wafv2MatchPriority = service.PriorityHeaderExact
)

var (
	errUnknownAction  = errors.New("unknown action")
	errInvalidRequest = errors.New("invalid request")
)

// Handler is the HTTP handler for the AWS WAFv2 API.
type Handler struct {
	Backend   *InMemoryBackend
	AccountID string
	Region    string
}

// NewHandler creates a new WAFv2 handler.
func NewHandler(backend *InMemoryBackend) *Handler {
	return &Handler{
		Backend:   backend,
		AccountID: backend.accountID,
		Region:    backend.region,
	}
}

// Name returns the service name.
func (h *Handler) Name() string { return "Wafv2" }

// GetSupportedOperations returns the list of supported WAFv2 operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CreateWebACL",
		"GetWebACL",
		"UpdateWebACL",
		"DeleteWebACL",
		"ListWebACLs",
		"CreateIPSet",
		"GetIPSet",
		"UpdateIPSet",
		"DeleteIPSet",
		"ListIPSets",
		"TagResource",
		"ListTagsForResource",
		"UntagResource",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return wafv2Service }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this handler handles.
func (h *Handler) ChaosRegions() []string { return []string{h.Region} }

// RouteMatcher returns a function that matches WAFv2 API requests.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		return strings.HasPrefix(c.Request().Header.Get("X-Amz-Target"), wafv2TargetPrefix)
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return wafv2MatchPriority }

// ExtractOperation extracts the operation name from the X-Amz-Target header.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	target := c.Request().Header.Get("X-Amz-Target")

	return strings.TrimPrefix(target, wafv2TargetPrefix)
}

// ExtractResource extracts the resource identifier from the request.
func (h *Handler) ExtractResource(c *echo.Context) string {
	return h.ExtractOperation(c)
}

// Handler returns the Echo handler function for WAFv2 requests.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		ctx := c.Request().Context()
		log := logger.Load(ctx)

		body, err := httputils.ReadBody(c.Request())
		if err != nil {
			log.ErrorContext(ctx, "wafv2: failed to read request body", "error", err)

			return c.String(http.StatusInternalServerError, "internal server error")
		}

		op := h.ExtractOperation(c)

		result, dispErr := h.dispatch(ctx, op, body)
		if dispErr != nil {
			return h.handleError(c, dispErr)
		}

		if result == nil {
			return c.JSONBlob(http.StatusOK, []byte("{}"))
		}

		return c.JSONBlob(http.StatusOK, result)
	}
}

func (h *Handler) dispatch(ctx context.Context, op string, body []byte) ([]byte, error) {
	switch op {
	case "CreateWebACL":
		return h.handleCreateWebACL(ctx, body)
	case "GetWebACL":
		return h.handleGetWebACL(body)
	case "UpdateWebACL":
		return h.handleUpdateWebACL(ctx, body)
	case "DeleteWebACL":
		return h.handleDeleteWebACL(ctx, body)
	case "ListWebACLs":
		return h.handleListWebACLs(body)
	case "CreateIPSet":
		return h.handleCreateIPSet(ctx, body)
	case "GetIPSet":
		return h.handleGetIPSet(body)
	case "UpdateIPSet":
		return h.handleUpdateIPSet(ctx, body)
	case "DeleteIPSet":
		return h.handleDeleteIPSet(ctx, body)
	case "ListIPSets":
		return h.handleListIPSets(body)
	case "TagResource":
		return h.handleTagResource(body)
	case "ListTagsForResource":
		return h.handleListTagsForResource(body)
	case "UntagResource":
		return h.handleUntagResource(body)
	default:
		return nil, fmt.Errorf("%w: %s", errUnknownAction, op)
	}
}

func (h *Handler) handleError(c *echo.Context, err error) error {
	var syntaxErr *json.SyntaxError
	var typeErr *json.UnmarshalTypeError

	switch {
	case errors.Is(err, awserr.ErrNotFound):
		payload, _ := json.Marshal(map[string]string{
			"__type":  "WAFNonexistentItemException",
			"message": err.Error(),
		})

		return c.JSONBlob(http.StatusBadRequest, payload)
	case errors.Is(err, awserr.ErrConflict):
		payload, _ := json.Marshal(map[string]string{
			"__type":  "WAFDuplicateItemException",
			"message": err.Error(),
		})

		return c.JSONBlob(http.StatusBadRequest, payload)
	case errors.Is(err, errInvalidRequest), errors.As(err, &syntaxErr), errors.As(err, &typeErr):
		payload, _ := json.Marshal(map[string]string{
			"__type":  "WAFInvalidParameterException",
			"message": err.Error(),
		})

		return c.JSONBlob(http.StatusBadRequest, payload)
	case errors.Is(err, errUnknownAction):
		payload, _ := json.Marshal(map[string]string{
			"__type":  "WAFInvalidOperationException",
			"message": err.Error(),
		})

		return c.JSONBlob(http.StatusBadRequest, payload)
	default:
		payload, _ := json.Marshal(map[string]string{
			"__type":  "WAFInternalErrorException",
			"message": err.Error(),
		})

		return c.JSONBlob(http.StatusInternalServerError, payload)
	}
}

// tagItem represents a key/value pair for Tags fields.
type tagItem struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
}

func tagsFromItems(items []tagItem) map[string]string {
	m := make(map[string]string, len(items))

	for _, t := range items {
		m[t.Key] = t.Value
	}

	return m
}

func tagsToItems(tags map[string]string) []tagItem {
	items := make([]tagItem, 0, len(tags))

	for k, v := range tags {
		items = append(items, tagItem{Key: k, Value: v})
	}

	sort.Slice(items, func(i, j int) bool {
		return items[i].Key < items[j].Key
	})

	return items
}

// createWebACLRequest is the request body for CreateWebACL.
type createWebACLRequest struct {
	Name          string    `json:"Name"`
	Scope         string    `json:"Scope"`
	Description   string    `json:"Description"`
	DefaultAction string    `json:"DefaultAction"`
	Tags          []tagItem `json:"Tags"`
}

func (h *Handler) handleCreateWebACL(ctx context.Context, body []byte) ([]byte, error) {
	var req createWebACLRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.Name == "" {
		return nil, fmt.Errorf("%w: Name is required", errInvalidRequest)
	}

	if req.Scope == "" {
		return nil, fmt.Errorf("%w: Scope is required", errInvalidRequest)
	}

	if req.DefaultAction == "" {
		req.DefaultAction = "ALLOW"
	}

	w, err := h.Backend.CreateWebACL(req.Name, req.Scope, req.Description, req.DefaultAction, tagsFromItems(req.Tags))
	if err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "wafv2: created web ACL", "name", w.Name, "id", w.ID)

	arnStr := h.Backend.WebACLARN(w.Name, w.ID, w.Scope)

	return json.Marshal(map[string]any{
		"Summary": map[string]string{
			"Id":        w.ID,
			"Name":      w.Name,
			"ARN":       arnStr,
			"LockToken": w.LockToken,
		},
	})
}

// getWebACLRequest is the request body for GetWebACL.
type getWebACLRequest struct {
	ID    string `json:"Id"`
	Name  string `json:"Name"`
	Scope string `json:"Scope"`
}

func (h *Handler) handleGetWebACL(body []byte) ([]byte, error) {
	var req getWebACLRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ID == "" {
		return nil, fmt.Errorf("%w: Id is required", errInvalidRequest)
	}

	w, err := h.Backend.GetWebACL(req.ID)
	if err != nil {
		return nil, err
	}

	arnStr := h.Backend.WebACLARN(w.Name, w.ID, w.Scope)
	defaultActionMap := buildDefaultActionMap(w.DefaultAction)

	return json.Marshal(map[string]any{
		"WebACL": map[string]any{
			"Id":               w.ID,
			"Name":             w.Name,
			"ARN":              arnStr,
			"LockToken":        w.LockToken,
			"Description":      w.Description,
			"DefaultAction":    defaultActionMap,
			"VisibilityConfig": map[string]any{},
		},
		"LockToken": w.LockToken,
	})
}

// updateWebACLRequest is the request body for UpdateWebACL.
type updateWebACLRequest struct {
	ID            string `json:"Id"`
	Name          string `json:"Name"`
	Scope         string `json:"Scope"`
	LockToken     string `json:"LockToken"`
	Description   string `json:"Description"`
	DefaultAction string `json:"DefaultAction"`
}

func (h *Handler) handleUpdateWebACL(ctx context.Context, body []byte) ([]byte, error) {
	var req updateWebACLRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ID == "" {
		return nil, fmt.Errorf("%w: Id is required", errInvalidRequest)
	}

	w, err := h.Backend.UpdateWebACL(req.ID, req.Description, req.DefaultAction)
	if err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "wafv2: updated web ACL", "id", req.ID)

	return json.Marshal(map[string]string{
		"NextLockToken": w.LockToken,
	})
}

// deleteWebACLRequest is the request body for DeleteWebACL.
type deleteWebACLRequest struct {
	ID        string `json:"Id"`
	Name      string `json:"Name"`
	Scope     string `json:"Scope"`
	LockToken string `json:"LockToken"`
}

func (h *Handler) handleDeleteWebACL(ctx context.Context, body []byte) ([]byte, error) {
	var req deleteWebACLRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ID == "" {
		return nil, fmt.Errorf("%w: Id is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteWebACL(req.ID); err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "wafv2: deleted web ACL", "id", req.ID)

	return nil, nil
}

// listWebACLsRequest is the request body for ListWebACLs.
type listWebACLsRequest struct {
	Scope      string `json:"Scope"`
	NextMarker string `json:"NextMarker"`
	Limit      int    `json:"Limit"`
}

func (h *Handler) handleListWebACLs(body []byte) ([]byte, error) {
	var req listWebACLsRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	webACLs := h.Backend.ListWebACLs()
	items := make([]map[string]string, 0, len(webACLs))

	for _, w := range webACLs {
		arnStr := h.Backend.WebACLARN(w.Name, w.ID, w.Scope)
		items = append(items, map[string]string{
			"Id":          w.ID,
			"Name":        w.Name,
			"ARN":         arnStr,
			"LockToken":   w.LockToken,
			"Description": w.Description,
		})
	}

	return json.Marshal(map[string]any{
		"WebACLs": items,
	})
}

// createIPSetRequest is the request body for CreateIPSet.
type createIPSetRequest struct {
	Name             string    `json:"Name"`
	Scope            string    `json:"Scope"`
	Description      string    `json:"Description"`
	IPAddressVersion string    `json:"IPAddressVersion"`
	Addresses        []string  `json:"Addresses"`
	Tags             []tagItem `json:"Tags"`
}

func (h *Handler) handleCreateIPSet(ctx context.Context, body []byte) ([]byte, error) {
	var req createIPSetRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.Name == "" {
		return nil, fmt.Errorf("%w: Name is required", errInvalidRequest)
	}

	if req.Scope == "" {
		return nil, fmt.Errorf("%w: Scope is required", errInvalidRequest)
	}

	if req.IPAddressVersion == "" {
		req.IPAddressVersion = "IPV4"
	}

	s, err := h.Backend.CreateIPSet(
		req.Name,
		req.Scope,
		req.Description,
		req.IPAddressVersion,
		req.Addresses,
		tagsFromItems(req.Tags),
	)
	if err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "wafv2: created IP set", "name", s.Name, "id", s.ID)

	arnStr := h.Backend.IPSetARN(s.Name, s.ID, s.Scope)

	return json.Marshal(map[string]any{
		"Summary": map[string]string{
			"Id":        s.ID,
			"Name":      s.Name,
			"ARN":       arnStr,
			"LockToken": s.LockToken,
		},
	})
}

// getIPSetRequest is the request body for GetIPSet.
type getIPSetRequest struct {
	ID    string `json:"Id"`
	Name  string `json:"Name"`
	Scope string `json:"Scope"`
}

func (h *Handler) handleGetIPSet(body []byte) ([]byte, error) {
	var req getIPSetRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ID == "" {
		return nil, fmt.Errorf("%w: Id is required", errInvalidRequest)
	}

	s, err := h.Backend.GetIPSet(req.ID)
	if err != nil {
		return nil, err
	}

	arnStr := h.Backend.IPSetARN(s.Name, s.ID, s.Scope)

	return json.Marshal(map[string]any{
		"IPSet": map[string]any{
			"Id":               s.ID,
			"Name":             s.Name,
			"ARN":              arnStr,
			"LockToken":        s.LockToken,
			"Description":      s.Description,
			"IPAddressVersion": s.IPAddressVersion,
			"Addresses":        s.Addresses,
		},
		"LockToken": s.LockToken,
	})
}

// updateIPSetRequest is the request body for UpdateIPSet.
type updateIPSetRequest struct {
	ID          string   `json:"Id"`
	Name        string   `json:"Name"`
	Scope       string   `json:"Scope"`
	LockToken   string   `json:"LockToken"`
	Description string   `json:"Description"`
	Addresses   []string `json:"Addresses"`
}

func (h *Handler) handleUpdateIPSet(ctx context.Context, body []byte) ([]byte, error) {
	var req updateIPSetRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ID == "" {
		return nil, fmt.Errorf("%w: Id is required", errInvalidRequest)
	}

	s, err := h.Backend.UpdateIPSet(req.ID, req.Description, req.Addresses)
	if err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "wafv2: updated IP set", "id", req.ID)

	return json.Marshal(map[string]string{
		"NextLockToken": s.LockToken,
	})
}

// deleteIPSetRequest is the request body for DeleteIPSet.
type deleteIPSetRequest struct {
	ID        string `json:"Id"`
	Name      string `json:"Name"`
	Scope     string `json:"Scope"`
	LockToken string `json:"LockToken"`
}

func (h *Handler) handleDeleteIPSet(ctx context.Context, body []byte) ([]byte, error) {
	var req deleteIPSetRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ID == "" {
		return nil, fmt.Errorf("%w: Id is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteIPSet(req.ID); err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "wafv2: deleted IP set", "id", req.ID)

	return nil, nil
}

// listIPSetsRequest is the request body for ListIPSets.
type listIPSetsRequest struct {
	Scope      string `json:"Scope"`
	NextMarker string `json:"NextMarker"`
	Limit      int    `json:"Limit"`
}

func (h *Handler) handleListIPSets(body []byte) ([]byte, error) {
	var req listIPSetsRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	ipSets := h.Backend.ListIPSets()
	items := make([]map[string]string, 0, len(ipSets))

	for _, s := range ipSets {
		arnStr := h.Backend.IPSetARN(s.Name, s.ID, s.Scope)
		items = append(items, map[string]string{
			"Id":          s.ID,
			"Name":        s.Name,
			"ARN":         arnStr,
			"LockToken":   s.LockToken,
			"Description": s.Description,
		})
	}

	return json.Marshal(map[string]any{
		"IPSets": items,
	})
}

// tagResourceRequest is the request body for TagResource.
type tagResourceRequest struct {
	ResourceARN string    `json:"ResourceARN"`
	Tags        []tagItem `json:"Tags"`
}

func (h *Handler) handleTagResource(body []byte) ([]byte, error) {
	var req tagResourceRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ResourceARN == "" {
		return nil, fmt.Errorf("%w: ResourceARN is required", errInvalidRequest)
	}

	if err := h.Backend.TagResource(req.ResourceARN, tagsFromItems(req.Tags)); err != nil {
		return nil, err
	}

	return nil, nil
}

// listTagsForResourceRequest is the request body for ListTagsForResource.
type listTagsForResourceRequest struct {
	ResourceARN string `json:"ResourceARN"`
}

func (h *Handler) handleListTagsForResource(body []byte) ([]byte, error) {
	var req listTagsForResourceRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ResourceARN == "" {
		return nil, fmt.Errorf("%w: ResourceARN is required", errInvalidRequest)
	}

	tags, err := h.Backend.ListTagsForResource(req.ResourceARN)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{
		"TagInfoForResource": map[string]any{
			"ResourceARN": req.ResourceARN,
			"TagList":     tagsToItems(tags),
		},
	})
}

// untagResourceRequest is the request body for UntagResource.
type untagResourceRequest struct {
	ResourceARN string   `json:"ResourceARN"`
	TagKeys     []string `json:"TagKeys"`
}

func (h *Handler) handleUntagResource(body []byte) ([]byte, error) {
	var req untagResourceRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ResourceARN == "" {
		return nil, fmt.Errorf("%w: ResourceARN is required", errInvalidRequest)
	}

	if err := h.Backend.UntagResource(req.ResourceARN, req.TagKeys); err != nil {
		return nil, err
	}

	return nil, nil
}

func buildDefaultActionMap(action string) map[string]any {
	switch strings.ToUpper(action) {
	case "BLOCK":
		return map[string]any{"Block": map[string]any{}}
	default:
		return map[string]any{"Allow": map[string]any{}}
	}
}
