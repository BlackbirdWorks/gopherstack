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
	keyTypeField        = "__type"
	keyMessageField     = "message"
	keySummary          = "Summary"
	keyName             = "Name"
	keyARN              = "ARN"
	keyLockToken        = "LockToken"
	keyDescription      = "Description"
	keyVisibilityConfig = "VisibilityConfig"
	keyNextLockToken    = "NextLockToken"
	keyScope            = "Scope"
)

const (
	wafv2Service       = "wafv2"
	wafv2TargetPrefix  = "AWSWAF_20190729."
	wafv2MatchPriority = service.PriorityHeaderExact
	defaultActionAllow = "ALLOW"
)

var (
	errUnknownAction  = errors.New("unknown action")
	errInvalidRequest = errors.New("invalid request")
)

// Handler is the HTTP handler for the AWS WAFv2 API.
type Handler struct {
	// Backend is the storage interface for WAFv2 operations.
	Backend   StorageBackend
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

// Reset clears all backend state.
func (h *Handler) Reset() { h.Backend.Reset() }

// Name returns the service name.
func (h *Handler) Name() string { return "Wafv2" }

// GetSupportedOperations returns the list of supported WAFv2 operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"AssociateWebACL",
		"CheckCapacity",
		"CreateAPIKey",
		"CreateIPSet",
		"CreateRegexPatternSet",
		"CreateRuleGroup",
		"CreateWebACL",
		"DeleteAPIKey",
		"DeleteFirewallManagerRuleGroups",
		"DeleteIPSet",
		"DeleteLoggingConfiguration",
		"DeletePermissionPolicy",
		"DeleteRegexPatternSet",
		"DeleteWebACL",
		"DisassociateWebACL",
		"GetDecryptedAPIKey",
		"GetIPSet",
		"GetLoggingConfiguration",
		"GetPermissionPolicy",
		"GetRegexPatternSet",
		"GetRuleGroup",
		"GetWebACL",
		"GetWebACLForResource",
		"ListAPIKeys",
		"ListIPSets",
		"ListRegexPatternSets",
		"ListResourcesForWebACL",
		"ListRuleGroups",
		"ListTagsForResource",
		"ListWebACLs",
		"PutLoggingConfiguration",
		"PutPermissionPolicy",
		"TagResource",
		"UntagResource",
		"UpdateIPSet",
		"UpdateRegexPatternSet",
		"UpdateRuleGroup",
		"UpdateWebACL",
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

func (h *Handler) buildDispatchTable(ctx context.Context) map[string]func([]byte) ([]byte, error) {
	return map[string]func([]byte) ([]byte, error){
		"CreateWebACL":          func(b []byte) ([]byte, error) { return h.handleCreateWebACL(ctx, b) },
		"GetWebACL":             h.handleGetWebACL,
		"UpdateWebACL":          func(b []byte) ([]byte, error) { return h.handleUpdateWebACL(ctx, b) },
		"DeleteWebACL":          func(b []byte) ([]byte, error) { return h.handleDeleteWebACL(ctx, b) },
		"ListWebACLs":           h.handleListWebACLs,
		"CreateIPSet":           func(b []byte) ([]byte, error) { return h.handleCreateIPSet(ctx, b) },
		"GetIPSet":              h.handleGetIPSet,
		"UpdateIPSet":           func(b []byte) ([]byte, error) { return h.handleUpdateIPSet(ctx, b) },
		"DeleteIPSet":           func(b []byte) ([]byte, error) { return h.handleDeleteIPSet(ctx, b) },
		"ListIPSets":            h.handleListIPSets,
		"TagResource":           h.handleTagResource,
		"ListTagsForResource":   h.handleListTagsForResource,
		"UntagResource":         h.handleUntagResource,
		"AssociateWebACL":       h.handleAssociateWebACL,
		"DisassociateWebACL":    h.handleDisassociateWebACL,
		"GetWebACLForResource":  h.handleGetWebACLForResource,
		"CheckCapacity":         h.handleCheckCapacity,
		"CreateAPIKey":          func(b []byte) ([]byte, error) { return h.handleCreateAPIKey(ctx, b) },
		"CreateRegexPatternSet": func(b []byte) ([]byte, error) { return h.handleCreateRegexPatternSet(ctx, b) },
		"CreateRuleGroup":       func(b []byte) ([]byte, error) { return h.handleCreateRuleGroup(ctx, b) },
		"DeleteAPIKey":          func(b []byte) ([]byte, error) { return h.handleDeleteAPIKey(ctx, b) },
		"DeleteFirewallManagerRuleGroups": func(b []byte) ([]byte, error) {
			return h.handleDeleteFirewallManagerRuleGroups(ctx, b)
		},
		"DeleteLoggingConfiguration": func(b []byte) ([]byte, error) {
			return h.handleDeleteLoggingConfiguration(ctx, b)
		},
		"DeletePermissionPolicy": func(b []byte) ([]byte, error) { return h.handleDeletePermissionPolicy(ctx, b) },
		"DeleteRegexPatternSet":  func(b []byte) ([]byte, error) { return h.handleDeleteRegexPatternSet(ctx, b) },
		"GetRegexPatternSet":     h.handleGetRegexPatternSet,
		"ListRegexPatternSets":   h.handleListRegexPatternSets,
		"UpdateRegexPatternSet":  func(b []byte) ([]byte, error) { return h.handleUpdateRegexPatternSet(ctx, b) },
		"GetRuleGroup":           h.handleGetRuleGroup,
		"ListRuleGroups":         h.handleListRuleGroups,
		"UpdateRuleGroup":        func(b []byte) ([]byte, error) { return h.handleUpdateRuleGroup(ctx, b) },
		"ListAPIKeys":            h.handleListAPIKeys,
		"GetDecryptedAPIKey":     h.handleGetDecryptedAPIKey,
		"PutLoggingConfiguration": func(b []byte) ([]byte, error) {
			return h.handlePutLoggingConfiguration(ctx, b)
		},
		"GetLoggingConfiguration": h.handleGetLoggingConfiguration,
		"PutPermissionPolicy":     func(b []byte) ([]byte, error) { return h.handlePutPermissionPolicy(ctx, b) },
		"GetPermissionPolicy":     h.handleGetPermissionPolicy,
		"ListResourcesForWebACL":  h.handleListResourcesForWebACL,
	}
}

func (h *Handler) dispatch(ctx context.Context, op string, body []byte) ([]byte, error) {
	table := h.buildDispatchTable(ctx)

	fn, ok := table[op]
	if !ok {
		return nil, fmt.Errorf("%w: %s", errUnknownAction, op)
	}

	return fn(body)
}

func (h *Handler) handleError(c *echo.Context, err error) error {
	var syntaxErr *json.SyntaxError
	var typeErr *json.UnmarshalTypeError

	switch {
	case errors.Is(err, awserr.ErrNotFound):
		payload, _ := json.Marshal(map[string]string{
			keyTypeField:    "WAFNonexistentItemException",
			keyMessageField: err.Error(),
		})

		return c.JSONBlob(http.StatusBadRequest, payload)
	case errors.Is(err, awserr.ErrConflict):
		payload, _ := json.Marshal(map[string]string{
			keyTypeField:    "WAFDuplicateItemException",
			keyMessageField: err.Error(),
		})

		return c.JSONBlob(http.StatusBadRequest, payload)
	case errors.Is(err, errInvalidRequest), errors.As(err, &syntaxErr), errors.As(err, &typeErr):
		payload, _ := json.Marshal(map[string]string{
			keyTypeField:    "WAFInvalidParameterException",
			keyMessageField: err.Error(),
		})

		return c.JSONBlob(http.StatusBadRequest, payload)
	case errors.Is(err, errUnknownAction):
		payload, _ := json.Marshal(map[string]string{
			keyTypeField:    "WAFInvalidOperationException",
			keyMessageField: err.Error(),
		})

		return c.JSONBlob(http.StatusBadRequest, payload)
	default:
		payload, _ := json.Marshal(map[string]string{
			keyTypeField:    "WAFInternalErrorException",
			keyMessageField: err.Error(),
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
	Name             string          `json:"Name"`
	Scope            string          `json:"Scope"`
	Description      string          `json:"Description"`
	DefaultAction    json.RawMessage `json:"DefaultAction"`
	VisibilityConfig json.RawMessage `json:"VisibilityConfig"`
	Tags             []tagItem       `json:"Tags"`
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

	if !validScope(req.Scope) {
		return nil, fmt.Errorf("%w: Scope must be %s or %s", errInvalidRequest, ScopeRegional, ScopeCloudFront)
	}

	defaultAction := extractDefaultAction(req.DefaultAction)
	visibilityConfig := string(req.VisibilityConfig)

	w, err := h.Backend.CreateWebACL(
		req.Name,
		req.Scope,
		req.Description,
		defaultAction,
		visibilityConfig,
		tagsFromItems(req.Tags),
	)
	if err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "wafv2: created web ACL", "name", w.Name, "id", w.ID)

	arnStr := h.Backend.WebACLARN(w.Name, w.ID, w.Scope)

	return json.Marshal(map[string]any{
		keySummary: map[string]string{
			"Id":         w.ID,
			keyName:      w.Name,
			keyARN:       arnStr,
			keyLockToken: w.LockToken,
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
	visConfig := parseVisibilityConfig(w.VisibilityConfig, w.Name)

	return json.Marshal(map[string]any{
		"WebACL": map[string]any{
			"Id":                w.ID,
			keyName:             w.Name,
			keyARN:              arnStr,
			keyLockToken:        w.LockToken,
			keyDescription:      w.Description,
			"DefaultAction":     defaultActionMap,
			keyVisibilityConfig: visConfig,
		},
		keyLockToken: w.LockToken,
	})
}

// updateWebACLRequest is the request body for UpdateWebACL.
type updateWebACLRequest struct {
	ID               string          `json:"Id"`
	Name             string          `json:"Name"`
	Scope            string          `json:"Scope"`
	LockToken        string          `json:"LockToken"`
	Description      string          `json:"Description"`
	DefaultAction    json.RawMessage `json:"DefaultAction"`
	VisibilityConfig json.RawMessage `json:"VisibilityConfig"`
}

func (h *Handler) handleUpdateWebACL(ctx context.Context, body []byte) ([]byte, error) {
	var req updateWebACLRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ID == "" {
		return nil, fmt.Errorf("%w: Id is required", errInvalidRequest)
	}

	defaultAction := extractDefaultAction(req.DefaultAction)
	visibilityConfig := string(req.VisibilityConfig)

	w, err := h.Backend.UpdateWebACL(req.ID, req.Description, defaultAction, visibilityConfig)
	if err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "wafv2: updated web ACL", "id", req.ID)

	return json.Marshal(map[string]string{
		keyNextLockToken: w.LockToken,
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
	items := buildSummaryItems(webACLs, req.Scope,
		func(w *WebACL) string { return w.Scope },
		func(w *WebACL) map[string]string {
			arnStr := h.Backend.WebACLARN(w.Name, w.ID, w.Scope)

			return map[string]string{
				"Id":           w.ID,
				keyName:        w.Name,
				keyARN:         arnStr,
				keyLockToken:   w.LockToken,
				keyDescription: w.Description,
			}
		},
	)

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

	if !validScope(req.Scope) {
		return nil, fmt.Errorf("%w: Scope must be %s or %s", errInvalidRequest, ScopeRegional, ScopeCloudFront)
	}

	if req.IPAddressVersion == "" {
		req.IPAddressVersion = IPVersionIPv4
	}

	if req.IPAddressVersion != IPVersionIPv4 && req.IPAddressVersion != IPVersionIPv6 {
		return nil, fmt.Errorf("%w: IPAddressVersion must be %s or %s", errInvalidRequest, IPVersionIPv4, IPVersionIPv6)
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
		keySummary: map[string]string{
			"Id":         s.ID,
			keyName:      s.Name,
			keyARN:       arnStr,
			keyLockToken: s.LockToken,
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
			keyName:            s.Name,
			keyARN:             arnStr,
			keyLockToken:       s.LockToken,
			keyDescription:     s.Description,
			"IPAddressVersion": s.IPAddressVersion,
			"Addresses":        s.Addresses,
		},
		keyLockToken: s.LockToken,
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
		keyNextLockToken: s.LockToken,
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
	items := buildSummaryItems(ipSets, req.Scope,
		func(s *IPSet) string { return s.Scope },
		func(s *IPSet) map[string]string {
			arnStr := h.Backend.IPSetARN(s.Name, s.ID, s.Scope)

			return map[string]string{
				"Id":           s.ID,
				keyName:        s.Name,
				keyARN:         arnStr,
				keyLockToken:   s.LockToken,
				keyDescription: s.Description,
			}
		},
	)

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

// extractDefaultAction parses a DefaultAction JSON object and returns "ALLOW" or "BLOCK".
func extractDefaultAction(raw json.RawMessage) string {
	if len(raw) == 0 {
		return defaultActionAllow
	}

	var m map[string]json.RawMessage
	if err := json.Unmarshal(raw, &m); err != nil {
		return defaultActionAllow
	}

	if _, ok := m["Block"]; ok {
		return "BLOCK"
	}

	return defaultActionAllow
}

// parseVisibilityConfig parses a stored VisibilityConfig JSON string or returns a default.
func parseVisibilityConfig(stored, metricName string) map[string]any {
	if stored != "" {
		var m map[string]any
		if err := json.Unmarshal([]byte(stored), &m); err == nil {
			return m
		}
	}

	return map[string]any{
		"CloudWatchMetricsEnabled": false,
		"MetricName":               metricName,
		"SampledRequestsEnabled":   false,
	}
}

// buildSummaryItems filters and maps a slice of resources to summary maps.
func buildSummaryItems[T any](
	items []T,
	scope string,
	getScope func(T) string,
	toMap func(T) map[string]string,
) []map[string]string {
	result := make([]map[string]string, 0, len(items))

	for _, item := range items {
		if scope != "" && getScope(item) != scope {
			continue
		}

		result = append(result, toMap(item))
	}

	return result
}

// associateWebACLRequest is the request body for AssociateWebACL.
type associateWebACLRequest struct {
	WebACLArn   string `json:"WebACLArn"`
	ResourceArn string `json:"ResourceArn"`
}

func (h *Handler) handleAssociateWebACL(body []byte) ([]byte, error) {
	var req associateWebACLRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.WebACLArn == "" {
		return nil, fmt.Errorf("%w: WebACLArn is required", errInvalidRequest)
	}

	if req.ResourceArn == "" {
		return nil, fmt.Errorf("%w: ResourceArn is required", errInvalidRequest)
	}

	if err := h.Backend.AssociateWebACL(req.WebACLArn, req.ResourceArn); err != nil {
		return nil, err
	}

	return nil, nil
}

// disassociateWebACLRequest is the request body for DisassociateWebACL.
type disassociateWebACLRequest struct {
	ResourceArn string `json:"ResourceArn"`
}

func (h *Handler) handleDisassociateWebACL(body []byte) ([]byte, error) {
	var req disassociateWebACLRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ResourceArn == "" {
		return nil, fmt.Errorf("%w: ResourceArn is required", errInvalidRequest)
	}

	if err := h.Backend.DisassociateWebACL(req.ResourceArn); err != nil {
		return nil, err
	}

	return nil, nil
}

// getWebACLForResourceRequest is the request body for GetWebACLForResource.
type getWebACLForResourceRequest struct {
	ResourceArn string `json:"ResourceArn"`
}

func (h *Handler) handleGetWebACLForResource(body []byte) ([]byte, error) {
	var req getWebACLForResourceRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ResourceArn == "" {
		return nil, fmt.Errorf("%w: ResourceArn is required", errInvalidRequest)
	}

	w, err := h.Backend.GetWebACLForResource(req.ResourceArn)
	if err != nil {
		return nil, err
	}

	arnStr := h.Backend.WebACLARN(w.Name, w.ID, w.Scope)
	defaultActionMap := buildDefaultActionMap(w.DefaultAction)
	visConfig := parseVisibilityConfig(w.VisibilityConfig, w.Name)

	return json.Marshal(map[string]any{
		"WebACL": map[string]any{
			"Id":                w.ID,
			keyName:             w.Name,
			keyARN:              arnStr,
			keyLockToken:        w.LockToken,
			keyScope:            w.Scope,
			keyDescription:      w.Description,
			"DefaultAction":     defaultActionMap,
			keyVisibilityConfig: visConfig,
			"Rules":             []any{},
		},
	})
}

// checkCapacityRequest is the request body for CheckCapacity.
type checkCapacityRequest struct {
	Scope string           `json:"Scope"`
	Rules []map[string]any `json:"Rules"`
}

func (h *Handler) handleCheckCapacity(body []byte) ([]byte, error) {
	var req checkCapacityRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.Scope == "" {
		return nil, fmt.Errorf("%w: Scope is required", errInvalidRequest)
	}

	capacity, err := h.Backend.CheckCapacity(req.Scope, req.Rules)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{
		"ConsumedCapacity": capacity,
	})
}

// createAPIKeyRequest is the request body for CreateAPIKey.
type createAPIKeyRequest struct {
	Scope        string   `json:"Scope"`
	TokenDomains []string `json:"TokenDomains"`
}

func (h *Handler) handleCreateAPIKey(ctx context.Context, body []byte) ([]byte, error) {
	var req createAPIKeyRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.Scope == "" {
		return nil, fmt.Errorf("%w: Scope is required", errInvalidRequest)
	}

	a, err := h.Backend.CreateAPIKey(req.Scope, req.TokenDomains)
	if err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "wafv2: created API key", "scope", a.Scope)

	return json.Marshal(map[string]any{
		"APIKey": a.APIKeyValue,
	})
}

// deleteAPIKeyRequest is the request body for DeleteAPIKey.
type deleteAPIKeyRequest struct {
	Scope  string `json:"Scope"`
	APIKey string `json:"APIKey"`
}

func (h *Handler) handleDeleteAPIKey(ctx context.Context, body []byte) ([]byte, error) {
	var req deleteAPIKeyRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.Scope == "" {
		return nil, fmt.Errorf("%w: Scope is required", errInvalidRequest)
	}

	if req.APIKey == "" {
		return nil, fmt.Errorf("%w: APIKey is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteAPIKey(req.Scope, req.APIKey); err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "wafv2: deleted API key", "scope", req.Scope)

	return nil, nil
}

// createRegexPatternSetRequest is the request body for CreateRegexPatternSet.
type createRegexPatternSetRequest struct {
	Name                  string    `json:"Name"`
	Scope                 string    `json:"Scope"`
	Description           string    `json:"Description"`
	RegularExpressionList []string  `json:"RegularExpressionList"`
	Tags                  []tagItem `json:"Tags"`
}

func (h *Handler) handleCreateRegexPatternSet(ctx context.Context, body []byte) ([]byte, error) {
	var req createRegexPatternSetRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.Name == "" {
		return nil, fmt.Errorf("%w: Name is required", errInvalidRequest)
	}

	if req.Scope == "" {
		return nil, fmt.Errorf("%w: Scope is required", errInvalidRequest)
	}

	if !validScope(req.Scope) {
		return nil, fmt.Errorf("%w: Scope must be %s or %s", errInvalidRequest, ScopeRegional, ScopeCloudFront)
	}

	rps, err := h.Backend.CreateRegexPatternSet(
		req.Name,
		req.Scope,
		req.Description,
		req.RegularExpressionList,
		tagsFromItems(req.Tags),
	)
	if err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "wafv2: created regex pattern set", "name", rps.Name, "id", rps.ID)

	arnStr := h.Backend.RegexPatternSetARN(rps.Name, rps.ID, rps.Scope)

	return json.Marshal(map[string]any{
		keySummary: map[string]string{
			"Id":         rps.ID,
			keyName:      rps.Name,
			keyARN:       arnStr,
			keyLockToken: rps.LockToken,
		},
	})
}

// deleteRegexPatternSetRequest is the request body for DeleteRegexPatternSet.
type deleteRegexPatternSetRequest struct {
	ID        string `json:"Id"`
	Name      string `json:"Name"`
	Scope     string `json:"Scope"`
	LockToken string `json:"LockToken"`
}

func (h *Handler) handleDeleteRegexPatternSet(ctx context.Context, body []byte) ([]byte, error) {
	var req deleteRegexPatternSetRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ID == "" {
		return nil, fmt.Errorf("%w: Id is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteRegexPatternSet(req.ID); err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "wafv2: deleted regex pattern set", "id", req.ID)

	return nil, nil
}

// createRuleGroupRequest is the request body for CreateRuleGroup.
type createRuleGroupRequest struct {
	Name             string           `json:"Name"`
	Scope            string           `json:"Scope"`
	Description      string           `json:"Description"`
	VisibilityConfig json.RawMessage  `json:"VisibilityConfig"`
	Rules            []map[string]any `json:"Rules"`
	Tags             []tagItem        `json:"Tags"`
	Capacity         int64            `json:"Capacity"`
}

func (h *Handler) handleCreateRuleGroup(ctx context.Context, body []byte) ([]byte, error) {
	var req createRuleGroupRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.Name == "" {
		return nil, fmt.Errorf("%w: Name is required", errInvalidRequest)
	}

	if req.Scope == "" {
		return nil, fmt.Errorf("%w: Scope is required", errInvalidRequest)
	}

	if !validScope(req.Scope) {
		return nil, fmt.Errorf("%w: Scope must be %s or %s", errInvalidRequest, ScopeRegional, ScopeCloudFront)
	}

	rg, err := h.Backend.CreateRuleGroup(
		req.Name,
		req.Scope,
		req.Description,
		string(req.VisibilityConfig),
		req.Capacity,
		req.Rules,
		tagsFromItems(req.Tags),
	)
	if err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "wafv2: created rule group", "name", rg.Name, "id", rg.ID)

	arnStr := h.Backend.RuleGroupARN(rg.Name, rg.ID, rg.Scope)

	return json.Marshal(map[string]any{
		keySummary: map[string]string{
			"Id":         rg.ID,
			keyName:      rg.Name,
			keyARN:       arnStr,
			keyLockToken: rg.LockToken,
		},
	})
}

// deleteFirewallManagerRuleGroupsRequest is the request body for DeleteFirewallManagerRuleGroups.
type deleteFirewallManagerRuleGroupsRequest struct {
	WebACLArn       string `json:"WebACLArn"`
	WebACLLockToken string `json:"WebACLLockToken"`
}

func (h *Handler) handleDeleteFirewallManagerRuleGroups(ctx context.Context, body []byte) ([]byte, error) {
	var req deleteFirewallManagerRuleGroupsRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.WebACLArn == "" {
		return nil, fmt.Errorf("%w: WebACLArn is required", errInvalidRequest)
	}

	w, err := h.Backend.DeleteFirewallManagerRuleGroups(req.WebACLArn)
	if err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "wafv2: deleted firewall manager rule groups", "webACLArn", req.WebACLArn)

	return json.Marshal(map[string]string{
		"NextWebACLLockToken": w.LockToken,
	})
}

// deleteLoggingConfigurationRequest is the request body for DeleteLoggingConfiguration.
type deleteLoggingConfigurationRequest struct {
	ResourceArn string `json:"ResourceArn"`
}

func (h *Handler) handleDeleteLoggingConfiguration(ctx context.Context, body []byte) ([]byte, error) {
	var req deleteLoggingConfigurationRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ResourceArn == "" {
		return nil, fmt.Errorf("%w: ResourceArn is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteLoggingConfiguration(req.ResourceArn); err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "wafv2: deleted logging configuration", "resourceArn", req.ResourceArn)

	return nil, nil
}

// deletePermissionPolicyRequest is the request body for DeletePermissionPolicy.
type deletePermissionPolicyRequest struct {
	ResourceArn string `json:"ResourceArn"`
}

func (h *Handler) handleDeletePermissionPolicy(ctx context.Context, body []byte) ([]byte, error) {
	var req deletePermissionPolicyRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ResourceArn == "" {
		return nil, fmt.Errorf("%w: ResourceArn is required", errInvalidRequest)
	}

	if err := h.Backend.DeletePermissionPolicy(req.ResourceArn); err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "wafv2: deleted permission policy", "resourceArn", req.ResourceArn)

	return nil, nil
}

// getRegexPatternSetRequest is the request body for GetRegexPatternSet.
type getRegexPatternSetRequest struct {
	ID    string `json:"Id"`
	Name  string `json:"Name"`
	Scope string `json:"Scope"`
}

func (h *Handler) handleGetRegexPatternSet(body []byte) ([]byte, error) {
	var req getRegexPatternSetRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ID == "" {
		return nil, fmt.Errorf("%w: Id is required", errInvalidRequest)
	}

	r, err := h.Backend.GetRegexPatternSet(req.ID)
	if err != nil {
		return nil, err
	}

	arnStr := h.Backend.RegexPatternSetARN(r.Name, r.ID, r.Scope)

	return json.Marshal(map[string]any{
		"RegexPatternSet": map[string]any{
			"Id":                    r.ID,
			keyName:                 r.Name,
			keyARN:                  arnStr,
			keyDescription:          r.Description,
			"RegularExpressionList": r.RegularExpressionList,
		},
		keyLockToken: r.LockToken,
	})
}

// listRegexPatternSetsRequest is the request body for ListRegexPatternSets.
type listRegexPatternSetsRequest struct {
	Scope      string `json:"Scope"`
	NextMarker string `json:"NextMarker"`
	Limit      int    `json:"Limit"`
}

func (h *Handler) handleListRegexPatternSets(body []byte) ([]byte, error) {
	var req listRegexPatternSetsRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	sets := h.Backend.ListRegexPatternSets()
	items := buildSummaryItems(sets, req.Scope,
		func(r *RegexPatternSet) string { return r.Scope },
		func(r *RegexPatternSet) map[string]string {
			return map[string]string{
				"Id":           r.ID,
				keyName:        r.Name,
				keyARN:         h.Backend.RegexPatternSetARN(r.Name, r.ID, r.Scope),
				keyLockToken:   r.LockToken,
				keyDescription: r.Description,
			}
		},
	)

	return json.Marshal(map[string]any{"RegexPatternSets": items})
}

// updateRegexPatternSetRequest is the request body for UpdateRegexPatternSet.
type updateRegexPatternSetRequest struct {
	ID                    string   `json:"Id"`
	Name                  string   `json:"Name"`
	Scope                 string   `json:"Scope"`
	LockToken             string   `json:"LockToken"`
	Description           string   `json:"Description"`
	RegularExpressionList []string `json:"RegularExpressionList"`
}

func (h *Handler) handleUpdateRegexPatternSet(ctx context.Context, body []byte) ([]byte, error) {
	var req updateRegexPatternSetRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ID == "" {
		return nil, fmt.Errorf("%w: Id is required", errInvalidRequest)
	}

	r, err := h.Backend.UpdateRegexPatternSet(req.ID, req.Description, req.RegularExpressionList)
	if err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "wafv2: updated regex pattern set", "id", req.ID)

	return json.Marshal(map[string]string{keyNextLockToken: r.LockToken})
}

// getRuleGroupRequest is the request body for GetRuleGroup.
type getRuleGroupRequest struct {
	ID    string `json:"Id"`
	Name  string `json:"Name"`
	Scope string `json:"Scope"`
	ARN   string `json:"ARN"`
}

func (h *Handler) handleGetRuleGroup(body []byte) ([]byte, error) {
	var req getRuleGroupRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ID == "" {
		return nil, fmt.Errorf("%w: Id is required", errInvalidRequest)
	}

	rg, err := h.Backend.GetRuleGroup(req.ID)
	if err != nil {
		return nil, err
	}

	arnStr := h.Backend.RuleGroupARN(rg.Name, rg.ID, rg.Scope)
	visConfig := parseVisibilityConfig(rg.VisibilityConfig, rg.Name)

	return json.Marshal(map[string]any{
		"RuleGroup": map[string]any{
			"Id":                rg.ID,
			keyName:             rg.Name,
			keyARN:              arnStr,
			keyDescription:      rg.Description,
			"Capacity":          rg.Capacity,
			"Rules":             rg.Rules,
			keyVisibilityConfig: visConfig,
		},
		keyLockToken: rg.LockToken,
	})
}

// listRuleGroupsRequest is the request body for ListRuleGroups.
type listRuleGroupsRequest struct {
	Scope      string `json:"Scope"`
	NextMarker string `json:"NextMarker"`
	Limit      int    `json:"Limit"`
}

func (h *Handler) handleListRuleGroups(body []byte) ([]byte, error) {
	var req listRuleGroupsRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	groups := h.Backend.ListRuleGroups()
	items := buildSummaryItems(groups, req.Scope,
		func(rg *RuleGroup) string { return rg.Scope },
		func(rg *RuleGroup) map[string]string {
			return map[string]string{
				"Id":           rg.ID,
				keyName:        rg.Name,
				keyARN:         h.Backend.RuleGroupARN(rg.Name, rg.ID, rg.Scope),
				keyLockToken:   rg.LockToken,
				keyDescription: rg.Description,
			}
		},
	)

	return json.Marshal(map[string]any{"RuleGroups": items})
}

// updateRuleGroupRequest is the request body for UpdateRuleGroup.
type updateRuleGroupRequest struct {
	ID               string           `json:"Id"`
	Name             string           `json:"Name"`
	Scope            string           `json:"Scope"`
	LockToken        string           `json:"LockToken"`
	Description      string           `json:"Description"`
	VisibilityConfig json.RawMessage  `json:"VisibilityConfig"`
	Rules            []map[string]any `json:"Rules"`
}

func (h *Handler) handleUpdateRuleGroup(ctx context.Context, body []byte) ([]byte, error) {
	var req updateRuleGroupRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ID == "" {
		return nil, fmt.Errorf("%w: Id is required", errInvalidRequest)
	}

	rg, err := h.Backend.UpdateRuleGroup(req.ID, req.Description, string(req.VisibilityConfig), req.Rules)
	if err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "wafv2: updated rule group", "id", req.ID)

	return json.Marshal(map[string]string{keyNextLockToken: rg.LockToken})
}

// listAPIKeysRequest is the request body for ListAPIKeys.
type listAPIKeysRequest struct {
	Scope      string `json:"Scope"`
	NextMarker string `json:"NextMarker"`
	Limit      int    `json:"Limit"`
}

func (h *Handler) handleListAPIKeys(body []byte) ([]byte, error) {
	var req listAPIKeysRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	keys := h.Backend.ListAPIKeys(req.Scope)
	items := make([]map[string]any, 0, len(keys))

	for _, k := range keys {
		items = append(items, map[string]any{
			"APIKey":       k.APIKeyValue,
			keyScope:       k.Scope,
			"TokenDomains": k.TokenDomains,
		})
	}

	return json.Marshal(map[string]any{"APIKeys": items})
}

// getDecryptedAPIKeyRequest is the request body for GetDecryptedAPIKey.
type getDecryptedAPIKeyRequest struct {
	Scope  string `json:"Scope"`
	APIKey string `json:"APIKey"`
}

func (h *Handler) handleGetDecryptedAPIKey(body []byte) ([]byte, error) {
	var req getDecryptedAPIKeyRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.Scope == "" {
		return nil, fmt.Errorf("%w: Scope is required", errInvalidRequest)
	}

	if req.APIKey == "" {
		return nil, fmt.Errorf("%w: APIKey is required", errInvalidRequest)
	}

	a, err := h.Backend.GetDecryptedAPIKey(req.Scope, req.APIKey)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{
		"TokenDomains": a.TokenDomains,
		keyScope:       a.Scope,
	})
}

// putLoggingConfigurationRequest is the request body for PutLoggingConfiguration.
type putLoggingConfigurationRequest struct {
	LoggingConfiguration map[string]any `json:"LoggingConfiguration"`
}

func (h *Handler) handlePutLoggingConfiguration(ctx context.Context, body []byte) ([]byte, error) {
	var req putLoggingConfigurationRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	resourceARN, _ := req.LoggingConfiguration["ResourceArn"].(string)
	if resourceARN == "" {
		return nil, fmt.Errorf("%w: LoggingConfiguration.ResourceArn is required", errInvalidRequest)
	}

	if err := h.Backend.PutLoggingConfiguration(resourceARN); err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "wafv2: put logging configuration", "resourceArn", resourceARN)

	return json.Marshal(map[string]any{
		"LoggingConfiguration": map[string]any{"ResourceArn": resourceARN},
	})
}

// getLoggingConfigurationRequest is the request body for GetLoggingConfiguration.
type getLoggingConfigurationRequest struct {
	ResourceArn string `json:"ResourceArn"`
}

func (h *Handler) handleGetLoggingConfiguration(body []byte) ([]byte, error) {
	var req getLoggingConfigurationRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ResourceArn == "" {
		return nil, fmt.Errorf("%w: ResourceArn is required", errInvalidRequest)
	}

	if _, err := h.Backend.GetLoggingConfiguration(req.ResourceArn); err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{
		"LoggingConfiguration": map[string]any{"ResourceArn": req.ResourceArn},
	})
}

// putPermissionPolicyRequest is the request body for PutPermissionPolicy.
type putPermissionPolicyRequest struct {
	ResourceArn string `json:"ResourceArn"`
	Policy      string `json:"Policy"`
}

func (h *Handler) handlePutPermissionPolicy(ctx context.Context, body []byte) ([]byte, error) {
	var req putPermissionPolicyRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ResourceArn == "" {
		return nil, fmt.Errorf("%w: ResourceArn is required", errInvalidRequest)
	}

	if err := h.Backend.PutPermissionPolicy(req.ResourceArn, req.Policy); err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "wafv2: put permission policy", "resourceArn", req.ResourceArn)

	return nil, nil
}

// getPermissionPolicyRequest is the request body for GetPermissionPolicy.
type getPermissionPolicyRequest struct {
	ResourceArn string `json:"ResourceArn"`
}

func (h *Handler) handleGetPermissionPolicy(body []byte) ([]byte, error) {
	var req getPermissionPolicyRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ResourceArn == "" {
		return nil, fmt.Errorf("%w: ResourceArn is required", errInvalidRequest)
	}

	policy, err := h.Backend.GetPermissionPolicy(req.ResourceArn)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]string{"Policy": policy})
}

// listResourcesForWebACLRequest is the request body for ListResourcesForWebACL.
type listResourcesForWebACLRequest struct {
	WebACLArn    string `json:"WebACLArn"`
	ResourceType string `json:"ResourceType"`
}

func (h *Handler) handleListResourcesForWebACL(body []byte) ([]byte, error) {
	var req listResourcesForWebACLRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.WebACLArn == "" {
		return nil, fmt.Errorf("%w: WebACLArn is required", errInvalidRequest)
	}

	resources, err := h.Backend.ListResourcesForWebACL(req.WebACLArn)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{"ResourceArns": resources})
}
