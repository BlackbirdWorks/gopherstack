package wafv2

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"slices"
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
	keyIPAddressVersion = "IPAddressVersion"
	keyAddresses        = "Addresses"
	keyRules            = "Rules"
	keyCapacity         = "Capacity"
	keyVendorName       = "VendorName"
)

const (
	wafv2Service       = "wafv2"
	wafv2TargetPrefix  = "AWSWAF_20190729."
	wafv2MatchPriority = service.PriorityHeaderExact
	defaultActionAllow = "ALLOW"

	// maxAPIKeyTokenDomains is the AWS-imposed limit for token domains per API key.
	maxAPIKeyTokenDomains = 5

	// maxSampledRequestsItems is the maximum value for GetSampledRequests.MaxItems.
	maxSampledRequestsItems = 500
)

// validLoggingDestinationPrefixes lists accepted ARN prefixes for logging destinations.
var validLoggingDestinationPrefixes = []string{ //nolint:gochecknoglobals // package-level lookup table
	"arn:aws:firehose:",
	"arn:aws:s3:::",
	"arn:aws:logs:",
}

// regionalResourceServices are the AWS service identifiers accepted for REGIONAL WebACL associations.
var regionalResourceServices = []string{ //nolint:gochecknoglobals // package-level lookup table
	"elasticloadbalancing",
	"execute-api",
	"appsync",
	"cognito-idp",
	"apprunner",
}

var (
	errUnknownAction  = errors.New("unknown action")
	errInvalidRequest = errors.New("invalid request")
)

// Handler is the HTTP handler for the AWS WAFv2 API.
type Handler struct {
	// Backend is the storage interface for WAFv2 operations.
	Backend StorageBackend
	// ops is the dispatch table built once at construction time.
	ops       map[string]func(context.Context, []byte) ([]byte, error)
	AccountID string
	Region    string
}

// NewHandler creates a new WAFv2 handler.
func NewHandler(backend *InMemoryBackend) *Handler {
	h := &Handler{
		Backend:   backend,
		AccountID: backend.accountID,
		Region:    backend.region,
	}
	h.ops = h.buildDispatchTable()

	return h
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
		"DeleteRuleGroup",
		"DeleteWebACL",
		"DescribeAllManagedProducts",
		"DescribeManagedProductsByVendor",
		"DescribeManagedRuleGroup",
		"DisassociateWebACL",
		"GenerateMobileSdkReleaseUrl",
		"GetDecryptedAPIKey",
		"GetIPSet",
		"GetLoggingConfiguration",
		"GetManagedRuleSet",
		"GetMobileSdkRelease",
		"GetPermissionPolicy",
		"GetRateBasedStatementManagedKeys",
		"GetRegexPatternSet",
		"GetRuleGroup",
		"GetSampledRequests",
		"GetTopPathStatisticsByTraffic",
		"GetWebACL",
		"GetWebACLForResource",
		"ListAPIKeys",
		"ListAvailableManagedRuleGroupVersions",
		"ListAvailableManagedRuleGroups",
		"ListIPSets",
		"ListLoggingConfigurations",
		"ListManagedRuleSets",
		"ListMobileSdkReleases",
		"ListRegexPatternSets",
		"ListResourcesForWebACL",
		"ListRuleGroups",
		"ListTagsForResource",
		"ListWebACLs",
		"PutLoggingConfiguration",
		"PutManagedRuleSetVersions",
		"PutPermissionPolicy",
		"TagResource",
		"UntagResource",
		"UpdateIPSet",
		"UpdateManagedRuleSetVersionExpiryDate",
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

//nolint:funlen // dispatch table must enumerate all operations; length is inherent to breadth of the API
func (h *Handler) buildDispatchTable() map[string]func(context.Context, []byte) ([]byte, error) {
	type fn = func(context.Context, []byte) ([]byte, error)

	wrapNoCtx := func(f func([]byte) ([]byte, error)) fn {
		return func(_ context.Context, b []byte) ([]byte, error) { return f(b) }
	}

	return map[string]fn{
		"CreateWebACL":  func(c context.Context, b []byte) ([]byte, error) { return h.handleCreateWebACL(c, b) },
		"GetWebACL":     func(c context.Context, b []byte) ([]byte, error) { return h.handleGetWebACL(c, b) },
		"UpdateWebACL":  func(c context.Context, b []byte) ([]byte, error) { return h.handleUpdateWebACL(c, b) },
		"DeleteWebACL":  func(c context.Context, b []byte) ([]byte, error) { return h.handleDeleteWebACL(c, b) },
		"ListWebACLs":   func(c context.Context, b []byte) ([]byte, error) { return h.handleListWebACLs(c, b) },
		"CreateIPSet":   func(c context.Context, b []byte) ([]byte, error) { return h.handleCreateIPSet(c, b) },
		"GetIPSet":      func(c context.Context, b []byte) ([]byte, error) { return h.handleGetIPSet(c, b) },
		"UpdateIPSet":   func(c context.Context, b []byte) ([]byte, error) { return h.handleUpdateIPSet(c, b) },
		"DeleteIPSet":   func(c context.Context, b []byte) ([]byte, error) { return h.handleDeleteIPSet(c, b) },
		"ListIPSets":    func(c context.Context, b []byte) ([]byte, error) { return h.handleListIPSets(c, b) },
		"TagResource":   func(c context.Context, b []byte) ([]byte, error) { return h.handleTagResource(c, b) },
		"UntagResource": func(c context.Context, b []byte) ([]byte, error) { return h.handleUntagResource(c, b) },
		"ListTagsForResource": func(c context.Context, b []byte) ([]byte, error) {
			return h.handleListTagsForResource(c, b)
		},
		"AssociateWebACL": func(c context.Context, b []byte) ([]byte, error) {
			return h.handleAssociateWebACL(c, b)
		},
		"DisassociateWebACL": func(c context.Context, b []byte) ([]byte, error) {
			return h.handleDisassociateWebACL(c, b)
		},
		"GetWebACLForResource": func(c context.Context, b []byte) ([]byte, error) {
			return h.handleGetWebACLForResource(c, b)
		},
		"CheckCapacity": func(c context.Context, b []byte) ([]byte, error) { return h.handleCheckCapacity(c, b) },
		"CreateAPIKey":  func(c context.Context, b []byte) ([]byte, error) { return h.handleCreateAPIKey(c, b) },
		"DeleteAPIKey":  func(c context.Context, b []byte) ([]byte, error) { return h.handleDeleteAPIKey(c, b) },
		"ListAPIKeys":   func(c context.Context, b []byte) ([]byte, error) { return h.handleListAPIKeys(c, b) },
		"GetRuleGroup":  func(c context.Context, b []byte) ([]byte, error) { return h.handleGetRuleGroup(c, b) },
		"DeleteRuleGroup": func(c context.Context, b []byte) ([]byte, error) {
			return h.handleDeleteRuleGroup(c, b)
		},
		"ListRuleGroups": func(c context.Context, b []byte) ([]byte, error) {
			return h.handleListRuleGroups(c, b)
		},
		"UpdateRuleGroup": func(c context.Context, b []byte) ([]byte, error) {
			return h.handleUpdateRuleGroup(c, b)
		},
		"CreateRuleGroup": func(c context.Context, b []byte) ([]byte, error) {
			return h.handleCreateRuleGroup(c, b)
		},
		"CreateRegexPatternSet": func(c context.Context, b []byte) ([]byte, error) {
			return h.handleCreateRegexPatternSet(c, b)
		},
		"DeleteRegexPatternSet": func(c context.Context, b []byte) ([]byte, error) {
			return h.handleDeleteRegexPatternSet(c, b)
		},
		"GetRegexPatternSet": func(c context.Context, b []byte) ([]byte, error) {
			return h.handleGetRegexPatternSet(c, b)
		},
		"ListRegexPatternSets": func(c context.Context, b []byte) ([]byte, error) {
			return h.handleListRegexPatternSets(c, b)
		},
		"UpdateRegexPatternSet": func(c context.Context, b []byte) ([]byte, error) {
			return h.handleUpdateRegexPatternSet(c, b)
		},
		"GetDecryptedAPIKey": func(c context.Context, b []byte) ([]byte, error) {
			return h.handleGetDecryptedAPIKey(c, b)
		},
		"DeleteFirewallManagerRuleGroups": func(c context.Context, b []byte) ([]byte, error) {
			return h.handleDeleteFirewallManagerRuleGroups(c, b)
		},
		"DeleteLoggingConfiguration": func(c context.Context, b []byte) ([]byte, error) {
			return h.handleDeleteLoggingConfiguration(c, b)
		},
		"DeletePermissionPolicy": func(c context.Context, b []byte) ([]byte, error) {
			return h.handleDeletePermissionPolicy(c, b)
		},
		"PutLoggingConfiguration": func(c context.Context, b []byte) ([]byte, error) {
			return h.handlePutLoggingConfiguration(c, b)
		},
		"GetLoggingConfiguration": func(c context.Context, b []byte) ([]byte, error) {
			return h.handleGetLoggingConfiguration(c, b)
		},
		"ListLoggingConfigurations": func(c context.Context, b []byte) ([]byte, error) {
			return h.handleListLoggingConfigurations(c, b)
		},
		"PutPermissionPolicy": func(c context.Context, b []byte) ([]byte, error) {
			return h.handlePutPermissionPolicy(c, b)
		},
		"GetPermissionPolicy": func(c context.Context, b []byte) ([]byte, error) {
			return h.handleGetPermissionPolicy(c, b)
		},
		"ListResourcesForWebACL": func(c context.Context, b []byte) ([]byte, error) {
			return h.handleListResourcesForWebACL(c, b)
		},
		"GetManagedRuleSet": func(c context.Context, b []byte) ([]byte, error) {
			return h.handleGetManagedRuleSet(c, b)
		},
		"ListManagedRuleSets": func(c context.Context, b []byte) ([]byte, error) {
			return h.handleListManagedRuleSets(c, b)
		},
		"PutManagedRuleSetVersions": func(c context.Context, b []byte) ([]byte, error) {
			return h.handlePutManagedRuleSetVersions(c, b)
		},
		"UpdateManagedRuleSetVersionExpiryDate": func(c context.Context, b []byte) ([]byte, error) {
			return h.handleUpdateManagedRuleSetVersionExpiryDate(c, b)
		},
		"GetRateBasedStatementManagedKeys": func(c context.Context, b []byte) ([]byte, error) {
			return h.handleGetRateBasedStatementManagedKeys(c, b)
		},
		"GetSampledRequests": func(c context.Context, b []byte) ([]byte, error) {
			return h.handleGetSampledRequests(c, b)
		},
		"GetTopPathStatisticsByTraffic": func(c context.Context, b []byte) ([]byte, error) {
			return h.handleGetTopPathStatisticsByTraffic(c, b)
		},
		"DescribeAllManagedProducts":            wrapNoCtx(h.handleDescribeAllManagedProducts),
		"DescribeManagedProductsByVendor":       wrapNoCtx(h.handleDescribeManagedProductsByVendor),
		"DescribeManagedRuleGroup":              wrapNoCtx(h.handleDescribeManagedRuleGroup),
		"GenerateMobileSdkReleaseUrl":           wrapNoCtx(h.handleGenerateMobileSdkReleaseURL),
		"GetMobileSdkRelease":                   wrapNoCtx(h.handleGetMobileSdkRelease),
		"ListMobileSdkReleases":                 wrapNoCtx(h.handleListMobileSdkReleases),
		"ListAvailableManagedRuleGroupVersions": wrapNoCtx(h.handleListAvailableManagedRuleGroupVersions),
		"ListAvailableManagedRuleGroups":        wrapNoCtx(h.handleListAvailableManagedRuleGroups),
	}
}

func (h *Handler) dispatch(ctx context.Context, op string, body []byte) ([]byte, error) {
	fn, ok := h.ops[op]
	if !ok {
		return nil, fmt.Errorf("%w: %s", errUnknownAction, op)
	}

	return fn(ctx, body)
}

func (h *Handler) handleError(c *echo.Context, err error) error {
	var syntaxErr *json.SyntaxError
	var typeErr *json.UnmarshalTypeError

	var errType string
	var statusCode int

	switch {
	case errors.Is(err, awserr.ErrNotFound):
		errType = "WAFNonexistentItemException"
		statusCode = http.StatusBadRequest
	case errors.Is(err, ErrOptimisticLock):
		errType = "WAFOptimisticLockException"
		statusCode = http.StatusBadRequest
	case errors.Is(err, ErrAssociatedItem):
		errType = "WAFAssociatedItemException"
		statusCode = http.StatusBadRequest
	case errors.Is(err, ErrLimitsExceeded):
		errType = "WAFLimitsExceededException"
		statusCode = http.StatusBadRequest
	case errors.Is(err, ErrTagOperation):
		errType = "WAFTagOperationException"
		statusCode = http.StatusBadRequest
	case errors.Is(err, ErrUnavailableEntity):
		errType = "WAFUnavailableEntityException"
		statusCode = http.StatusBadRequest
	case errors.Is(err, awserr.ErrConflict):
		errType = "WAFDuplicateItemException"
		statusCode = http.StatusBadRequest
	case errors.Is(err, ErrConfigurationWarning):
		errType = "WAFConfigurationWarningException"
		statusCode = http.StatusBadRequest
	case errors.Is(err, errInvalidRequest), errors.As(err, &syntaxErr), errors.As(err, &typeErr):
		errType = "WAFInvalidParameterException"
		statusCode = http.StatusBadRequest
	case errors.Is(err, errUnknownAction):
		errType = "WAFInvalidOperationException"
		statusCode = http.StatusBadRequest
	default:
		errType = "WAFInternalErrorException"
		statusCode = http.StatusInternalServerError
	}

	payload, _ := json.Marshal(map[string]string{
		keyTypeField:    errType,
		keyMessageField: err.Error(),
	})

	c.Response().Header().Set("X-Amzn-Errortype", errType)

	return c.JSONBlob(statusCode, payload)
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
	DefaultAction        json.RawMessage  `json:"DefaultAction"`
	VisibilityConfig     json.RawMessage  `json:"VisibilityConfig"`
	CustomResponseBodies json.RawMessage  `json:"CustomResponseBodies"`
	AssociationConfig    json.RawMessage  `json:"AssociationConfig"`
	CaptchaConfig        json.RawMessage  `json:"CaptchaConfig"`
	ChallengeConfig      json.RawMessage  `json:"ChallengeConfig"`
	Name                 string           `json:"Name"`
	Scope                string           `json:"Scope"`
	Description          string           `json:"Description"`
	Tags                 []tagItem        `json:"Tags"`
	TokenDomains         []string         `json:"TokenDomains"`
	Rules                []map[string]any `json:"Rules"`
}

func (h *Handler) handleCreateWebACL(ctx context.Context, body []byte) ([]byte, error) {
	var req createWebACLRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.Name == "" {
		return nil, fmt.Errorf("%w: Name is required", errInvalidRequest)
	}

	if err := validateResourceName(req.Name); err != nil {
		return nil, err
	}

	if err := validateDescription(req.Description); err != nil {
		return nil, err
	}

	if req.Scope == "" {
		return nil, fmt.Errorf("%w: Scope is required", errInvalidRequest)
	}

	if !validScope(req.Scope) {
		return nil, fmt.Errorf("%w: Scope must be %s or %s", errInvalidRequest, ScopeRegional, ScopeCloudFront)
	}

	if err := validateVisibilityConfig(req.VisibilityConfig); err != nil {
		return nil, err
	}

	if err := validateDefaultAction(req.DefaultAction); err != nil {
		return nil, err
	}

	if err := validateRules(req.Rules); err != nil {
		return nil, err
	}

	tags := tagsFromItems(req.Tags)
	if err := validateTags(tags); err != nil {
		return nil, err
	}

	w, err := h.Backend.CreateWebACL(
		ctx,
		req.Name,
		req.Scope,
		req.Description,
		req.DefaultAction,
		req.VisibilityConfig,
		req.Rules,
		req.TokenDomains,
		req.CustomResponseBodies,
		req.AssociationConfig,
		req.CaptchaConfig,
		req.ChallengeConfig,
		tags,
	)
	if err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "wafv2: created web ACL", "name", w.Name, "id", w.ID)

	arnStr := h.Backend.WebACLARN(w.Name, w.ID, w.Scope)

	return json.Marshal(map[string]any{
		keySummary: map[string]string{
			"Id":           w.ID,
			keyName:        w.Name,
			keyARN:         arnStr,
			keyLockToken:   w.LockToken,
			keyDescription: w.Description,
		},
	})
}

// validateDefaultAction validates that exactly one of Allow/Block is set in DefaultAction.
func validateDefaultAction(raw json.RawMessage) error {
	if len(raw) == 0 {
		return nil
	}

	var m map[string]json.RawMessage
	if err := json.Unmarshal(raw, &m); err != nil {
		return fmt.Errorf("%w: invalid DefaultAction JSON: %w", errInvalidRequest, err)
	}

	_, hasAllow := m["Allow"]
	_, hasBlock := m["Block"]

	if hasAllow && hasBlock {
		return fmt.Errorf("%w: DefaultAction must specify exactly one of Allow or Block", errInvalidRequest)
	}

	return nil
}

// getWebACLRequest is the request body for GetWebACL.
type getWebACLRequest struct {
	ID    string `json:"Id"`
	Name  string `json:"Name"`
	Scope string `json:"Scope"`
}

func (h *Handler) handleGetWebACL(ctx context.Context, body []byte) ([]byte, error) {
	var req getWebACLRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ID == "" {
		return nil, fmt.Errorf("%w: Id is required", errInvalidRequest)
	}

	w, err := h.Backend.GetWebACL(ctx, req.ID)
	if err != nil {
		return nil, err
	}

	if req.Scope != "" && w.Scope != req.Scope {
		return nil, fmt.Errorf("%w: web ACL %q has scope %s, not %s", ErrWebACLNotFound, req.ID, w.Scope, req.Scope)
	}

	return h.marshalWebACL(w)
}

// marshalWebACL builds the canonical WebACL JSON response.
func (h *Handler) marshalWebACL(w *WebACL) ([]byte, error) {
	arnStr := h.Backend.WebACLARN(w.Name, w.ID, w.Scope)
	visConfig := parseVisibilityConfig(w.VisibilityConfig, w.Name)

	defaultActionJSON := w.DefaultAction
	if len(defaultActionJSON) == 0 {
		defaultActionJSON = json.RawMessage(`{"Allow":{}}`)
	}

	var defaultActionMap any
	if err := json.Unmarshal(defaultActionJSON, &defaultActionMap); err != nil {
		defaultActionMap = map[string]any{"Allow": map[string]any{}}
	}

	rules := w.Rules
	if rules == nil {
		rules = []map[string]any{}
	}

	webACLMap := map[string]any{
		"Id":                w.ID,
		keyName:             w.Name,
		keyARN:              arnStr,
		keyLockToken:        w.LockToken,
		keyDescription:      w.Description,
		"DefaultAction":     defaultActionMap,
		keyVisibilityConfig: visConfig,
		keyRules:            rules,
	}

	if len(w.TokenDomains) > 0 {
		webACLMap["TokenDomains"] = w.TokenDomains
	}

	if len(w.CustomResponseBodies) > 0 {
		var crb any
		if json.Unmarshal(w.CustomResponseBodies, &crb) == nil {
			webACLMap["CustomResponseBodies"] = crb
		}
	}

	if len(w.AssociationConfig) > 0 {
		var ac any
		if json.Unmarshal(w.AssociationConfig, &ac) == nil {
			webACLMap["AssociationConfig"] = ac
		}
	}

	if len(w.CaptchaConfig) > 0 {
		var cc any
		if json.Unmarshal(w.CaptchaConfig, &cc) == nil {
			webACLMap["CaptchaConfig"] = cc
		}
	}

	if len(w.ChallengeConfig) > 0 {
		var chc any
		if json.Unmarshal(w.ChallengeConfig, &chc) == nil {
			webACLMap["ChallengeConfig"] = chc
		}
	}

	return json.Marshal(map[string]any{
		"WebACL":     webACLMap,
		keyLockToken: w.LockToken,
	})
}

// updateWebACLRequest is the request body for UpdateWebACL.
type updateWebACLRequest struct {
	DefaultAction        json.RawMessage  `json:"DefaultAction"`
	VisibilityConfig     json.RawMessage  `json:"VisibilityConfig"`
	CustomResponseBodies json.RawMessage  `json:"CustomResponseBodies"`
	AssociationConfig    json.RawMessage  `json:"AssociationConfig"`
	CaptchaConfig        json.RawMessage  `json:"CaptchaConfig"`
	ChallengeConfig      json.RawMessage  `json:"ChallengeConfig"`
	ID                   string           `json:"Id"`
	Name                 string           `json:"Name"`
	Scope                string           `json:"Scope"`
	LockToken            string           `json:"LockToken"`
	Description          string           `json:"Description"`
	TokenDomains         []string         `json:"TokenDomains"`
	Rules                []map[string]any `json:"Rules"`
}

func (h *Handler) handleUpdateWebACL(ctx context.Context, body []byte) ([]byte, error) {
	var req updateWebACLRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ID == "" {
		return nil, fmt.Errorf("%w: Id is required", errInvalidRequest)
	}

	if err := validateVisibilityConfig(req.VisibilityConfig); err != nil {
		return nil, err
	}

	if err := validateDefaultAction(req.DefaultAction); err != nil {
		return nil, err
	}

	if err := validateRules(req.Rules); err != nil {
		return nil, err
	}

	w, err := h.Backend.UpdateWebACL(
		ctx,
		req.ID,
		req.Description,
		req.LockToken,
		req.DefaultAction,
		req.VisibilityConfig,
		req.Rules,
		req.TokenDomains,
		req.CustomResponseBodies,
		req.AssociationConfig,
		req.CaptchaConfig,
		req.ChallengeConfig,
	)
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

	if err := h.Backend.DeleteWebACL(ctx, req.ID, req.LockToken); err != nil {
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

// handleListWebACLs handles the ListWebACLs request.
//
//nolint:dupl // list handlers share structural similarity but operate on different types
func (h *Handler) handleListWebACLs(ctx context.Context, body []byte) ([]byte, error) {
	var req listWebACLsRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	webACLs := h.Backend.ListWebACLs(ctx)

	// Filter by scope.
	filtered := make([]*WebACL, 0, len(webACLs))

	for _, w := range webACLs {
		if req.Scope != "" && w.Scope != req.Scope {
			continue
		}

		filtered = append(filtered, w)
	}

	// Apply pagination.
	items, nextMarker := paginateByName(
		filtered,
		func(w *WebACL) string { return w.Name },
		req.NextMarker,
		req.Limit,
	)

	summaries := make([]map[string]string, 0, len(items))

	for _, w := range items {
		arnStr := h.Backend.WebACLARN(w.Name, w.ID, w.Scope)
		summaries = append(summaries, map[string]string{
			"Id":           w.ID,
			keyName:        w.Name,
			keyARN:         arnStr,
			keyLockToken:   w.LockToken,
			keyDescription: w.Description,
		})
	}

	resp := map[string]any{"WebACLs": summaries}
	if nextMarker != "" {
		resp["NextMarker"] = nextMarker
	}

	return json.Marshal(resp)
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

	if err := validateResourceName(req.Name); err != nil {
		return nil, err
	}

	if err := validateDescription(req.Description); err != nil {
		return nil, err
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

	if err := validateCIDRs(req.Addresses, req.IPAddressVersion); err != nil {
		return nil, err
	}

	tags := tagsFromItems(req.Tags)
	if err := validateTags(tags); err != nil {
		return nil, err
	}

	s, err := h.Backend.CreateIPSet(
		ctx,
		req.Name,
		req.Scope,
		req.Description,
		req.IPAddressVersion,
		req.Addresses,
		tags,
	)
	if err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "wafv2: created IP set", "name", s.Name, "id", s.ID)

	arnStr := h.Backend.IPSetARN(s.Name, s.ID, s.Scope)

	return json.Marshal(map[string]any{
		keySummary: map[string]string{
			"Id":           s.ID,
			keyName:        s.Name,
			keyARN:         arnStr,
			keyLockToken:   s.LockToken,
			keyDescription: s.Description,
		},
	})
}

// getIPSetRequest is the request body for GetIPSet.
type getIPSetRequest struct {
	ID    string `json:"Id"`
	Name  string `json:"Name"`
	Scope string `json:"Scope"`
}

func (h *Handler) handleGetIPSet(ctx context.Context, body []byte) ([]byte, error) {
	var req getIPSetRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ID == "" {
		return nil, fmt.Errorf("%w: Id is required", errInvalidRequest)
	}

	s, err := h.Backend.GetIPSet(ctx, req.ID)
	if err != nil {
		return nil, err
	}

	if req.Scope != "" && s.Scope != req.Scope {
		return nil, fmt.Errorf("%w: IP set %q has scope %s, not %s", ErrIPSetNotFound, req.ID, s.Scope, req.Scope)
	}

	arnStr := h.Backend.IPSetARN(s.Name, s.ID, s.Scope)

	return json.Marshal(map[string]any{
		"IPSet": map[string]any{
			"Id":                s.ID,
			keyName:             s.Name,
			keyARN:              arnStr,
			keyLockToken:        s.LockToken,
			keyDescription:      s.Description,
			keyIPAddressVersion: s.IPAddressVersion,
			keyAddresses:        s.Addresses,
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

	// Validate CIDRs against stored IP version — fetch first.
	existing, err := h.Backend.GetIPSet(ctx, req.ID)
	if err != nil {
		return nil, err
	}

	if req.Addresses != nil {
		if cidrErr := validateCIDRs(req.Addresses, existing.IPAddressVersion); cidrErr != nil {
			return nil, cidrErr
		}
	}

	s, err := h.Backend.UpdateIPSet(ctx, req.ID, req.Description, req.LockToken, req.Addresses)
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

	if err := h.Backend.DeleteIPSet(ctx, req.ID, req.LockToken); err != nil {
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

//nolint:dupl // list handlers share structural similarity but operate on different types
func (h *Handler) handleListIPSets(ctx context.Context, body []byte) ([]byte, error) {
	var req listIPSetsRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	ipSets := h.Backend.ListIPSets(ctx)

	filtered := make([]*IPSet, 0, len(ipSets))

	for _, s := range ipSets {
		if req.Scope != "" && s.Scope != req.Scope {
			continue
		}

		filtered = append(filtered, s)
	}

	items, nextMarker := paginateByName(
		filtered,
		func(s *IPSet) string { return s.Name },
		req.NextMarker,
		req.Limit,
	)

	summaries := make([]map[string]string, 0, len(items))

	for _, s := range items {
		arnStr := h.Backend.IPSetARN(s.Name, s.ID, s.Scope)
		summaries = append(summaries, map[string]string{
			"Id":           s.ID,
			keyName:        s.Name,
			keyARN:         arnStr,
			keyLockToken:   s.LockToken,
			keyDescription: s.Description,
		})
	}

	resp := map[string]any{"IPSets": summaries}
	if nextMarker != "" {
		resp["NextMarker"] = nextMarker
	}

	return json.Marshal(resp)
}

// tagResourceRequest is the request body for TagResource.
type tagResourceRequest struct {
	ResourceARN string    `json:"ResourceARN"`
	Tags        []tagItem `json:"Tags"`
}

func (h *Handler) handleTagResource(ctx context.Context, body []byte) ([]byte, error) {
	var req tagResourceRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ResourceARN == "" {
		return nil, fmt.Errorf("%w: ResourceARN is required", errInvalidRequest)
	}

	tags := tagsFromItems(req.Tags)
	if err := validateTags(tags); err != nil {
		return nil, err
	}

	if err := h.Backend.TagResource(ctx, req.ResourceARN, tags); err != nil {
		return nil, err
	}

	return nil, nil
}

// listTagsForResourceRequest is the request body for ListTagsForResource.
type listTagsForResourceRequest struct {
	ResourceARN string `json:"ResourceARN"`
}

func (h *Handler) handleListTagsForResource(ctx context.Context, body []byte) ([]byte, error) {
	var req listTagsForResourceRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ResourceARN == "" {
		return nil, fmt.Errorf("%w: ResourceARN is required", errInvalidRequest)
	}

	tags, err := h.Backend.ListTagsForResource(ctx, req.ResourceARN)
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

func (h *Handler) handleUntagResource(ctx context.Context, body []byte) ([]byte, error) {
	var req untagResourceRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ResourceARN == "" {
		return nil, fmt.Errorf("%w: ResourceARN is required", errInvalidRequest)
	}

	if err := h.Backend.UntagResource(ctx, req.ResourceARN, req.TagKeys); err != nil {
		return nil, err
	}

	return nil, nil
}

// parseVisibilityConfig parses a stored VisibilityConfig JSON string or returns a default.
func parseVisibilityConfig(stored json.RawMessage, metricName string) map[string]any {
	if len(stored) > 0 {
		var m map[string]any
		if err := json.Unmarshal(stored, &m); err == nil {
			return m
		}
	}

	return map[string]any{
		"CloudWatchMetricsEnabled": false,
		"MetricName":               metricName,
		"SampledRequestsEnabled":   false,
	}
}

// paginateByName implements cursor-based pagination over name-sorted items.
// The cursor is base64(last-name-seen). Returns the page of items and the
// next marker (empty string if no more pages).
func paginateByName[T any](items []T, getName func(T) string, nextMarker string, limit int) ([]T, string) {
	// Decode cursor.
	startAfter := ""

	if nextMarker != "" {
		decoded, err := base64.StdEncoding.DecodeString(nextMarker)
		if err == nil {
			startAfter = string(decoded)
		}
	}

	// Skip items up to and including the cursor name.
	start := 0

	if startAfter != "" {
		for start < len(items) && getName(items[start]) <= startAfter {
			start++
		}
	}

	items = items[start:]

	// Apply limit.
	if limit <= 0 || limit > len(items) {
		return items, ""
	}

	page := items[:limit]
	lastName := getName(page[len(page)-1])
	newMarker := base64.StdEncoding.EncodeToString([]byte(lastName))

	return page, newMarker
}

// associateWebACLRequest is the request body for AssociateWebACL.
type associateWebACLRequest struct {
	WebACLArn   string `json:"WebACLArn"`
	ResourceArn string `json:"ResourceArn"`
}

func (h *Handler) handleAssociateWebACL(ctx context.Context, body []byte) ([]byte, error) {
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

	if err := h.validateAssociationScope(req.WebACLArn, req.ResourceArn); err != nil {
		return nil, err
	}

	if err := h.Backend.AssociateWebACL(ctx, req.WebACLArn, req.ResourceArn); err != nil {
		return nil, err
	}

	return nil, nil
}

// validateAssociationScope checks that the resource ARN's service is compatible with the WebACL scope.
func (h *Handler) validateAssociationScope(webACLArn, resourceArn string) error {
	// Determine WebACL scope from ARN (global → CLOUDFRONT, regional → REGIONAL).
	if strings.Contains(webACLArn, "/global/") {
		return fmt.Errorf(
			"%w: CLOUDFRONT WebACL associations must be managed through the CloudFront API",
			ErrInvalidOperation,
		)
	}

	// For REGIONAL WebACLs, validate service.
	service := extractARNService(resourceArn)
	if slices.Contains(regionalResourceServices, service) {
		return nil
	}

	// If service is unrecognised, still allow (for compatibility with unknown resource types).
	return nil
}

const arnServiceIndex = 2 // position of service segment in arn:partition:SERVICE:...

// extractARNService extracts the service segment from an ARN (arn:partition:SERVICE:...).
func extractARNService(arnStr string) string {
	const minARNParts = 3

	parts := strings.Split(arnStr, ":")
	if len(parts) >= minARNParts {
		return parts[arnServiceIndex]
	}

	return ""
}

// disassociateWebACLRequest is the request body for DisassociateWebACL.
type disassociateWebACLRequest struct {
	ResourceArn string `json:"ResourceArn"`
}

func (h *Handler) handleDisassociateWebACL(ctx context.Context, body []byte) ([]byte, error) {
	var req disassociateWebACLRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ResourceArn == "" {
		return nil, fmt.Errorf("%w: ResourceArn is required", errInvalidRequest)
	}

	if err := h.Backend.DisassociateWebACL(ctx, req.ResourceArn); err != nil {
		return nil, err
	}

	return nil, nil
}

// getWebACLForResourceRequest is the request body for GetWebACLForResource.
type getWebACLForResourceRequest struct {
	ResourceArn string `json:"ResourceArn"`
}

func (h *Handler) handleGetWebACLForResource(ctx context.Context, body []byte) ([]byte, error) {
	var req getWebACLForResourceRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ResourceArn == "" {
		return nil, fmt.Errorf("%w: ResourceArn is required", errInvalidRequest)
	}

	w, err := h.Backend.GetWebACLForResource(ctx, req.ResourceArn)
	if err != nil {
		return nil, err
	}

	return h.marshalWebACL(w)
}

// checkCapacityRequest is the request body for CheckCapacity.
type checkCapacityRequest struct {
	Scope string           `json:"Scope"`
	Rules []map[string]any `json:"Rules"`
}

func (h *Handler) handleCheckCapacity(ctx context.Context, body []byte) ([]byte, error) {
	var req checkCapacityRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.Scope == "" {
		return nil, fmt.Errorf("%w: Scope is required", errInvalidRequest)
	}

	capacity, err := h.Backend.CheckCapacity(ctx, req.Scope, req.Rules)
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

	if len(req.TokenDomains) < 1 || len(req.TokenDomains) > maxAPIKeyTokenDomains {
		return nil, fmt.Errorf(
			"%w: TokenDomains must have 1 to %d entries",
			errInvalidRequest,
			maxAPIKeyTokenDomains,
		)
	}

	a, err := h.Backend.CreateAPIKey(ctx, req.Scope, req.TokenDomains)
	if err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "wafv2: created API key", "scope", a.Scope)

	// Emit a base64-encoded key value as AWS does.
	encodedKey := base64.StdEncoding.EncodeToString([]byte(a.APIKeyValue))

	return json.Marshal(map[string]any{
		"APIKey": encodedKey,
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

	// API keys may be passed as base64-encoded values; try decoding first.
	lookupKey := req.APIKey
	if decoded, err := base64.StdEncoding.DecodeString(req.APIKey); err == nil {
		lookupKey = string(decoded)
	}

	if err := h.Backend.DeleteAPIKey(ctx, req.Scope, lookupKey); err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "wafv2: deleted API key", "scope", req.Scope)

	return nil, nil
}

// createRegexPatternSetRequest is the request body for CreateRegexPatternSet.
// RegularExpressionList accepts both []string and []{RegexString:...} shapes.
type createRegexPatternSetRequest struct {
	Name                  string          `json:"Name"`
	Scope                 string          `json:"Scope"`
	Description           string          `json:"Description"`
	RegularExpressionList json.RawMessage `json:"RegularExpressionList"`
	Tags                  []tagItem       `json:"Tags"`
}

func (h *Handler) handleCreateRegexPatternSet(ctx context.Context, body []byte) ([]byte, error) {
	var req createRegexPatternSetRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.Name == "" {
		return nil, fmt.Errorf("%w: Name is required", errInvalidRequest)
	}

	if err := validateResourceName(req.Name); err != nil {
		return nil, err
	}

	if err := validateDescription(req.Description); err != nil {
		return nil, err
	}

	if req.Scope == "" {
		return nil, fmt.Errorf("%w: Scope is required", errInvalidRequest)
	}

	if !validScope(req.Scope) {
		return nil, fmt.Errorf("%w: Scope must be %s or %s", errInvalidRequest, ScopeRegional, ScopeCloudFront)
	}

	entries, err := parseRegexEntries(req.RegularExpressionList)
	if err != nil {
		return nil, err
	}

	if validateErr := validateRegexEntries(entries); validateErr != nil {
		return nil, validateErr
	}

	tags := tagsFromItems(req.Tags)
	if tagsErr := validateTags(tags); tagsErr != nil {
		return nil, tagsErr
	}

	rps, err := h.Backend.CreateRegexPatternSet(
		ctx,
		req.Name,
		req.Scope,
		req.Description,
		entries,
		tags,
	)
	if err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "wafv2: created regex pattern set", "name", rps.Name, "id", rps.ID)

	arnStr := h.Backend.RegexPatternSetARN(rps.Name, rps.ID, rps.Scope)

	return json.Marshal(map[string]any{
		keySummary: map[string]string{
			"Id":           rps.ID,
			keyName:        rps.Name,
			keyARN:         arnStr,
			keyLockToken:   rps.LockToken,
			keyDescription: rps.Description,
		},
	})
}

// parseRegexEntries accepts either []RegexEntry or []string and normalises to []RegexEntry.
func parseRegexEntries(raw json.RawMessage) ([]RegexEntry, error) {
	if len(raw) == 0 {
		return []RegexEntry{}, nil
	}

	// Try object form first: [{"RegexString": "..."}]
	var entries []RegexEntry
	if err := json.Unmarshal(raw, &entries); err == nil {
		return entries, nil
	}

	// Fall back to plain string form: ["...", "..."]
	var strs []string
	if err := json.Unmarshal(raw, &strs); err != nil {
		return nil, fmt.Errorf("%w: RegularExpressionList must be an array of objects or strings", errInvalidRequest)
	}

	result := make([]RegexEntry, len(strs))
	for i, s := range strs {
		result[i] = RegexEntry{RegexString: s}
	}

	return result, nil
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

	if err := h.Backend.DeleteRegexPatternSet(ctx, req.ID, req.LockToken); err != nil {
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

	if err := validateResourceName(req.Name); err != nil {
		return nil, err
	}

	if err := validateDescription(req.Description); err != nil {
		return nil, err
	}

	if req.Scope == "" {
		return nil, fmt.Errorf("%w: Scope is required", errInvalidRequest)
	}

	if !validScope(req.Scope) {
		return nil, fmt.Errorf("%w: Scope must be %s or %s", errInvalidRequest, ScopeRegional, ScopeCloudFront)
	}

	if req.Capacity < minRuleGroupCapacity || req.Capacity > maxRuleGroupCapacity {
		return nil, fmt.Errorf(
			"%w: Capacity must be between %d and %d",
			errInvalidRequest,
			minRuleGroupCapacity,
			maxRuleGroupCapacity,
		)
	}

	if err := validateVisibilityConfig(req.VisibilityConfig); err != nil {
		return nil, err
	}

	tags := tagsFromItems(req.Tags)
	if err := validateTags(tags); err != nil {
		return nil, err
	}

	rg, err := h.Backend.CreateRuleGroup(
		ctx,
		req.Name,
		req.Scope,
		req.Description,
		string(req.VisibilityConfig),
		req.Capacity,
		req.Rules,
		tags,
	)
	if err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "wafv2: created rule group", "name", rg.Name, "id", rg.ID)

	arnStr := h.Backend.RuleGroupARN(rg.Name, rg.ID, rg.Scope)

	return json.Marshal(map[string]any{
		keySummary: map[string]string{
			"Id":           rg.ID,
			keyName:        rg.Name,
			keyARN:         arnStr,
			keyLockToken:   rg.LockToken,
			keyDescription: rg.Description,
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

	w, err := h.Backend.DeleteFirewallManagerRuleGroups(ctx, req.WebACLArn)
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

	if err := h.Backend.DeleteLoggingConfiguration(ctx, req.ResourceArn); err != nil {
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

	if err := h.Backend.DeletePermissionPolicy(ctx, req.ResourceArn); err != nil {
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

func (h *Handler) handleGetRegexPatternSet(ctx context.Context, body []byte) ([]byte, error) {
	var req getRegexPatternSetRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ID == "" {
		return nil, fmt.Errorf("%w: Id is required", errInvalidRequest)
	}

	r, err := h.Backend.GetRegexPatternSet(ctx, req.ID)
	if err != nil {
		return nil, err
	}

	if req.Scope != "" && r.Scope != req.Scope {
		return nil, fmt.Errorf(
			"%w: regex pattern set %q has scope %s, not %s",
			ErrRegexPatternSetNotFound,
			req.ID,
			r.Scope,
			req.Scope,
		)
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

//nolint:dupl // list handlers share structural similarity but operate on different types
func (h *Handler) handleListRegexPatternSets(ctx context.Context, body []byte) ([]byte, error) {
	var req listRegexPatternSetsRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	sets := h.Backend.ListRegexPatternSets(ctx)

	filtered := make([]*RegexPatternSet, 0, len(sets))

	for _, r := range sets {
		if req.Scope != "" && r.Scope != req.Scope {
			continue
		}

		filtered = append(filtered, r)
	}

	items, nextMarker := paginateByName(
		filtered,
		func(r *RegexPatternSet) string { return r.Name },
		req.NextMarker,
		req.Limit,
	)

	summaries := make([]map[string]string, 0, len(items))

	for _, r := range items {
		summaries = append(summaries, map[string]string{
			"Id":           r.ID,
			keyName:        r.Name,
			keyARN:         h.Backend.RegexPatternSetARN(r.Name, r.ID, r.Scope),
			keyLockToken:   r.LockToken,
			keyDescription: r.Description,
		})
	}

	resp := map[string]any{"RegexPatternSets": summaries}
	if nextMarker != "" {
		resp["NextMarker"] = nextMarker
	}

	return json.Marshal(resp)
}

// updateRegexPatternSetRequest is the request body for UpdateRegexPatternSet.
type updateRegexPatternSetRequest struct {
	ID                    string          `json:"Id"`
	Name                  string          `json:"Name"`
	Scope                 string          `json:"Scope"`
	LockToken             string          `json:"LockToken"`
	Description           string          `json:"Description"`
	RegularExpressionList json.RawMessage `json:"RegularExpressionList"`
}

func (h *Handler) handleUpdateRegexPatternSet(ctx context.Context, body []byte) ([]byte, error) {
	var req updateRegexPatternSetRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ID == "" {
		return nil, fmt.Errorf("%w: Id is required", errInvalidRequest)
	}

	entries, err := parseRegexEntries(req.RegularExpressionList)
	if err != nil {
		return nil, err
	}

	if validateErr := validateRegexEntries(entries); validateErr != nil {
		return nil, validateErr
	}

	r, err := h.Backend.UpdateRegexPatternSet(ctx, req.ID, req.Description, req.LockToken, entries)
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

func (h *Handler) handleGetRuleGroup(ctx context.Context, body []byte) ([]byte, error) {
	var req getRuleGroupRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ID == "" {
		return nil, fmt.Errorf("%w: Id is required", errInvalidRequest)
	}

	rg, err := h.Backend.GetRuleGroup(ctx, req.ID)
	if err != nil {
		return nil, err
	}

	if req.Scope != "" && rg.Scope != req.Scope {
		return nil, fmt.Errorf(
			"%w: rule group %q has scope %s, not %s",
			ErrRuleGroupNotFound,
			req.ID,
			rg.Scope,
			req.Scope,
		)
	}

	arnStr := h.Backend.RuleGroupARN(rg.Name, rg.ID, rg.Scope)
	visConfig := parseVisibilityConfig(json.RawMessage(rg.VisibilityConfig), rg.Name)

	return json.Marshal(map[string]any{
		"RuleGroup": map[string]any{
			"Id":                rg.ID,
			keyName:             rg.Name,
			keyARN:              arnStr,
			keyDescription:      rg.Description,
			keyCapacity:         rg.Capacity,
			keyRules:            rg.Rules,
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

//nolint:dupl // list handlers share structural similarity but operate on different types
func (h *Handler) handleListRuleGroups(ctx context.Context, body []byte) ([]byte, error) {
	var req listRuleGroupsRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	groups := h.Backend.ListRuleGroups(ctx)

	filtered := make([]*RuleGroup, 0, len(groups))

	for _, rg := range groups {
		if req.Scope != "" && rg.Scope != req.Scope {
			continue
		}

		filtered = append(filtered, rg)
	}

	items, nextMarker := paginateByName(
		filtered,
		func(rg *RuleGroup) string { return rg.Name },
		req.NextMarker,
		req.Limit,
	)

	summaries := make([]map[string]string, 0, len(items))

	for _, rg := range items {
		summaries = append(summaries, map[string]string{
			"Id":           rg.ID,
			keyName:        rg.Name,
			keyARN:         h.Backend.RuleGroupARN(rg.Name, rg.ID, rg.Scope),
			keyLockToken:   rg.LockToken,
			keyDescription: rg.Description,
		})
	}

	resp := map[string]any{"RuleGroups": summaries}
	if nextMarker != "" {
		resp["NextMarker"] = nextMarker
	}

	return json.Marshal(resp)
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

	if err := validateVisibilityConfig(req.VisibilityConfig); err != nil {
		return nil, err
	}

	rg, err := h.Backend.UpdateRuleGroup(
		ctx,
		req.ID,
		req.Description,
		string(req.VisibilityConfig),
		req.LockToken,
		req.Rules,
	)
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

func (h *Handler) handleListAPIKeys(ctx context.Context, body []byte) ([]byte, error) {
	var req listAPIKeysRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	keys := h.Backend.ListAPIKeys(ctx, req.Scope)

	// Apply pagination.
	page, nextMarker := paginateByName(
		keys,
		func(k *APIKey) string { return k.APIKeyValue },
		req.NextMarker,
		req.Limit,
	)

	items := make([]map[string]any, 0, len(page))

	for _, k := range page {
		items = append(items, map[string]any{
			"APIKey":       base64.StdEncoding.EncodeToString([]byte(k.APIKeyValue)),
			keyScope:       k.Scope,
			"TokenDomains": k.TokenDomains,
		})
	}

	resp := map[string]any{"APIKeys": items}
	if nextMarker != "" {
		resp["NextMarker"] = nextMarker
	}

	return json.Marshal(resp)
}

// getDecryptedAPIKeyRequest is the request body for GetDecryptedAPIKey.
type getDecryptedAPIKeyRequest struct {
	Scope  string `json:"Scope"`
	APIKey string `json:"APIKey"`
}

func (h *Handler) handleGetDecryptedAPIKey(ctx context.Context, body []byte) ([]byte, error) {
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

	// API keys may be passed as base64-encoded values; try decoding first.
	lookupKey := req.APIKey
	if decoded, err := base64.StdEncoding.DecodeString(req.APIKey); err == nil {
		lookupKey = string(decoded)
	}

	a, err := h.Backend.GetDecryptedAPIKey(ctx, req.Scope, lookupKey)
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
	LoggingConfiguration json.RawMessage `json:"LoggingConfiguration"`
}

func (h *Handler) handlePutLoggingConfiguration(ctx context.Context, body []byte) ([]byte, error) {
	var req putLoggingConfigurationRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	// Extract ResourceArn for validation.
	var cfg map[string]json.RawMessage
	if err := json.Unmarshal(req.LoggingConfiguration, &cfg); err != nil {
		return nil, fmt.Errorf("%w: invalid LoggingConfiguration JSON: %w", errInvalidRequest, err)
	}

	var resourceARN string
	if raw, ok := cfg["ResourceArn"]; ok {
		if err := json.Unmarshal(raw, &resourceARN); err != nil || resourceARN == "" {
			return nil, fmt.Errorf("%w: LoggingConfiguration.ResourceArn is required", errInvalidRequest)
		}
	} else {
		return nil, fmt.Errorf("%w: LoggingConfiguration.ResourceArn is required", errInvalidRequest)
	}

	// Validate destination ARN prefixes.
	if destRaw, ok := cfg["LogDestinationConfigs"]; ok {
		var destinations []string
		if unmarshalErr := json.Unmarshal(destRaw, &destinations); unmarshalErr == nil {
			for _, dest := range destinations {
				if destErr := validateLoggingDestination(dest); destErr != nil {
					return nil, destErr
				}
			}
		}
	}

	if err := h.Backend.PutLoggingConfiguration(ctx, resourceARN, req.LoggingConfiguration); err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "wafv2: put logging configuration", "resourceArn", resourceARN)

	var respCfg any
	if err := json.Unmarshal(req.LoggingConfiguration, &respCfg); err != nil {
		respCfg = map[string]any{"ResourceArn": resourceARN}
	}

	return json.Marshal(map[string]any{
		"LoggingConfiguration": respCfg,
	})
}

// validateLoggingDestination checks that a destination ARN has an accepted prefix.
func validateLoggingDestination(dest string) error {
	for _, prefix := range validLoggingDestinationPrefixes {
		if strings.HasPrefix(dest, prefix) {
			return nil
		}
	}

	return fmt.Errorf(
		"%w: LogDestinationConfig ARN %q must start with one of: firehose, s3, or CloudWatch Logs",
		errInvalidRequest,
		dest,
	)
}

// getLoggingConfigurationRequest is the request body for GetLoggingConfiguration.
type getLoggingConfigurationRequest struct {
	ResourceArn string `json:"ResourceArn"`
}

func (h *Handler) handleGetLoggingConfiguration(ctx context.Context, body []byte) ([]byte, error) {
	var req getLoggingConfigurationRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ResourceArn == "" {
		return nil, fmt.Errorf("%w: ResourceArn is required", errInvalidRequest)
	}

	cfgJSON, err := h.Backend.GetLoggingConfiguration(ctx, req.ResourceArn)
	if err != nil {
		return nil, err
	}

	var cfg any
	if unmarshalErr := json.Unmarshal(cfgJSON, &cfg); unmarshalErr != nil {
		cfg = map[string]any{"ResourceArn": req.ResourceArn}
	}

	return json.Marshal(map[string]any{
		"LoggingConfiguration": cfg,
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

	if err := h.Backend.PutPermissionPolicy(ctx, req.ResourceArn, req.Policy); err != nil {
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

func (h *Handler) handleGetPermissionPolicy(ctx context.Context, body []byte) ([]byte, error) {
	var req getPermissionPolicyRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ResourceArn == "" {
		return nil, fmt.Errorf("%w: ResourceArn is required", errInvalidRequest)
	}

	policy, err := h.Backend.GetPermissionPolicy(ctx, req.ResourceArn)
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

func (h *Handler) handleListResourcesForWebACL(ctx context.Context, body []byte) ([]byte, error) {
	var req listResourcesForWebACLRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.WebACLArn == "" {
		return nil, fmt.Errorf("%w: WebACLArn is required", errInvalidRequest)
	}

	resources, err := h.Backend.ListResourcesForWebACL(ctx, req.WebACLArn)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{"ResourceArns": resources})
}

// deleteRuleGroupRequest is the request body for DeleteRuleGroup.
type deleteRuleGroupRequest struct {
	ID        string `json:"Id"`
	Name      string `json:"Name"`
	Scope     string `json:"Scope"`
	LockToken string `json:"LockToken"`
}

func (h *Handler) handleDeleteRuleGroup(ctx context.Context, body []byte) ([]byte, error) {
	var req deleteRuleGroupRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ID == "" {
		return nil, fmt.Errorf("%w: Id is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteRuleGroup(ctx, req.ID, req.LockToken); err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "wafv2: deleted rule group", "id", req.ID)

	return nil, nil
}

// handleDescribeAllManagedProducts returns the catalog of managed products.
func (h *Handler) handleDescribeAllManagedProducts(_ []byte) ([]byte, error) {
	products := make([]map[string]any, 0, len(getManagedRuleGroups()))

	for _, mrg := range getManagedRuleGroups() {
		products = append(products, map[string]any{
			keyVendorName:        mrg.VendorName,
			"ManagedRuleSetName": mrg.Name,
			"ProductDescription": mrg.Description,
		})
	}

	return json.Marshal(map[string]any{"ManagedProducts": products})
}

// describeManagedProductsByVendorRequest is the request body for DescribeManagedProductsByVendor.
type describeManagedProductsByVendorRequest struct {
	Scope      string `json:"Scope"`
	VendorName string `json:"VendorName"`
}

// handleDescribeManagedProductsByVendor returns catalog entries filtered by vendor.
func (h *Handler) handleDescribeManagedProductsByVendor(body []byte) ([]byte, error) {
	var req describeManagedProductsByVendorRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	products := make([]map[string]any, 0)

	for _, mrg := range getManagedRuleGroups() {
		if req.VendorName != "" && mrg.VendorName != req.VendorName {
			continue
		}

		products = append(products, map[string]any{
			keyVendorName:        mrg.VendorName,
			"ManagedRuleSetName": mrg.Name,
			"ProductDescription": mrg.Description,
		})
	}

	return json.Marshal(map[string]any{"ManagedProducts": products})
}

// describeManagedRuleGroupRequest is the request body for DescribeManagedRuleGroup.
type describeManagedRuleGroupRequest struct {
	Scope       string `json:"Scope"`
	VendorName  string `json:"VendorName"`
	Name        string `json:"Name"`
	VersionName string `json:"VersionName"`
}

// handleDescribeManagedRuleGroup returns catalog data for the requested managed rule group.
func (h *Handler) handleDescribeManagedRuleGroup(body []byte) ([]byte, error) {
	var req describeManagedRuleGroupRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	// Look up catalog entry.
	for _, mrg := range getManagedRuleGroups() {
		if mrg.VendorName == req.VendorName && mrg.Name == req.Name {
			return json.Marshal(map[string]any{
				keyCapacity:       mrg.Capacity,
				keyRules:          buildRuleList(mrg.Rules),
				"SnsTopicArn":     "",
				"AvailableLabels": buildLabelList(mrg.Rules),
				"ConsumedLabels":  []any{},
				"Description":     mrg.Description,
			})
		}
	}

	return nil, fmt.Errorf(
		"%w: managed rule group %q/%q not found",
		ErrManagedRuleGroupNotFound, req.VendorName, req.Name,
	)
}

// buildRuleList converts catalog rule entries to the AWS DescribeManagedRuleGroup Rules format.
func buildRuleList(rules []managedRuleInfo) []any {
	if len(rules) == 0 {
		return []any{}
	}

	out := make([]any, len(rules))
	for i, r := range rules {
		out[i] = map[string]any{
			keyName:  r.Name,
			"Action": map[string]any{capitalizeAction(r.DefaultAction): map[string]any{}},
		}
	}

	return out
}

// capitalizeAction maps lowercase action names to the title-case form AWS uses in responses.
func capitalizeAction(action string) string {
	switch action {
	case actionBlock:
		return "Block"
	case actionCount:
		return "Count"
	case "allow":
		return "Allow"
	default:
		return action
	}
}

// buildLabelList collects all unique labels from a rule set into AWS DescribeManagedRuleGroup
// AvailableLabels format: [{Name: "label"}].
func buildLabelList(rules []managedRuleInfo) []any {
	seen := make(map[string]bool)
	var out []any

	for _, r := range rules {
		for _, lbl := range r.Labels {
			if !seen[lbl] {
				seen[lbl] = true
				out = append(out, map[string]any{"Name": lbl})
			}
		}
	}

	if out == nil {
		return []any{}
	}

	return out
}

// generateMobileSdkReleaseUrlRequest is the request body for GenerateMobileSdkReleaseUrl.
type generateMobileSdkReleaseURLRequest struct {
	Platform       string `json:"Platform"`
	ReleaseVersion string `json:"ReleaseVersion"`
}

// handleGenerateMobileSdkReleaseURL returns a presigned-style URL for the requested mobile SDK release.
func (h *Handler) handleGenerateMobileSdkReleaseURL(body []byte) ([]byte, error) {
	var req generateMobileSdkReleaseURLRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.Platform == "" {
		return nil, fmt.Errorf("%w: Platform is required", errInvalidRequest)
	}

	if req.ReleaseVersion == "" {
		return nil, fmt.Errorf("%w: ReleaseVersion is required", errInvalidRequest)
	}

	if getMobileSdkRelease(req.Platform, req.ReleaseVersion) == nil {
		return nil, fmt.Errorf(
			"%w: mobile SDK release %q/%q not found",
			ErrMobileSdkReleaseNotFound,
			req.Platform,
			req.ReleaseVersion,
		)
	}

	url := "https://d1mh8l8x6wqrj9.cloudfront.net/waf-mobile-sdk/" +
		req.Platform + "/" + req.ReleaseVersion + "/aws-waf-mobile-sdk.zip?X-Amz-Signature=simulated"

	return json.Marshal(map[string]any{
		"Url": url,
	})
}

// getManagedRuleSetRequest is the request body for GetManagedRuleSet.
type getManagedRuleSetRequest struct {
	ID    string `json:"Id"`
	Name  string `json:"Name"`
	Scope string `json:"Scope"`
}

// handleGetManagedRuleSet returns the stored managed rule set.
func (h *Handler) handleGetManagedRuleSet(ctx context.Context, body []byte) ([]byte, error) {
	var req getManagedRuleSetRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ID == "" {
		return nil, fmt.Errorf("%w: Id is required", errInvalidRequest)
	}

	ms, err := h.Backend.GetManagedRuleSet(ctx, req.ID)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{
		"ManagedRuleSet": map[string]any{
			"Id":                 ms.ID,
			keyName:              ms.Name,
			keyARN:               ms.ARN,
			keyLockToken:         ms.LockToken,
			"PublishedVersions":  ms.PublishedVersions,
			"RecommendedVersion": ms.RecommendedVersion,
		},
		keyLockToken: ms.LockToken,
	})
}

// getMobileSdkReleaseRequest is the request body for GetMobileSdkRelease.
type getMobileSdkReleaseRequest struct {
	Platform       string `json:"Platform"`
	ReleaseVersion string `json:"ReleaseVersion"`
}

// handleGetMobileSdkRelease returns the mobile SDK release from the catalog.
func (h *Handler) handleGetMobileSdkRelease(body []byte) ([]byte, error) {
	var req getMobileSdkReleaseRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.Platform == "" {
		return nil, fmt.Errorf("%w: Platform is required", errInvalidRequest)
	}

	if req.ReleaseVersion == "" {
		return nil, fmt.Errorf("%w: ReleaseVersion is required", errInvalidRequest)
	}

	release := getMobileSdkRelease(req.Platform, req.ReleaseVersion)
	if release == nil {
		return nil, fmt.Errorf(
			"%w: mobile SDK release %q/%q not found",
			ErrMobileSdkReleaseNotFound,
			req.Platform,
			req.ReleaseVersion,
		)
	}

	return json.Marshal(map[string]any{
		"MobileSdkRelease": map[string]any{
			"ReleaseVersion": release.ReleaseVersion,
			"Timestamp":      release.Timestamp,
			"ReleaseNotes":   release.ReleaseNotes,
			"Tags":           []any{},
		},
	})
}

// getRateBasedStatementManagedKeysRequest is the request body for GetRateBasedStatementManagedKeys.
type getRateBasedStatementManagedKeysRequest struct {
	Scope             string `json:"Scope"`
	WebACLName        string `json:"WebACLName"`
	WebACLId          string `json:"WebACLId"`
	RuleGroupRuleName string `json:"RuleGroupRuleName"`
	RuleName          string `json:"RuleName"`
}

// handleGetRateBasedStatementManagedKeys returns empty rate-based managed keys.
func (h *Handler) handleGetRateBasedStatementManagedKeys(ctx context.Context, body []byte) ([]byte, error) {
	var req getRateBasedStatementManagedKeysRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.Scope == "" {
		return nil, fmt.Errorf("%w: Scope is required", errInvalidRequest)
	}

	if req.WebACLName == "" {
		return nil, fmt.Errorf("%w: WebACLName is required", errInvalidRequest)
	}

	if req.WebACLId == "" {
		return nil, fmt.Errorf("%w: WebACLId is required", errInvalidRequest)
	}

	if req.RuleName == "" {
		return nil, fmt.Errorf("%w: RuleName is required", errInvalidRequest)
	}

	if _, err := h.Backend.GetWebACL(ctx, req.WebACLId); err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{
		"ManagedKeysIPV4": map[string]any{keyIPAddressVersion: "IPV4", keyAddresses: []any{}},
		"ManagedKeysIPV6": map[string]any{keyIPAddressVersion: "IPV6", keyAddresses: []any{}},
	})
}

// getSampledRequestsRequest is the request body for GetSampledRequests.
type getSampledRequestsRequest struct {
	TimeWindow     map[string]any `json:"TimeWindow"`
	WebACLArn      string         `json:"WebAclArn"`
	RuleMetricName string         `json:"RuleMetricName"`
	Scope          string         `json:"Scope"`
	MaxItems       int64          `json:"MaxItems"`
}

// handleGetSampledRequests returns an empty sampled requests response.
func (h *Handler) handleGetSampledRequests(ctx context.Context, body []byte) ([]byte, error) {
	var req getSampledRequestsRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.Scope == "" {
		return nil, fmt.Errorf("%w: Scope is required", errInvalidRequest)
	}

	if req.WebACLArn == "" {
		return nil, fmt.Errorf("%w: WebAclArn is required", errInvalidRequest)
	}

	if req.RuleMetricName == "" {
		return nil, fmt.Errorf("%w: RuleMetricName is required", errInvalidRequest)
	}

	if req.MaxItems < 1 || req.MaxItems > maxSampledRequestsItems {
		return nil, fmt.Errorf("%w: MaxItems must be between 1 and %d", errInvalidRequest, maxSampledRequestsItems)
	}

	if req.TimeWindow == nil {
		return nil, fmt.Errorf("%w: TimeWindow is required", errInvalidRequest)
	}

	// Extract WebACL ID from the ARN (last path segment).
	arnParts := strings.Split(req.WebACLArn, "/")
	webACLID := arnParts[len(arnParts)-1]

	if _, err := h.Backend.GetWebACL(ctx, webACLID); err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{
		"SampledRequests": []any{},
		"PopulationSize":  int64(0),
		"TimeWindow":      req.TimeWindow,
	})
}

// getTopPathStatisticsByTrafficRequest is the request body for GetTopPathStatisticsByTraffic.
type getTopPathStatisticsByTrafficRequest struct {
	TimeWindow map[string]any `json:"TimeWindow"`
	Scope      string         `json:"Scope"`
	WebACLName string         `json:"WebACLName"`
	WebACLId   string         `json:"WebACLId"`
}

// handleGetTopPathStatisticsByTraffic returns empty top path statistics.
func (h *Handler) handleGetTopPathStatisticsByTraffic(ctx context.Context, body []byte) ([]byte, error) {
	var req getTopPathStatisticsByTrafficRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.Scope == "" {
		return nil, fmt.Errorf("%w: Scope is required", errInvalidRequest)
	}

	if req.WebACLName == "" {
		return nil, fmt.Errorf("%w: WebACLName is required", errInvalidRequest)
	}

	if req.WebACLId == "" {
		return nil, fmt.Errorf("%w: WebACLId is required", errInvalidRequest)
	}

	if req.TimeWindow == nil {
		return nil, fmt.Errorf("%w: TimeWindow is required", errInvalidRequest)
	}

	if _, err := h.Backend.GetWebACL(ctx, req.WebACLId); err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{"UrlStatistics": []any{}})
}

// listAvailableManagedRuleGroupVersionsRequest is the request body for ListAvailableManagedRuleGroupVersions.
type listAvailableManagedRuleGroupVersionsRequest struct {
	Scope      string `json:"Scope"`
	VendorName string `json:"VendorName"`
	Name       string `json:"Name"`
	NextMarker string `json:"NextMarker"`
	Limit      int    `json:"Limit"`
}

// handleListAvailableManagedRuleGroupVersions returns versions for managed rule groups that support versioning.
func (h *Handler) handleListAvailableManagedRuleGroupVersions(body []byte) ([]byte, error) {
	var req listAvailableManagedRuleGroupVersionsRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	// Look for versioning support in catalog.
	for _, mrg := range getManagedRuleGroups() {
		if mrg.VendorName == req.VendorName && mrg.Name == req.Name && mrg.VersioningSupported {
			return json.Marshal(map[string]any{
				"Versions": []map[string]any{
					{"Name": "Version_1.0", "LastUpdateTimestamp": nil},
				},
				"CurrentDefaultVersion": "Version_1.0",
			})
		}
	}

	return json.Marshal(map[string]any{"Versions": []any{}, "CurrentDefaultVersion": ""})
}

// listAvailableManagedRuleGroupsRequest is the request body for ListAvailableManagedRuleGroups.
type listAvailableManagedRuleGroupsRequest struct {
	Scope      string `json:"Scope"`
	NextMarker string `json:"NextMarker"`
	Limit      int    `json:"Limit"`
}

// handleListAvailableManagedRuleGroups returns the catalog of managed rule groups.
func (h *Handler) handleListAvailableManagedRuleGroups(body []byte) ([]byte, error) {
	var req listAvailableManagedRuleGroupsRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	groups := make([]map[string]any, 0, len(getManagedRuleGroups()))

	for _, mrg := range getManagedRuleGroups() {
		groups = append(groups, map[string]any{
			keyVendorName:         mrg.VendorName,
			keyName:               mrg.Name,
			keyDescription:        mrg.Description,
			"VersioningSupported": mrg.VersioningSupported,
		})
	}

	return json.Marshal(map[string]any{"ManagedRuleGroups": groups})
}

// handleListLoggingConfigurations lists all logging configurations.
func (h *Handler) handleListLoggingConfigurations(ctx context.Context, _ []byte) ([]byte, error) {
	configs := h.Backend.ListLoggingConfigurations(ctx)
	items := make([]any, 0, len(configs))

	for _, cfg := range configs {
		var v any
		if err := json.Unmarshal(cfg, &v); err == nil {
			items = append(items, v)
		}
	}

	return json.Marshal(map[string]any{"LoggingConfigurations": items})
}

// listManagedRuleSetsRequest is the request body for ListManagedRuleSets.
type listManagedRuleSetsRequest struct {
	Scope      string `json:"Scope"`
	NextMarker string `json:"NextMarker"`
	Limit      int    `json:"Limit"`
}

// handleListManagedRuleSets lists all stored managed rule sets, filtered by scope.
func (h *Handler) handleListManagedRuleSets(ctx context.Context, body []byte) ([]byte, error) {
	var req listManagedRuleSetsRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	sets := h.Backend.ListManagedRuleSets(ctx, req.Scope)

	items, nextMarker := paginateByName(
		sets,
		func(ms *ManagedRuleSet) string { return ms.Name },
		req.NextMarker,
		req.Limit,
	)

	summaries := make([]map[string]any, 0, len(items))

	for _, ms := range items {
		summaries = append(summaries, map[string]any{
			"Id":         ms.ID,
			keyName:      ms.Name,
			keyARN:       ms.ARN,
			keyLockToken: ms.LockToken,
		})
	}

	resp := map[string]any{"ManagedRuleSets": summaries}
	if nextMarker != "" {
		resp["NextMarker"] = nextMarker
	}

	return json.Marshal(resp)
}

// listMobileSdkReleasesRequest is the request body for ListMobileSdkReleases.
type listMobileSdkReleasesRequest struct {
	Platform   string `json:"Platform"`
	Scope      string `json:"Scope"`
	NextMarker string `json:"NextMarker"`
	Limit      int    `json:"Limit"`
}

// handleListMobileSdkReleases lists mobile SDK releases from the catalog.
func (h *Handler) handleListMobileSdkReleases(body []byte) ([]byte, error) {
	var req listMobileSdkReleasesRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	releases := getMobileSdkReleases(req.Platform)

	summaries := make([]map[string]any, 0, len(releases))

	for _, r := range releases {
		summaries = append(summaries, map[string]any{
			"ReleaseVersion": r.ReleaseVersion,
			"Timestamp":      r.Timestamp,
		})
	}

	return json.Marshal(map[string]any{"ReleaseSummaries": summaries})
}

// putManagedRuleSetVersionsRequest is the request body for PutManagedRuleSetVersions.
type putManagedRuleSetVersionsRequest struct {
	VersionsToPublish  map[string]any `json:"VersionsToPublish"`
	ID                 string         `json:"Id"`
	Name               string         `json:"Name"`
	Scope              string         `json:"Scope"`
	LockToken          string         `json:"LockToken"`
	RecommendedVersion string         `json:"RecommendedVersion"`
}

func (h *Handler) handlePutManagedRuleSetVersions(ctx context.Context, body []byte) ([]byte, error) {
	var req putManagedRuleSetVersionsRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ID == "" {
		return nil, fmt.Errorf("%w: Id is required", errInvalidRequest)
	}

	ms, err := h.Backend.PutManagedRuleSetVersions(
		ctx,
		req.ID,
		req.Name,
		req.Scope,
		req.LockToken,
		req.RecommendedVersion,
		req.VersionsToPublish,
	)
	if err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "wafv2: put managed rule set versions", "id", req.ID)

	return json.Marshal(map[string]string{keyNextLockToken: ms.LockToken})
}

// updateManagedRuleSetVersionExpiryDateRequest is the request body for UpdateManagedRuleSetVersionExpiryDate.
type updateManagedRuleSetVersionExpiryDateRequest struct {
	ExpiryTimestamp *int64 `json:"ExpiryTimestamp"`
	ID              string `json:"Id"`
	Name            string `json:"Name"`
	Scope           string `json:"Scope"`
	LockToken       string `json:"LockToken"`
	VersionToExpire string `json:"VersionToExpire"`
}

func (h *Handler) handleUpdateManagedRuleSetVersionExpiryDate(ctx context.Context, body []byte) ([]byte, error) {
	var req updateManagedRuleSetVersionExpiryDateRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ID == "" {
		return nil, fmt.Errorf("%w: Id is required", errInvalidRequest)
	}

	ms, err := h.Backend.UpdateManagedRuleSetVersionExpiryDate(
		ctx,
		req.ID,
		req.LockToken,
		req.VersionToExpire,
		req.ExpiryTimestamp,
	)
	if err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "wafv2: updated managed rule set version expiry date", "id", req.ID)

	return json.Marshal(map[string]any{
		keyNextLockToken:  ms.LockToken,
		"ExpiringVersion": req.VersionToExpire,
		"ExpiryTimestamp": req.ExpiryTimestamp,
	})
}
