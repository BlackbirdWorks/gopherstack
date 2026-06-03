package waf

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	wafTargetPrefix = "AWSWAF_20150824."
	wafContentType  = "application/x-amz-json-1.1"

	keyChangeToken = "ChangeToken"
	keyRule        = "Rule"
)

// Handler serves WAF Classic JSON operations.
type Handler struct {
	Backend StorageBackend
	ops     map[string]func([]byte) (any, error)
}

// NewHandler creates a WAF Classic handler backed by b.
func NewHandler(b StorageBackend) *Handler {
	h := &Handler{Backend: b}
	h.ops = h.buildOps()

	return h
}

// Name returns the service name.
func (h *Handler) Name() string { return "WAF" }

// Reset clears backend state.
func (h *Handler) Reset() { h.Backend.Reset() }

// MatchPriority returns header matching priority.
func (h *Handler) MatchPriority() int { return service.PriorityHeaderExact }

// RouteMatcher matches WAF Classic X-Amz-Target headers.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		return strings.HasPrefix(c.Request().Header.Get("X-Amz-Target"), wafTargetPrefix)
	}
}

// ExtractOperation extracts the WAF Classic action from the X-Amz-Target header.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	target := c.Request().Header.Get("X-Amz-Target")
	if !strings.HasPrefix(target, wafTargetPrefix) {
		return "Unknown"
	}

	return strings.TrimPrefix(target, wafTargetPrefix)
}

// ExtractResource extracts a resource identifier from the JSON body.
func (h *Handler) ExtractResource(_ *echo.Context) string {
	return ""
}

// GetSupportedOperations returns all implemented operation names.
func (h *Handler) GetSupportedOperations() []string {
	ops := make([]string, 0, len(h.ops))
	for name := range h.ops {
		ops = append(ops, name)
	}

	return ops
}

// Snapshot returns a serialized snapshot of the backend state.
func (h *Handler) Snapshot() []byte { return h.Backend.Snapshot() }

// Restore restores the backend state from a snapshot.
func (h *Handler) Restore(data []byte) error { return h.Backend.Restore(data) }

// Handler returns the Echo handler function.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		return service.HandleTarget(
			c, logger.Load(c.Request().Context()), h.Name(), wafContentType,
			h.GetSupportedOperations(), h.dispatch, h.handleError,
		)
	}
}

func (h *Handler) dispatch(_ context.Context, action string, body []byte) ([]byte, error) {
	fn, ok := h.ops[action]
	if !ok {
		return nil, ErrInvalidParameter
	}

	result, err := fn(body)
	if err != nil {
		return nil, err
	}

	return json.Marshal(result)
}

func (h *Handler) handleError(_ context.Context, c *echo.Context, _ string, err error) error {
	code := "WAFInternalErrorException"
	status := http.StatusInternalServerError

	switch {
	case errors.Is(err, ErrNotFound):
		code, status = errResourceNotFound, http.StatusBadRequest
	case errors.Is(err, ErrStaleToken):
		code, status = errStaleData, http.StatusBadRequest
	case errors.Is(err, ErrInvalidParameter):
		code, status = errInvalidParameter, http.StatusBadRequest
	case errors.Is(err, ErrReferencedItem):
		code, status = errReferencedItem, http.StatusBadRequest
	}

	return c.JSON(status, service.JSONErrorResponse{Type: code, Message: err.Error()})
}

func (h *Handler) buildOps() map[string]func([]byte) (any, error) {
	return map[string]func([]byte) (any, error){
		"GetChangeToken":       h.opGetChangeToken,
		"GetChangeTokenStatus": h.opGetChangeTokenStatus,

		"CreateWebACL": h.opCreateWebACL,
		"GetWebACL":    h.opGetWebACL,
		"UpdateWebACL": h.opUpdateWebACL,
		"DeleteWebACL": h.opDeleteWebACL,
		"ListWebACLs":  h.opListWebACLs,

		"CreateRule": h.opCreateRule,
		"GetRule":    h.opGetRule,
		"UpdateRule": h.opUpdateRule,
		"DeleteRule": h.opDeleteRule,
		"ListRules":  h.opListRules,

		"CreateIPSet": h.opCreateIPSet,
		"GetIPSet":    h.opGetIPSet,
		"UpdateIPSet": h.opUpdateIPSet,
		"DeleteIPSet": h.opDeleteIPSet,
		"ListIPSets":  h.opListIPSets,

		"CreateByteMatchSet": h.opCreateByteMatchSet,
		"GetByteMatchSet":    h.opGetByteMatchSet,
		"UpdateByteMatchSet": h.opUpdateByteMatchSet,
		"DeleteByteMatchSet": h.opDeleteByteMatchSet,
		"ListByteMatchSets":  h.opListByteMatchSets,

		"CreateSizeConstraintSet": h.opCreateSizeConstraintSet,
		"GetSizeConstraintSet":    h.opGetSizeConstraintSet,
		"UpdateSizeConstraintSet": h.opUpdateSizeConstraintSet,
		"DeleteSizeConstraintSet": h.opDeleteSizeConstraintSet,
		"ListSizeConstraintSets":  h.opListSizeConstraintSets,

		"CreateSqlInjectionMatchSet": h.opCreateSqlInjectionMatchSet,
		"GetSqlInjectionMatchSet":    h.opGetSqlInjectionMatchSet,
		"UpdateSqlInjectionMatchSet": h.opUpdateSqlInjectionMatchSet,
		"DeleteSqlInjectionMatchSet": h.opDeleteSqlInjectionMatchSet,
		"ListSqlInjectionMatchSets":  h.opListSqlInjectionMatchSets,

		"CreateXssMatchSet": h.opCreateXssMatchSet,
		"GetXssMatchSet":    h.opGetXssMatchSet,
		"UpdateXssMatchSet": h.opUpdateXssMatchSet,
		"DeleteXssMatchSet": h.opDeleteXssMatchSet,
		"ListXssMatchSets":  h.opListXssMatchSets,

		"CreateGeoMatchSet": h.opCreateGeoMatchSet,
		"GetGeoMatchSet":    h.opGetGeoMatchSet,
		"UpdateGeoMatchSet": h.opUpdateGeoMatchSet,
		"DeleteGeoMatchSet": h.opDeleteGeoMatchSet,
		"ListGeoMatchSets":  h.opListGeoMatchSets,

		"CreateRateBasedRule":         h.opCreateRateBasedRule,
		"GetRateBasedRule":            h.opGetRateBasedRule,
		"UpdateRateBasedRule":         h.opUpdateRateBasedRule,
		"DeleteRateBasedRule":         h.opDeleteRateBasedRule,
		"ListRateBasedRules":          h.opListRateBasedRules,
		"GetRateBasedRuleManagedKeys": h.opGetRateBasedRuleManagedKeys,

		"CreateRegexPatternSet": h.opCreateRegexPatternSet,
		"GetRegexPatternSet":    h.opGetRegexPatternSet,
		"UpdateRegexPatternSet": h.opUpdateRegexPatternSet,
		"DeleteRegexPatternSet": h.opDeleteRegexPatternSet,
		"ListRegexPatternSets":  h.opListRegexPatternSets,

		"CreateRegexMatchSet": h.opCreateRegexMatchSet,
		"GetRegexMatchSet":    h.opGetRegexMatchSet,
		"UpdateRegexMatchSet": h.opUpdateRegexMatchSet,
		"DeleteRegexMatchSet": h.opDeleteRegexMatchSet,
		"ListRegexMatchSets":  h.opListRegexMatchSets,

		"CreateRuleGroup":               h.opCreateRuleGroup,
		"GetRuleGroup":                  h.opGetRuleGroup,
		"UpdateRuleGroup":               h.opUpdateRuleGroup,
		"DeleteRuleGroup":               h.opDeleteRuleGroup,
		"ListRuleGroups":                h.opListRuleGroups,
		"ListActivatedRulesInRuleGroup": h.opListActivatedRulesInRuleGroup,
		"ListSubscribedRuleGroups":      h.opListSubscribedRuleGroups,

		"PutLoggingConfiguration":    h.opPutLoggingConfiguration,
		"GetLoggingConfiguration":    h.opGetLoggingConfiguration,
		"DeleteLoggingConfiguration": h.opDeleteLoggingConfiguration,
		"ListLoggingConfigurations":  h.opListLoggingConfigurations,

		"PutPermissionPolicy":    h.opPutPermissionPolicy,
		"GetPermissionPolicy":    h.opGetPermissionPolicy,
		"DeletePermissionPolicy": h.opDeletePermissionPolicy,

		"CreateWebACLMigrationStack": h.opCreateWebACLMigrationStack,

		"TagResource":         h.opTagResource,
		"UntagResource":       h.opUntagResource,
		"ListTagsForResource": h.opListTagsForResource,

		"GetSampledRequests": h.opGetSampledRequests,
	}
}

// --- change token ---

func (h *Handler) opGetChangeToken(_ []byte) (any, error) {
	return map[string]any{
		keyChangeToken: h.Backend.GetChangeToken(),
	}, nil
}

func (h *Handler) opGetChangeTokenStatus(body []byte) (any, error) {
	var in struct {
		ChangeToken string `json:"ChangeToken"`
	}

	if err := unmarshal(body, &in); err != nil {
		return nil, err
	}

	return map[string]any{
		"ChangeTokenStatus": h.Backend.GetChangeTokenStatus(in.ChangeToken),
	}, nil
}

// --- WebACL ---

func (h *Handler) opCreateWebACL(body []byte) (any, error) {
	var in struct {
		ChangeToken   string    `json:"ChangeToken"`
		Name          string    `json:"Name"`
		MetricName    string    `json:"MetricName"`
		DefaultAction WafAction `json:"DefaultAction"`
		Tags          []Tag     `json:"Tags"`
	}

	if err := unmarshal(body, &in); err != nil {
		return nil, err
	}

	acl, err := h.Backend.CreateWebACL(in.Name, in.MetricName, in.DefaultAction, tagsToMap(in.Tags))
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"WebACL":       acl,
		keyChangeToken: in.ChangeToken,
	}, nil
}

func (h *Handler) opGetWebACL(body []byte) (any, error) {
	var in struct {
		WebACLId string `json:"WebACLId"`
	}

	if err := unmarshal(body, &in); err != nil {
		return nil, err
	}

	acl, err := h.Backend.GetWebACL(in.WebACLId)
	if err != nil {
		return nil, err
	}

	return map[string]any{"WebACL": acl}, nil
}

func (h *Handler) opUpdateWebACL(body []byte) (any, error) {
	var in struct {
		ChangeToken   string         `json:"ChangeToken"`
		WebACLId      string         `json:"WebACLId"`
		DefaultAction *WafAction     `json:"DefaultAction"`
		Updates       []WebACLUpdate `json:"Updates"`
	}

	if err := unmarshal(body, &in); err != nil {
		return nil, err
	}

	if err := h.Backend.UpdateWebACL(in.WebACLId, in.ChangeToken, in.DefaultAction, in.Updates); err != nil {
		return nil, err
	}

	return map[string]any{keyChangeToken: in.ChangeToken}, nil
}

func (h *Handler) opDeleteWebACL(body []byte) (any, error) {
	var in struct {
		ChangeToken string `json:"ChangeToken"`
		WebACLId    string `json:"WebACLId"`
	}

	if err := unmarshal(body, &in); err != nil {
		return nil, err
	}

	if err := h.Backend.DeleteWebACL(in.WebACLId, in.ChangeToken); err != nil {
		return nil, err
	}

	return map[string]any{keyChangeToken: in.ChangeToken}, nil
}

func (h *Handler) opListWebACLs(body []byte) (any, error) {
	var in struct {
		NextMarker string `json:"NextMarker"`
		Limit      int    `json:"Limit"`
	}

	_ = unmarshal(body, &in)

	return map[string]any{"WebACLs": h.Backend.ListWebACLs()}, nil
}

// --- Rule ---

func (h *Handler) opCreateRule(body []byte) (any, error) {
	var in struct {
		ChangeToken string `json:"ChangeToken"`
		Name        string `json:"Name"`
		MetricName  string `json:"MetricName"`
		Tags        []Tag  `json:"Tags"`
	}

	if err := unmarshal(body, &in); err != nil {
		return nil, err
	}

	rule, err := h.Backend.CreateRule(in.Name, in.MetricName, in.ChangeToken, tagsToMap(in.Tags))
	if err != nil {
		return nil, err
	}

	return map[string]any{
		keyRule:        rule,
		keyChangeToken: in.ChangeToken,
	}, nil
}

func (h *Handler) opGetRule(body []byte) (any, error) {
	var in struct {
		RuleId string `json:"RuleId"` //nolint:revive,staticcheck // AWS SDK field name
	}

	if err := unmarshal(body, &in); err != nil {
		return nil, err
	}

	rule, err := h.Backend.GetRule(in.RuleId)
	if err != nil {
		return nil, err
	}

	return map[string]any{keyRule: rule}, nil
}

func (h *Handler) opUpdateRule(body []byte) (any, error) {
	var in struct {
		ChangeToken string       `json:"ChangeToken"`
		RuleId      string       `json:"RuleId"` //nolint:revive,staticcheck // AWS SDK field name
		Updates     []RuleUpdate `json:"Updates"`
	}

	if err := unmarshal(body, &in); err != nil {
		return nil, err
	}

	if err := h.Backend.UpdateRule(in.RuleId, in.ChangeToken, in.Updates); err != nil {
		return nil, err
	}

	return map[string]any{keyChangeToken: in.ChangeToken}, nil
}

func (h *Handler) opDeleteRule(body []byte) (any, error) {
	var in struct {
		ChangeToken string `json:"ChangeToken"`
		RuleId      string `json:"RuleId"` //nolint:revive,staticcheck // AWS SDK field name
	}

	if err := unmarshal(body, &in); err != nil {
		return nil, err
	}

	if err := h.Backend.DeleteRule(in.RuleId, in.ChangeToken); err != nil {
		return nil, err
	}

	return map[string]any{keyChangeToken: in.ChangeToken}, nil
}

func (h *Handler) opListRules(body []byte) (any, error) {
	var in struct {
		NextMarker string `json:"NextMarker"`
		Limit      int    `json:"Limit"`
	}

	_ = unmarshal(body, &in)

	return map[string]any{"Rules": h.Backend.ListRules()}, nil
}

// --- IPSet ---

func (h *Handler) opCreateIPSet(body []byte) (any, error) {
	var in struct {
		ChangeToken string `json:"ChangeToken"`
		Name        string `json:"Name"`
	}

	if err := unmarshal(body, &in); err != nil {
		return nil, err
	}

	ipSet, err := h.Backend.CreateIPSet(in.Name, in.ChangeToken, nil)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"IPSet":        ipSet,
		keyChangeToken: in.ChangeToken,
	}, nil
}

func (h *Handler) opGetIPSet(body []byte) (any, error) {
	var in struct {
		IPSetId string `json:"IPSetId"` //nolint:revive,staticcheck // AWS SDK field name
	}

	if err := unmarshal(body, &in); err != nil {
		return nil, err
	}

	ipSet, err := h.Backend.GetIPSet(in.IPSetId)
	if err != nil {
		return nil, err
	}

	return map[string]any{"IPSet": ipSet}, nil
}

func (h *Handler) opUpdateIPSet(body []byte) (any, error) {
	var in struct {
		ChangeToken string        `json:"ChangeToken"`
		IPSetId     string        `json:"IPSetId"` //nolint:revive,staticcheck // AWS SDK field name
		Updates     []IPSetUpdate `json:"Updates"`
	}

	if err := unmarshal(body, &in); err != nil {
		return nil, err
	}

	if err := h.Backend.UpdateIPSet(in.IPSetId, in.ChangeToken, in.Updates); err != nil {
		return nil, err
	}

	return map[string]any{keyChangeToken: in.ChangeToken}, nil
}

func (h *Handler) opDeleteIPSet(body []byte) (any, error) {
	var in struct {
		ChangeToken string `json:"ChangeToken"`
		IPSetId     string `json:"IPSetId"` //nolint:revive,staticcheck // AWS SDK field name
	}

	if err := unmarshal(body, &in); err != nil {
		return nil, err
	}

	if err := h.Backend.DeleteIPSet(in.IPSetId, in.ChangeToken); err != nil {
		return nil, err
	}

	return map[string]any{keyChangeToken: in.ChangeToken}, nil
}

func (h *Handler) opListIPSets(body []byte) (any, error) {
	var in struct {
		NextMarker string `json:"NextMarker"`
		Limit      int    `json:"Limit"`
	}

	_ = unmarshal(body, &in)

	return map[string]any{"IPSets": h.Backend.ListIPSets()}, nil
}

// --- ByteMatchSet ---

func (h *Handler) opCreateByteMatchSet(body []byte) (any, error) {
	var in struct {
		ChangeToken string `json:"ChangeToken"`
		Name        string `json:"Name"`
	}

	if err := unmarshal(body, &in); err != nil {
		return nil, err
	}

	bms, err := h.Backend.CreateByteMatchSet(in.Name, in.ChangeToken)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"ByteMatchSet": bms,
		keyChangeToken: in.ChangeToken,
	}, nil
}

func (h *Handler) opGetByteMatchSet(body []byte) (any, error) {
	var in struct {
		ByteMatchSetId string `json:"ByteMatchSetId"` //nolint:revive,staticcheck // AWS SDK field name
	}

	if err := unmarshal(body, &in); err != nil {
		return nil, err
	}

	bms, err := h.Backend.GetByteMatchSet(in.ByteMatchSetId)
	if err != nil {
		return nil, err
	}

	return map[string]any{"ByteMatchSet": bms}, nil
}

func (h *Handler) opUpdateByteMatchSet(body []byte) (any, error) {
	var in struct {
		ChangeToken    string               `json:"ChangeToken"`
		ByteMatchSetId string               `json:"ByteMatchSetId"` //nolint:revive,staticcheck // AWS SDK field name
		Updates        []ByteMatchSetUpdate `json:"Updates"`
	}

	if err := unmarshal(body, &in); err != nil {
		return nil, err
	}

	if err := h.Backend.UpdateByteMatchSet(in.ByteMatchSetId, in.ChangeToken, in.Updates); err != nil {
		return nil, err
	}

	return map[string]any{keyChangeToken: in.ChangeToken}, nil
}

func (h *Handler) opDeleteByteMatchSet(body []byte) (any, error) {
	var in struct {
		ChangeToken    string `json:"ChangeToken"`
		ByteMatchSetId string `json:"ByteMatchSetId"` //nolint:revive,staticcheck // AWS SDK field name
	}

	if err := unmarshal(body, &in); err != nil {
		return nil, err
	}

	if err := h.Backend.DeleteByteMatchSet(in.ByteMatchSetId, in.ChangeToken); err != nil {
		return nil, err
	}

	return map[string]any{keyChangeToken: in.ChangeToken}, nil
}

func (h *Handler) opListByteMatchSets(body []byte) (any, error) {
	var in struct {
		NextMarker string `json:"NextMarker"`
		Limit      int    `json:"Limit"`
	}

	_ = unmarshal(body, &in)

	return map[string]any{"ByteMatchSets": h.Backend.ListByteMatchSets()}, nil
}

// --- SizeConstraintSet ---

func (h *Handler) opCreateSizeConstraintSet(body []byte) (any, error) {
	var in struct {
		ChangeToken string `json:"ChangeToken"`
		Name        string `json:"Name"`
	}

	if err := unmarshal(body, &in); err != nil {
		return nil, err
	}

	scs, err := h.Backend.CreateSizeConstraintSet(in.Name, in.ChangeToken)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"SizeConstraintSet": scs,
		keyChangeToken:      in.ChangeToken,
	}, nil
}

func (h *Handler) opGetSizeConstraintSet(body []byte) (any, error) {
	var in struct {
		SizeConstraintSetId string `json:"SizeConstraintSetId"` //nolint:revive,staticcheck // AWS SDK naming
	}

	if err := unmarshal(body, &in); err != nil {
		return nil, err
	}

	scs, err := h.Backend.GetSizeConstraintSet(in.SizeConstraintSetId)
	if err != nil {
		return nil, err
	}

	return map[string]any{"SizeConstraintSet": scs}, nil
}

func (h *Handler) opUpdateSizeConstraintSet(body []byte) (any, error) {
	var in struct {
		ChangeToken string `json:"ChangeToken"`
		//nolint:revive,staticcheck // AWS SDK naming
		SizeConstraintSetId string                    `json:"SizeConstraintSetId"`
		Updates             []SizeConstraintSetUpdate `json:"Updates"`
	}

	if err := unmarshal(body, &in); err != nil {
		return nil, err
	}

	if err := h.Backend.UpdateSizeConstraintSet(in.SizeConstraintSetId, in.ChangeToken, in.Updates); err != nil {
		return nil, err
	}

	return map[string]any{keyChangeToken: in.ChangeToken}, nil
}

func (h *Handler) opDeleteSizeConstraintSet(body []byte) (any, error) {
	var in struct {
		ChangeToken         string `json:"ChangeToken"`
		SizeConstraintSetId string `json:"SizeConstraintSetId"` //nolint:revive,staticcheck // AWS SDK naming
	}

	if err := unmarshal(body, &in); err != nil {
		return nil, err
	}

	if err := h.Backend.DeleteSizeConstraintSet(in.SizeConstraintSetId, in.ChangeToken); err != nil {
		return nil, err
	}

	return map[string]any{keyChangeToken: in.ChangeToken}, nil
}

func (h *Handler) opListSizeConstraintSets(body []byte) (any, error) {
	var in struct {
		NextMarker string `json:"NextMarker"`
		Limit      int    `json:"Limit"`
	}

	_ = unmarshal(body, &in)

	return map[string]any{"SizeConstraintSets": h.Backend.ListSizeConstraintSets()}, nil
}

// --- SqlInjectionMatchSet ---

//nolint:revive,staticcheck // AWS SDK naming
func (h *Handler) opCreateSqlInjectionMatchSet(body []byte) (any, error) {
	var in struct {
		ChangeToken string `json:"ChangeToken"`
		Name        string `json:"Name"`
	}

	if err := unmarshal(body, &in); err != nil {
		return nil, err
	}

	sims, err := h.Backend.CreateSqlInjectionMatchSet(in.Name, in.ChangeToken)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"SqlInjectionMatchSet": sims,
		keyChangeToken:         in.ChangeToken,
	}, nil
}

//nolint:revive,staticcheck // AWS SDK naming
func (h *Handler) opGetSqlInjectionMatchSet(body []byte) (any, error) {
	var in struct {
		SqlInjectionMatchSetId string `json:"SqlInjectionMatchSetId"` //nolint:revive,staticcheck // AWS SDK naming
	}

	if err := unmarshal(body, &in); err != nil {
		return nil, err
	}

	sims, err := h.Backend.GetSqlInjectionMatchSet(in.SqlInjectionMatchSetId)
	if err != nil {
		return nil, err
	}

	return map[string]any{"SqlInjectionMatchSet": sims}, nil
}

//nolint:revive,staticcheck // AWS SDK naming
func (h *Handler) opUpdateSqlInjectionMatchSet(body []byte) (any, error) {
	var in struct {
		ChangeToken string `json:"ChangeToken"`
		//nolint:revive,staticcheck // AWS SDK naming
		SqlInjectionMatchSetId string                       `json:"SqlInjectionMatchSetId"`
		Updates                []SqlInjectionMatchSetUpdate `json:"Updates"`
	}

	if err := unmarshal(body, &in); err != nil {
		return nil, err
	}

	if err := h.Backend.UpdateSqlInjectionMatchSet(in.SqlInjectionMatchSetId, in.ChangeToken, in.Updates); err != nil {
		return nil, err
	}

	return map[string]any{keyChangeToken: in.ChangeToken}, nil
}

//nolint:revive,staticcheck // AWS SDK naming
func (h *Handler) opDeleteSqlInjectionMatchSet(body []byte) (any, error) {
	var in struct {
		ChangeToken            string `json:"ChangeToken"`
		SqlInjectionMatchSetId string `json:"SqlInjectionMatchSetId"` //nolint:revive,staticcheck // AWS SDK naming
	}

	if err := unmarshal(body, &in); err != nil {
		return nil, err
	}

	if err := h.Backend.DeleteSqlInjectionMatchSet(in.SqlInjectionMatchSetId, in.ChangeToken); err != nil {
		return nil, err
	}

	return map[string]any{keyChangeToken: in.ChangeToken}, nil
}

//nolint:revive,staticcheck // AWS SDK naming
func (h *Handler) opListSqlInjectionMatchSets(body []byte) (any, error) {
	var in struct {
		NextMarker string `json:"NextMarker"`
		Limit      int    `json:"Limit"`
	}

	_ = unmarshal(body, &in)

	return map[string]any{"SqlInjectionMatchSets": h.Backend.ListSqlInjectionMatchSets()}, nil
}

// --- XssMatchSet ---

//nolint:revive,staticcheck // AWS SDK naming
func (h *Handler) opCreateXssMatchSet(body []byte) (any, error) {
	var in struct {
		ChangeToken string `json:"ChangeToken"`
		Name        string `json:"Name"`
	}

	if err := unmarshal(body, &in); err != nil {
		return nil, err
	}

	xms, err := h.Backend.CreateXssMatchSet(in.Name, in.ChangeToken)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"XssMatchSet":  xms,
		keyChangeToken: in.ChangeToken,
	}, nil
}

//nolint:revive,staticcheck // AWS SDK naming
func (h *Handler) opGetXssMatchSet(body []byte) (any, error) {
	var in struct {
		XssMatchSetId string `json:"XssMatchSetId"` //nolint:revive,staticcheck // AWS SDK field name
	}

	if err := unmarshal(body, &in); err != nil {
		return nil, err
	}

	xms, err := h.Backend.GetXssMatchSet(in.XssMatchSetId)
	if err != nil {
		return nil, err
	}

	return map[string]any{"XssMatchSet": xms}, nil
}

//nolint:revive,staticcheck // AWS SDK naming
func (h *Handler) opUpdateXssMatchSet(body []byte) (any, error) {
	var in struct {
		ChangeToken   string              `json:"ChangeToken"`
		XssMatchSetId string              `json:"XssMatchSetId"` //nolint:revive,staticcheck // AWS SDK field name
		Updates       []XssMatchSetUpdate `json:"Updates"`
	}

	if err := unmarshal(body, &in); err != nil {
		return nil, err
	}

	if err := h.Backend.UpdateXssMatchSet(in.XssMatchSetId, in.ChangeToken, in.Updates); err != nil {
		return nil, err
	}

	return map[string]any{keyChangeToken: in.ChangeToken}, nil
}

//nolint:revive,staticcheck // AWS SDK naming
func (h *Handler) opDeleteXssMatchSet(body []byte) (any, error) {
	var in struct {
		ChangeToken   string `json:"ChangeToken"`
		XssMatchSetId string `json:"XssMatchSetId"` //nolint:revive,staticcheck // AWS SDK field name
	}

	if err := unmarshal(body, &in); err != nil {
		return nil, err
	}

	if err := h.Backend.DeleteXssMatchSet(in.XssMatchSetId, in.ChangeToken); err != nil {
		return nil, err
	}

	return map[string]any{keyChangeToken: in.ChangeToken}, nil
}

//nolint:revive,staticcheck // AWS SDK naming
func (h *Handler) opListXssMatchSets(body []byte) (any, error) {
	var in struct {
		NextMarker string `json:"NextMarker"`
		Limit      int    `json:"Limit"`
	}

	_ = unmarshal(body, &in)

	return map[string]any{"XssMatchSets": h.Backend.ListXssMatchSets()}, nil
}

// --- GeoMatchSet ---

func (h *Handler) opCreateGeoMatchSet(body []byte) (any, error) {
	var in struct {
		ChangeToken string `json:"ChangeToken"`
		Name        string `json:"Name"`
	}

	if err := unmarshal(body, &in); err != nil {
		return nil, err
	}

	gms, err := h.Backend.CreateGeoMatchSet(in.Name, in.ChangeToken)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"GeoMatchSet":  gms,
		keyChangeToken: in.ChangeToken,
	}, nil
}

func (h *Handler) opGetGeoMatchSet(body []byte) (any, error) {
	var in struct {
		GeoMatchSetId string `json:"GeoMatchSetId"` //nolint:revive,staticcheck // AWS SDK field name
	}

	if err := unmarshal(body, &in); err != nil {
		return nil, err
	}

	gms, err := h.Backend.GetGeoMatchSet(in.GeoMatchSetId)
	if err != nil {
		return nil, err
	}

	return map[string]any{"GeoMatchSet": gms}, nil
}

func (h *Handler) opUpdateGeoMatchSet(body []byte) (any, error) {
	var in struct {
		ChangeToken   string              `json:"ChangeToken"`
		GeoMatchSetId string              `json:"GeoMatchSetId"` //nolint:revive,staticcheck // AWS SDK field name
		Updates       []GeoMatchSetUpdate `json:"Updates"`
	}

	if err := unmarshal(body, &in); err != nil {
		return nil, err
	}

	if err := h.Backend.UpdateGeoMatchSet(in.GeoMatchSetId, in.ChangeToken, in.Updates); err != nil {
		return nil, err
	}

	return map[string]any{keyChangeToken: in.ChangeToken}, nil
}

func (h *Handler) opDeleteGeoMatchSet(body []byte) (any, error) {
	var in struct {
		ChangeToken   string `json:"ChangeToken"`
		GeoMatchSetId string `json:"GeoMatchSetId"` //nolint:revive,staticcheck // AWS SDK field name
	}

	if err := unmarshal(body, &in); err != nil {
		return nil, err
	}

	if err := h.Backend.DeleteGeoMatchSet(in.GeoMatchSetId, in.ChangeToken); err != nil {
		return nil, err
	}

	return map[string]any{keyChangeToken: in.ChangeToken}, nil
}

func (h *Handler) opListGeoMatchSets(body []byte) (any, error) {
	var in struct {
		NextMarker string `json:"NextMarker"`
		Limit      int    `json:"Limit"`
	}

	_ = unmarshal(body, &in)

	return map[string]any{"GeoMatchSets": h.Backend.ListGeoMatchSets()}, nil
}

// --- Tags ---

func (h *Handler) opTagResource(body []byte) (any, error) {
	var in struct {
		ResourceARN string `json:"ResourceARN"`
		Tags        []Tag  `json:"Tags"`
	}

	if err := unmarshal(body, &in); err != nil {
		return nil, err
	}

	if err := h.Backend.TagResource(in.ResourceARN, tagsToMap(in.Tags)); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

func (h *Handler) opUntagResource(body []byte) (any, error) {
	var in struct {
		ResourceARN string   `json:"ResourceARN"`
		TagKeys     []string `json:"TagKeys"`
	}

	if err := unmarshal(body, &in); err != nil {
		return nil, err
	}

	if err := h.Backend.UntagResource(in.ResourceARN, in.TagKeys); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

func (h *Handler) opListTagsForResource(body []byte) (any, error) {
	var in struct {
		ResourceARN string `json:"ResourceARN"`
		NextMarker  string `json:"NextMarker"`
		Limit       int    `json:"Limit"`
	}

	if err := unmarshal(body, &in); err != nil {
		return nil, err
	}

	tags, err := h.Backend.ListTagsForResource(in.ResourceARN)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"TagInfoForResource": map[string]any{
			"ResourceARN": in.ResourceARN,
			"TagList":     tags,
		},
	}, nil
}

// --- GetSampledRequests ---

func (h *Handler) opGetSampledRequests(body []byte) (any, error) {
	var in struct {
		WebAclId string `json:"WebAclId"` //nolint:revive,staticcheck // AWS SDK field name
		RuleId   string `json:"RuleId"`   //nolint:revive,staticcheck // AWS SDK field name
		MaxItems int64  `json:"MaxItems"`
	}

	if err := unmarshal(body, &in); err != nil {
		return nil, err
	}

	samples := h.Backend.GetSampledRequests(in.WebAclId, in.RuleId, in.MaxItems)

	return map[string]any{
		"SampledRequests": samples,
		"PopulationSize":  int64(len(samples)),
	}, nil
}

// --- RateBasedRule ---

func (h *Handler) opCreateRateBasedRule(body []byte) (any, error) {
	var in struct {
		ChangeToken string `json:"ChangeToken"`
		Name        string `json:"Name"`
		MetricName  string `json:"MetricName"`
		RateKey     string `json:"RateKey"`
		Tags        []Tag  `json:"Tags"`
		RateLimit   int64  `json:"RateLimit"`
	}

	if err := unmarshal(body, &in); err != nil {
		return nil, err
	}

	rule, err := h.Backend.CreateRateBasedRule(
		in.Name, in.MetricName, in.RateKey, in.RateLimit, in.ChangeToken, tagsToMap(in.Tags),
	)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		keyRule:        rule,
		keyChangeToken: in.ChangeToken,
	}, nil
}

func (h *Handler) opGetRateBasedRule(body []byte) (any, error) {
	var in struct {
		RuleId string `json:"RuleId"` //nolint:revive,staticcheck // AWS SDK field name
	}

	if err := unmarshal(body, &in); err != nil {
		return nil, err
	}

	rule, err := h.Backend.GetRateBasedRule(in.RuleId)
	if err != nil {
		return nil, err
	}

	return map[string]any{keyRule: rule}, nil
}

func (h *Handler) opUpdateRateBasedRule(body []byte) (any, error) {
	var in struct {
		ChangeToken string       `json:"ChangeToken"`
		RuleId      string       `json:"RuleId"` //nolint:revive,staticcheck // AWS SDK field name
		Updates     []RuleUpdate `json:"Updates"`
		RateLimit   int64        `json:"RateLimit"`
	}

	if err := unmarshal(body, &in); err != nil {
		return nil, err
	}

	if err := h.Backend.UpdateRateBasedRule(in.RuleId, in.ChangeToken, in.RateLimit, in.Updates); err != nil {
		return nil, err
	}

	return map[string]any{keyChangeToken: in.ChangeToken}, nil
}

func (h *Handler) opDeleteRateBasedRule(body []byte) (any, error) {
	var in struct {
		ChangeToken string `json:"ChangeToken"`
		RuleId      string `json:"RuleId"` //nolint:revive,staticcheck // AWS SDK field name
	}

	if err := unmarshal(body, &in); err != nil {
		return nil, err
	}

	if err := h.Backend.DeleteRateBasedRule(in.RuleId, in.ChangeToken); err != nil {
		return nil, err
	}

	return map[string]any{keyChangeToken: in.ChangeToken}, nil
}

func (h *Handler) opListRateBasedRules(body []byte) (any, error) {
	var in struct {
		NextMarker string `json:"NextMarker"`
		Limit      int    `json:"Limit"`
	}

	_ = unmarshal(body, &in)

	return map[string]any{"Rules": h.Backend.ListRateBasedRules()}, nil
}

func (h *Handler) opGetRateBasedRuleManagedKeys(body []byte) (any, error) {
	var in struct {
		RuleId     string `json:"RuleId"` //nolint:revive,staticcheck // AWS SDK field name
		NextMarker string `json:"NextMarker"`
	}

	if err := unmarshal(body, &in); err != nil {
		return nil, err
	}

	keys, err := h.Backend.GetRateBasedRuleManagedKeys(in.RuleId)
	if err != nil {
		return nil, err
	}

	return map[string]any{"ManagedKeys": keys}, nil
}

// --- RegexPatternSet ---

func (h *Handler) opCreateRegexPatternSet(body []byte) (any, error) {
	var in struct {
		ChangeToken string `json:"ChangeToken"`
		Name        string `json:"Name"`
	}

	if err := unmarshal(body, &in); err != nil {
		return nil, err
	}

	rps, err := h.Backend.CreateRegexPatternSet(in.Name, in.ChangeToken)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"RegexPatternSet": rps,
		keyChangeToken:    in.ChangeToken,
	}, nil
}

func (h *Handler) opGetRegexPatternSet(body []byte) (any, error) {
	var in struct {
		RegexPatternSetId string `json:"RegexPatternSetId"` //nolint:revive,staticcheck // AWS SDK field name
	}

	if err := unmarshal(body, &in); err != nil {
		return nil, err
	}

	rps, err := h.Backend.GetRegexPatternSet(in.RegexPatternSetId)
	if err != nil {
		return nil, err
	}

	return map[string]any{"RegexPatternSet": rps}, nil
}

func (h *Handler) opUpdateRegexPatternSet(body []byte) (any, error) {
	var in struct {
		ChangeToken       string                  `json:"ChangeToken"`
		RegexPatternSetId string                  `json:"RegexPatternSetId"` //nolint:revive,staticcheck // AWS SDK field name
		Updates           []RegexPatternSetUpdate `json:"Updates"`
	}

	if err := unmarshal(body, &in); err != nil {
		return nil, err
	}

	if err := h.Backend.UpdateRegexPatternSet(in.RegexPatternSetId, in.ChangeToken, in.Updates); err != nil {
		return nil, err
	}

	return map[string]any{keyChangeToken: in.ChangeToken}, nil
}

func (h *Handler) opDeleteRegexPatternSet(body []byte) (any, error) {
	var in struct {
		ChangeToken       string `json:"ChangeToken"`
		RegexPatternSetId string `json:"RegexPatternSetId"` //nolint:revive,staticcheck // AWS SDK field name
	}

	if err := unmarshal(body, &in); err != nil {
		return nil, err
	}

	if err := h.Backend.DeleteRegexPatternSet(in.RegexPatternSetId, in.ChangeToken); err != nil {
		return nil, err
	}

	return map[string]any{keyChangeToken: in.ChangeToken}, nil
}

func (h *Handler) opListRegexPatternSets(body []byte) (any, error) {
	var in struct {
		NextMarker string `json:"NextMarker"`
		Limit      int    `json:"Limit"`
	}

	_ = unmarshal(body, &in)

	return map[string]any{"RegexPatternSets": h.Backend.ListRegexPatternSets()}, nil
}

// --- RegexMatchSet ---

func (h *Handler) opCreateRegexMatchSet(body []byte) (any, error) {
	var in struct {
		ChangeToken string `json:"ChangeToken"`
		Name        string `json:"Name"`
	}

	if err := unmarshal(body, &in); err != nil {
		return nil, err
	}

	rms, err := h.Backend.CreateRegexMatchSet(in.Name, in.ChangeToken)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"RegexMatchSet": rms,
		keyChangeToken:  in.ChangeToken,
	}, nil
}

func (h *Handler) opGetRegexMatchSet(body []byte) (any, error) {
	var in struct {
		RegexMatchSetId string `json:"RegexMatchSetId"` //nolint:revive,staticcheck // AWS SDK field name
	}

	if err := unmarshal(body, &in); err != nil {
		return nil, err
	}

	rms, err := h.Backend.GetRegexMatchSet(in.RegexMatchSetId)
	if err != nil {
		return nil, err
	}

	return map[string]any{"RegexMatchSet": rms}, nil
}

func (h *Handler) opUpdateRegexMatchSet(body []byte) (any, error) {
	var in struct {
		ChangeToken     string                `json:"ChangeToken"`
		RegexMatchSetId string                `json:"RegexMatchSetId"` //nolint:revive,staticcheck // AWS SDK field name
		Updates         []RegexMatchSetUpdate `json:"Updates"`
	}

	if err := unmarshal(body, &in); err != nil {
		return nil, err
	}

	if err := h.Backend.UpdateRegexMatchSet(in.RegexMatchSetId, in.ChangeToken, in.Updates); err != nil {
		return nil, err
	}

	return map[string]any{keyChangeToken: in.ChangeToken}, nil
}

func (h *Handler) opDeleteRegexMatchSet(body []byte) (any, error) {
	var in struct {
		ChangeToken     string `json:"ChangeToken"`
		RegexMatchSetId string `json:"RegexMatchSetId"` //nolint:revive,staticcheck // AWS SDK field name
	}

	if err := unmarshal(body, &in); err != nil {
		return nil, err
	}

	if err := h.Backend.DeleteRegexMatchSet(in.RegexMatchSetId, in.ChangeToken); err != nil {
		return nil, err
	}

	return map[string]any{keyChangeToken: in.ChangeToken}, nil
}

func (h *Handler) opListRegexMatchSets(body []byte) (any, error) {
	var in struct {
		NextMarker string `json:"NextMarker"`
		Limit      int    `json:"Limit"`
	}

	_ = unmarshal(body, &in)

	return map[string]any{"RegexMatchSets": h.Backend.ListRegexMatchSets()}, nil
}

// --- RuleGroup ---

func (h *Handler) opCreateRuleGroup(body []byte) (any, error) {
	var in struct {
		ChangeToken string `json:"ChangeToken"`
		Name        string `json:"Name"`
		MetricName  string `json:"MetricName"`
		Tags        []Tag  `json:"Tags"`
	}

	if err := unmarshal(body, &in); err != nil {
		return nil, err
	}

	rg, err := h.Backend.CreateRuleGroup(in.Name, in.MetricName, in.ChangeToken, tagsToMap(in.Tags))
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"RuleGroup":    rg,
		keyChangeToken: in.ChangeToken,
	}, nil
}

func (h *Handler) opGetRuleGroup(body []byte) (any, error) {
	var in struct {
		RuleGroupId string `json:"RuleGroupId"` //nolint:revive,staticcheck // AWS SDK field name
	}

	if err := unmarshal(body, &in); err != nil {
		return nil, err
	}

	rg, err := h.Backend.GetRuleGroup(in.RuleGroupId)
	if err != nil {
		return nil, err
	}

	return map[string]any{"RuleGroup": rg}, nil
}

func (h *Handler) opUpdateRuleGroup(body []byte) (any, error) {
	var in struct {
		ChangeToken string                `json:"ChangeToken"`
		RuleGroupId string                `json:"RuleGroupId"` //nolint:revive,staticcheck // AWS SDK field name
		Updates     []ActivatedRuleUpdate `json:"Updates"`
	}

	if err := unmarshal(body, &in); err != nil {
		return nil, err
	}

	if err := h.Backend.UpdateRuleGroup(in.RuleGroupId, in.ChangeToken, in.Updates); err != nil {
		return nil, err
	}

	return map[string]any{keyChangeToken: in.ChangeToken}, nil
}

func (h *Handler) opDeleteRuleGroup(body []byte) (any, error) {
	var in struct {
		ChangeToken string `json:"ChangeToken"`
		RuleGroupId string `json:"RuleGroupId"` //nolint:revive,staticcheck // AWS SDK field name
	}

	if err := unmarshal(body, &in); err != nil {
		return nil, err
	}

	if err := h.Backend.DeleteRuleGroup(in.RuleGroupId, in.ChangeToken); err != nil {
		return nil, err
	}

	return map[string]any{keyChangeToken: in.ChangeToken}, nil
}

func (h *Handler) opListRuleGroups(body []byte) (any, error) {
	var in struct {
		NextMarker string `json:"NextMarker"`
		Limit      int    `json:"Limit"`
	}

	_ = unmarshal(body, &in)

	return map[string]any{"RuleGroups": h.Backend.ListRuleGroups()}, nil
}

func (h *Handler) opListActivatedRulesInRuleGroup(body []byte) (any, error) {
	var in struct {
		RuleGroupId string `json:"RuleGroupId"` //nolint:revive,staticcheck // AWS SDK field name
		NextMarker  string `json:"NextMarker"`
		Limit       int    `json:"Limit"`
	}

	if err := unmarshal(body, &in); err != nil {
		return nil, err
	}

	rules, err := h.Backend.ListActivatedRulesInRuleGroup(in.RuleGroupId)
	if err != nil {
		return nil, err
	}

	return map[string]any{"ActivatedRules": rules}, nil
}

func (h *Handler) opListSubscribedRuleGroups(_ []byte) (any, error) {
	return map[string]any{"RuleGroups": h.Backend.ListSubscribedRuleGroups()}, nil
}

// --- Logging ---

func (h *Handler) opPutLoggingConfiguration(body []byte) (any, error) {
	var in struct {
		LoggingConfiguration LoggingConfiguration `json:"LoggingConfiguration"`
	}

	if err := unmarshal(body, &in); err != nil {
		return nil, err
	}

	cfg, err := h.Backend.PutLoggingConfiguration(in.LoggingConfiguration)
	if err != nil {
		return nil, err
	}

	return map[string]any{"LoggingConfiguration": cfg}, nil
}

func (h *Handler) opGetLoggingConfiguration(body []byte) (any, error) {
	var in struct {
		ResourceArn string `json:"ResourceArn"`
	}

	if err := unmarshal(body, &in); err != nil {
		return nil, err
	}

	cfg, err := h.Backend.GetLoggingConfiguration(in.ResourceArn)
	if err != nil {
		return nil, err
	}

	return map[string]any{"LoggingConfiguration": cfg}, nil
}

func (h *Handler) opDeleteLoggingConfiguration(body []byte) (any, error) {
	var in struct {
		ResourceArn string `json:"ResourceArn"`
	}

	if err := unmarshal(body, &in); err != nil {
		return nil, err
	}

	if err := h.Backend.DeleteLoggingConfiguration(in.ResourceArn); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

func (h *Handler) opListLoggingConfigurations(body []byte) (any, error) {
	var in struct {
		NextMarker string `json:"NextMarker"`
		Limit      int    `json:"Limit"`
	}

	_ = unmarshal(body, &in)

	return map[string]any{"LoggingConfigurations": h.Backend.ListLoggingConfigurations()}, nil
}

// --- PermissionPolicy ---

func (h *Handler) opPutPermissionPolicy(body []byte) (any, error) {
	var in struct {
		ResourceArn string `json:"ResourceArn"`
		Policy      string `json:"Policy"`
	}

	if err := unmarshal(body, &in); err != nil {
		return nil, err
	}

	if err := h.Backend.PutPermissionPolicy(in.ResourceArn, in.Policy); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

func (h *Handler) opGetPermissionPolicy(body []byte) (any, error) {
	var in struct {
		ResourceArn string `json:"ResourceArn"`
	}

	if err := unmarshal(body, &in); err != nil {
		return nil, err
	}

	policy, err := h.Backend.GetPermissionPolicy(in.ResourceArn)
	if err != nil {
		return nil, err
	}

	return map[string]any{"Policy": policy}, nil
}

func (h *Handler) opDeletePermissionPolicy(body []byte) (any, error) {
	var in struct {
		ResourceArn string `json:"ResourceArn"`
	}

	if err := unmarshal(body, &in); err != nil {
		return nil, err
	}

	if err := h.Backend.DeletePermissionPolicy(in.ResourceArn); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

// --- Migration ---

func (h *Handler) opCreateWebACLMigrationStack(body []byte) (any, error) {
	var in struct {
		WebACLId              string `json:"WebACLId"`
		S3BucketName          string `json:"S3BucketName"`
		IgnoreUnsupportedType bool   `json:"IgnoreUnsupportedType"`
	}

	if err := unmarshal(body, &in); err != nil {
		return nil, err
	}

	s3URL := "https://" + in.S3BucketName + ".s3.amazonaws.com/webacl/" + in.WebACLId + "/template.json"

	return map[string]any{"S3ObjectUrl": s3URL}, nil
}

// --- helpers ---

func unmarshal(body []byte, v any) error {
	if len(body) == 0 {
		return nil
	}

	return json.Unmarshal(body, v)
}

func tagsToMap(tags []Tag) map[string]string {
	if len(tags) == 0 {
		return nil
	}

	m := make(map[string]string, len(tags))
	for _, t := range tags {
		m[t.Key] = t.Value
	}

	return m
}
