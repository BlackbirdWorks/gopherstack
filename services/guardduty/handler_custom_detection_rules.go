package guardduty

import (
	"encoding/json/v2"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

const (
	opCreateCustomDetectionRuleAssociation = "CreateCustomDetectionRuleAssociation"
	opCreateCustomDetectionRuleOrgConfig   = "CreateCustomDetectionRuleOrgConfiguration"
	opDeleteCustomDetectionRuleAssociation = "DeleteCustomDetectionRuleAssociation"
	opDeleteCustomDetectionRuleOrgConfig   = "DeleteCustomDetectionRuleOrgConfiguration"
	opGetCustomDetectionRule               = "GetCustomDetectionRule"
	opGetCustomDetectionRuleAssociation    = "GetCustomDetectionRuleAssociation"
	opGetCustomDetectionRuleOrgConfig      = "GetCustomDetectionRuleOrgConfiguration"
	opListCustomDetectionRuleAssociations  = "ListCustomDetectionRuleAssociations"
	opListCustomDetectionRuleOrgConfigs    = "ListCustomDetectionRuleOrgConfigurations"
	opListCustomDetectionRules             = "ListCustomDetectionRules"
	opUpdateCustomDetectionRuleAssociation = "UpdateCustomDetectionRuleAssociation"
	opUpdateCustomDetectionRuleOrgConfig   = "UpdateCustomDetectionRuleOrgConfiguration"
	pathCustomDetectionRule                = "custom-detection-rule"
	segAssociation                         = "association"
	segOrgConfiguration                    = "org-configuration"
	segRule                                = "rule"
	customRuleItemParts                    = 3 // custom-detection-rule/{kind}/{id}
	customRuleAssociationItemParts         = 5 // custom-detection-rule/rule/{id}/association/{aid}
	maxCustomRuleAssociationIDLength       = 64
	keyRuleAssociation                     = "ruleAssociation"
	keyMode                                = "mode"
	keyRuleID                              = "ruleId"
	keyArn                                 = "arn"
	keyExpiresAt                           = "expiresAt"
	keyNextToken                           = "nextToken"
	keyDescription                         = "description"
	keySeverity                            = "severity"
	customRuleCollectionParts              = 2
	customRuleRuleIDIndex                  = 2
	customRuleAssociationIDIndex           = 4
	keyMaxResults                          = "maxResults"
)

func customDetectionRuleOps() []string {
	return []string{
		opCreateCustomDetectionRuleAssociation,
		opCreateCustomDetectionRuleOrgConfig,
		opDeleteCustomDetectionRuleAssociation,
		opDeleteCustomDetectionRuleOrgConfig,
		opGetCustomDetectionRule,
		opGetCustomDetectionRuleAssociation,
		opGetCustomDetectionRuleOrgConfig,
		opListCustomDetectionRuleAssociations,
		opListCustomDetectionRuleOrgConfigs,
		opListCustomDetectionRules,
		opUpdateCustomDetectionRuleAssociation,
		opUpdateCustomDetectionRuleOrgConfig,
	}
}

func isCustomDetectionRuleOp(op string) bool {
	return strings.Contains(op, "CustomDetectionRule")
}

type customRuleRoute struct{ get, put, post, del string }

func (r customRuleRoute) op(method string) string {
	switch method {
	case http.MethodGet:
		return r.get
	case http.MethodPut:
		return r.put
	case http.MethodPost:
		return r.post
	case http.MethodDelete:
		return r.del
	}

	return ""
}

//nolint:gochecknoglobals // route table, not mutable state
var customRuleRoutes = map[string]customRuleRoute{
	segAssociation: {
		post: opCreateCustomDetectionRuleAssociation,
		get:  opListCustomDetectionRuleAssociations,
	},
	segOrgConfiguration: {
		post: opCreateCustomDetectionRuleOrgConfig,
		get:  opListCustomDetectionRuleOrgConfigs,
	},
	segRule: {post: opListCustomDetectionRules},
}

//nolint:gochecknoglobals // route table, not mutable state
var customRuleItemRoutes = map[string]customRuleRoute{
	segOrgConfiguration: {
		get: opGetCustomDetectionRuleOrgConfig, put: opUpdateCustomDetectionRuleOrgConfig,
		del: opDeleteCustomDetectionRuleOrgConfig,
	},
	segRule: {get: opGetCustomDetectionRule},
}

//nolint:gochecknoglobals // route table, not mutable state
var customRuleAssociationItemRoute = customRuleRoute{
	get: opGetCustomDetectionRuleAssociation, put: opUpdateCustomDetectionRuleAssociation,
	del: opDeleteCustomDetectionRuleAssociation,
}

func parseCustomDetectionRulePath(method string, parts []string) (string, string) {
	switch len(parts) {
	case customRuleCollectionParts:
		return customRuleRoutes[parts[1]].op(method), ""
	case customRuleItemParts:
		return customRuleItemRoutes[parts[1]].op(method), parts[2]
	case customRuleAssociationItemParts:
		if parts[1] == segRule && parts[3] == segAssociation {
			return customRuleAssociationItemRoute.op(method), parts[2]
		}
	}

	return "", ""
}

func parseCustomRulePathOrUnknown(method string, parts []string) (string, string) {
	if op, res := parseCustomDetectionRulePath(method, parts); op != "" {
		return op, res
	}

	return opUnknown, ""
}

func validAssociationID(id string) bool {
	if id == "" || len(id) > maxCustomRuleAssociationIDLength {
		return false
	}

	for _, r := range id {
		isAlnum := (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9')
		if !isAlnum && r != '_' && r != '-' {
			return false
		}
	}

	return true
}

func customRulePathIDs(path string) (string, string) {
	parts := strings.Split(strings.TrimPrefix(path, "/"), "/")

	return unescapedPart(parts, customRuleRuleIDIndex), unescapedPart(parts, customRuleAssociationIDIndex)
}

func unescapedPart(parts []string, i int) string {
	if i >= len(parts) {
		return ""
	}

	v, err := url.PathUnescape(parts[i])
	if err != nil {
		return parts[i]
	}

	return v
}

func (h *Handler) dispatchCustomDetectionRuleOps(op, path, query string, body []byte) (any, int, bool, error) {
	if !isCustomDetectionRuleOp(op) {
		return nil, 0, false, nil
	}

	ruleID, associationID := customRulePathIDs(path)
	if ruleID != "" && (len(ruleID) > maxRuleIDLength || !validRuleID(ruleID)) ||
		associationID != "" && !validAssociationID(associationID) {
		return nil, http.StatusBadRequest, true, ErrValidation
	}

	q, err := url.ParseQuery(query)
	if err != nil {
		return nil, http.StatusBadRequest, true, ErrValidation
	}

	result, code, err := h.runCustomDetectionRuleOp(op, ruleID, associationID, q, body)

	return result, code, true, err
}

func (h *Handler) runCustomDetectionRuleOp(
	op, ruleID, associationID string, q url.Values, body []byte,
) (any, int, error) {
	switch op {
	case opGetCustomDetectionRule:
		return h.handleGetCustomDetectionRule(ruleID)
	case opListCustomDetectionRules:
		return h.handleListCustomDetectionRules(body)
	case opCreateCustomDetectionRuleAssociation, opGetCustomDetectionRuleAssociation,
		opListCustomDetectionRuleAssociations, opUpdateCustomDetectionRuleAssociation,
		opDeleteCustomDetectionRuleAssociation:
		return h.runCustomRuleAssociationOp(op, ruleID, associationID, q, body)
	}

	return h.runCustomRuleOrgConfigOp(op, ruleID, q, body)
}

func (h *Handler) runCustomRuleAssociationOp(
	op, ruleID, associationID string, q url.Values, body []byte,
) (any, int, error) {
	switch op {
	case opCreateCustomDetectionRuleAssociation:
		return h.handleCreateCustomRuleAssociation(body)
	case opGetCustomDetectionRuleAssociation:
		return h.handleGetCustomRuleAssociation(ruleID, associationID)
	case opListCustomDetectionRuleAssociations:
		return h.handleListCustomRuleAssociations(q)
	case opUpdateCustomDetectionRuleAssociation:
		return h.handleUpdateCustomRuleAssociation(ruleID, associationID, body)
	}

	return nil, http.StatusOK, h.Backend.DeleteCustomDetectionRuleAssociation(ruleID, associationID)
}

func (h *Handler) runCustomRuleOrgConfigOp(op, ruleID string, q url.Values, body []byte) (any, int, error) {
	switch op {
	case opCreateCustomDetectionRuleOrgConfig:
		return h.handleCreateCustomRuleOrgConfig(body)
	case opGetCustomDetectionRuleOrgConfig:
		return h.handleGetCustomRuleOrgConfig(ruleID, q.Get(keyMode))
	case opListCustomDetectionRuleOrgConfigs:
		return h.handleListCustomRuleOrgConfigs(q)
	case opUpdateCustomDetectionRuleOrgConfig:
		return h.handleUpdateCustomRuleOrgConfig(ruleID, body)
	}

	return nil, http.StatusOK, h.Backend.DeleteCustomDetectionRuleOrgConfiguration(ruleID, q.Get(keyMode))
}

func queryPage(q url.Values) (int32, string, error) {
	var maxResults int32

	if raw := q.Get(keyMaxResults); raw != "" {
		n, err := strconv.ParseInt(raw, 10, 32)
		if err != nil {
			return 0, "", ErrValidation
		}

		maxResults = int32(n)
		if maxResults < 1 {
			return 0, "", ErrValidation
		}
	}

	return maxResults, q.Get(keyNextToken), nil
}

func ruleSummaryJSON(b StorageBackend, r *CustomDetectionRule) map[string]any {
	m := map[string]any{
		keyArn:         b.CustomRuleARN(r.RuleID),
		keyCreatedAt:   awstime.Epoch(r.CreatedAt),
		"dataSource":   r.DataSource,
		keyDescription: r.Description,
		keyName:        r.Name,
		keyRuleID:      r.RuleID,
		"service":      r.Service,
		keySeverity:    r.Severity,
		"tactic":       r.Tactic,
		"technique":    r.Technique,
		keyUpdatedAt:   awstime.Epoch(r.UpdatedAt),
	}

	if r.Language != "" {
		m["language"] = r.Language
	}

	if r.Schema != "" {
		m["schema"] = r.Schema
	}

	return m
}

func (h *Handler) handleGetCustomDetectionRule(ruleID string) (any, int, error) {
	r, err := h.Backend.GetCustomDetectionRule(ruleID)
	if err != nil {
		return nil, http.StatusNotFound, err
	}

	m := ruleSummaryJSON(h.Backend, r)
	m["definition"] = map[string]any{"expression": r.Expression}

	return map[string]any{"rule": m}, http.StatusOK, nil
}

func (h *Handler) handleListCustomDetectionRules(body []byte) (any, int, error) {
	var req struct {
		MaxResults *int32 `json:"maxResults"`
		NextToken  string `json:"nextToken"`
		Filters    []struct {
			Name      string   `json:"name"`
			Condition string   `json:"condition"`
			Values    []string `json:"values"`
		} `json:"filters"`
	}

	if len(body) > 0 {
		if err := json.Unmarshal(body, &req, jsonx.V1()); err != nil {
			return nil, http.StatusBadRequest, ErrValidation
		}
	}

	filters := make([]DetectionRuleFilter, 0, len(req.Filters))
	for _, f := range req.Filters {
		filters = append(filters, DetectionRuleFilter{Name: f.Name, Condition: f.Condition, Values: f.Values})
	}

	var maxResults int32

	if req.MaxResults != nil {
		maxResults = *req.MaxResults
		if maxResults < 1 {
			return nil, http.StatusBadRequest, ErrValidation
		}
	}

	rules, next, err := h.Backend.ListCustomDetectionRules(filters, maxResults, req.NextToken)
	if err != nil {
		return nil, http.StatusBadRequest, err
	}

	out := make([]map[string]any, 0, len(rules))
	for _, r := range rules {
		out = append(out, ruleSummaryJSON(h.Backend, r))
	}

	resp := map[string]any{"rules": out}
	if next != "" {
		resp[keyNextToken] = next
	}

	return resp, http.StatusOK, nil
}

func (h *Handler) associationJSON(a *CustomDetectionRuleAssociation, detail bool) map[string]any {
	m := map[string]any{
		keyArn:          h.Backend.CustomRuleAssociationARN(a.RuleID, a.AssociationID),
		"associationId": a.AssociationID,
		keyCreatedAt:    awstime.Epoch(a.CreatedAt),
		keyMode:         a.Mode,
		keyRuleID:       a.RuleID,
		keyUpdatedAt:    awstime.Epoch(a.UpdatedAt),
	}

	if detail {
		m["accountId"] = a.AccountID
	}

	if a.ExpiresAt != nil {
		m[keyExpiresAt] = awstime.Epoch(*a.ExpiresAt)
	}

	return m
}

func (h *Handler) handleCreateCustomRuleAssociation(body []byte) (any, int, error) {
	var req struct {
		Tags        map[string]string `json:"tags"`
		Mode        string            `json:"mode"`
		RuleID      string            `json:"ruleId"`
		ClientToken string            `json:"clientToken"`
	}

	if err := json.Unmarshal(body, &req, jsonx.V1()); err != nil {
		return nil, http.StatusBadRequest, ErrValidation
	}

	if req.Mode == "" || req.RuleID == "" {
		return nil, http.StatusBadRequest, ErrValidation
	}

	token := req.ClientToken
	req.ClientToken = ""

	id, err := h.createOnce(
		opCreateCustomDetectionRuleAssociation, req.RuleID, token, req,
		func(id string) error {
			_, _, getErr := h.Backend.GetCustomDetectionRuleAssociation(req.RuleID, id)

			return getErr
		},
		func() (string, error) {
			a, createErr := h.Backend.CreateCustomDetectionRuleAssociation(req.RuleID, req.Mode, req.Tags)
			if createErr != nil {
				return "", createErr
			}

			return a.AssociationID, nil
		},
	)
	if err != nil {
		return nil, http.StatusBadRequest, err
	}

	a, _, err := h.Backend.GetCustomDetectionRuleAssociation(req.RuleID, id)
	if err != nil {
		return nil, http.StatusNotFound, err
	}

	return map[string]any{keyRuleAssociation: h.associationJSON(a, true)}, http.StatusOK, nil
}

func (h *Handler) handleGetCustomRuleAssociation(ruleID, associationID string) (any, int, error) {
	a, tags, err := h.Backend.GetCustomDetectionRuleAssociation(ruleID, associationID)
	if err != nil {
		return nil, http.StatusNotFound, err
	}

	return map[string]any{
		keyRuleAssociation: h.associationJSON(a, true),
		keyTags:            tagsOrEmpty(tags),
	}, http.StatusOK, nil
}

func (h *Handler) handleListCustomRuleAssociations(q url.Values) (any, int, error) {
	maxResults, token, err := queryPage(q)
	if err != nil {
		return nil, http.StatusBadRequest, err
	}

	items, next, err := h.Backend.ListCustomDetectionRuleAssociations(
		q.Get("ruleId"),
		q.Get(keyMode),
		maxResults,
		token,
	)
	if err != nil {
		return nil, http.StatusBadRequest, err
	}

	out := make([]map[string]any, 0, len(items))
	for _, a := range items {
		out = append(out, h.associationJSON(a, false))
	}

	resp := map[string]any{"ruleAssociations": out}
	if next != "" {
		resp[keyNextToken] = next
	}

	return resp, http.StatusOK, nil
}

func (h *Handler) handleUpdateCustomRuleAssociation(ruleID, associationID string, body []byte) (any, int, error) {
	var req struct {
		Mode string `json:"mode"`
	}

	if err := json.Unmarshal(body, &req, jsonx.V1()); err != nil || req.Mode == "" {
		return nil, http.StatusBadRequest, ErrValidation
	}

	return nil, http.StatusOK, h.Backend.UpdateCustomDetectionRuleAssociation(ruleID, associationID, req.Mode)
}

type customRuleOrgConfigRequest struct {
	Mode              string   `json:"mode"`
	RuleID            string   `json:"ruleId"`
	ClientToken       string   `json:"clientToken"`
	IncludeAccountIDs []string `json:"includeAccountIds"`
	ExcludeAccountIDs []string `json:"excludeAccountIds"`
}

func (h *Handler) handleCreateCustomRuleOrgConfig(body []byte) (any, int, error) {
	var req customRuleOrgConfigRequest

	if err := json.Unmarshal(body, &req, jsonx.V1()); err != nil || req.Mode == "" || req.RuleID == "" {
		return nil, http.StatusBadRequest, ErrValidation
	}

	token := req.ClientToken
	req.ClientToken = ""

	_, err := h.createOnce(
		opCreateCustomDetectionRuleOrgConfig, req.RuleID, token, req,
		func(string) error {
			_, getErr := h.Backend.GetCustomDetectionRuleOrgConfiguration(req.RuleID, req.Mode)

			return getErr
		},
		func() (string, error) {
			return req.RuleID + "|" + req.Mode, h.Backend.CreateCustomDetectionRuleOrgConfiguration(
				req.RuleID, req.Mode, req.IncludeAccountIDs, req.ExcludeAccountIDs,
			)
		},
	)
	if err != nil {
		return nil, http.StatusBadRequest, err
	}

	return nil, http.StatusOK, nil
}

func orgConfigJSON(c *CustomDetectionRuleOrgConfig, detail bool) map[string]any {
	m := map[string]any{
		keyCreatedAt: awstime.Epoch(c.CreatedAt),
		keyMode:      c.Mode,
		keyRuleID:    c.RuleID,
		keyStatus:    c.Status,
		keyUpdatedAt: awstime.Epoch(c.UpdatedAt),
	}

	if detail {
		m["includeAccountIds"] = orEmptyAny(c.IncludeAccountIDs)
		m["excludeAccountIds"] = orEmptyAny(c.ExcludeAccountIDs)
	}

	if c.ExpiresAt != nil {
		m[keyExpiresAt] = awstime.Epoch(*c.ExpiresAt)
	}

	if c.StatusReason != "" {
		m["statusReason"] = c.StatusReason
	}

	return m
}

func (h *Handler) handleGetCustomRuleOrgConfig(ruleID, mode string) (any, int, error) {
	c, err := h.Backend.GetCustomDetectionRuleOrgConfiguration(ruleID, mode)
	if err != nil {
		return nil, http.StatusNotFound, err
	}

	return map[string]any{"configuration": orgConfigJSON(c, true)}, http.StatusOK, nil
}

func (h *Handler) handleListCustomRuleOrgConfigs(q url.Values) (any, int, error) {
	maxResults, token, err := queryPage(q)
	if err != nil {
		return nil, http.StatusBadRequest, err
	}

	items, next, err := h.Backend.ListCustomDetectionRuleOrgConfigurations(q.Get(keyStatus), maxResults, token)
	if err != nil {
		return nil, http.StatusBadRequest, err
	}

	out := make([]map[string]any, 0, len(items))
	for _, c := range items {
		out = append(out, orgConfigJSON(c, false))
	}

	resp := map[string]any{"configurations": out}
	if next != "" {
		resp[keyNextToken] = next
	}

	return resp, http.StatusOK, nil
}

func (h *Handler) handleUpdateCustomRuleOrgConfig(ruleID string, body []byte) (any, int, error) {
	var req customRuleOrgConfigRequest

	if err := json.Unmarshal(body, &req, jsonx.V1()); err != nil || req.Mode == "" {
		return nil, http.StatusBadRequest, ErrValidation
	}

	return nil, http.StatusOK, h.Backend.UpdateCustomDetectionRuleOrgConfiguration(
		ruleID, req.Mode, req.IncludeAccountIDs, req.ExcludeAccountIDs,
	)
}
