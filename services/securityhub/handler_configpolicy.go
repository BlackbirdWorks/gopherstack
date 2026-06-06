package securityhub

import (
	"errors"
	"net/http"
	"strconv"
	"strings"

	"github.com/labstack/echo/v5"
)

// --- Classify functions ---

func classifyConfigPolicyPath(method, path string) (string, string) {
	switch {
	case method == http.MethodPost && path == "/configurationPolicy/create":
		return opCreateConfigurationPolicy, ""
	case method == http.MethodGet && strings.HasPrefix(path, "/configurationPolicy/get/"):
		return opGetConfigurationPolicy, strings.TrimPrefix(path, "/configurationPolicy/get/")
	case method == http.MethodPatch && strings.HasPrefix(path, "/configurationPolicy/") &&
		!strings.HasPrefix(path, "/configurationPolicy/create") &&
		!strings.HasPrefix(path, "/configurationPolicy/get") &&
		!strings.HasPrefix(path, "/configurationPolicy/list"):
		return opUpdateConfigurationPolicy, strings.TrimPrefix(path, "/configurationPolicy/")
	case method == http.MethodDelete && strings.HasPrefix(path, "/configurationPolicy/") &&
		!strings.HasPrefix(path, "/configurationPolicy/create") &&
		!strings.HasPrefix(path, "/configurationPolicy/get") &&
		!strings.HasPrefix(path, "/configurationPolicy/list"):
		return opDeleteConfigurationPolicy, strings.TrimPrefix(path, "/configurationPolicy/")
	case method == http.MethodGet && path == "/configurationPolicy/list":
		return opListConfigurationPolicies, ""
	}

	return opUnknown, ""
}

func classifyConfigPolicyAssocPath(method, path string) (string, string) {
	switch {
	case method == http.MethodPost && path == "/configurationPolicyAssociation/batchget":
		return opBatchGetConfigurationPolicyAssociations, ""
	case method == http.MethodPost && path == "/configurationPolicyAssociation/get":
		return opGetConfigurationPolicyAssociation, ""
	case method == http.MethodPost && path == "/configurationPolicyAssociation/list":
		return opListConfigurationPolicyAssociations, ""
	case method == http.MethodPost && path == "/configurationPolicyAssociation/associate":
		return opStartConfigurationPolicyAssociation, ""
	case method == http.MethodPost && path == "/configurationPolicyAssociation/disassociate":
		return opStartConfigurationPolicyDisassociation, ""
	}

	return opUnknown, ""
}

// --- Handler functions ---

func (h *Handler) handleCreateConfigurationPolicy(c *echo.Context, body map[string]any) error {
	name, _ := body["Name"].(string)
	description, _ := body["Description"].(string)

	var policy map[string]any

	if p, ok := body["ConfigurationPolicy"].(map[string]any); ok {
		policy = p
	}

	var tags map[string]string

	if t, ok := body["Tags"].(map[string]any); ok {
		tags = make(map[string]string, len(t))

		for k, v := range t {
			tags[k], _ = v.(string)
		}
	}

	cp, err := h.Backend.CreateConfigurationPolicy(name, description, policy, tags)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]any{keyMessage: err.Error()})
	}

	return c.JSON(http.StatusOK, configPolicyToResponse(cp))
}

func (h *Handler) handleGetConfigurationPolicy(c *echo.Context, identifier string) error {
	cp, err := h.Backend.GetConfigurationPolicy(identifier)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return c.JSON(http.StatusNotFound, map[string]any{keyMessage: "Configuration policy not found"})
		}

		return c.JSON(http.StatusInternalServerError, map[string]any{keyMessage: err.Error()})
	}

	return c.JSON(http.StatusOK, configPolicyToResponse(cp))
}

func (h *Handler) handleUpdateConfigurationPolicy(c *echo.Context, identifier string, body map[string]any) error {
	name, _ := body["Name"].(string)
	description, _ := body["Description"].(string)

	var policy map[string]any

	if p, ok := body["ConfigurationPolicy"].(map[string]any); ok {
		policy = p
	}

	cp, err := h.Backend.UpdateConfigurationPolicy(identifier, name, description, policy)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return c.JSON(http.StatusNotFound, map[string]any{keyMessage: "Configuration policy not found"})
		}

		return c.JSON(http.StatusInternalServerError, map[string]any{keyMessage: err.Error()})
	}

	return c.JSON(http.StatusOK, configPolicyToResponse(cp))
}

func (h *Handler) handleDeleteConfigurationPolicy(c *echo.Context, identifier string) error {
	if err := h.Backend.DeleteConfigurationPolicy(identifier); err != nil {
		if errors.Is(err, ErrNotFound) {
			return c.JSON(http.StatusNotFound, map[string]any{keyMessage: "Configuration policy not found"})
		}

		return c.JSON(http.StatusInternalServerError, map[string]any{keyMessage: err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleListConfigurationPolicies(c *echo.Context) error {
	nextToken := c.QueryParam("NextToken")
	maxResults := 0

	if v := c.QueryParam("MaxResults"); v != "" {
		maxResults, _ = strconv.Atoi(v)
	}

	policies, next := h.Backend.ListConfigurationPolicies(nextToken, maxResults)

	var out []map[string]any

	for _, p := range policies {
		out = append(out, map[string]any{
			"Arn":         p.Arn,
			"Id":          p.Id,
			"Name":        p.Name,
			"Description": p.Description,
			"UpdatedAt":   p.UpdatedAt,
		})
	}

	if out == nil {
		out = []map[string]any{}
	}

	resp := map[string]any{"ConfigurationPolicySummaryList": out}

	if next != "" {
		resp["NextToken"] = next
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleGetConfigurationPolicyAssociation(c *echo.Context, body map[string]any) error {
	targetID := ""
	targetType := ""

	if t, ok := body["Target"].(map[string]any); ok {
		targetID, _ = t["AccountId"].(string)
		if targetID == "" {
			targetID, _ = t["OrganizationalUnitId"].(string)
			if targetID == "" {
				targetID, _ = t["RootId"].(string)
			}
		}

		targetType, _ = t["TargetType"].(string)
	}

	assoc, err := h.Backend.GetConfigurationPolicyAssociation(targetID, targetType)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return c.JSON(http.StatusNotFound, map[string]any{keyMessage: "Association not found"})
		}

		return c.JSON(http.StatusInternalServerError, map[string]any{keyMessage: err.Error()})
	}

	return c.JSON(http.StatusOK, configPolicyAssocToResponse(assoc))
}

func (h *Handler) handleListConfigurationPolicyAssociations(c *echo.Context, body map[string]any) error {
	filterPolicyID := ""
	filterType := ""
	nextToken := ""
	maxResults := 0

	if f, ok := body["Filters"].(map[string]any); ok {
		filterPolicyID, _ = f["ConfigurationPolicyId"].(string)
		filterType, _ = f["AssociationType"].(string)
	}

	if v, ok := body["NextToken"].(string); ok {
		nextToken = v
	}

	if v, ok := body[keyMaxResults].(float64); ok {
		maxResults = int(v)
	}

	assocs, next := h.Backend.ListConfigurationPolicyAssociations(filterPolicyID, filterType, nextToken, maxResults)

	var out []map[string]any

	for _, a := range assocs {
		out = append(out, configPolicyAssocToResponse(a))
	}

	if out == nil {
		out = []map[string]any{}
	}

	resp := map[string]any{"ConfigurationPolicyAssociationSummaryList": out}

	if next != "" {
		resp["NextToken"] = next
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleStartConfigurationPolicyAssociation(c *echo.Context, body map[string]any) error {
	policyID := ""

	if t, ok := body["ConfigurationPolicyIdentifier"].(string); ok {
		policyID = t
	}

	targetID := ""
	targetType := ""

	if t, ok := body["Target"].(map[string]any); ok {
		targetID, _ = t["AccountId"].(string)
		if targetID == "" {
			targetID, _ = t["OrganizationalUnitId"].(string)
			if targetID == "" {
				targetID, _ = t["RootId"].(string)
			}
		}

		targetType, _ = t["TargetType"].(string)
	}

	assoc, err := h.Backend.StartConfigurationPolicyAssociation(policyID, targetID, targetType)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return c.JSON(http.StatusNotFound, map[string]any{keyMessage: "Configuration policy not found"})
		}

		return c.JSON(http.StatusInternalServerError, map[string]any{keyMessage: err.Error()})
	}

	return c.JSON(http.StatusOK, configPolicyAssocToResponse(assoc))
}

func (h *Handler) handleStartConfigurationPolicyDisassociation(c *echo.Context, body map[string]any) error {
	policyID := ""

	if t, ok := body["ConfigurationPolicyIdentifier"].(string); ok {
		policyID = t
	}

	targetID := ""
	targetType := ""

	if t, ok := body["Target"].(map[string]any); ok {
		targetID, _ = t["AccountId"].(string)
		if targetID == "" {
			targetID, _ = t["OrganizationalUnitId"].(string)
			if targetID == "" {
				targetID, _ = t["RootId"].(string)
			}
		}

		targetType, _ = t["TargetType"].(string)
	}

	if err := h.Backend.StartConfigurationPolicyDisassociation(policyID, targetID, targetType); err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]any{keyMessage: err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleBatchGetConfigurationPolicyAssociations(c *echo.Context, body map[string]any) error {
	var requests []map[string]any

	if raw, ok := body["ConfigurationPolicyAssociationIdentifiers"].([]any); ok {
		for _, item := range raw {
			if m, ok := item.(map[string]any); ok {
				requests = append(requests, m)
			}
		}
	}

	found, unprocessed := h.Backend.BatchGetConfigurationPolicyAssociations(requests)

	var out []map[string]any

	for _, a := range found {
		out = append(out, configPolicyAssocToResponse(a))
	}

	if out == nil {
		out = []map[string]any{}
	}

	resp := map[string]any{
		"ConfigurationPolicyAssociations": out,
	}

	if len(unprocessed) > 0 {
		resp["UnprocessedConfigurationPolicyAssociations"] = unprocessed
	} else {
		resp["UnprocessedConfigurationPolicyAssociations"] = []any{}
	}

	return c.JSON(http.StatusOK, resp)
}

// --- Helpers ---

func configPolicyToResponse(p *ConfigurationPolicy) map[string]any {
	return map[string]any{
		"Arn":                 p.Arn,
		"Id":                  p.Id,
		"Name":                p.Name,
		"Description":         p.Description,
		"CreatedAt":           p.CreatedAt,
		"UpdatedAt":           p.UpdatedAt,
		"ConfigurationPolicy": p.ConfigurationPolicy,
	}
}

func configPolicyAssocToResponse(a *ConfigurationPolicyAssociation) map[string]any {
	return map[string]any{
		"ConfigurationPolicyId":    a.ConfigurationPolicyId,
		"TargetId":                 a.TargetId,
		"TargetType":               a.TargetType,
		"AssociationType":          a.AssociationType,
		"UpdatedAt":                a.UpdatedAt,
		"AssociationStatus":        a.AssociationStatus,
		"AssociationStatusMessage": a.AssociationStatusMessage,
	}
}

// configPolicyPath uses strings to avoid import warning.
var _ = strings.HasPrefix
